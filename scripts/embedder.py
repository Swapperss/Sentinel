import os
import json
import re
import argparse
import chromadb
from dotenv import load_dotenv
from openai import OpenAI
from utils.logger import get_logger

# 1. Setup paths and environment
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data", "landing", "sentinel_landing", "sentinel_s3_landing")
VECTOR_DB_PATH = os.path.join(PROJECT_ROOT, "data", "landing", "sentinel_landing", "vectors", "sentinel_unstructured")

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
logger = get_logger("EMBEDDER")


def get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY is not set. Add it to .env or export it before running the script.")
        raise ValueError("OPENAI_API_KEY is not set. Add it to .env or export it before running the script.")
    logger.info(f"OpenAI client initialized with API key ending in ...{api_key[-4:]}")
    return OpenAI(api_key=api_key)

def get_chroma_collection():
    chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)
    return chroma_client.get_or_create_collection(name="sentinel_unstructured")


def extract_suffix_token(filename):
    match = re.search(r"(\d+)\.[^.]+$", filename)
    if not match:
        return None
    return match.group(1)


def load_metadata_index(base_dir):
    metadata_dir = os.path.join(base_dir, "metadata")
    logger.info(f"Loading metadata index from {metadata_dir}")
    indexed_metadata = {}

    if not os.path.isdir(metadata_dir):
        logger.warning(f"Metadata directory not found: {metadata_dir}")
        return indexed_metadata

    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue

        suffix = extract_suffix_token(filename)
        if not suffix:
            logger.debug(f"Skipping metadata file {filename} - no suffix extracted")
            continue

        file_path = os.path.join(metadata_dir, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as file_handle:
                payload = json.load(file_handle)
                indexed_metadata[suffix] = {
                    "filename": filename,
                    "payload": payload,
                }
                logger.debug(f"Loaded metadata: {filename} (suffix={suffix})")
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(f"Failed to load metadata {filename}: {exc}")
            continue

    logger.info(f"Metadata index loaded: {len(indexed_metadata)} entries")
    return indexed_metadata

def get_embedding(text, client, model="text-embedding-3-small"):
    """Fetches vector from OpenAI"""
    text = text.replace("\n", " ")
    request_timeout = int(os.getenv("SENTINEL_OPENAI_TIMEOUT_SECONDS", "60"))
    max_attempts = int(os.getenv("SENTINEL_OPENAI_MAX_RETRIES", "2"))

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.embeddings.create(
                input=[text],
                model=model,
                timeout=request_timeout,
            )
            return response.data[0].embedding
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Embedding request failed attempt=%s/%s timeout=%ss error=%s",
                attempt,
                max_attempts,
                request_timeout,
                exc,
            )

    raise RuntimeError(f"Failed to fetch embedding after {max_attempts} attempts: {last_error}")

def process_local_to_chroma(base_dir, max_files=None, filename_suffix=None):
    client = get_openai_client()
    collection = get_chroma_collection()

    # Focus on the 'notes' folder for driver commentary
    notes_dir = os.path.join(base_dir, 'notes')
    logger.info(f"step=process_local_to_chroma base_dir={base_dir} notes_dir={notes_dir}")
    
    if not os.path.isdir(notes_dir):
        logger.error(f"Notes directory not found: {notes_dir}")
        raise FileNotFoundError(f"Notes directory not found: {notes_dir}")
    
    notes_files = [f for f in os.listdir(notes_dir) if f.endswith(".txt")]

    if filename_suffix:
        suffix_token = f"_{filename_suffix}_"
        notes_files = [f for f in notes_files if suffix_token in f]
        logger.info(f"Filtering note files by suffix={filename_suffix}; matched={len(notes_files)}")

    # Bound each run for predictable DAG duration and cost.
    if max_files is not None:
        # Process newest files first.
        notes_files = sorted(
            notes_files,
            key=lambda name: os.path.getmtime(os.path.join(notes_dir, name)),
            reverse=True,
        )[:max_files]
    else:
        notes_files = sorted(notes_files)

    logger.info(f"Found {len(notes_files)} note files to process")
    
    metadata_index = load_metadata_index(base_dir)
    logger.info(f"Loaded metadata index with {len(metadata_index)} entries")
    
    for idx, filename in enumerate(notes_files):
        if filename.endswith(".txt"):
            file_path = os.path.join(notes_dir, filename)
            logger.info(f"Processing file [{idx+1}/{len(notes_files)}]: {filename}")
            
            try:
                with open(file_path, 'r', encoding="utf-8") as f:
                    content = f.read()
                
                logger.info(f"Read {len(content)} bytes from {filename}")
                
                # Generate the vector
                logger.info(f"Requesting embedding from OpenAI for {filename}...")
                vector = get_embedding(content, client=client)
                logger.info(f"Embedding received: {len(vector)} dimensions")

                suffix = extract_suffix_token(filename)
                metadata_record = metadata_index.get(suffix, {}) if suffix else {}
                metadata_payload = metadata_record.get("payload", {}) if metadata_record else {}
                correlation_id = metadata_payload.get("correlation_id")
                vehicle_id = metadata_payload.get("vehicle_id")
                if not correlation_id and suffix:
                    correlation_id = f"CORR-{suffix}"
                metadata_filename = metadata_record.get("filename")
                source_file_id = metadata_payload.get("file_id")
                if not source_file_id and suffix:
                    source_file_id = f"FILE-{suffix}"

                enriched_metadata = {
                    "source": filename,
                    "type": "driver_note",
                    "correlation_id": correlation_id,
                    "vehicle_id": vehicle_id,
                    "source_file_id": source_file_id,
                    "metadata_file": metadata_filename,
                }

                # Chroma metadata values should be scalar; store a compact JSON representation for auditing.
                enriched_metadata["metadata_payload"] = json.dumps(metadata_payload, separators=(",", ":")) if metadata_payload else ""
                
                # 3. Store in ChromaDB
                # Use a deterministic ID so reruns upsert the same logical note.
                note_id = f"note::{filename}"
                logger.info(f"Upserting to ChromaDB: {note_id} with correlation_id={correlation_id}")
                collection.upsert(
                    embeddings=[vector],
                    documents=[content],
                    metadatas=[enriched_metadata],
                    ids=[note_id]
                )
                logger.info(f"Successfully upserted {filename} to ChromaDB")
            except Exception as exc:
                logger.error(f"Failed to process {filename}: {exc}", exc_info=True)
                raise

    logger.info(f"step=process_local_to_chroma status=complete total_files={len(notes_files)}")
    print("\n✅ All unstructured notes are now vectorized and stored in ChromaDB!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embed Sentinel unstructured notes into ChromaDB")
    parser.add_argument("--base-dir", default=DATA_ROOT, help="Base directory containing notes/metadata folders")
    parser.add_argument("--max-files", type=int, default=None, help="Maximum number of notes to process")
    parser.add_argument("--filename-suffix", default=None, help="Run-specific filename suffix to filter files")
    args = parser.parse_args()

    process_local_to_chroma(
        base_dir=args.base_dir,
        max_files=args.max_files,
        filename_suffix=args.filename_suffix,
    )