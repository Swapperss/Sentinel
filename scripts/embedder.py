import os
import json
import re
import chromadb
from dotenv import load_dotenv
from openai import OpenAI

# 1. Setup paths and environment
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data", "landing", "sentinel_landing", "sentinel_s3_landing")
VECTOR_DB_PATH = os.path.join(PROJECT_ROOT, "data", "landing", "sentinel_landing", "vectors", "sentinel_unstructured")

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set. Add it to .env or export it before running the script.")
    return OpenAI(api_key=api_key)


client = get_openai_client()
chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)

# 2. Create/Get the Collection (Like a table in MySQL)
collection = chroma_client.get_or_create_collection(name="sentinel_unstructured")


def extract_suffix_token(filename):
    match = re.search(r"(\d+)\.[^.]+$", filename)
    if not match:
        return None
    return match.group(1)


def load_metadata_index(base_dir):
    metadata_dir = os.path.join(base_dir, "metadata")
    indexed_metadata = {}

    if not os.path.isdir(metadata_dir):
        return indexed_metadata

    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue

        suffix = extract_suffix_token(filename)
        if not suffix:
            continue

        file_path = os.path.join(metadata_dir, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as file_handle:
                payload = json.load(file_handle)
                indexed_metadata[suffix] = {
                    "filename": filename,
                    "payload": payload,
                }
        except (json.JSONDecodeError, OSError):
            # Skip unreadable metadata files and continue processing other records.
            continue

    return indexed_metadata

def get_embedding(text, model="text-embedding-3-small"):
    """Fetches vector from OpenAI"""
    text = text.replace("\n", " ")
    return client.embeddings.create(input=[text], model=model).data[0].embedding

def process_local_to_chroma(base_dir):
    # Focus on the 'notes' folder for driver commentary
    notes_dir = os.path.join(base_dir, 'notes')
    metadata_index = load_metadata_index(base_dir)
    
    for idx, filename in enumerate(sorted(os.listdir(notes_dir))):
        if filename.endswith(".txt"):
            file_path = os.path.join(notes_dir, filename)
            
            with open(file_path, 'r', encoding="utf-8") as f:
                content = f.read()
            
            print(f"Embedding and Storing: {filename}...")
            
            # Generate the vector
            vector = get_embedding(content)

            suffix = extract_suffix_token(filename)
            metadata_record = metadata_index.get(suffix, {}) if suffix else {}
            metadata_payload = metadata_record.get("payload", {}) if metadata_record else {}
            correlation_id = metadata_payload.get("correlation_id")
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
                "source_file_id": source_file_id,
                "metadata_file": metadata_filename,
            }

            # Chroma metadata values should be scalar; store a compact JSON representation for auditing.
            enriched_metadata["metadata_payload"] = json.dumps(metadata_payload, separators=(",", ":")) if metadata_payload else ""
            
            # 3. Store in ChromaDB
            # We use filename as the ID to link back to local files
            collection.upsert(
                embeddings=[vector],
                documents=[content],
                metadatas=[enriched_metadata],
                ids=[f"note_{idx}"]
            )

    print("\n✅ All unstructured notes are now vectorized and stored in ChromaDB!")

if __name__ == "__main__":
    process_local_to_chroma(DATA_ROOT)