import mysql.connector
import chromadb
import os
import json
import re
from dotenv import load_dotenv
from openai import OpenAI
from utils.logger import get_logger

# 1. Setup Paths & Env
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

VECTOR_DB_PATH = os.path.join(PROJECT_ROOT, "data", "landing", "sentinel_landing", "vectors", "sentinel_unstructured")
EMBEDDING_MODEL = "text-embedding-3-small"
DIAGNOSTIC_MODEL = os.getenv("OPENAI_DIAGNOSTIC_MODEL", "gpt-4.1-mini")
DIAGNOSTIC_SUMMARY_MAX_CHARS = int(os.getenv("DIAGNOSTIC_SUMMARY_MAX_CHARS", "255"))
ENGINE_LOAD_ANOMALY_THRESHOLD = float(os.getenv("ENGINE_LOAD_ANOMALY_THRESHOLD", "80"))
PEAK_TEMP_ANOMALY_THRESHOLD = float(os.getenv("PEAK_TEMP_ANOMALY_THRESHOLD", "100"))

LANDING_SCHEMA = os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing")
CURATION_SCHEMA = os.getenv("MYSQL_CURATION_SCHEMA", "sentinel_curation")
logger = get_logger("SENTINEL_BRIDGE")


def get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set. Add it to .env before running curation.")
    return OpenAI(api_key=api_key)

def get_vector_collection():
    chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)
    return chroma_client.get_or_create_collection(name="sentinel_unstructured")


def extract_suffix_token(filename):
    match = re.search(r"(\d+)\.[^.]+$", filename)
    if not match:
        return None
    return match.group(1)


def parse_batch_file_key(filename, extension_pattern):
    match = re.search(extension_pattern, filename)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def load_metadata_index(base_dir):
    metadata_dir = os.path.join(base_dir, "metadata")
    indexed_metadata = {}

    if not os.path.isdir(metadata_dir):
        logger.warning(f"Metadata directory not found: {metadata_dir}")
        return indexed_metadata

    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue

        batch_id, item_index = parse_batch_file_key(
            filename,
            r"metadata_(\d{8}_\d{6})_(\d+)\.json$",
        )
        if not batch_id or not item_index:
            continue

        file_path = os.path.join(metadata_dir, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as file_handle:
                payload = json.load(file_handle)
                indexed_metadata[f"{batch_id}::{item_index}"] = {
                    "filename": filename,
                    "payload": payload,
                }
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(f"Failed to load metadata {filename}: {exc}")
            continue

    return indexed_metadata


def resolve_latest_batch_suffix(notes_dir):
    if not os.path.isdir(notes_dir):
        return None

    batch_suffixes = []
    for filename in os.listdir(notes_dir):
        if not filename.endswith(".txt"):
            continue
        match = re.search(r"driver_notes_(\d{8}_\d{6})_\d+\.txt$", filename)
        if match:
            batch_suffixes.append(match.group(1))

    if not batch_suffixes:
        return None

    return max(batch_suffixes)


def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_DB_USER", "root"),
        password=os.getenv("MYSQL_DB_PASSWORD", "root"),
        database=LANDING_SCHEMA,
    )


def generate_diagnostic_summary(openai_client, driver_note, tech, is_anomaly):
    anomaly_label = "YES" if is_anomaly else "NO"
    prompt = f"""
You are an automotive diagnostics assistant.

Vehicle ID: {tech.get('vehicle_id')}
Trip Timestamp: {tech.get('trip_timestamp')}
Average Engine Load: {tech.get('avg_engine_load')}
Peak Temperature C: {tech.get('peak_temp_c')}
Correlation ID: {tech.get('correlation_id')}
Driver complaint: {driver_note}
Anomaly threshold exceeded: {anomaly_label}

Return a concise summary (max 3 sentences) with:
1) probable issue,
2) risk level,
3) immediate recommended next check.
""".strip()

    request_timeout = int(os.getenv("SENTINEL_OPENAI_TIMEOUT_SECONDS", "60"))
    max_attempts = int(os.getenv("SENTINEL_OPENAI_MAX_RETRIES", "2"))
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = openai_client.chat.completions.create(
                model=DIAGNOSTIC_MODEL,
                messages=[
                    {"role": "system", "content": "You produce clear, factual vehicle diagnostics."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                timeout=request_timeout,
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            last_error = exc
            logger.warning(
                "diagnostic_summary_failed attempt=%s/%s timeout=%ss error=%s",
                attempt,
                max_attempts,
                request_timeout,
                exc,
            )

    raise RuntimeError(f"Diagnostic summary generation failed after {max_attempts} attempts: {last_error}")


def trim_summary(summary):
    if not summary:
        return ""
    if len(summary) <= DIAGNOSTIC_SUMMARY_MAX_CHARS:
        return summary
    # Keep output within DB column limit and preserve readability.
    return summary[: DIAGNOSTIC_SUMMARY_MAX_CHARS - 3].rstrip() + "..."


def fetch_telemetry(cursor, correlation_id, source_id, source_file_id, source_filename, vehicle_id):
    if correlation_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE correlation_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (correlation_id,))
        tech = cursor.fetchone()
        if tech:
            logger.info(f"Matched telemetry by correlation_id={correlation_id}")
            return tech

    if source_file_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE file_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (source_file_id,))
        tech = cursor.fetchone()
        if tech:
            logger.info(f"Matched telemetry by source_file_id={source_file_id}")
            return tech

    if source_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE file_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (source_id,))
        tech = cursor.fetchone()
        if tech:
            logger.info(f"Matched telemetry by source_id={source_id}")
            return tech

    if source_filename:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE file_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (source_filename,))
        tech = cursor.fetchone()
        if tech:
            logger.info(f"Matched telemetry by source_filename={source_filename}")
            return tech

    if vehicle_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE vehicle_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (vehicle_id,))
        tech = cursor.fetchone()
        if tech:
            logger.info(f"Matched telemetry by vehicle_id={vehicle_id}")
            return tech

    return None


def vehicle_exists(cursor, vehicle_id):
    query = f"SELECT vin FROM {LANDING_SCHEMA}.vehicles WHERE vin = %s LIMIT 1"
    cursor.execute(query, (vehicle_id,))
    return cursor.fetchone() is not None


def curate_note_batch(openai_client, collection, cursor, db_connection, base_dir, filename_suffix=None, required_count=None):
    notes_dir = os.path.join(base_dir, "notes")
    if not os.path.isdir(notes_dir):
        raise FileNotFoundError(f"Notes directory not found: {notes_dir}")

    metadata_index = load_metadata_index(base_dir)
    note_files = [f for f in os.listdir(notes_dir) if f.endswith(".txt")]

    if not filename_suffix:
        filename_suffix = resolve_latest_batch_suffix(notes_dir)
        logger.info(f"Batch curation resolved latest batch suffix={filename_suffix}")

    if filename_suffix:
        suffix_token = f"_{filename_suffix}_"
        note_files = [f for f in note_files if suffix_token in f]
        logger.info(f"Batch curation filtered by suffix={filename_suffix}; matched={len(note_files)}")

    note_files = sorted(note_files)
    curated_count = 0
    skipped_count = 0

    for idx, filename in enumerate(note_files):
        file_path = os.path.join(notes_dir, filename)
        with open(file_path, "r", encoding="utf-8") as file_handle:
            driver_note = file_handle.read().strip()

        batch_id, item_index = parse_batch_file_key(
            filename,
            r"driver_notes_(\d{8}_\d{6})_(\d+)\.txt$",
        )
        metadata_key = f"{batch_id}::{item_index}" if batch_id and item_index else None
        metadata_record = metadata_index.get(metadata_key, {}) if metadata_key else {}
        metadata = metadata_record.get("payload", {}) if metadata_record else {}

        correlation_id = metadata.get("correlation_id")
        source_file_id = metadata.get("file_id")
        source_filename = filename
        vehicle_id = metadata.get("vehicle_id")

        source_id = source_file_id or source_filename
        tech = fetch_telemetry(cursor, correlation_id, source_id, source_file_id, source_filename, vehicle_id)
        if not tech:
            skipped_count += 1
            raise RuntimeError(
                f"No correlated telemetry found for note={filename} correlation_id={correlation_id} vehicle_id={vehicle_id}"
            )

        if not vehicle_exists(cursor, tech["vehicle_id"]):
            raise RuntimeError(
                f"Vehicle gate failed for vehicle_id={tech['vehicle_id']}; row missing in {LANDING_SCHEMA}.vehicles"
            )

        is_anomaly = (
            float(tech["avg_engine_load"]) > ENGINE_LOAD_ANOMALY_THRESHOLD
            or float(tech["peak_temp_c"]) > PEAK_TEMP_ANOMALY_THRESHOLD
        )
        try:
            summary = generate_diagnostic_summary(openai_client, driver_note, tech, is_anomaly)
        except Exception:
            summary = (
                f"Driver complaint correlated with telemetry for vehicle {tech['vehicle_id']} "
                f"at {tech['trip_timestamp']}. "
                f"Anomaly={is_anomaly}. Check engine load and thermal system first."
            )
        summary = trim_summary(summary)

        insert_query = f"""
            INSERT INTO {CURATION_SCHEMA}.sentinel_ai_curation 
            (vehicle_id, event_timestamp, driver_complaint, technical_anomaly_detected, diagnostic_summary, correlation_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            driver_complaint = VALUES(driver_complaint),
            technical_anomaly_detected = VALUES(technical_anomaly_detected),
            diagnostic_summary = VALUES(diagnostic_summary),
            correlation_id = VALUES(correlation_id),
            processed_at = CURRENT_TIMESTAMP
        """
        values = (
            tech["vehicle_id"],
            tech["trip_timestamp"],
            driver_note,
            int(is_anomaly),
            summary,
            tech.get("correlation_id"),
        )

        cursor.execute(insert_query, values)
        db_connection.commit()
        curated_count += 1
        logger.info(
            f"Curated batch row {idx + 1}/{len(note_files)} vehicle_id={tech['vehicle_id']} correlation_id={tech.get('correlation_id')}"
        )

    if required_count is not None and curated_count < int(required_count):
        raise RuntimeError(
            f"Batch curation incomplete curated_count={curated_count} required_count={required_count} skipped_count={skipped_count}"
        )

    logger.info(
        "Batch curation complete curated_count=%s skipped_count=%s required_count=%s",
        curated_count,
        skipped_count,
        required_count,
    )
    return curated_count

def diagnostic_lookup_and_curate(user_query=None, base_dir=None, filename_suffix=None, required_count=None):
    openai_client = get_openai_client()
    collection = get_vector_collection()
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    if base_dir:
        try:
            return curate_note_batch(
                openai_client=openai_client,
                collection=collection,
                cursor=cursor,
                db_connection=db,
                base_dir=base_dir,
                filename_suffix=filename_suffix,
                required_count=required_count,
            )
        finally:
            cursor.close()
            db.close()

    # --- STEP 1: Semantic Retrieval ---
    try:
        request_timeout = int(os.getenv("SENTINEL_OPENAI_TIMEOUT_SECONDS", "60"))
        max_attempts = int(os.getenv("SENTINEL_OPENAI_MAX_RETRIES", "2"))
        last_error = None

        response = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = openai_client.embeddings.create(
                    input=[user_query],
                    model=EMBEDDING_MODEL,
                    timeout=request_timeout,
                )
                break
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "curation_embedding_failed attempt=%s/%s timeout=%ss error=%s",
                    attempt,
                    max_attempts,
                    request_timeout,
                    exc,
                )

        if response is None:
            raise RuntimeError(f"Curation embedding failed after {max_attempts} attempts: {last_error}")

        query_vector = response.data[0].embedding
        results = collection.query(query_embeddings=[query_vector], n_results=3)
        logger.info(f"Fetched semantic matches for query='{user_query}'")

        if not results["ids"] or not results["ids"][0]:
            return "No data found in vector store."

        curated_count = 0
        ids = results.get("ids", [[]])[0]
        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]

        for idx, source_id in enumerate(ids):
            driver_note = documents[idx] if idx < len(documents) else ""
            metadata = metadatas[idx] if idx < len(metadatas) and metadatas[idx] else {}
            correlation_id = metadata.get("correlation_id")
            source_file_id = metadata.get("source_file_id")
            source_filename = metadata.get("source")
            vehicle_id = metadata.get("vehicle_id")

            # --- STEP 2: Deterministic Correlation ---
            tech = fetch_telemetry(cursor, correlation_id, source_id, source_file_id, source_filename, vehicle_id)
            if not tech:
                logger.info(
                    f"No correlated telemetry found for source_id={source_id} correlation_id={correlation_id} vehicle_id={vehicle_id}"
                )
                continue

            if not vehicle_exists(cursor, tech["vehicle_id"]):
                print(
                    f"⚠️ Vehicle gate failed for vehicle_id={tech['vehicle_id']}. "
                    f"Record not found in {LANDING_SCHEMA}.vehicles"
                )
                continue

            # --- STEP 3: Push into curation layer ---
            is_anomaly = (
                float(tech["avg_engine_load"]) > ENGINE_LOAD_ANOMALY_THRESHOLD
                or float(tech["peak_temp_c"]) > PEAK_TEMP_ANOMALY_THRESHOLD
            )
            try:
                summary = generate_diagnostic_summary(openai_client, driver_note, tech, is_anomaly)
            except Exception:
                summary = (
                    f"Driver complaint correlated with telemetry for vehicle {tech['vehicle_id']} "
                    f"at {tech['trip_timestamp']}. "
                    f"Anomaly={is_anomaly}. Check engine load and thermal system first."
                )
            summary = trim_summary(summary)

            insert_query = f"""
                INSERT INTO {CURATION_SCHEMA}.sentinel_ai_curation 
                (vehicle_id, event_timestamp, driver_complaint, technical_anomaly_detected, diagnostic_summary, correlation_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                driver_complaint = VALUES(driver_complaint),
                technical_anomaly_detected = VALUES(technical_anomaly_detected),
                diagnostic_summary = VALUES(diagnostic_summary),
                correlation_id = VALUES(correlation_id),
                processed_at = CURRENT_TIMESTAMP
            """
            values = (
                tech["vehicle_id"],
                tech["trip_timestamp"],
                driver_note,
                int(is_anomaly),
                summary,
                tech.get("correlation_id"),
            )

            cursor.execute(insert_query, values)
            db.commit()
            logger.info(f"Successfully curated record for vehicle_id={tech['vehicle_id']} correlation_id={tech.get('correlation_id')}")
            print(f"Successfully curated record for {tech['vehicle_id']} into Curation layer.")
            curated_count += 1

        if curated_count <= 0:
            logger.warning("No correlated telemetry found for top semantic matches.")
            print("No correlated telemetry found for top semantic matches.")
        else:
            logger.info("Curated %s records into sentinel_curation.sentinel_ai_curation.", curated_count)
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    # In Airflow, this query could be passed as a parameter or triggered by a file name
    diagnostic_lookup_and_curate("Why was the car struggling on the hill?")