import mysql.connector
import chromadb
import os
from dotenv import load_dotenv
from openai import OpenAI

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


def get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set. Add it to .env before running curation.")
    return OpenAI(api_key=api_key)

# 2. Initialize Clients
chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)
collection = chroma_client.get_collection(name="sentinel_unstructured")


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

    response = openai_client.chat.completions.create(
        model=DIAGNOSTIC_MODEL,
        messages=[
            {"role": "system", "content": "You produce clear, factual vehicle diagnostics."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    return response.choices[0].message.content.strip()


def trim_summary(summary):
    if not summary:
        return ""
    if len(summary) <= DIAGNOSTIC_SUMMARY_MAX_CHARS:
        return summary
    # Keep output within DB column limit and preserve readability.
    return summary[: DIAGNOSTIC_SUMMARY_MAX_CHARS - 3].rstrip() + "..."


def fetch_telemetry(cursor, correlation_id, source_id, source_file_id, source_filename):
    if correlation_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE correlation_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (correlation_id,))
        tech = cursor.fetchone()
        if tech:
            return tech

    if source_file_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE file_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (source_file_id,))
        tech = cursor.fetchone()
        if tech:
            return tech

    if source_id:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE file_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (source_id,))
        tech = cursor.fetchone()
        if tech:
            return tech

    if source_filename:
        query = f"SELECT * FROM {LANDING_SCHEMA}.telemetry_facts WHERE file_id = %s ORDER BY trip_timestamp DESC LIMIT 1"
        cursor.execute(query, (source_filename,))
        tech = cursor.fetchone()
        if tech:
            return tech

    return None


def vehicle_exists(cursor, vehicle_id):
    query = f"SELECT vin FROM {LANDING_SCHEMA}.vehicles WHERE vin = %s LIMIT 1"
    cursor.execute(query, (vehicle_id,))
    return cursor.fetchone() is not None

def diagnostic_lookup_and_curate(user_query):
    openai_client = get_openai_client()
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # --- STEP 1: Semantic Retrieval ---
    try:
        response = openai_client.embeddings.create(input=[user_query], model=EMBEDDING_MODEL)
        query_vector = response.data[0].embedding
        results = collection.query(query_embeddings=[query_vector], n_results=3)

        if not results["ids"] or not results["ids"][0]:
            return "No data found in vector store."

        curated = False
        ids = results.get("ids", [[]])[0]
        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]

        for idx, source_id in enumerate(ids):
            driver_note = documents[idx] if idx < len(documents) else ""
            metadata = metadatas[idx] if idx < len(metadatas) and metadatas[idx] else {}
            correlation_id = metadata.get("correlation_id")
            source_file_id = metadata.get("source_file_id")
            source_filename = metadata.get("source")

            # --- STEP 2: Deterministic Correlation ---
            tech = fetch_telemetry(cursor, correlation_id, source_id, source_file_id, source_filename)
            if not tech:
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
            print(f"✅ Successfully curated record for {tech['vehicle_id']} into Curation layer.")
            curated = True
            break

        if not curated:
            print("⚠️ No correlated telemetry found for top semantic matches.")
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    # In Airflow, this query could be passed as a parameter or triggered by a file name
    diagnostic_lookup_and_curate("Why was the car struggling on the hill?")