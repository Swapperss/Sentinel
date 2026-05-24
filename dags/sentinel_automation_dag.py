from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import json
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

PROJECT_PATH = os.getenv("AIRFLOW_PROJECT_ROOT", "/opt/airflow/project")
sys.path.append(PROJECT_PATH)
sys.path.append(f"{PROJECT_PATH}/scripts")

from embedder import process_local_to_chroma
from sentinel_bridge import diagnostic_lookup_and_curate
from sentinal_unstructured_data_generator import generate_sentinel_files
from src.ingestion.vehicles_source_simulator import run_simulator as run_vehicle_source_simulator
from src.ingestion.vehicles_cdc_consumer import run_loader as run_vehicles_cdc_loader
from src.ingestion.kafka_producer import run_producer as run_telemetry_source_producer
from src.ingestion.kafka_to_mysql_consumer import run_loader as run_telemetry_loader
from src.curation.populate_sentinel_star_schema import run_population as run_star_schema_population
from utils.db import get_admin_connection
from utils.logger import get_logger

logger = get_logger("SENTINEL_DATA_DAG")

default_args = {
    "owner": "sentinel_dev",
    "start_date": datetime(2026, 5, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_for_complete_batch():
    base = f"{PROJECT_PATH}/data/landing/sentinel_landing/sentinel_s3_landing"
    logs_dir = f"{base}/logs"
    notes_dir = f"{base}/notes"
    meta_dir = f"{base}/metadata"

    has_logs = os.path.isdir(logs_dir) and any(f.endswith(".log") for f in os.listdir(logs_dir))
    has_notes = os.path.isdir(notes_dir) and any(f.endswith(".txt") for f in os.listdir(notes_dir))
    has_meta = os.path.isdir(meta_dir) and any(f.endswith(".json") for f in os.listdir(meta_dir))

    logger.info(
        "check=landing_batch_files has_logs=%s has_notes=%s has_meta=%s",
        has_logs,
        has_notes,
        has_meta,
    )
    return has_logs and has_notes and has_meta


def check_landing_vehicles_snapshot(**context):
    connection = get_admin_connection(database=os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing"))
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM vehicles")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    expected_min = runtime_int_value(context, "source_vehicle_distinct_rows", "SENTINEL_SOURCE_DISTINCT_ROWS", 0)
    logger.info("check=landing_vehicles_snapshot rows=%s", count_rows)
    if count_rows <= 0:
        raise ValueError("sentinel_landing.vehicles has no rows")
    if expected_min > 0 and count_rows < expected_min:
        raise ValueError(
            f"sentinel_landing.vehicles row_count={count_rows} is below expected_min={expected_min}"
        )


def check_landing_vehicles_cdc_events(**context):
    connection = get_admin_connection(database=os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing"))
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM vehicles_cdc_events")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    expected_min = runtime_int_value(context, "source_vehicle_distinct_rows", "SENTINEL_SOURCE_DISTINCT_ROWS", 0)
    logger.info("check=landing_vehicles_cdc_events rows=%s", count_rows)
    if count_rows <= 0:
        raise ValueError("sentinel_landing.vehicles_cdc_events has no rows")
    if expected_min > 0 and count_rows < expected_min:
        raise ValueError(
            f"sentinel_landing.vehicles_cdc_events row_count={count_rows} is below expected_min={expected_min}"
        )


def runtime_int_value(context, conf_key, env_key, default_value):
    dag_run = context.get("dag_run") if context else None
    conf_value = None
    if dag_run is not None and getattr(dag_run, "conf", None):
        conf_value = dag_run.conf.get(conf_key)

    raw_value = conf_value if conf_value is not None else os.getenv(env_key, str(default_value))
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            "runtime_config_invalid conf_key=%s env_key=%s value=%s fallback=%s",
            conf_key,
            env_key,
            raw_value,
            default_value,
        )
        return int(default_value)


def runtime_string_value(context, conf_key, env_key, default_value):
    dag_run = context.get("dag_run") if context else None
    conf_value = None
    if dag_run is not None and getattr(dag_run, "conf", None):
        conf_value = dag_run.conf.get(conf_key)

    raw_value = conf_value if conf_value is not None else os.getenv(env_key, default_value)
    if raw_value is None:
        return default_value
    normalized = str(raw_value).strip()
    return normalized if normalized else default_value


def ingest_landing_vehicles_cdc_events(**context):
    expected_distinct_rows = runtime_int_value(
        context,
        "source_vehicle_distinct_rows",
        "SENTINEL_SOURCE_DISTINCT_ROWS",
        0,
    )
    consumer_group = runtime_string_value(
        context,
        "vehicles_cdc_group",
        "KAFKA_VEHICLES_CDC_GROUP",
        "sentinel-vehicles-cdc-loader",
    )
    offset_reset = runtime_string_value(
        context,
        "vehicles_cdc_offset_reset",
        "KAFKA_VEHICLES_OFFSET_RESET",
        "latest",
    )
    if expected_distinct_rows > 0:
        offset_reset = "earliest"
    max_events = runtime_int_value(context, "vehicles_cdc_max_events", "SENTINEL_VEHICLES_CDC_MAX_EVENTS", 200)
    idle_timeout_seconds = runtime_int_value(context, "vehicles_cdc_idle_timeout_seconds", "SENTINEL_VEHICLES_CDC_IDLE_TIMEOUT_SECONDS", 20)
    logger.info(
        "step=ingest_landing_vehicles_cdc_events group_id=%s offset_reset=%s",
        consumer_group,
        offset_reset,
    )
    processed = run_vehicles_cdc_loader(
        max_events=max_events,
        idle_timeout_seconds=idle_timeout_seconds,
        group_id=consumer_group,
        offset_reset=offset_reset,
    )
    logger.info("step=ingest_landing_vehicles_cdc_events processed=%s", processed)
    if processed <= 0:
        logger.warning(
            "step=ingest_landing_vehicles_cdc_events status=idle_checkpoint processed=%s group_id=%s offset_reset=%s",
            processed,
            consumer_group,
            offset_reset,
        )
        return
    logger.info("step=ingest_landing_vehicles_cdc_events status=complete")


def generate_source_vehicle_updates_for_cdc(**context):
    total_events = runtime_int_value(context, "source_vehicle_events", "SENTINEL_SOURCE_VEHICLE_EVENTS", 6)
    interval_seconds = runtime_int_value(context, "source_vehicle_interval_seconds", "SENTINEL_SOURCE_VEHICLE_INTERVAL_SECONDS", 1)
    distinct_rows = runtime_int_value(context, "source_vehicle_distinct_rows", "SENTINEL_SOURCE_DISTINCT_ROWS", 0)
    logger.info(
        "step=generate_source_vehicle_updates_for_cdc status=start total_events=%s interval_seconds=%s distinct_rows=%s",
        total_events,
        interval_seconds,
        distinct_rows,
    )
    run_vehicle_source_simulator(
        total_events=total_events,
        interval_seconds=interval_seconds,
        distinct_rows=distinct_rows,
    )
    logger.info("step=generate_source_vehicle_updates_for_cdc status=complete")


def check_landing_telemetry_facts(**context):
    connection = get_admin_connection(database=os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing"))
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM telemetry_facts WHERE DATE(trip_timestamp) = CURDATE()")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    expected_min = runtime_int_value(context, "source_telemetry_events", "SENTINEL_SOURCE_TELEMETRY_EVENTS", 0)
    logger.info("check=landing_telemetry_facts_today rows=%s", count_rows)
    if count_rows <= 0:
        raise ValueError("sentinel_landing.telemetry_facts has no rows for today")
    if expected_min > 0 and count_rows < expected_min:
        raise ValueError(
            f"sentinel_landing.telemetry_facts today_row_count={count_rows} is below expected_min={expected_min}"
        )


def check_landing_telemetry_ingest_events(**context):
    connection = get_admin_connection(database=os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing"))
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM telemetry_ingest_events WHERE DATE(ingested_at) = CURDATE()")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    expected_min = runtime_int_value(context, "source_telemetry_events", "SENTINEL_SOURCE_TELEMETRY_EVENTS", 0)
    logger.info("check=landing_telemetry_ingest_events_today rows=%s", count_rows)
    if count_rows <= 0:
        raise ValueError("sentinel_landing.telemetry_ingest_events has no rows for today")
    if expected_min > 0 and count_rows < expected_min:
        raise ValueError(
            f"sentinel_landing.telemetry_ingest_events today_row_count={count_rows} is below expected_min={expected_min}"
        )


def generate_source_telemetry_events(**context):
    total_events = runtime_int_value(context, "source_telemetry_events", "SENTINEL_SOURCE_TELEMETRY_EVENTS", 10)
    interval_seconds = runtime_int_value(context, "source_telemetry_interval_seconds", "SENTINEL_SOURCE_TELEMETRY_INTERVAL_SECONDS", 1)
    distinct_rows = runtime_int_value(
        context,
        "source_telemetry_distinct_rows",
        "SENTINEL_TELEMETRY_DISTINCT_ROWS",
        runtime_int_value(context, "source_vehicle_distinct_rows", "SENTINEL_SOURCE_DISTINCT_ROWS", 0),
    )
    logger.info(
        "step=generate_source_telemetry_events status=start total_events=%s interval_seconds=%s distinct_rows=%s",
        total_events,
        interval_seconds,
        distinct_rows,
    )
    run_telemetry_source_producer(
        total_events=total_events,
        interval_seconds=interval_seconds,
        distinct_rows=distinct_rows,
    )
    logger.info("step=generate_source_telemetry_events status=complete")


def ingest_landing_telemetry_events(**context):
    expected_source_events = runtime_int_value(
        context,
        "source_telemetry_events",
        "SENTINEL_SOURCE_TELEMETRY_EVENTS",
        0,
    )
    consumer_group = runtime_string_value(
        context,
        "telemetry_group",
        "KAFKA_TELEMETRY_CONSUMER_GROUP",
        "sentinel-telemetry-mysql-loader",
    )
    offset_reset = runtime_string_value(
        context,
        "telemetry_offset_reset",
        "KAFKA_AUTO_OFFSET_RESET",
        "latest",
    )
    if expected_source_events > 0:
        offset_reset = "earliest"
    max_events = runtime_int_value(context, "telemetry_max_events", "SENTINEL_TELEMETRY_MAX_EVENTS", 200)
    idle_timeout_seconds = runtime_int_value(context, "telemetry_idle_timeout_seconds", "SENTINEL_TELEMETRY_IDLE_TIMEOUT_SECONDS", 20)
    logger.info(
        "step=ingest_landing_telemetry_events status=start group_id=%s offset_reset=%s",
        consumer_group,
        offset_reset,
    )
    processed = run_telemetry_loader(
        max_events=max_events,
        idle_timeout_seconds=idle_timeout_seconds,
        group_id=consumer_group,
        offset_reset=offset_reset,
    )
    logger.info("step=ingest_landing_telemetry_events processed=%s", processed)
    if processed <= 0:
        logger.warning(
            "step=ingest_landing_telemetry_events status=idle_checkpoint processed=%s group_id=%s offset_reset=%s",
            processed,
            consumer_group,
            offset_reset,
        )
        return
    logger.info("step=ingest_landing_telemetry_events status=complete")


def populate_analytics_star_schema():
    logger.info("step=populate_analytics_star_schema status=start")
    run_star_schema_population()
    logger.info("step=populate_analytics_star_schema status=complete")


def check_analytics_dim_vehicle_today(**context):
    connection = get_admin_connection(database="sentinel_analytics")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_vehicle WHERE DATE(updated_at) = CURDATE()")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    expected_min = runtime_int_value(context, "source_vehicle_distinct_rows", "SENTINEL_SOURCE_DISTINCT_ROWS", 0)
    logger.info("check=analytics_dim_vehicle_today rows=%s expected_min=%s", count_rows, expected_min)
    if count_rows <= 0:
        raise ValueError("sentinel_analytics.dim_vehicle has no rows updated today")
    if expected_min > 0 and count_rows < expected_min:
        raise ValueError(
            f"sentinel_analytics.dim_vehicle today_row_count={count_rows} is below expected_min={expected_min}"
        )


def generate_unstructured_batch_files(**context):
    source_distinct_rows = runtime_int_value(context, "source_vehicle_distinct_rows", "SENTINEL_SOURCE_DISTINCT_ROWS", 0)
    distinct_rows = runtime_int_value(context, "unstructured_distinct_rows", "SENTINEL_UNSTRUCTURED_DISTINCT_ROWS", source_distinct_rows)
    batch_size = runtime_int_value(context, "unstructured_batch_size", "SENTINEL_UNSTRUCTURED_BATCH_SIZE", 2)
    logger.info(
        "step=generate_unstructured_batch_files status=start batch_size=%s distinct_rows=%s source_distinct_rows=%s",
        batch_size,
        distinct_rows,
        source_distinct_rows,
    )
    result = generate_sentinel_files(batch_size=batch_size, distinct_rows=distinct_rows)
    logger.info(
        "step=generate_unstructured_batch_files status=complete suffix=%s generated_files=%s",
        result.get("timestamp_suffix"),
        result.get("generated_files"),
    )
    return result


def check_generated_unstructured_batch(**context):
    ti = context.get("ti")
    if ti is None:
        logger.info("check=generated_unstructured_batch status=waiting reason=no_task_instance")
        return False

    payload = ti.xcom_pull(task_ids="generate_unstructured_batch_files")
    if not payload:
        logger.info("check=generated_unstructured_batch status=waiting reason=no_xcom_payload")
        return False

    suffix = payload.get("timestamp_suffix")
    batch_size = int(payload.get("batch_size", 0))
    if not suffix or batch_size <= 0:
        logger.info("check=generated_unstructured_batch status=waiting reason=invalid_payload")
        return False

    base = f"{PROJECT_PATH}/data/landing/sentinel_landing/sentinel_s3_landing"
    logs_dir = f"{base}/logs"
    notes_dir = f"{base}/notes"
    meta_dir = f"{base}/metadata"

    log_count = len([f for f in os.listdir(logs_dir) if f.startswith(f"sentinel_diag_{suffix}_") and f.endswith(".log")]) if os.path.isdir(logs_dir) else 0
    note_count = len([f for f in os.listdir(notes_dir) if f.startswith(f"driver_notes_{suffix}_") and f.endswith(".txt")]) if os.path.isdir(notes_dir) else 0
    meta_count = len([f for f in os.listdir(meta_dir) if f.startswith(f"metadata_{suffix}_") and f.endswith(".json")]) if os.path.isdir(meta_dir) else 0

    expected = batch_size
    ready = log_count >= expected and note_count >= expected and meta_count >= expected

    logger.info(
        "check=generated_unstructured_batch suffix=%s expected=%s logs=%s notes=%s metadata=%s ready=%s",
        suffix,
        expected,
        log_count,
        note_count,
        meta_count,
        ready,
    )
    return ready


def process_unstructured_to_vector_db(**context):
    ti = context.get("ti")
    base_dir = f"{PROJECT_PATH}/data/landing/sentinel_landing/sentinel_s3_landing"
    payload = ti.xcom_pull(task_ids="generate_unstructured_batch_files") if ti is not None else {}
    payload = payload or {}
    suffix = payload.get("timestamp_suffix")
    batch_size = int(payload.get("batch_size", os.getenv("SENTINEL_UNSTRUCTURED_BATCH_SIZE", "5")))
    logger.info(
        "step=vector_ingestion base_dir=%s suffix=%s max_notes=%s",
        base_dir,
        suffix,
        batch_size,
    )

    timeout_seconds = int(os.getenv("SENTINEL_VECTOR_INGEST_TIMEOUT_SECONDS", "420"))
    embedder_script = f"{PROJECT_PATH}/scripts/embedder.py"
    command = [
        sys.executable,
        embedder_script,
        "--base-dir",
        base_dir,
        "--max-files",
        str(batch_size),
    ]

    if suffix:
        command.extend(["--filename-suffix", suffix])

    logger.info("step=vector_ingestion_subprocess timeout_seconds=%s command=%s", timeout_seconds, command)
    try:
        completed = subprocess.run(
            command,
            check=True,
            timeout=timeout_seconds,
            capture_output=True,
            text=True,
        )
        if completed.stdout:
            logger.info("vector_ingestion_stdout=%s", completed.stdout.strip())
        if completed.stderr:
            logger.warning("vector_ingestion_stderr=%s", completed.stderr.strip())
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(
            f"Vector ingestion exceeded timeout of {timeout_seconds}s and was force-terminated"
        ) from exc
    except subprocess.CalledProcessError as exc:
        error_stdout = (exc.stdout or "").strip()
        error_stderr = (exc.stderr or "").strip()
        raise RuntimeError(
            f"Vector ingestion subprocess failed exit_code={exc.returncode} stdout={error_stdout} stderr={error_stderr}"
        ) from exc


def check_vector_db_population():
    import sqlite3

    sqlite_path = (
        Path(PROJECT_PATH)
        / "data"
        / "landing"
        / "sentinel_landing"
        / "vectors"
        / "sentinel_unstructured"
        / "chroma.sqlite3"
    )

    if not sqlite_path.exists():
        raise FileNotFoundError(f"Vector DB sqlite file not found: {sqlite_path}")

    connection = sqlite3.connect(str(sqlite_path), timeout=10)
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM embeddings")
    count_vectors = int(cursor.fetchone()[0])
    cursor.close()
    connection.close()

    logger.info("check=vector_db_population vectors=%s sqlite_path=%s", count_vectors, sqlite_path)
    if count_vectors <= 0:
        raise ValueError("Vector DB collection sentinel_unstructured has no vectors")


def run_curation_layer(**context):
    ti = context.get("ti")
    payload = ti.xcom_pull(task_ids="generate_unstructured_batch_files") if ti is not None else {}
    payload = payload or {}
    suffix = payload.get("timestamp_suffix")
    batch_size = int(payload.get("batch_size", os.getenv("SENTINEL_UNSTRUCTURED_BATCH_SIZE", "2")))
    base_dir = f"{PROJECT_PATH}/data/landing/sentinel_landing/sentinel_s3_landing"
    logger.info(
        "step=curation_layer_correlation status=start suffix=%s batch_size=%s base_dir=%s",
        suffix,
        batch_size,
        base_dir,
    )
    diagnostic_lookup_and_curate(
        base_dir=base_dir,
        filename_suffix=suffix,
        required_count=batch_size,
    )
    logger.info("step=curation_layer_correlation status=complete")


def run_curation_relational_sync():
    logger.info("step=curation_relational_sync status=start")
    connection = get_admin_connection(database="sentinel_curation")
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO sentinel_curation.vehicles
                (vin, owner_name, manufacturer, vehicle_model, manufacture_year, status, last_seen_at, source_updated_at, created_at, updated_at)
            SELECT
                v.vin,
                v.owner_name,
                v.manufacturer,
                v.vehicle_model,
                v.manufacture_year,
                v.status,
                v.updated_at,
                v.updated_at,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM sentinel_landing.vehicles v
            ON DUPLICATE KEY UPDATE
                owner_name = VALUES(owner_name),
                manufacturer = VALUES(manufacturer),
                vehicle_model = VALUES(vehicle_model),
                manufacture_year = VALUES(manufacture_year),
                status = VALUES(status),
                last_seen_at = VALUES(last_seen_at),
                source_updated_at = VALUES(source_updated_at),
                updated_at = CURRENT_TIMESTAMP
            """
        )
        vehicles_affected = cursor.rowcount

        cursor.execute(
            """
            INSERT INTO sentinel_curation.telemetry_facts
                (file_id, file_name, vehicle_id, trip_timestamp, avg_engine_load, peak_temp_c, trip_distance_km, trip_duration_seconds, correlation_id, created_at, updated_at)
            SELECT
                tf.file_id,
                tf.file_name,
                tf.vehicle_id,
                tf.trip_timestamp,
                tf.avg_engine_load,
                tf.peak_temp_c,
                NULL AS trip_distance_km,
                NULL AS trip_duration_seconds,
                tf.correlation_id,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM sentinel_landing.telemetry_facts tf
            WHERE DATE(tf.trip_timestamp) = CURDATE()
            ON DUPLICATE KEY UPDATE
                file_name = VALUES(file_name),
                vehicle_id = VALUES(vehicle_id),
                trip_timestamp = VALUES(trip_timestamp),
                avg_engine_load = VALUES(avg_engine_load),
                peak_temp_c = VALUES(peak_temp_c),
                trip_distance_km = VALUES(trip_distance_km),
                trip_duration_seconds = VALUES(trip_duration_seconds),
                correlation_id = VALUES(correlation_id),
                updated_at = CURRENT_TIMESTAMP
            """
        )
        telemetry_affected = cursor.rowcount

        connection.commit()
        logger.info(
            "step=curation_relational_sync status=complete vehicles_affected=%s telemetry_affected=%s",
            vehicles_affected,
            telemetry_affected,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def check_curation_telemetry_facts(**context):
    connection = get_admin_connection(database="sentinel_curation")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM telemetry_facts WHERE DATE(trip_timestamp) = CURDATE()")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()

    expected_min = runtime_int_value(context, "source_telemetry_events", "SENTINEL_SOURCE_TELEMETRY_EVENTS", 0)
    logger.info("check=curation_telemetry_facts_today rows=%s expected_min=%s", count_rows, expected_min)
    if count_rows <= 0:
        raise ValueError("sentinel_curation.telemetry_facts has no rows for today")
    if expected_min > 0 and count_rows < expected_min:
        raise ValueError(
            f"sentinel_curation.telemetry_facts today_row_count={count_rows} is below expected_min={expected_min}"
        )


def check_curation_output():
    connection = get_admin_connection(database="sentinel_curation")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM sentinel_ai_curation")
    count_rows = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    logger.info("check=curation_output rows=%s", count_rows)
    if count_rows <= 0:
        raise ValueError("sentinel_curation.sentinel_ai_curation has no rows")


with DAG(
    "sentinel_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description="Detailed Sentinel data DAG: landing checks -> vector ingestion -> curation",
    tags=["sentinel", "ingestion", "curation"],
) as dag:
    wait_for_batch = PythonSensor(
        task_id="wait_for_complete_sentinel_batch",
        python_callable=check_for_complete_batch,
        poke_interval=30,
        timeout=600,
    )

    check_vehicles_snapshot = PythonOperator(
        task_id="check_landing_vehicles_snapshot",
        python_callable=check_landing_vehicles_snapshot,
    )

    check_vehicles_cdc_events = PythonOperator(
        task_id="check_landing_vehicles_cdc_events",
        python_callable=check_landing_vehicles_cdc_events,
    )

    ingest_vehicles_cdc_events = PythonOperator(
        task_id="ingest_landing_vehicles_cdc_events",
        python_callable=ingest_landing_vehicles_cdc_events,
    )

    generate_source_vehicle_updates = PythonOperator(
        task_id="generate_source_vehicle_updates_for_cdc",
        python_callable=generate_source_vehicle_updates_for_cdc,
    )

    generate_unstructured_batch = PythonOperator(
        task_id="generate_unstructured_batch_files",
        python_callable=generate_unstructured_batch_files,
    )

    check_generated_unstructured = PythonSensor(
        task_id="check_generated_unstructured_batch",
        python_callable=check_generated_unstructured_batch,
        poke_interval=10,
        timeout=120,
    )

    check_telemetry_facts = PythonOperator(
        task_id="check_landing_telemetry_facts",
        python_callable=check_landing_telemetry_facts,
    )

    check_telemetry_ingest_events = PythonOperator(
        task_id="check_landing_telemetry_ingest_events",
        python_callable=check_landing_telemetry_ingest_events,
    )

    generate_source_telemetry = PythonOperator(
        task_id="generate_source_telemetry_events",
        python_callable=generate_source_telemetry_events,
    )

    ingest_telemetry_events = PythonOperator(
        task_id="ingest_landing_telemetry_events",
        python_callable=ingest_landing_telemetry_events,
    )

    run_vector_ingestion = PythonOperator(
        task_id="ingest_unstructured_to_vector_db",
        python_callable=process_unstructured_to_vector_db,
        execution_timeout=timedelta(minutes=8),
    )

    check_vector_db = PythonOperator(
        task_id="check_vector_db_population",
        python_callable=check_vector_db_population,
    )

    run_curation = PythonOperator(
        task_id="curation_layer_correlation",
        python_callable=run_curation_layer,
    )

    run_curation_relational = PythonOperator(
        task_id="curation_relational_sync",
        python_callable=run_curation_relational_sync,
    )

    run_star_schema = PythonOperator(
        task_id="populate_analytics_star_schema",
        python_callable=populate_analytics_star_schema,
    )

    check_analytics_dim_vehicle = PythonOperator(
        task_id="check_analytics_dim_vehicle_today",
        python_callable=check_analytics_dim_vehicle_today,
    )

    check_curation = PythonOperator(
        task_id="check_curation_output",
        python_callable=check_curation_output,
    )

    check_curation_telemetry = PythonOperator(
        task_id="check_curation_telemetry_facts",
        python_callable=check_curation_telemetry_facts,
    )

    def run_postrun_validation(**context):
        validation_dir = Path("/opt/airflow/logs/validation_runs")
        validation_dir.mkdir(parents=True, exist_ok=True)

        run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_base = f"validation_{stamp}_{run_id.replace(':', '_').replace('/', '_')}"
        report_json_path = validation_dir / f"{report_base}.json"
        report_log_path = validation_dir / f"{report_base}.log"
        expected_vehicle_rows = runtime_int_value(
            context,
            "source_vehicle_distinct_rows",
            "SENTINEL_SOURCE_DISTINCT_ROWS",
            runtime_int_value(context, "postrun_expected_vehicle_rows", "SENTINEL_POSTRUN_EXPECTED_VEHICLE_ROWS", 10),
        )
        expected_telemetry_rows = runtime_int_value(
            context,
            "source_telemetry_events",
            "SENTINEL_SOURCE_TELEMETRY_EVENTS",
            runtime_int_value(context, "postrun_expected_telemetry_rows", "SENTINEL_POSTRUN_EXPECTED_TELEMETRY_ROWS", 10),
        )
        expected_ai_rows = runtime_int_value(
            context,
            "unstructured_distinct_rows",
            "SENTINEL_UNSTRUCTURED_DISTINCT_ROWS",
            runtime_int_value(context, "postrun_expected_ai_rows", "SENTINEL_POSTRUN_EXPECTED_AI_ROWS", expected_vehicle_rows),
        )

        verifier_script_candidates = [
            f"{PROJECT_PATH}/tests/verify_migration.py",
            f"{PROJECT_PATH}/scripts/verify_migration.py",
        ]
        verifier_script = None
        for script_path in verifier_script_candidates:
            if os.path.exists(script_path):
                verifier_script = script_path
                break

        if verifier_script is None:
            raise FileNotFoundError(
                f"Validation script not found in candidates={verifier_script_candidates}"
            )

        command = [
            sys.executable,
            verifier_script,
            "--expected-vehicle-rows",
            str(max(1, expected_vehicle_rows)),
            "--expected-telemetry-rows",
            str(max(1, expected_telemetry_rows)),
            "--expected-ai-rows",
            str(max(1, expected_ai_rows)),
            "--report-json",
            str(report_json_path),
            "--report-log",
            str(report_log_path),
        ]

        logger.info(
            "postrun_validation_start expected_vehicle_rows=%s expected_telemetry_rows=%s expected_ai_rows=%s",
            expected_vehicle_rows,
            expected_telemetry_rows,
            expected_ai_rows,
        )
        logger.info("postrun_validation_command=%s", command)

        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.stdout:
            logger.info("postrun_validation_stdout=%s", completed.stdout.strip())
        if completed.stderr:
            logger.warning("postrun_validation_stderr=%s", completed.stderr.strip())

        logger.info("postrun_validation_report_json=%s", report_json_path)
        logger.info("postrun_validation_report_log=%s", report_log_path)

        if completed.returncode != 0:
            raise ValueError(
                "Postrun validation failed: "
                f"returncode={completed.returncode} stdout={completed.stdout.strip()} stderr={completed.stderr.strip()}"
            )

    postrun_validation = PythonOperator(
        task_id="postrun_data_validation",
        python_callable=run_postrun_validation,
    )

    wait_for_batch >> generate_source_vehicle_updates >> ingest_vehicles_cdc_events >> check_vehicles_cdc_events
    wait_for_batch >> generate_source_telemetry >> ingest_telemetry_events >> [check_telemetry_facts, check_telemetry_ingest_events]
    wait_for_batch >> check_vehicles_snapshot
    wait_for_batch >> generate_unstructured_batch >> check_generated_unstructured
    [
        check_vehicles_snapshot,
        check_vehicles_cdc_events,
        check_telemetry_facts,
        check_telemetry_ingest_events,
        check_generated_unstructured,
    ] >> run_vector_ingestion
    run_vector_ingestion >> check_vector_db >> run_curation >> check_curation
    [check_vehicles_snapshot, check_vehicles_cdc_events, check_telemetry_facts, check_telemetry_ingest_events] >> run_curation_relational >> check_curation_telemetry
    [check_curation, check_curation_telemetry] >> run_star_schema
    run_star_schema >> check_analytics_dim_vehicle >> postrun_validation