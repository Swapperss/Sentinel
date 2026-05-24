# Sentinel Pipeline Runbook

This runbook helps you execute and validate the Sentinel pipeline end-to-end on Windows.

## 1) Scope

This runbook covers:
- Bringing up required services
- Triggering the Airflow DAG
- Monitoring progress
- Validating data flow through landing, vector, and curation
- Handling common failures quickly

Pipeline DAG:
- sentinel_data_pipeline

## 2) Prerequisites

- Windows PowerShell
- Docker Desktop running
- Project checked out at:
  - D:\Swapnil\DSML_LLM\AILLM\Projects\Sentinel
- Python virtual environment available at:
  - .venv
- Valid .env file in repo root with required credentials

Minimum .env variables to verify:
- OPENAI_API_KEY
- MYSQL_HOST
- MYSQL_PORT
- MYSQL_DB_USER
- MYSQL_DB_PASSWORD
- MYSQL_LANDING_SCHEMA
- MYSQL_CURATION_SCHEMA
- AIRFLOW_PROJECT_ROOT (recommended: /opt/airflow/project for containers)

Optional tuning variables:
- SENTINEL_UNSTRUCTURED_BATCH_SIZE=2
- SENTINEL_OPENAI_TIMEOUT_SECONDS=60
- SENTINEL_OPENAI_MAX_RETRIES=2
- SENTINEL_VECTOR_INGEST_TIMEOUT_SECONDS=420

## 3) One-Time Sanity Check

From repo root:

1. Activate venv:
   - (Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned) ; (& .\.venv\Scripts\Activate.ps1)

2. Validate Python files compile:
   - python -m py_compile dags\sentinel_automation_dag.py scripts\embedder.py scripts\sentinel_bridge.py

3. Start Debezium stack:
   - docker-compose --env-file .env -f docker\debezium\docker-compose.debezium.yml up -d

4. Start Airflow stack (if not running):
   - docker-compose --env-file .env -f docker\airflow\docker-compose.airflow.yml up -d

5. Check containers:
   - docker ps --format "table {{.Names}}\t{{.Status}}"

Expected key containers:
- sentinel-airflow-webserver
- sentinel-airflow-scheduler
- sentinel-airflow-postgres
- Debezium/connect container from debezium compose

## 4) Trigger a Fresh DAG Run

1. Create run id:
   - $runId = "manual__try_$(Get-Date -Format yyyyMMdd_HHmmss)"

2. Trigger DAG:
   - docker exec sentinel-airflow-webserver airflow dags trigger sentinel_data_pipeline -r $runId

3. Confirm run created:
   - docker exec sentinel-airflow-webserver airflow dags list-runs -d sentinel_data_pipeline --no-backfill | Select-Object -First 5

## 5) Monitor Live Progress

Use this repeatedly:
- docker exec sentinel-airflow-webserver airflow tasks states-for-dag-run sentinel_data_pipeline $runId

Healthy task order (high level):
1. wait_for_complete_sentinel_batch
2. generate/check source + landing checks
3. ingest_landing_vehicles_cdc_events
4. check_landing_vehicles_cdc_events
5. ingest_unstructured_to_vector_db
6. check_vector_db_population
7. curation_layer_correlation
8. check_curation_output
9. postrun_data_validation

Typical timings from latest stable run:
- Vector ingestion: ~4-5 seconds
- Curation: ~4-6 seconds
- Total run: ~1-2 minutes (depends on CDC pace)

## 6) Validate Data After Run

When DAG state becomes success, validate counts.

### Landing checks
- docker exec sentinel-airflow-postgres psql -U airflow -d airflow -c "SELECT 1;"  (connectivity check)
- Validate MySQL row counts via your preferred MySQL client for:
  - sentinel_landing.vehicles
  - sentinel_landing.vehicles_cdc_events
  - sentinel_landing.telemetry_facts
   - sentinel_landing.telemetry_ingest_events

- Validate fresh (today) landing data:
   - SELECT COUNT(*) FROM sentinel_landing.telemetry_facts WHERE DATE(trip_timestamp)=CURDATE();
   - SELECT COUNT(*) FROM sentinel_landing.telemetry_ingest_events WHERE DATE(ingested_at)=CURDATE();

### Vector checks
- Vector DB sqlite file:
  - data\landing\sentinel_landing\vectors\sentinel_unstructured\chroma.sqlite3

- Quick vector row count via Python:
  - python -c "import sqlite3; c=sqlite3.connect('data/landing/sentinel_landing/vectors/sentinel_unstructured/chroma.sqlite3'); cur=c.cursor(); cur.execute('SELECT COUNT(*) FROM embeddings'); print(cur.fetchone()[0]); c.close()"

### Curation checks
- sentinel_curation.sentinel_ai_curation should have rows
- View consumption view row count:
  - sentinel_consumption.v_sentinel_ai_curation

### Analytics checks
- Validate fresh (today) star-dimension updates:
   - SELECT COUNT(*) FROM sentinel_analytics.dim_vehicle WHERE DATE(updated_at)=CURDATE();

## 7) Fast Troubleshooting Guide

### A) DAG stuck at vector or curation
Likely reason:
- SQLite lock contention from long-held vector client handle

What is already fixed:
- Lazy initialization in embedder and curation bridge
- Subprocess wrapper for vector ingestion with timeout
- SQLite-based vector check in DAG

What to do:
1. Restart Airflow containers:
   - docker restart sentinel-airflow-scheduler sentinel-airflow-webserver
2. Trigger fresh run with new run id

### B) OpenAI step slow/failing
Checks:
- Verify OPENAI_API_KEY in .env
- Review retry/timeout vars:
  - SENTINEL_OPENAI_TIMEOUT_SECONDS
  - SENTINEL_OPENAI_MAX_RETRIES

### C) CDC ingestion not moving
Checks:
- Debezium/connect container is healthy
- Connector is RUNNING
- MySQL replication grants exist

### D) Postrun validation fails
Use task log tail to find failing check:
- logs\dag_id=sentinel_data_pipeline\run_id=<RUN_ID>\task_id=postrun_data_validation\attempt=1.log

## 8) Controlled Recovery (If Run Must Be Abandoned)

Use only when a run is clearly stuck and you want a clean retry.

1. Mark stuck task failed:
- docker exec sentinel-airflow-postgres psql -U airflow -d airflow -c "UPDATE task_instance SET state='failed', end_date=NOW() WHERE dag_id='sentinel_data_pipeline' AND run_id='<RUN_ID>' AND state IN ('running','queued','restarting','up_for_retry');"

2. Mark dag run failed:
- docker exec sentinel-airflow-postgres psql -U airflow -d airflow -c "UPDATE dag_run SET state='failed', end_date=NOW() WHERE dag_id='sentinel_data_pipeline' AND run_id='<RUN_ID>' AND state='running';"

3. Trigger a fresh run id and continue.

## 9) Success Criteria Checklist

A run is considered healthy when all are true:
- DAG run state = success
- ingest_unstructured_to_vector_db = success within timeout window
- check_vector_db_population = success
- curation_layer_correlation = success
- check_curation_output = success
- postrun_data_validation = success

## 10) Handy Command Bundle

Use this compact monitoring bundle:

- $runId = "manual__try_YYYYMMDD_HHMMSS"
- docker exec sentinel-airflow-webserver airflow dags list-runs -d sentinel_data_pipeline --no-backfill | Select-Object -First 3
- docker exec sentinel-airflow-webserver airflow tasks states-for-dag-run sentinel_data_pipeline $runId

If needed, inspect task log quickly:
- Get-Content logs\dag_id=sentinel_data_pipeline\run_id=$runId\task_id=ingest_unstructured_to_vector_db\attempt=1.log -Tail 120
- Get-Content logs\dag_id=sentinel_data_pipeline\run_id=$runId\task_id=curation_layer_correlation\attempt=1.log -Tail 120
- Get-Content logs\dag_id=sentinel_data_pipeline\run_id=$runId\task_id=postrun_data_validation\attempt=1.log -Tail 120
