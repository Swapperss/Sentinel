# Sentinel Master Plan (As-Built + To-Build)

## 1) Purpose
Build an interview-ready Data Engineering + AI platform with clear layer boundaries, reproducible schema management, CDC reliability, curated AI outputs, and analytics-serving marts.

This plan is the single source of truth for implementation.

---

## 2) Working Protocol (Non-Negotiable)
1. One step per turn only.
2. I provide exactly one action (SQL/code/command).
3. You execute and respond with `done` plus result/error.
4. We move to next step only after `done`.
5. If a step fails, we debug only that step until green.
6. Every step includes one interview takeaway.

---

## 3) Locked Architecture Decisions

### 3.1 Layer Model
1. Source: operational tables where changes originate.
2. Landing: ingestion/state/history tables.
3. Curation: canonical transformed copies + AI-enriched outputs.
4. Consumption: reusable views for any project to consume curated data.
5. Analytics: project-specific star schema (facts/dimensions) in `sentinel_analytics` for our BI/analytics workloads.
6. Airflow: orchestration control plane for ingestion checks, curation runs, quality gates, and publish readiness.

### 3.2 Table Contract (Locked)

#### Source
1. `sentinel_db.vehicles_sentinel`

#### Landing
1. `sentinel_landing.vehicles` (snapshot)
2. `sentinel_landing.vehicles_cdc_events` (append-only CDC event history)
3. `sentinel_landing.telemetry_facts` (telemetry landing facts)

#### Curation
1. `sentinel_curation.vehicles` (canonical transformed copy)
2. `sentinel_curation.telemetry_facts` (canonical transformed copy)
3. `sentinel_curation.sentinel_ai_curation` (AI-curated/enriched diagnostics)

#### Consumption
1. Views only (read-only serving layer on curated outputs for any project)

#### Analytics (Project-Specific)
1. `sentinel_analytics` database: star schema with facts/dimensions for Sentinel-specific BI, dashboards, and analytics workloads.

### 3.3 Redundancy Rule
1. Landing to Curation copies are allowed by design.
2. Curation copies must be canonicalized (dedup/type/quality normalized), not blind copies.

---

## 4) Offset, Replay, and Idempotency Policy
1. Every CDC step must explicitly state:
   - consumer group
   - offset mode (`earliest` / `latest`)
2. Replay/debug: new group + earliest.
3. Normal run: stable group + latest.
4. Idempotency key standard: `topic-partition-offset` (or deterministic equivalent).
5. CDC writes must be replay-safe (no duplicate state mutation).
6. For every replay step, validate row deltas and duplicate counts.

---

## 5) Migration and Schema Governance
1. All schema/table/view changes must be via migration files in `db/migrations`.
2. Apply with `scripts/db_migrate.py` only.
3. `sentinel_admin.schema_migrations` is the applied-version ledger.
4. Migration rerun must be safe and skip applied files.
5. Admin connectivity uses explicit admin env controls (`MYSQL_ADMIN_HOST`, `MYSQL_ADMIN_USER`, `MYSQL_ADMIN_PASSWORD`) where needed.

---

## 6) Current State (As-Built So Far)
1. CDC source table established (`sentinel_db.vehicles_sentinel`).
2. Debezium connector configured and updated through REST API.
3. CDC topic path validated (with topic naming corrections and payload conversion adjustments).
4. Vehicles CDC consumer path functional after:
   - topic correction
   - payload format correction
   - MySQL grants correction
5. Landing snapshot updates verified in `sentinel_landing.vehicles`.
6. Telemetry path exists and writes to `sentinel_landing.telemetry_facts`.
7. Migration framework exists and is runnable.
8. `interview_corpus.md` initialized and maintained.

---

## 7) Implementation Roadmap (Future Work)

Phase E is Priority-1 and must be executed first before the remaining roadmap phases.

### Phase A: Landing Reliability Hardening
1. Ensure vehicles CDC consumer writes both:
   - `sentinel_landing.vehicles`
   - `sentinel_landing.vehicles_cdc_events`
2. Persist deterministic `event_key` for event history dedup/replay.
3. Ensure telemetry ingestion remains idempotent.
4. Add structured per-event logs:
   - operation, key entity, topic, partition, offset, action result.

#### Acceptance Criteria
1. One insert/update/delete at source appears correctly in landing snapshot.
2. Same three events appended in landing CDC event table.
3. Replay does not produce duplicate logical mutations.

---

### Phase B: Curation Canonicalization
1. Create and maintain:
   - `sentinel_curation.vehicles`
   - `sentinel_curation.telemetry_facts`
2. Implement canonical load logic from landing:
   - type normalization
   - key integrity
   - dedup handling
   - quality flags for suspect records
3. Keep `sentinel_curation.sentinel_ai_curation` for AI-enriched diagnostics.

#### Acceptance Criteria
1. Curation tables refresh deterministically from landing.
2. Duplicate/replay inputs do not corrupt curated canonical state.
3. Data quality anomalies are observable.

---

### Phase C: Curation Scheduling Strategy
1. Start with scheduled micro-batch cadence (10 minutes).
2. Keep real-time optional; not mandatory initially.
3. Evaluate Spark Structured Streaming adoption once canonical batch is stable.

#### Acceptance Criteria
1. 10-minute curated refresh is stable for repeated runs.
2. No drift between source-of-truth landing and curated canonical states.

---

### Phase D: Consumption Views
1. Build/refresh read-only views in `sentinel_consumption` over curation outputs.
2. No heavy business logic in consumption.

#### Acceptance Criteria
1. Consumption views remain stable across curation internal changes.
2. Query latency acceptable for dashboard/API usage.

---

### Phase E (Priority-1): Star Schema Analytics in `sentinel_analytics`
1. Create fact/dimension model optimized for Sentinel-specific BI and analytics queries.
2. Initial target model:
   - dimensions: vehicle, time, status (expand as needed)
   - facts: telemetry events, health/diagnostic events
3. Populate from curated canonical + AI outputs.

#### Acceptance Criteria
1. Facts/dimensions reconcile with curated source counts.
2. KPI aggregates are reproducible and explainable.

---

### Phase F: Testing and Quality Gates
1. Unit tests:
   - CDC create/update/delete semantics
   - idempotency behavior
   - event-key uniqueness expectations
2. Integration tests:
   - source -> Kafka -> landing -> curation -> consumption
3. Migration tests:
   - rerun idempotency
   - object existence
4. Data tests:
   - row-count reconciliation
   - null/key constraint checks

#### Acceptance Criteria
1. Required test suite passes before moving phases.
2. Failures block promotion until fixed.

---

### Phase G: Airflow Orchestration
1. Implement orchestration DAG in `dags/sentinel_automation_dag.py` for full pipeline control.
2. Orchestrate end-to-end sequence:
   - ingestion health checks (CDC + telemetry freshness)
   - landing readiness checks (required row deltas and quality thresholds)
   - curation canonical refresh
   - AI curation refresh
   - consumption view refresh/validation
3. Add failure behavior:
   - task-level retries where safe
   - fail-fast on data quality gate failures
   - explicit logging and alert-friendly task outputs
4. Start with schedule cadence aligned to curation micro-batch (10 minutes), then tune.

#### Acceptance Criteria
1. DAG run can orchestrate full path without manual intervention.
2. Failed quality checks prevent downstream publish tasks.
3. Run metadata and task logs are sufficient for incident triage.

---

## 8) Risks and Mitigations
1. Payload format mismatch (binary vs JSON):
   - Mitigation: explicit converter settings + smoke consumer validation.
2. Topic naming drift:
   - Mitigation: locked naming contract in config and tests.
3. Permission drift (DB/Kafka ACL):
   - Mitigation: grant checklist + preflight checks.
4. Replay duplicates:
   - Mitigation: deterministic event keys + idempotent writes + validation queries.
5. Uncontrolled schema drift:
   - Mitigation: migrations only + version ledger + PR review gate.

---

## 9) Interview Strategy Integration (Mandatory)
1. After each completed milestone, add 5-10 targeted questions to `interview_corpus.md`.
2. Include at least one question in each bucket:
   - architecture
   - debugging
   - reliability/testing
   - scalability
   - governance
3. Keep examples tied to actual Sentinel implementation decisions.

---

## 10) Immediate Next Step (Current Checkpoint)
1. Confirm ETL infra DAG health:
   - `sentinel_pipeline` is active and running (`db_migrate` -> `pipeline_complete`).
2. Run the data pipeline DAG for AI ingestion/curation:
   - unpause `sentinel_data_pipeline`
   - trigger manual run after fresh landing files are generated
   - verify tasks: `wait_for_complete_sentinel_batch` -> `ingestion_layer_processing` -> `curation_layer_correlation`
3. Validate curation outputs after DAG success:
   - row count and latest records in `sentinel_curation.sentinel_ai_curation`
   - correlation quality between notes/logs metadata and telemetry.

Interview takeaway:
Operational readiness requires two separate controls: infrastructure/schema DAG stability and data-quality DAG execution against fresh batches.
