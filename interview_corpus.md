# Interview Corpus: Sentinel (Data Engineering + AI + System Design)

This corpus is a living question bank derived from the architecture and implementation decisions in this project. It is intentionally system-design heavy for interview prep.

## 1) Architecture and Layering
1. Why split the platform into Source, Landing, Curation, and Consumption instead of using one schema?
2. What are the tradeoffs between keeping only SCD1 snapshot tables versus maintaining both snapshot and history tables?
3. Why should Consumption be modeled from curated datasets instead of directly from Landing tables?
4. How would you explain the boundaries and ownership of each layer to a mixed team of data engineers and ML engineers?
5. In what scenarios would you allow direct access to Landing tables, and how would you control that access?

## 2) CDC and Event Flow Design
1. Describe the end-to-end flow for vehicle CDC in this project: source table to Debezium to Kafka to Landing table.
2. Why can telemetry ingestion work while vehicle CDC is broken, even though both use Kafka?
3. How do you design idempotency for CDC consumers using topic-partition-offset?
4. What is the role of an append-only CDC events table if you already have a latest snapshot table?
5. How do you safely process delete events from Debezium and preserve lineage?
6. What is the risk of consuming from the wrong topic or consumer group, and how do you detect it quickly?

## 3) Schema and Contract Strategy
1. When would you choose JSON converter versus Avro/Protobuf contracts for Debezium topics?
2. What are the implications of enabling/disabling schema metadata in Kafka Connect JsonConverter?
3. How would you introduce data contracts later without breaking existing consumers?
4. What compatibility strategy would you enforce for evolving vehicle CDC payloads?
5. How would you validate key-field stability (for example VIN) across producers and consumers?

## 4) Batch vs Streaming Curation
1. Why is micro-batch Structured Streaming every 10 minutes often a better starting point than full real-time curation?
2. How would you implement event-time and as-of joins between telemetry and vehicle history?
3. What are the tradeoffs between a Python bridge script and Spark Structured Streaming curation?
4. How do checkpoints, watermarks, and late-arrival handling influence correctness?
5. How would you guarantee replay-safe outputs in curation during reruns?

## 5) Data Modeling for Consumption
1. What dimensions and facts would you create for vehicle diagnostics analytics, and why?
2. How do you decide whether a metric belongs in a dimension attribute versus a fact table measure?
3. How should anomaly events be modeled to support both root-cause and trend analytics?
4. What are the benefits of storing both current profile and SCD2 profile in curation before mart building?
5. How would you design consumption views to stay stable while curation internals evolve?

## 6) Reliability, Operations, and Observability
1. What key logs and metrics must exist in CDC consumers for production support?
2. How do you detect and recover from poison messages or payload format mismatches?
3. What are practical rollback strategies when connector config changes break ingestion?
4. How do you prove end-to-end data freshness from source update to consumption view?
5. What retention and archival policy would you set for CDC event history tables?

## 7) Quality and Testing
1. What unit tests are essential for CDC op semantics (create, update, delete)?
2. How would you test SCD2 interval correctness (valid_from, valid_to, is_current)?
3. What integration tests validate source change to Kafka event to Landing persistence?
4. How should migration tests ensure idempotency and schema drift safety?
5. What minimal test suite is required before switching dashboards to a new curated pipeline?

## 8) AI/LLM Integration and Governance
1. Where should LLM diagnostics be applied in the pipeline to avoid contaminating raw data?
2. How do you keep deterministic curation logic separate from probabilistic AI outputs?
3. What lineage fields should be stored to audit an AI-generated diagnostic summary?
4. How would you evaluate and monitor quality drift in AI diagnostic outputs?
5. What safeguards are needed before exposing AI summaries in consumption dashboards?

## 9) Scenario-Based Deep-Dive Questions
1. A CDC consumer restarts and reprocesses old offsets. How do you prevent duplicate state mutations?
2. A connector emits binary payloads while your consumer expects JSON. What is your triage sequence?
3. Landing snapshot and history disagree for a VIN. How do you reconcile and backfill safely?
4. Spark curation falls behind by 45 minutes. How do you recover while preserving correctness?
5. Product asks for hourly SLA with strict correctness. How would you redesign trigger, watermark, and storage strategy?

## 10) Strong Interview Narrative Prompts
1. Explain one major architecture mistake made initially in this project and how you corrected it.
2. Explain why you added migration code and version tracking instead of manual SQL.
3. Explain how you balanced fast delivery with production-grade data governance.
4. Explain the transition roadmap from script-based curation to Spark streaming curation.
5. Explain what production-readiness means for this platform in terms of reliability, testing, and model governance.

## How to Keep Extending This File
1. After each design or implementation milestone, add 5 to 10 new questions in the relevant section.
2. Prefer scenario-based questions tied to actual bugs and decisions from this project.
3. Add one high-level architecture question, one debugging question, one testing question, one scalability question, and one governance question per milestone.
4. Periodically prune repetitive questions and keep the best formulation for interview practice.

## 11) Telemetry Event Lineage and Replay (New Milestone)
1. Why should telemetry ingestion use a dual-write design (`telemetry_facts` plus `telemetry_ingest_events`) instead of writing facts only?
2. How does `event_key = topic-partition-offset` guarantee idempotent replay behavior in a Kafka consumer?
3. What fields must be stored in telemetry ingest events to enable root-cause debugging for data quality incidents?
4. How would you design retry semantics when fact write fails but ingest-event write succeeds, or vice versa?
5. What tradeoffs exist between storing raw payload JSON versus decoded/normalized fields in the telemetry event log?
6. How do you use ingest status (`SUCCESS`/`FAILED`) to create operational SLO dashboards for streaming reliability?
7. A PM reports missing telemetry for one VIN. What step-by-step SQL investigation would you run across `telemetry_ingest_events` and `telemetry_facts`?
8. If the same Kafka message is re-consumed after a rebalance, how do your unique keys and upserts prevent duplicate fact rows?

## 12) Star Schema and Analytics Database (Phase E - Complete)
1. Why is it important to separate sentinel_consumption (reusable views) from sentinel_analytics (project-specific star schema)?
2. How do surrogate keys (vehicle_key, time_key) in dimensions enable efficient fact table joins compared to natural keys?
3. What query patterns become faster with a star schema versus querying from normalized landing/curation tables?
4. How should you handle SCD Type 1 vs Type 2 dimensions when populating from landing data with multiple versions?
5. What is the risk of using NOT EXISTS joins during population, and how do you ensure idempotency?
6. If you need to add a new dimension (e.g., dim_dealer), what steps ensure downstream fact tables remain consistent?
7. How would you validate that all fact rows have corresponding dimension keys (referential integrity)?
8. Describe a query that would detect orphaned fact rows due to a missed dimension insert during population.

## 13) Schema Drift and Nullable Column Strategy (Next Milestone)
1. Why should `owner_name` remain distinct from `manufacturer` instead of being aliased during transformation?
2. When you add a new nullable column like `manufacturer`, which layers should get it first and why?
3. How do you evolve source, landing, curation, and consumption together without breaking downstream consumers?
4. What is the difference between adding a nullable column and backfilling a column with a default value?
5. How would you make Spark curation jobs tolerate missing columns without failing on schema drift?
6. Why should consumption views mirror curation columns instead of re-deriving business logic?
7. How would you validate that a newly added column is present in all required layers after a migration?
8. If a new upstream field arrives with partial nulls, how do you decide whether it belongs in source, landing, curation, or analytics?

## 14) Answered Interview Corpus: CDC Replay, Contracts, and End-to-End Flow
1. Q: What was the root cause of the vehicle pipeline failure?
	A: The pipeline was treating the vehicle CDC consumer like a pure streaming tailer, but the DAG expected batch-style replay of the same run's generated data. The consumer used a stable group with `auto.offset.reset=latest`, so it started after the new events had already landed and saw zero records. That caused the landing checks to fail below the expected 100-row threshold.
2. Q: Why did telemetry continue working when vehicle CDC failed?
	A: The telemetry path and vehicle CDC path had different timing and offset behavior. Telemetry already had a backlog or compatible offsets, so it could consume records successfully. Vehicle CDC was more sensitive because the run was generating source rows and then trying to consume them in the same orchestration window.
3. Q: What change fixed the vehicle CDC issue?
	A: The DAG now forces replay-safe consumption by switching the vehicle CDC loader to `offset_reset=earliest` when the run has expected source rows. That lets the consumer replay the relevant topic history and catch up to the run's generated changes instead of missing them.
4. Q: Why is `latest` correct in some systems but wrong here?
	A: `latest` is correct for a long-running tailer that only needs future events. It is wrong for a DAG-run-local batch contract because the data may already exist by the time the consumer starts. In that case, you need replay semantics, not tail-only semantics.
5. Q: Why did the pipeline need both checkpoint-safe and threshold-based validation?
	A: Checkpoint-safe logic prevents false failures when a task legitimately processes zero records in a run. Threshold-based validation makes sure the pipeline still enforces business correctness when data is expected. Together, they separate "no data available" from "data was expected but not processed."
6. Q: Why add relational curation sync if a semantic bridge already existed?
	A: The semantic bridge wrote AI-curated outputs, but the relational curation tables were still part of the downstream contract for analytics. The pipeline needed a deterministic sync from landing into curation so analytics could build on curated relational data even if AI-assisted enrichment only covered part of the flow.
7. Q: How do you explain idempotency in this pipeline?
	A: Idempotency means rerunning the same record does not create duplicate business state. In CDC and telemetry consumers, that is usually achieved with stable event keys, upserts, and unique constraints. The goal is that retries, rebalances, or replays produce the same final table state.
8. Q: What is the purpose of append-only event tables alongside snapshot tables?
	A: Snapshot tables answer the question "what is the latest state?" Event tables answer "what happened, when, and how many times?" The event tables preserve lineage, support debugging, and make replay or audit scenarios possible without losing the change history.
9. Q: How would you test this kind of pipeline before calling it production-ready?
	A: I would test create, update, and delete semantics for CDC; verify replay behavior after offset resets; confirm that landing, curation, and analytics counts reach the expected threshold; and run integration checks that compare source row counts against downstream rows. I would also test failure recovery, schema drift, and duplicate-message handling.
10. Q: What is the main design lesson from this bug?
	 A: The key lesson is that orchestration semantics and consumer semantics must match. If the pipeline is designed as a run-scoped batch contract, every stage must either replay the relevant inputs or operate on already-materialized data. A stable streaming consumer alone is not enough.

## 15) Quick Answers for Practice
1. Q: Why did the landing count need a minimum threshold instead of an exact count?
	A: A minimum threshold verifies the pipeline processed at least the expected dataset while still allowing extra records from retries, backlog, or replay. It is more robust than an exact count in CDC systems where duplicate-safe processing and historical records are normal.
2. Q: Why do we keep telemetry facts and telemetry ingest events separate?
	A: The facts table stores analytical business data, while the ingest-events table stores operational lineage and processing status. Separating them keeps analytics clean and gives you a durable audit trail for debugging.
3. Q: How do curation and analytics stay consistent after reruns?
	A: They stay consistent through deterministic upserts, stable keys, and rerunnable population logic. A rerun should converge to the same final state instead of appending conflicting duplicates.
4. Q: Why is schema drift dangerous in layered data platforms?
	A: Schema drift can break consumers at different layers in different ways. A field that is optional in landing may be required in analytics, so migrations and transformations must be version-aware and tolerant of nullable evolution.
5. Q: What would you say in an interview about this project improvement?
	A: I would explain that I moved the platform from a fragile streaming assumption to an explicit end-to-end data contract. That included replay-safe ingestion, stronger validation, deterministic curation sync, and downstream checks that proved the data really flowed through every layer.

## 16) Realtime Design: Late Logs, Offsets, and Completeness SLA
1. Q: If vehicle CDC and telemetry arrive today, but diagnostic logs arrive after 2 to 3 days, should enrichment still happen?
	A: Yes. The correct design is eventual enrichment. Late logs must still be consumed and correlated to existing vehicle and telemetry data when they arrive.
2. Q: Why is offset-based ingestion important for late-arriving logs?
	A: Offsets provide replay-safe consumption and ordering within a partition. If consumers are restartable and idempotent, late events are still processed without data loss or duplicated business state.
3. Q: Why is strict same-day completeness usually wrong for realtime systems?
	A: Realtime pipelines are eventually consistent. Network delays, producer lag, and upstream retries can shift arrival time. Same-day hard checks can create false failures even when data arrives shortly after.
4. Q: What keys should be used to correlate late logs with prior telemetry and vehicle records?
	A: Use durable business and lineage keys such as VIN, correlation_id, file_id, and event time windows. Avoid relying only on ingestion date.
5. Q: How do you prevent duplicates when late logs are replayed or reprocessed?
	A: Use deterministic upsert keys (for example vehicle_id + event_timestamp, or stable correlation_id), unique constraints, and idempotent write logic in curation tables.
6. Q: Should a DAG fail immediately if logs are missing for today's VINs?
	A: Not in an eventual-consistency model. It should record pending enrichment status, continue core processing, and let a reconciliation task finalize completeness within a defined SLA window.
7. Q: What SLA pattern is practical for AI curation completeness?
	A: Use staged SLO/SLA targets, such as 95% enrichment within 24 hours and 100% within 72 hours, then alert only if unresolved after the deadline.
8. Q: What operational controls are needed for this design?
	A: Add lag-aware monitoring, pending backlog counts, reconciliation retries, and dead-letter handling with reason codes so unresolved VINs are auditable and actionable.
9. Q: How should interviewers evaluate this architecture decision?
	A: Strong answers distinguish between hard correctness for core facts and eventual completeness for late context signals. The design should prioritize no data loss, idempotency, and measurable recovery windows.
10. Q: What is the final business guarantee in this model?
	A: Every eligible vehicle record is eventually enriched when supporting logs arrive, and unresolved cases are explicitly tracked, retried, and escalated by SLA rather than silently dropped.
