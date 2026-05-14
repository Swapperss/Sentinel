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
