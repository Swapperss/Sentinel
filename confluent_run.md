# Confluent Run Guide (Sentinel)

This guide documents the Confluent Cloud setup and operating steps for this repository.
It includes topics, access, connector registration, and verification checks.

## 1) What this project uses in Confluent

Data flows:
1. Source table updates in MySQL are captured by Debezium.
2. Debezium publishes CDC events to Kafka topic `cdc_prod.sentinel_db.vehicles_sentinel`.
3. Telemetry producer publishes Protobuf events to Kafka topic `telemetry.raw`.
4. Consumers read those topics and load MySQL landing tables.

Code references:
- Telemetry producer topic default: `src/ingestion/kafka_producer.py`
- Telemetry consumer topic/group defaults: `src/ingestion/kafka_to_mysql_consumer.py`
- Vehicles CDC consumer topic/group defaults: `src/ingestion/vehicles_cdc_consumer.py`
- Debezium connector config (topic prefix + schema history topic): `register-mysql.json`
- Debezium Connect runtime env: `docker/debezium/docker-compose.debezium.yml`

## 2) Confluent resources to create

Create these in one Confluent Environment:
1. Kafka cluster (Basic/Standard is enough for demo/test).
2. Schema Registry (same region as Kafka cluster).
3. Service Account for application runtime (Kafka producer/consumer + Debezium Connect).
4. Service Account for Schema Registry access (or reuse same account if needed).
5. API keys:
   - Kafka API key + secret.
   - Schema Registry API key + secret.

## 3) Environment variables used by this repo

Set these in `.env` (never commit real secrets):

```dotenv
KAFKA_BOOTSTRAP_SERVER=<pkc-...:9092>
KAFKA_API_KEY=<kafka_api_key>
KAFKA_API_SECRET=<kafka_api_secret>

SCHEMA_REGISTRY_URL=<https://psrc-...>
SCHEMA_REGISTRY_API_KEY=<sr_api_key>
SCHEMA_REGISTRY_API_SECRET=<sr_api_secret>

# Optional overrides
KAFKA_TELEMETRY_TOPIC=telemetry.raw
KAFKA_TELEMETRY_CONSUMER_GROUP=sentinel-telemetry-mysql-loader
KAFKA_VEHICLES_CDC_TOPIC=cdc_prod.sentinel_db.vehicles_sentinel
KAFKA_VEHICLES_CDC_GROUP=sentinel-vehicles-cdc-loader
```

Important:
- Current `.env` in this workspace contains real credentials. Rotate them after reward-program work ends.
- Containers must be recreated to pick updated `.env`: use `docker compose ... up -d --force-recreate`.

## 4) Topics required

### 4.1 Application topics

Create (or ensure available):
1. `telemetry.raw`
   - Purpose: Protobuf telemetry events from producer.
   - Suggested config: partitions=6, cleanup.policy=delete, retention.ms=604800000 (7 days).

2. `cdc_prod.sentinel_db.vehicles_sentinel`
   - Purpose: Debezium CDC for `sentinel_db.vehicles_sentinel`.
   - Usually auto-created by Debezium based on `topic.prefix=cdc_prod`.
   - If auto-create is restricted, create it manually.
   - Suggested config: partitions=6, cleanup.policy=delete, retention.ms=604800000 (7 days).

### 4.2 Debezium Connect internal topics

From `docker/debezium/docker-compose.debezium.yml`:
1. `dbz-configs`
2. `dbz-offsets`
3. `dbz-status`

From `register-mysql.json`:
4. `dbz-schema-history`

Recommended config for internal topics:
- `dbz-configs`: cleanup.policy=compact
- `dbz-offsets`: cleanup.policy=compact
- `dbz-status`: cleanup.policy=compact
- `dbz-schema-history`: cleanup.policy=compact
- Replication factor: 3 (if cluster tier supports it)

Note:
- Another compose file `docker/docker-compose.yml` uses `dbz-configs-main`, `dbz-offsets-main`, `dbz-status-main`.
- Pick one set consistently for the runtime you actually use.

## 5) Access model (minimum required)

Use least privilege.

### 5.1 Kafka permissions (for runtime key)

Required actions by resource:
1. Topic `telemetry.raw`
   - WRITE (producer)
   - READ (telemetry consumer)
   - DESCRIBE

2. Topic `cdc_prod.sentinel_db.vehicles_sentinel`
   - READ (CDC consumer)
   - DESCRIBE

3. Topics `dbz-configs`, `dbz-offsets`, `dbz-status`, `dbz-schema-history`
   - READ/WRITE/CREATE/DESCRIBE as needed by Kafka Connect/Debezium

4. Consumer groups
   - `sentinel-telemetry-mysql-loader` (or dag-run dynamic groups)
   - `sentinel-vehicles-cdc-loader` (or dag-run dynamic groups)
   - Debezium connect group: `debezium-cluster-v1`
   - Grant READ (group consume) + DESCRIBE

### 5.2 Schema Registry permissions

For subject(s):
- `telemetry.raw-value`

Needed:
- READ + WRITE (serializer registers/reads schema)

## 6) Debezium connector registration

Connector file: `register-mysql.json`

Key values in this repo:
- `topic.prefix = cdc_prod`
- `database.include.list = sentinel_db`
- `table.include.list = sentinel_db.vehicles_sentinel`
- `schema.history.internal.kafka.topic = dbz-schema-history`

Register connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @register-mysql.json
```

Check connector:

```bash
curl http://localhost:8083/connectors/sentinel-mysql-connector/status
```

## 7) Runtime startup order

1. Ensure `.env` is correct.
2. Start Debezium Connect stack.
3. Register connector and verify `RUNNING` state.
4. Start telemetry producer path (script or Airflow task).
5. Start consumers (or Airflow DAG).
6. Verify Kafka topic traffic and MySQL landing updates.

## 8) Verification checklist

### 8.1 Kafka-level checks

1. `telemetry.raw` is receiving new messages.
2. `cdc_prod.sentinel_db.vehicles_sentinel` is receiving CDC events after source upserts.
3. Consumer lag is near zero after processing windows.

### 8.2 Database checks

Use MySQL checks:

```sql
SELECT COUNT(*) FROM sentinel_landing.telemetry_ingest_events WHERE DATE(ingested_at)=CURDATE();
SELECT COUNT(*) FROM sentinel_landing.telemetry_facts WHERE DATE(trip_timestamp)=CURDATE();
SELECT COUNT(*) FROM sentinel_landing.vehicles_cdc_events WHERE DATE(ingested_at)=CURDATE();
```

Distinct source vehicles check:

```sql
SELECT COUNT(DISTINCT vin) FROM sentinel_db.vehicles_sentinel;
```

## 9) Known gotchas in this project

1. Updating `.env` without recreating containers will not apply new env values.
2. Fresh consumer groups with `auto.offset.reset=earliest` replay backlog and can inflate counts.
3. Source table `vehicles_sentinel` is upsert-by-VIN; event count is not equal to distinct row count.
4. Current demo fleet has 4 VINs by default unless simulator is changed.

## 10) Reward-program quick run

If you need one clean demo run quickly:
1. Use a new consumer group suffix per run (already done in DAG runtime functions).
2. Keep topic names fixed as above.
3. Verify these success points:
   - Debezium connector `RUNNING`
   - `telemetry.raw` message growth
   - `cdc_prod.sentinel_db.vehicles_sentinel` message growth
   - landing table row growth in MySQL

## 11) Security cleanup after program deadline

1. Rotate Kafka API key/secret.
2. Rotate Schema Registry API key/secret.
3. Replace local `.env` secrets with placeholders if sharing repo.
4. Restrict ACLs to exact topic/group names used in production.
