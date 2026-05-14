import os

from utils.logger import get_logger
from utils.db import get_sentinel_analytics_connection


logger = get_logger("SENTINEL_STAR_SCHEMA")


def table_has_column(cursor, schema_name, table_name, column_name):
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (schema_name, table_name, column_name),
    )
    return cursor.fetchone()[0] > 0


def populate_dim_vehicle(cursor):
    logger.info("step=dim_vehicle status=start")
    has_owner_name = table_has_column(cursor, "sentinel_analytics", "dim_vehicle", "owner_name")

    if has_owner_name:
        cursor.execute(
            """
            INSERT INTO sentinel_analytics.dim_vehicle
                (vin, owner_name, manufacturer, model, model_year, vehicle_status, first_seen_at, last_seen_at, source_updated_at)
            SELECT
                v.vin,
                v.owner_name,
                v.manufacturer,
                v.vehicle_model AS model,
                v.manufacture_year AS model_year,
                v.status AS vehicle_status,
                v.updated_at AS first_seen_at,
                v.updated_at AS last_seen_at,
                v.updated_at AS source_updated_at
            FROM sentinel_landing.vehicles v
            ON DUPLICATE KEY UPDATE
                model = VALUES(model),
                model_year = VALUES(model_year),
                vehicle_status = VALUES(vehicle_status),
                last_seen_at = VALUES(last_seen_at),
                source_updated_at = VALUES(source_updated_at)
            """
        )
    else:
        cursor.execute(
            """
            INSERT INTO sentinel_analytics.dim_vehicle
                (vin, manufacturer, model, model_year, vehicle_status, first_seen_at, last_seen_at, source_updated_at)
            SELECT
                v.vin,
                v.manufacturer,
                v.vehicle_model AS model,
                v.manufacture_year AS model_year,
                v.status AS vehicle_status,
                v.updated_at AS first_seen_at,
                v.updated_at AS last_seen_at,
                v.updated_at AS source_updated_at
            FROM sentinel_landing.vehicles v
            ON DUPLICATE KEY UPDATE
                model = VALUES(model),
                model_year = VALUES(model_year),
                vehicle_status = VALUES(vehicle_status),
                last_seen_at = VALUES(last_seen_at),
                source_updated_at = VALUES(source_updated_at)
            """
        )
    logger.info(f"step=dim_vehicle status=done affected_rows={cursor.rowcount}")


def populate_dim_time(cursor):
    logger.info("step=dim_time status=start")
    cursor.execute(
        """
        INSERT INTO sentinel_analytics.dim_time
            (event_ts, event_date, year, quarter, month, day, hour, minute, day_of_week, is_weekend)
        SELECT
            t.event_ts,
            DATE(t.event_ts) AS event_date,
            YEAR(t.event_ts) AS year,
            QUARTER(t.event_ts) AS quarter,
            MONTH(t.event_ts) AS month,
            DAY(t.event_ts) AS day,
            HOUR(t.event_ts) AS hour,
            MINUTE(t.event_ts) AS minute,
            DAYOFWEEK(t.event_ts) - 1 AS day_of_week,
            IF(DAYOFWEEK(t.event_ts) IN (1, 7), 1, 0) AS is_weekend
        FROM (
            SELECT DISTINCT tf.trip_timestamp AS event_ts
            FROM sentinel_landing.telemetry_facts tf
            WHERE tf.trip_timestamp IS NOT NULL
            UNION
            SELECT DISTINCT sac.event_timestamp AS event_ts
            FROM sentinel_curation.sentinel_ai_curation sac
            WHERE sac.event_timestamp IS NOT NULL
        ) t
        ON DUPLICATE KEY UPDATE
            event_date = VALUES(event_date),
            year = VALUES(year),
            quarter = VALUES(quarter),
            month = VALUES(month),
            day = VALUES(day),
            hour = VALUES(hour),
            minute = VALUES(minute),
            day_of_week = VALUES(day_of_week),
            is_weekend = VALUES(is_weekend)
        """
    )
    logger.info(f"step=dim_time status=done affected_rows={cursor.rowcount}")


def populate_dim_diagnostic_type(cursor):
    logger.info("step=dim_diagnostic_type status=start")
    cursor.execute(
        """
        INSERT INTO sentinel_analytics.dim_diagnostic_type
            (diagnostic_code, diagnostic_name, severity)
        SELECT
            COALESCE(sac.correlation_id, CONCAT('NO-CORR-', sac.id)) AS diagnostic_code,
            LEFT(COALESCE(sac.diagnostic_summary, 'Unknown'), 128) AS diagnostic_name,
            CASE
                WHEN sac.technical_anomaly_detected = 1 THEN 'high'
                ELSE 'info'
            END AS severity
        FROM sentinel_curation.sentinel_ai_curation sac
        ON DUPLICATE KEY UPDATE
            diagnostic_name = VALUES(diagnostic_name),
            severity = VALUES(severity)
        """
    )
    logger.info(f"step=dim_diagnostic_type status=done affected_rows={cursor.rowcount}")


def populate_fct_vehicle_telemetry(cursor):
    logger.info("step=fct_vehicle_telemetry status=start")
    has_file_id = table_has_column(cursor, "sentinel_analytics", "fct_vehicle_telemetry", "file_id")

    if has_file_id:
        cursor.execute(
            """
            INSERT INTO sentinel_analytics.fct_vehicle_telemetry
                (vehicle_key, time_key, diagnostic_type_key, file_id, file_name, correlation_id,
                 avg_engine_load, peak_temp_c, trip_distance_km, trip_duration_seconds, source_event_ts, ingested_at)
            SELECT
                dv.vehicle_key,
                dt.time_key,
                ddt.diagnostic_type_key,
                tf.file_id,
                tf.file_name,
                tf.correlation_id,
                tf.avg_engine_load,
                tf.peak_temp_c,
                tf.trip_distance_km,
                tf.trip_duration_seconds,
                tf.trip_timestamp AS source_event_ts,
                CURRENT_TIMESTAMP AS ingested_at
            FROM sentinel_landing.telemetry_facts tf
            JOIN sentinel_analytics.dim_vehicle dv
                ON dv.vin = tf.vehicle_id
            JOIN sentinel_analytics.dim_time dt
                ON dt.event_ts = tf.trip_timestamp
            LEFT JOIN sentinel_analytics.dim_diagnostic_type ddt
                ON ddt.diagnostic_code = tf.correlation_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM sentinel_analytics.fct_vehicle_telemetry f
                WHERE f.vehicle_key = dv.vehicle_key
                  AND f.time_key = dt.time_key
                  AND (
                        (f.correlation_id = tf.correlation_id)
                        OR (f.correlation_id IS NULL AND tf.correlation_id IS NULL)
                      )
            )
            """
        )
    else:
        cursor.execute(
            """
            INSERT INTO sentinel_analytics.fct_vehicle_telemetry
                (vehicle_key, time_key, diagnostic_type_key, correlation_id, engine_temp,
                 battery_voltage, speed_kph, fuel_level_pct, odometer_km, source_event_ts, ingested_at)
            SELECT
                dv.vehicle_key,
                dt.time_key,
                ddt.diagnostic_type_key,
                tf.correlation_id,
                tf.peak_temp_c AS engine_temp,
                NULL AS battery_voltage,
                NULL AS speed_kph,
                NULL AS fuel_level_pct,
                NULL AS odometer_km,
                tf.trip_timestamp AS source_event_ts,
                CURRENT_TIMESTAMP AS ingested_at
            FROM sentinel_landing.telemetry_facts tf
            JOIN sentinel_analytics.dim_vehicle dv
                ON dv.vin = tf.vehicle_id
            JOIN sentinel_analytics.dim_time dt
                ON dt.event_ts = tf.trip_timestamp
            LEFT JOIN sentinel_analytics.dim_diagnostic_type ddt
                ON ddt.diagnostic_code = tf.correlation_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM sentinel_analytics.fct_vehicle_telemetry f
                WHERE f.vehicle_key = dv.vehicle_key
                  AND f.time_key = dt.time_key
                  AND (
                        (f.correlation_id = tf.correlation_id)
                        OR (f.correlation_id IS NULL AND tf.correlation_id IS NULL)
                      )
            )
            """
        )
    logger.info(f"step=fct_vehicle_telemetry status=done affected_rows={cursor.rowcount}")


def populate_fct_vehicle_health_event(cursor):
    logger.info("step=fct_vehicle_health_event status=start")
    cursor.execute(
        """
        INSERT INTO sentinel_analytics.fct_vehicle_health_event
            (vehicle_key, time_key, diagnostic_type_key, correlation_id, health_score, anomaly_flag,
             summary, recommendation, source_event_ts, ingested_at)
        SELECT
            dv.vehicle_key,
            dt.time_key,
            ddt.diagnostic_type_key,
            sac.correlation_id,
            NULL AS health_score,
            COALESCE(sac.technical_anomaly_detected, 0) AS anomaly_flag,
            LEFT(sac.diagnostic_summary, 255) AS summary,
            LEFT(sac.diagnostic_summary, 255) AS recommendation,
            sac.event_timestamp AS source_event_ts,
            CURRENT_TIMESTAMP AS ingested_at
        FROM sentinel_curation.sentinel_ai_curation sac
        JOIN sentinel_analytics.dim_vehicle dv
            ON dv.vin = sac.vehicle_id
        JOIN sentinel_analytics.dim_time dt
            ON dt.event_ts = sac.event_timestamp
        LEFT JOIN sentinel_analytics.dim_diagnostic_type ddt
            ON ddt.diagnostic_code = COALESCE(sac.correlation_id, CONCAT('NO-CORR-', sac.id))
        WHERE NOT EXISTS (
            SELECT 1
            FROM sentinel_analytics.fct_vehicle_health_event f
            WHERE f.vehicle_key = dv.vehicle_key
              AND f.time_key = dt.time_key
              AND (
                    (f.correlation_id = sac.correlation_id)
                    OR (f.correlation_id IS NULL AND sac.correlation_id IS NULL)
                  )
        )
        """
    )
    logger.info(f"step=fct_vehicle_health_event status=done affected_rows={cursor.rowcount}")


def run_population():
    logger.info("job=sentinel_star_schema status=start")
    db = get_sentinel_analytics_connection()
    cursor = db.cursor()

    try:
        populate_dim_vehicle(cursor)
        db.commit()

        populate_dim_time(cursor)
        db.commit()

        populate_dim_diagnostic_type(cursor)
        db.commit()

        populate_fct_vehicle_telemetry(cursor)
        db.commit()

        populate_fct_vehicle_health_event(cursor)
        db.commit()

        logger.info("job=sentinel_star_schema status=success")
    except Exception as exc:
        db.rollback()
        logger.error(f"job=sentinel_star_schema status=failed error={exc}")
        raise
    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    run_population()