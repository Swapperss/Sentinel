"""
Phase B: Curation Transform (PySpark)
Transforms landing data to canonical curation layer with AI enrichment.
Runs as a micro-batch every 10 minutes using Apache Spark.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, upper, now, lit, when, date_sub,
)
from utils.logger import get_logger
from utils.db import get_landing_connection_config


logger = get_logger("CURATION_TRANSFORM_SPARK")


def optional_column(df, column_name):
    """Return a DataFrame column if present, otherwise NULL for schema drift tolerance."""
    if column_name in df.columns:
        return col(column_name)
    return lit(None).alias(column_name)


def get_mysql_connector_jar_path():
    """Resolve MySQL JDBC connector JAR from known workspace locations."""
    repo_root = Path(__file__).resolve().parents[2]
    candidate_paths = [
        repo_root / "plugins" / "mysql-connector-j-9.7.0" / "mysql-connector-j-9.7.0" / "mysql-connector-j-9.7.0.jar",
        repo_root / "plugins" / "debezium-mysql" / "mysql-connector-j-9.1.0.jar",
    ]

    for candidate in candidate_paths:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError(
        "MySQL JDBC connector JAR not found in expected plugin paths. "
        "Ensure mysql-connector-j*.jar exists under plugins/."
    )


def get_spark_session():
    """Initialize PySpark session with MySQL JDBC driver."""
    mysql_jar = get_mysql_connector_jar_path()
    return SparkSession.builder \
        .appName("SentinelCurationTransform") \
        .config("spark.jars", mysql_jar) \
        .config("spark.driver.extraClassPath", mysql_jar) \
        .config("spark.executor.extraClassPath", mysql_jar) \
        .getOrCreate()


def get_mysql_connection_url(database):
    """Build JDBC connection URL for MySQL."""
    config = get_landing_connection_config()
    host = config["host"]
    port = config["port"]
    return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&serverTimezone=UTC"


def get_mysql_jdbc_options():
    """Build JDBC auth options with safe defaults for Spark reads/writes."""
    config = get_landing_connection_config()
    return {
        "user": config["user"],
        "password": config["password"],
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def transform_vehicles(spark):
    """Transform vehicles from landing snapshot to curation canonical form using Spark."""
    logger.info("step=transform_vehicles status=start")
    
    try:
        jdbc_auth = get_mysql_jdbc_options()
        # Read landing vehicles into DataFrame
        landing_url = get_mysql_connection_url("sentinel_landing")
        vehicles_df = spark.read \
            .format("jdbc") \
            .option("url", landing_url) \
            .option("dbtable", "vehicles") \
            .option("user", jdbc_auth["user"]) \
            .option("password", jdbc_auth["password"]) \
            .option("driver", jdbc_auth["driver"]) \
            .load()
        
        # Transform: normalize and deduplicate
        transformed_df = vehicles_df \
            .select(
                col("vin"),
                optional_column(vehicles_df, "owner_name").alias("owner_name"),
                optional_column(vehicles_df, "manufacturer").alias("manufacturer"),
                upper(col("vehicle_model")).alias("vehicle_model"),
                col("manufacture_year"),
                upper(col("status")).alias("status"),
                col("updated_at").alias("last_seen_at"),
                col("updated_at").alias("source_updated_at"),
                now().alias("created_at"),
                now().alias("updated_at")
            ) \
            .dropDuplicates(["vin"])
        
        # Write to curation (append mode for idempotency via ON DUPLICATE KEY)
        curation_url = get_mysql_connection_url("sentinel_curation")
        transformed_df.write \
            .format("jdbc") \
            .option("url", curation_url) \
            .option("dbtable", "vehicles") \
            .option("user", jdbc_auth["user"]) \
            .option("password", jdbc_auth["password"]) \
            .option("driver", jdbc_auth["driver"]) \
            .mode("overwrite") \
            .save()
        
        affected_rows = transformed_df.count()
        logger.info(f"step=transform_vehicles status=done affected_rows={affected_rows}")
        
    except Exception as e:
        logger.error(f"step=transform_vehicles status=failed error={str(e)}")
        raise


def transform_telemetry_facts(spark):
    """Transform telemetry from landing facts to curation canonical form using Spark."""
    logger.info("step=transform_telemetry_facts status=start")
    
    try:
        jdbc_auth = get_mysql_jdbc_options()
        # Read landing telemetry facts into DataFrame
        landing_url = get_mysql_connection_url("sentinel_landing")
        telemetry_df = spark.read \
            .format("jdbc") \
            .option("url", landing_url) \
            .option("dbtable", "telemetry_facts") \
            .option("user", jdbc_auth["user"]) \
            .option("password", jdbc_auth["password"]) \
            .option("driver", jdbc_auth["driver"]) \
            .load()
        
        # Filter for last 24 hours and transform
        transformed_df = telemetry_df \
            .filter(col("trip_timestamp") >= date_sub(now(), 1)) \
            .select(
                col("file_id"),
                optional_column(telemetry_df, "file_name").alias("file_name"),
                col("vehicle_id"),
                col("correlation_id"),
                col("trip_timestamp"),
                optional_column(telemetry_df, "avg_engine_load").alias("avg_engine_load"),
                col("peak_temp_c"),
                coalesce(optional_column(telemetry_df, "trip_distance_km"), lit(0)).alias("trip_distance_km"),
                coalesce(optional_column(telemetry_df, "trip_duration_seconds"), lit(0)).alias("trip_duration_seconds"),
                now().alias("created_at"),
                now().alias("updated_at")
            ) \
            .dropDuplicates(["file_id"])
        
        # Write to curation
        curation_url = get_mysql_connection_url("sentinel_curation")
        transformed_df.write \
            .format("jdbc") \
            .option("url", curation_url) \
            .option("dbtable", "telemetry_facts") \
            .option("user", jdbc_auth["user"]) \
            .option("password", jdbc_auth["password"]) \
            .option("driver", jdbc_auth["driver"]) \
            .mode("append") \
            .save()
        
        affected_rows = transformed_df.count()
        logger.info(f"step=transform_telemetry_facts status=done affected_rows={affected_rows}")
        
    except Exception as e:
        logger.error(f"step=transform_telemetry_facts status=failed error={str(e)}")
        raise


def enrich_with_ai_diagnostics(spark):
    """Create AI-enriched diagnostics from telemetry patterns using Spark."""
    logger.info("step=enrich_with_ai_diagnostics status=start")
    
    try:
        jdbc_auth = get_mysql_jdbc_options()
        # Read transformed telemetry facts
        curation_url = get_mysql_connection_url("sentinel_curation")
        telemetry_df = spark.read \
            .format("jdbc") \
            .option("url", curation_url) \
            .option("dbtable", "telemetry_facts") \
            .option("user", jdbc_auth["user"]) \
            .option("password", jdbc_auth["password"]) \
            .option("driver", jdbc_auth["driver"]) \
            .load()
        
        # Apply AI enrichment logic
        enriched_df = telemetry_df \
            .filter(col("trip_timestamp") >= date_sub(now(), 1)) \
            .select(
                col("vehicle_id"),
                col("correlation_id"),
                col("trip_timestamp").alias("event_timestamp"),
                when(col("peak_temp_c") > 95, lit("High engine temperature detected"))
                    .when(col("peak_temp_c") < 20, lit("Low engine temperature detected"))
                    .when(col("trip_distance_km") > 500, lit("Long distance trip recorded"))
                    .otherwise(lit("Normal operating conditions"))
                    .alias("diagnostic_summary"),
                when((col("peak_temp_c") > 95) | (col("peak_temp_c") < 20), lit(1))
                    .otherwise(lit(0))
                    .alias("technical_anomaly_detected")
            ) \
            .dropDuplicates(["vehicle_id", "correlation_id", "event_timestamp"])
        
        # Write to AI curation table
        enriched_df.write \
            .format("jdbc") \
            .option("url", curation_url) \
            .option("dbtable", "sentinel_ai_curation") \
            .option("user", jdbc_auth["user"]) \
            .option("password", jdbc_auth["password"]) \
            .option("driver", jdbc_auth["driver"]) \
            .mode("append") \
            .save()
        
        affected_rows = enriched_df.count()
        logger.info(f"step=enrich_with_ai_diagnostics status=done affected_rows={affected_rows}")
        
    except Exception as e:
        logger.error(f"step=enrich_with_ai_diagnostics status=failed error={str(e)}")
        raise


def run_curation():
    """Execute full curation transformation pipeline using PySpark."""
    logger.info("job=curation_transform status=start")
    
    spark = get_spark_session()
    
    try:
        transform_vehicles(spark)
        transform_telemetry_facts(spark)
        enrich_with_ai_diagnostics(spark)
        logger.info("job=curation_transform status=success")
    except Exception as exc:
        logger.error(f"job=curation_transform status=failed error={exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_curation()
