import argparse
import json
import os
import sys
from datetime import datetime

import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.logger import get_logger


logger = get_logger("VERIFY_MIGRATION")


def build_admin_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_ADMIN_HOST") or os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_ADMIN_PORT") or os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_ADMIN_USER") or os.getenv("MYSQL_DB_USER", "root"),
        password=os.getenv("MYSQL_ADMIN_PASSWORD") or os.getenv("MYSQL_DB_PASSWORD", "root"),
    )


def run_count_check(cursor, name, query, expected_min):
    cursor.execute(query)
    actual_count = int(cursor.fetchone()[0])
    status = "pass" if actual_count >= expected_min else "fail"
    result = {
        "check": name,
        "query": query,
        "expected_min": expected_min,
        "actual_count": actual_count,
        "status": status,
    }
    logger.info(
        "validation_check=%s status=%s expected_min=%s actual_count=%s",
        name,
        status,
        expected_min,
        actual_count,
    )
    return result


def build_checks(expected_vehicle_rows, expected_telemetry_rows, expected_ai_rows):
    return [
        (
            "landing_vehicles",
            "SELECT COUNT(*) FROM sentinel_landing.vehicles",
            expected_vehicle_rows,
        ),
        (
            "landing_vehicles_cdc_events",
            "SELECT COUNT(*) FROM sentinel_landing.vehicles_cdc_events",
            expected_vehicle_rows,
        ),
        (
            "landing_telemetry_facts_today",
            "SELECT COUNT(*) FROM sentinel_landing.telemetry_facts WHERE DATE(trip_timestamp)=CURDATE()",
            expected_telemetry_rows,
        ),
        (
            "landing_telemetry_ingest_events_today",
            "SELECT COUNT(*) FROM sentinel_landing.telemetry_ingest_events WHERE DATE(ingested_at)=CURDATE()",
            expected_telemetry_rows,
        ),
        (
            "curation_vehicles",
            "SELECT COUNT(*) FROM sentinel_curation.vehicles",
            expected_vehicle_rows,
        ),
        (
            "curation_telemetry_facts_today",
            "SELECT COUNT(*) FROM sentinel_curation.telemetry_facts WHERE DATE(trip_timestamp)=CURDATE()",
            expected_telemetry_rows,
        ),
        (
            "curation_sentinel_ai",
            "SELECT COUNT(*) FROM sentinel_curation.sentinel_ai_curation",
            expected_ai_rows,
        ),
        (
            "consumption_v_vehicles",
            "SELECT COUNT(*) FROM sentinel_consumption.v_vehicles",
            expected_vehicle_rows,
        ),
        (
            "consumption_v_telemetry_facts",
            "SELECT COUNT(*) FROM sentinel_consumption.v_telemetry_facts",
            expected_telemetry_rows,
        ),
        (
            "consumption_v_sentinel_ai_curation",
            "SELECT COUNT(*) FROM sentinel_consumption.v_sentinel_ai_curation",
            expected_ai_rows,
        ),
        (
            "analytics_dim_vehicle_today",
            "SELECT COUNT(*) FROM sentinel_analytics.dim_vehicle WHERE DATE(updated_at)=CURDATE()",
            expected_vehicle_rows,
        ),
        (
            "analytics_fct_vehicle_telemetry",
            "SELECT COUNT(*) FROM sentinel_analytics.fct_vehicle_telemetry",
            expected_telemetry_rows,
        ),
    ]


def write_reports(report, report_json_path=None, report_log_path=None):
    if report_json_path:
        with open(report_json_path, "w", encoding="utf-8") as report_json_file:
            json.dump(report, report_json_file, indent=2)
        logger.info("validation_report_json=%s", report_json_path)

    if report_log_path:
        with open(report_log_path, "w", encoding="utf-8") as report_log_file:
            report_log_file.write(f"result={report['result']}\n")
            report_log_file.write(f"generated_at_utc={report['generated_at_utc']}\n")
            for item in report["checks"]:
                report_log_file.write(
                    "check={check} status={status} expected_min={expected_min} actual_count={actual_count}\n".format(
                        check=item["check"],
                        status=item["status"],
                        expected_min=item["expected_min"],
                        actual_count=item["actual_count"],
                    )
                )
        logger.info("validation_report_log=%s", report_log_path)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Validate Sentinel end-to-end table counts")
    parser.add_argument("--expected-vehicle-rows", type=int, default=10)
    parser.add_argument("--expected-telemetry-rows", type=int, default=10)
    parser.add_argument("--expected-ai-rows", type=int, default=10)
    parser.add_argument("--report-json", default=None)
    parser.add_argument("--report-log", default=None)
    args = parser.parse_args()

    checks = build_checks(
        expected_vehicle_rows=max(1, int(args.expected_vehicle_rows)),
        expected_telemetry_rows=max(1, int(args.expected_telemetry_rows)),
        expected_ai_rows=max(1, int(args.expected_ai_rows)),
    )

    connection = build_admin_connection()
    cursor = connection.cursor()
    results = []
    failed_checks = []

    try:
        for check_name, query, expected_min in checks:
            result = run_count_check(cursor, check_name, query, expected_min)
            results.append(result)
            if result["status"] == "fail":
                failed_checks.append(result)
    finally:
        cursor.close()
        connection.close()

    report = {
        "result": "pass" if not failed_checks else "fail",
        "generated_at_utc": datetime.utcnow().isoformat(),
        "failed_count": len(failed_checks),
        "checks": results,
    }

    write_reports(report, report_json_path=args.report_json, report_log_path=args.report_log)

    if failed_checks:
        failed_names = [item["check"] for item in failed_checks]
        raise SystemExit(f"Validation failed for checks: {failed_names}")

    print("Validation passed for all checks.")


if __name__ == "__main__":
    main()
