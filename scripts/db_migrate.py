import os
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv
from utils.logger import get_logger


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = PROJECT_ROOT / "db" / "migrations"


def get_admin_connection(database=None):
    admin_host = os.getenv("MYSQL_ADMIN_HOST") or os.getenv("MYSQL_HOST") or "localhost"
    admin_port = int(os.getenv("MYSQL_ADMIN_PORT") or os.getenv("MYSQL_PORT") or "3306")

    return mysql.connector.connect(
        host=admin_host,
        port=admin_port,
        user=os.getenv("MYSQL_ADMIN_USER", "root"),
        password=os.getenv("MYSQL_ADMIN_PASSWORD", "root"),
        database=database,
    )


def ensure_migration_table():
    connection = get_admin_connection()
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS sentinel_admin")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sentinel_admin.schema_migrations (
            version VARCHAR(64) PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.commit()
    cursor.close()
    connection.close()


def fetch_applied_versions():
    connection = get_admin_connection(database="sentinel_admin")
    cursor = connection.cursor()
    cursor.execute("SELECT version FROM schema_migrations")
    versions = {row[0] for row in cursor.fetchall()}
    cursor.close()
    connection.close()
    return versions


def apply_migration(version, sql_text):
    connection = get_admin_connection()
    cursor = connection.cursor()

    for statement in [part.strip() for part in sql_text.split(";") if part.strip()]:
        cursor.execute(statement)

    connection.commit()
    cursor.close()
    connection.close()

    connection = get_admin_connection(database="sentinel_admin")
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO schema_migrations (version)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE applied_at = CURRENT_TIMESTAMP
        """,
        (version,),
    )
    connection.commit()
    cursor.close()
    connection.close()


def run_migrations():
    load_dotenv(PROJECT_ROOT / ".env")
    logger = get_logger("DB_MIGRATE")
    ensure_migration_table()
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not migration_files:
        logger.info("No migration files found.")
        return

    for migration_file in migration_files:
        version = migration_file.name
        logger.info(f"Applying migration version={version} status=reapply_idempotent")
        sql_text = migration_file.read_text(encoding="utf-8")
        try:
            apply_migration(version, sql_text)
            logger.info(f"Applied migration version={version} status=success")
        except Exception as exc:
            logger.error(f"Failed migration version={version} error={exc}")
            raise

    logger.info("Migration run complete.")


if __name__ == "__main__":
    run_migrations()
