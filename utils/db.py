import os
import mysql.connector


def get_landing_connection_config():
    """Get normalized connection config for sentinel_landing schema."""
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_DB_USER", "root"),
        "password": os.getenv("MYSQL_DB_PASSWORD", "root"),
        "database": os.getenv("MYSQL_LANDING_SCHEMA", "sentinel_landing"),
    }


def get_landing_connection():
    """Get MySQL connection to sentinel_landing schema."""
    config = get_landing_connection_config()
    return mysql.connector.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
    )


def get_sentinel_analytics_connection():
    """Get MySQL connection to sentinel_analytics database (star schema)."""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_DB_USER", "root"),
        password=os.getenv("MYSQL_DB_PASSWORD", "root"),
        database=os.getenv("MYSQL_ANALYTICS_SCHEMA", "sentinel_analytics"),
    )


def get_admin_connection(database=None):
    """Get MySQL connection with admin privileges for migrations."""
    admin_host = os.getenv("MYSQL_ADMIN_HOST") or os.getenv("MYSQL_HOST") or "localhost"
    admin_port = int(os.getenv("MYSQL_ADMIN_PORT") or os.getenv("MYSQL_PORT") or "3306")

    return mysql.connector.connect(
        host=admin_host,
        port=admin_port,
        user=os.getenv("MYSQL_ADMIN_USER", "root"),
        password=os.getenv("MYSQL_ADMIN_PASSWORD", "root"),
        database=database,
    )
