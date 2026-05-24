import os
import mysql.connector


def env_value_or_default(key, default):
    """Return env value when non-empty; otherwise fallback to default."""
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return value


def get_landing_connection_config():
    """Get normalized connection config for sentinel_landing schema."""
    return {
        "host": env_value_or_default("MYSQL_HOST", "localhost"),
        "port": int(env_value_or_default("MYSQL_PORT", "3306")),
        "user": env_value_or_default("MYSQL_DB_USER", "root"),
        "password": env_value_or_default("MYSQL_DB_PASSWORD", "root"),
        "database": env_value_or_default("MYSQL_LANDING_SCHEMA", "sentinel_landing"),
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
        host=env_value_or_default("MYSQL_HOST", "localhost"),
        port=int(env_value_or_default("MYSQL_PORT", "3306")),
        user=env_value_or_default("MYSQL_DB_USER", "root"),
        password=env_value_or_default("MYSQL_DB_PASSWORD", "root"),
        database=env_value_or_default("MYSQL_ANALYTICS_SCHEMA", "sentinel_analytics"),
    )


def get_admin_connection(database=None):
    """Get MySQL connection with admin privileges for migrations."""
    admin_host = env_value_or_default("MYSQL_ADMIN_HOST", env_value_or_default("MYSQL_HOST", "localhost"))
    admin_port = int(env_value_or_default("MYSQL_ADMIN_PORT", env_value_or_default("MYSQL_PORT", "3306")))

    return mysql.connector.connect(
        host=admin_host,
        port=admin_port,
        user=env_value_or_default("MYSQL_ADMIN_USER", env_value_or_default("MYSQL_DB_USER", "root")),
        password=env_value_or_default("MYSQL_ADMIN_PASSWORD", env_value_or_default("MYSQL_DB_PASSWORD", "root")),
        database=database,
    )
