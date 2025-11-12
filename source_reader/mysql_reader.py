from typing import Dict, Any
import os
from pyspark.sql import SparkSession, DataFrame


def _load_env_file(env_path: str) -> None:
    """Simple loader for key=value lines into os.environ (no external deps).

    Existing environment variables are preserved.
    """
    if not env_path:
        return
    if not os.path.exists(env_path):
        return
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def read_mysql(spark: SparkSession, config: Dict[str, Any], logger) -> DataFrame:
    """
    Reads data from MySQL using JDBC.
    
    Args:
        spark: SparkSession
        config: Source configuration dictionary
        logger: Logger instance
    
    Returns:
        DataFrame: Data from MySQL query
    """
    # config may provide either credentials_env (path to .env) or credentials_key (legacy)
    credentials_env = config.get("credentials_env")
    # credentials_key = config.get("credentials_key")
    query = config.get("query")

    if not query:
        raise ValueError("Missing 'query' in MySQL configuration")

    # Load env file if provided (this populates os.environ for the connection)
    if credentials_env:
        _load_env_file(credentials_env)

    # Read connection settings from environment (fall back to defaults if missing)
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    database = os.environ.get("MYSQL_DB", "database")
    user = os.environ.get("MYSQL_USER", "username")
    password = os.environ.get("MYSQL_PASSWORD", "password")

    jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
    connection_properties = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": user,
        "password": password
    }
    
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) as tmp",
            properties=connection_properties
        )
        return df
        
    except Exception as e:
        logger.error(f"Error reading from MySQL: {str(e)}")
        raise