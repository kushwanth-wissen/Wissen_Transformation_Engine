from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame

def read_postgres(spark: SparkSession, config: Dict[str, Any], logger) -> DataFrame:
    """
    Reads data from PostgreSQL using JDBC.
    
    Args:
        spark: SparkSession
        config: Source configuration dictionary
        logger: Logger instance
    
    Returns:
        DataFrame: Data from PostgreSQL query
    """
    credentials_key = config.get("credentials_key")
    query = config.get("query")
    
    if not credentials_key or not query:
        raise ValueError("Missing credentials_key or query in PostgreSQL configuration")
    
    # These should come from a secure credential store in production
    jdbc_url = f"jdbc:postgresql://localhost:5432/database"
    connection_properties = {
        "driver": "org.postgresql.Driver",
        "user": "username",
        "password": "password"
    }
    
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) as tmp",
            properties=connection_properties
        )
        return df
        
    except Exception as e:
        logger.error(f"Error reading from PostgreSQL: {str(e)}")
        raise