from typing import Dict, Any
import os
import re
from pyspark.sql import SparkSession, DataFrame


def _load_env_file(env_path: str) -> None:
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


def read_mongodb(spark: SparkSession, config: Dict[str, Any], logger) -> DataFrame:
    """
    Reads data from MongoDB using the MongoDB Spark Connector.
    
    Args:
        spark: SparkSession
        config: Source configuration dictionary
        logger: Logger instance
    
    Returns:
        DataFrame: Data from MongoDB collection
    """
    # Accept credentials_env (path to .env) or credentials_key (legacy)
    credentials_env = config.get("credentials_env")
    credentials_key = config.get("credentials_key")
    dynamic_schema = config.get("dynamic_schema", True)

    # Load env file if provided
    if credentials_env:
        _load_env_file(credentials_env)

    # Determine connection info from environment or defaults
    mongo_uri = os.environ.get("MONGO_URI")
    host = os.environ.get("MONGO_HOST", "localhost")
    port = os.environ.get("MONGO_PORT", "27017")
    database = os.environ.get("MONGO_DB", None)

    # collection may be provided in config or supplied via CLI (config may be per-iteration)
    collection = config.get("collection")

    # If collection is provided as 'db.collection' parse it
    if collection and '.' in collection:
        parts = collection.split('.', 1)
        database = parts[0]
        collection = parts[1]

    # Build URI if not provided
    if not mongo_uri:
        mongo_uri = f"mongodb://{host}:{port}"

    if not collection:
        raise ValueError("Missing collection in MongoDB configuration or CLI input")

    # First attempt: use the MongoDB Spark connector if available
    try:
        reader = spark.read.format("mongodb").option("uri", mongo_uri)
        if database:
            reader = reader.option("database", database)
        reader = reader.option("collection", collection).option("sampleSize", 1000 if dynamic_schema else 0)
        df = reader.load()
        return df
    except Exception as e:
        # If the MongoDB Spark connector isn't available on the classpath, fall back to pymongo
        msg = str(e)
        logger.warning(f"Mongo Spark connector read failed, falling back to pymongo: {msg}")

    # Fallback: use pymongo to read documents and convert to a Spark DataFrame
    try:
        import pymongo

        # Build a simple filter: if user passed a .find(...) expression we don't parse the predicate — read all docs
        client = None
        # If MONGO_URI contains credentials, use it; otherwise build client from host/port
        if mongo_uri and mongo_uri.startswith("mongodb"):
            client = pymongo.MongoClient(mongo_uri)
        else:
            client = pymongo.MongoClient(host=host, port=int(port))

        dbname = database if database else os.environ.get("MONGO_DB")
        if not dbname:
            raise ValueError("MongoDB database not set (set MONGO_DB in env or provide db.collection in input)")

        db = client[dbname]
        coll = db[collection]
        docs = list(coll.find({}))

        # Remove MongoDB ObjectId objects by converting to strings
        def sanitize(doc):
            out = {}
            for k, v in doc.items():
                try:
                    # ObjectId has __str__
                    if hasattr(v, "__class__") and v.__class__.__name__ == 'ObjectId':
                        out[k] = str(v)
                    else:
                        out[k] = v
                except Exception:
                    out[k] = v
            return out

        rows = [sanitize(d) for d in docs]
        if not rows:
            # create empty df with no rows
            return spark.createDataFrame([], schema=None)

        df = spark.createDataFrame(rows)
        return df
    except Exception as e:
        logger.error(f"Error reading from MongoDB via pymongo fallback: {str(e)}")
        raise