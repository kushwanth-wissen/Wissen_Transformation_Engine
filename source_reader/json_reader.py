from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import os

def read_json(spark: SparkSession, config: Dict[str, Any], logger) -> DataFrame:
    """
    Reads JSON files based on configuration.
    
    Args:
        spark: SparkSession
        config: Source configuration dictionary
        logger: Logger instance
    
    Returns:
        DataFrame: Combined DataFrame from all JSON files
    """
    path_list = config.get("path_list", [])
    multiline = config.get("multiline", True)
    
    if not path_list:
        raise ValueError("No JSON files specified in path_list")
    
    # Validate file existence
    valid_paths = []
    for path in path_list:
        if os.path.exists(path):
            valid_paths.append(path)
        else:
            logger.warning(f"File not found: {path}")
    
    if not valid_paths:
        raise FileNotFoundError("No valid JSON files found")
    
    # Read and combine all JSON files
    df = None
    for path in valid_paths:
        current_df = spark.read \
                         .option("multiline", multiline) \
                         .json(path)
        
        if df is None:
            df = current_df
        else:
            # Combine DataFrames if schemas match
            if df.schema == current_df.schema:
                df = df.unionByName(current_df)
            else:
                logger.warning(f"Schema mismatch for file {path}, skipping...")
    
    return df