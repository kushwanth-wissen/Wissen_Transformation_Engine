from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import os

def read_csv(spark: SparkSession, config: Dict[str, Any], logger) -> DataFrame:
    """
    Reads CSV files based on configuration.
    
    Args:
        spark: SparkSession
        config: Source configuration dictionary
        logger: Logger instance
    
    Returns:
        DataFrame: Combined DataFrame from all CSV files
    """
    path_list = config.get("path_list", [])
    header = config.get("header", True)
    infer_schema = config.get("infer_schema", True)
    
    if not path_list:
        raise ValueError("No CSV files specified in path_list")
    
    # Validate file existence
    valid_paths = []
    for path in path_list:
        if os.path.exists(path):
            valid_paths.append(path)
        else:
            logger.warning(f"File not found: {path}")
    
    if not valid_paths:
        raise FileNotFoundError("No valid CSV files found")
    
    # Read and combine all CSV files
    df = None
    for path in valid_paths:
        current_df = spark.read.option("header", header) \
                              .option("inferSchema", infer_schema) \
                              .csv(path)
        if target_df is not None:
        # Merge: keep latest version or just avoid duplicates
            merged_df = current_df.join(target_df,on="employee_id",  # Primary key
                        how="left_anti").union(target_df)
        else:
            merged_df = current_df
        if df is None:
            df = current_df
        else:
            # Combine DataFrames if schemas match
            if df.schema == current_df.schema:
                df = df.unionByName(current_df)
            else:
                logger.warning(f"Schema mismatch for file {path}, skipping...")
    
    return df