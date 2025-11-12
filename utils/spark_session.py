from pyspark.sql import SparkSession
import os

def create_spark_session(app_name="DynamicTransformationEngine"):
    """
    Creates and returns a SparkSession with configurations for the transformation engine.
    
    Args:
        app_name (str): Name of the Spark application
    
    Returns:
        SparkSession: Configured SparkSession object
    """
    # Initialize builder
    builder = SparkSession.builder.appName(app_name)
    
    # Add configurations
    builder = builder.config("spark.driver.memory", "2g") \
                    .config("spark.executor.memory", "2g") \
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Load external JARs if they exist
    libs_dir = "libs"
    if os.path.exists(libs_dir):
        jars = [os.path.join(libs_dir, jar) for jar in os.listdir(libs_dir) if jar.endswith('.jar')]
        if jars:
            builder = builder.config("spark.jars", ",".join(jars))
    
    # Create session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def stop_spark_session(spark):
    """
    Safely stops the SparkSession.
    
    Args:
        spark (SparkSession): The SparkSession to stop
    """
    if spark:
        spark.stop()