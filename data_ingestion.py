import sys
import os
from datetime import datetime
import shutil
from typing import Dict, Any
import copy
import re

from utils.logger import Logger
from utils.spark_session import create_spark_session, stop_spark_session
from utils.config_validation import ConfigValidator
from source_reader.csv_reader import read_csv
from source_reader.mysql_reader import read_mysql
from source_reader.mongodb_reader import read_mongodb
from source_reader.postgres_reader import read_postgres
from source_reader.json_reader import read_json
from source_reader.api_reader import read_api

class DynamicTransformationEngine:
    def __init__(self):
        self.logger = Logger()
        self.spark = None
        self.config = None
    
    def initialize(self, config_path: str) -> None:
        """Initialize the transformation engine."""
        try:
            # Load and validate configuration
            self.config = ConfigValidator.load_config(config_path)
            
            # Create Spark session
            self.spark = create_spark_session()
            
        except Exception as e:
            self.logger.exception(f"Initialization failed: {str(e)}")
            raise
    
    def read_source(self, source_type: str, source_config: Dict[str, Any]) -> Any:
        """Read data from the specified source."""
        readers = {
            'csv': read_csv,
            'mysql': read_mysql,
            'mongodb': read_mongodb,
            'postgres': read_postgres,
            'json': read_json
            , 'api': read_api
        }
        
        reader = readers.get(source_type.lower())
        if not reader:
            raise ValueError(f"Unsupported source type: {source_type}")
            
        return reader(self.spark, source_config, self.logger)
    
    def apply_transformations(self, df, transformations: Dict[str, Any]) -> Any:
        """Apply configured transformations to the DataFrame."""
        if transformations.get("duplicate_handling", {}).get("enabled", False):
            subset = transformations["duplicate_handling"].get("subset", [])
            keep = transformations["duplicate_handling"].get("keep", "first")
            
            if subset:
                df = df.dropDuplicates(subset=subset)
            else:
                df = df.dropDuplicates()
        
        return df
    
    def write_output(self, df, source_type: str, filename: str, dest_config: Dict[str, Any] = None) -> str:
        """Write DataFrame to the destination.

        dest_config may be passed (selected by caller). If not provided, the engine
        will pick a sensible default from the loaded configuration (prefer 'local').
        """
        # Resolve destination config if not passed
        if dest_config is None:
            dest_config = self._select_destination_config(None)

        if not isinstance(dest_config, dict):
            raise ValueError("Resolved destination config is not a mapping")

        base_path = dest_config.get("base_path")
        format_type = dest_config.get("format")
        mode = dest_config.get("mode")
        
        # Create output path
        timestamp = datetime.now().strftime(dest_config["timestamp_format"])
        output_dir = os.path.join(base_path, "bronze", source_type)
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output filename
        output_filename = dest_config["naming_pattern"].format(
            filename=filename,
            current_date=timestamp,
            format=format_type
        )
        output_path = os.path.join(output_dir, output_filename)
        
        # Write as single file
        df.coalesce(1).write.mode(mode).parquet(output_path + "_temp")
        
        # Move the single parquet file to final destination
        temp_dir = output_path + "_temp"
        part_file = None
        for file in os.listdir(temp_dir):
            if file.startswith("part-") and file.endswith(".parquet"):
                part_file = os.path.join(temp_dir, file)
                break
        
        if part_file:
            shutil.move(part_file, output_path)
        
        # Cleanup
        shutil.rmtree(temp_dir)
        
        return output_path

    def _select_destination_config(self, destination: str | None) -> Dict[str, Any]:
        """Select a destination configuration dict from the top-level config.

        If `destination` is provided it will try to find the matching entry by 'type'.
        If not provided, will prefer an entry with type 'local' or fall back to the first entry.
        """
        dests = self.config.get("destination")
        if isinstance(dests, dict):
            return dests
        if isinstance(dests, list):
            if destination:
                for d in dests:
                    if isinstance(d, dict) and d.get("type") == destination:
                        return d
            # prefer local
            for d in dests:
                if isinstance(d, dict) and d.get("type") == "local":
                    return d
            # fallback to first valid mapping
            for d in dests:
                if isinstance(d, dict):
                    return d

        raise ValueError("No valid destination configuration found in config file")
    
    def process(self, source_type: str, input_config: str | dict, destination: str = None) -> None:
        """Process the data according to configuration."""
        try:
            # Find matching source configuration
            source_config = next(
                (s for s in self.config["sources"] if s["type"] == source_type and s["enabled"]),
                None
            )
            
            if not source_config:
                raise ValueError(f"No enabled configuration found for source type: {source_type}")

            # Deep copy source config to avoid modifying original
            source_config = dict(source_config)
            
            # For API, handle input as config dict that can override/extend source config
            if source_type == "api" and isinstance(input_config, dict):
                if "params" in input_config:
                    source_config["params"] = {
                        **(source_config.get("params", {}) or {}),
                        **input_config["params"]
                    }
                queries = ["api"]  # Just process once for API
            else:
                # Support multiple queries for SQL sources (comma-separated)
                if source_type == "csv":
                    queries = [input_config]
                elif source_type in ("mysql", "postgres", "mongodb"):
                    # split by comma so user can pass multiple queries/collections
                    queries = [q.strip() for q in input_config.split(',') if q.strip()]
                else:
                    queries = [input_config]

            # Process each input (for SQL sources this will be each query)
            for idx, q in enumerate(queries):
                # Only print header for first file in a group
                if idx == 0:
                    self.logger.print_header(
                        self.config["project"],
                        "BRONZE LAYER (Data Ingestion)",
                        source_type.upper(),
                        q
                    )

                # Work on a shallow copy of source_config to avoid mutation across iterations
                scfg = copy.deepcopy(source_config)
                if source_type == "csv":
                    scfg["path_list"] = [q]
                elif source_type in ("mysql", "postgres"):
                    scfg["query"] = q.rstrip(';')
                else:
                    # json, mongodb, etc: treat q as input path
                    if source_type == "json":
                        scfg["path_list"] = [q]
                    if source_type == "mongodb":
                        # Accept formats like: "db.collection.find({})", "db.collection", or "collection"
                        # strip any .find(...) suffix
                        clean = re.sub(r"\.find\s*\(.*\)\s*$", "", q, flags=re.IGNORECASE).strip()
                        scfg["collection"] = clean

                # Read source data
                start_time = datetime.now()
                try:
                    df = self.read_source(source_type, scfg)
                except Exception as e:
                    # Log full traceback to help diagnose downstream errors (e.g. JSON parsing issues)
                    self.logger.exception(f"Source read failed for '{q}': {e}")
                    # continue to next query instead of aborting all
                    continue

                # Log source reading stats
                source_stats = {
                    "Status": "✅ SUCCESS",
                    "Total Records Read": f"{df.count():,}",
                    "Columns Detected": str(len(df.columns)),
                    "Schema Inferred": "True"
                }
                self.logger.print_section("STEP 1: SOURCE READING", source_stats)

                # Apply transformations
                initial_count = df.count()
                df = self.apply_transformations(df, self.config["data_transformations"])
                final_count = df.count()

                # Log transformation stats
                transform_stats = {
                    "Duplicate Handling": "Enabled (Keep = First)",
                    "Records Removed": str(initial_count - final_count),
                    "Final Record Count": f"{final_count:,}"
                }
                self.logger.print_section("STEP 2: TRANSFORMATION", transform_stats)

                # Write output for this query/input
                # Determine a safe filename for the target based on source type and this iteration's config (`scfg`).
                if source_type == "csv":
                    filename = os.path.splitext(os.path.basename(q))[0]
                elif source_type in ("mysql", "postgres"):
                    # Prefer scfg's query (set per-iteration) then fallback to q
                    query_text = scfg.get("query", "") or q
                    m = re.search(r"from\s+[`\"']?([\w\.]+)[`\"']?", query_text, re.IGNORECASE)
                    if m:
                        table = m.group(1).split('.')[-1]
                        table = re.sub(r"[^A-Za-z0-9_\-]", "_", table)
                        filename = table
                    else:
                        filename = source_type
                elif source_type == "mongodb":
                    # Use only the collection name (not db.collection) for filename
                    coll = scfg.get("collection", source_type)
                    if isinstance(coll, str):
                        coll_name = coll.split('.')[-1]
                        coll_name = re.sub(r"[^A-Za-z0-9_\-]", "_", coll_name)
                        filename = coll_name
                    else:
                        filename = source_type
                elif source_type == "api":
                    # Use configured name for API sources when available
                    api_name = scfg.get("name") or scfg.get("id") or "api"
                    api_name = re.sub(r"[^A-Za-z0-9_\-]", "_", str(api_name))
                    filename = api_name
                else:
                    filename = os.path.splitext(os.path.basename(q))[0]

                output_path = self.write_output(df, source_type, filename, dest_config=self._select_destination_config(destination))

                # Log target writing stats for this iteration
                target_stats = {
                    "Output Path": output_path,
                    "Write Mode": "overwrite",
                    "Output Files": "1 (single parquet)",
                    "Compression": "snappy"
                }
                self.logger.print_section("STEP 3: TARGET WRITING", target_stats)

                # Log summary
                reduction_pct = ((initial_count - final_count) / initial_count * 100) if initial_count > 0 else 0
                summary = {
                    "✅ PIPELINE STATUS": "SUCCESS",
                    "📦 FINAL OUTPUT": output_path,
                    "⏱️  TOTAL DURATION": str(datetime.now() - start_time).split('.')[0]
                }
                self.logger.print_summary(summary)
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            raise
        
    def cleanup(self) -> None:
        """Clean up resources."""
        if self.spark:
            stop_spark_session(self.spark)

def main():
    # New behavior: accept a single JSON jobs file as the only argument.
    # Usage: python data_ingestion.py params.json
    if len(sys.argv) != 2:
        print("Usage: python data_ingestion.py <jobs.json>")
        sys.exit(1)

    jobs_file = sys.argv[1]
    if not os.path.exists(jobs_file):
        print(f"Jobs file not found: {jobs_file}")
        sys.exit(2)

    # Load jobs
    import json
    try:
        with open(jobs_file, 'r', encoding='utf-8') as f:
            jobs_spec = json.load(f)
    except Exception as e:
        print(f"Failed to load jobs file: {e}")
        sys.exit(3)

    # Initialize engine once
    engine = DynamicTransformationEngine()
    try:
        engine.initialize("configs/source_config.yaml")
    except Exception as e:
        print(f"Engine initialization failed: {e}")
        sys.exit(4)

    logger = engine.logger
    successes = []
    failures = []

    def _process_job(source_type: str, input_value: str, target: str = None):
        """Helper to process a single job and optionally rename output if target requested."""
        try:
            # Header is now printed in process() method for the first file only
            start = datetime.now()
            engine.process(source_type, input_value, destination=target)
            successes.append({'source': source_type, 'input': input_value, 'target': target})

            # Log success in a single section
            duration = datetime.now() - start
            logger.print_section('PROCESSING COMPLETE', {
                'Status': 'Success',
                'Source': source_type.upper(),
                'Input': input_value,
                'Duration': f"{duration.total_seconds():.2f}s"
            })

        except Exception as e:
            # Log full traceback for job failures
            logger.exception(f"Job failed for source={source_type}, input={input_value}: {e}")
            failures.append({'source': source_type, 'input': input_value, 'error': str(e)})

    # Determine allowed destination types from config (mapping or list)
    dest_cfg = engine.config.get('destination', {})
    if isinstance(dest_cfg, dict):
        allowed_dest_list = [dest_cfg.get('type')]
    elif isinstance(dest_cfg, list):
        allowed_dest_list = [d.get('type') for d in dest_cfg if isinstance(d, dict) and 'type' in d]
    else:
        allowed_dest_list = []

    # Process CSV jobs
    for job in jobs_spec.get('csv_jobs', []) or []:
        if isinstance(job, str):
            path = job
            destination = None
        else:
            path = job.get('path')
            destination = job.get('destination')
        if not path:
            logger.error('Skipping CSV job with missing path')
            failures.append({'source': 'csv', 'input': str(job), 'error': 'missing path'})
            continue
        # Validate destination against allowed destination types (if provided)
        if destination and destination not in allowed_dest_list:
            logger.error(f"Skipping CSV job because destination '{destination}' is not in allowed destinations: {allowed_dest_list}")
            failures.append({'source': 'csv', 'input': path, 'error': f"invalid destination '{destination}'"})
            continue

        _process_job('csv', path, None)

    # Process JSON jobs
    for job in jobs_spec.get('json_jobs', []) or []:
        if isinstance(job, str):
            path = job
            destination = None
        else:
            path = job.get('path')
            destination = job.get('destination')
        if not path:
            logger.error('Skipping JSON job with missing path')
            failures.append({'source': 'json', 'input': str(job), 'error': 'missing path'})
            continue

        # Validate destination against allowed destination types (if provided)
        if destination and destination not in allowed_dest_list:
            logger.error(f"Skipping JSON job because destination '{destination}' is not in allowed destinations: {allowed_dest_list}")
            failures.append({'source': 'json', 'input': path, 'error': f"invalid destination '{destination}'"})
            continue

        _process_job('json', path, None)

    # Process MySQL jobs
    for job in jobs_spec.get('mysql_jobs', []) or []:
        # job can be string (table) or dict {table/query, target}
        destination = None
        if isinstance(job, str):
            table = job
            query = f"select * from {table}"
        else:
            table = job.get('table')
            query = job.get('query') or (f"select * from {table}" if table else None)
            destination = job.get('destination')

        if not query:
            logger.error('Skipping MySQL job with missing table/query')
            failures.append({'source': 'mysql', 'input': str(job), 'error': 'missing table/query'})
            continue

        # Process this MySQL job
        _process_job('mysql', query, destination)

    # Process API jobs
    for job in jobs_spec.get('api_jobs', []) or []:
        # job can be dict with optional 'input' (ignored) and 'target' (output name)
        if isinstance(job, str):
            input_val = job
            destination = None
        else:
            input_val = job.get('input', 'ignore')
            destination = job.get('destination') or job.get('name')

        _process_job('api', input_val, destination)

    # Summary (use a concise summary output instead of printing a full header labelled SUMMARY)
    logger.print_summary({
        'PROJECT': engine.config.get('project', ''),
        'PIPELINE STAGE': 'BRONZE LAYER (Data Ingestion)',
        'successful_jobs': len(successes),
        'failed_jobs': len(failures)
    })

    if failures:
        logger.print_section('FAILURES', {str(i+1): f for i, f in enumerate(failures)})
        sys.exit(5)

if __name__ == "__main__":
    main()