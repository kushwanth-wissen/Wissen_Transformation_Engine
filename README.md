# Dynamic PySpark Transformation Engine

A configuration-driven, cloud-native data transformation engine using PySpark, supporting multiple data sources and the Medallion Architecture.

## 🌟 Features

- Configuration-driven transformations via YAML
- Multiple source support (CSV, JSON, MySQL, PostgreSQL, MongoDB)
- Medallion Architecture (Bronze → Silver → Gold)
- Dynamic command-line arguments
## Data Ingestion Runner

This repository includes a PySpark ingestion engine and a JSON-driven runner that processes multiple jobs in a single run.

Quick start
-----------
1. Edit `configs/source_config.yaml` to ensure destinations are defined (e.g. a `local` destination with `base_path: "data/output/"`).
2. Create a jobs file `params.json` with the job lists (see example below).
3. Run the runner:

```cmd
python data_ingestion.py params.json
```

Example `params.json`
---------------------

```json
{
     "csv_jobs": [
          { "path": "data/input/employee.csv", "destination": "local" }
     ],
     "json_jobs": [
          { "path": "data/input/sample1.json", "destination": "local" }
     ]
}
```

Notes
-----

- The runner accepts one CLI argument: the JSON file path.
- `destination` must match a `type` value in `configs/source_config.yaml` (e.g., `local`, `s3`). Jobs with invalid destinations are skipped.
- The runner initializes the engine once and processes jobs sequentially. The engine controls final output filenames.

Output
------

- For `local` destination, outputs are written to: `data/output/bronze/<source>/`.

If you want deterministic basenames per job, I can make the engine return the produced output path so the runner can rename files deterministically.

License: MIT
