# GTFS Source

This source provides General Transit Feed Specification (GTFS) data for transit schedules.

## Example Usage

```python
config = {
    "provider": "gtfs",
    "items": [
        {
            "country_code": "cz",
            "city": "Brno",
            "gtfs_type": "schedule",
            "provider": "Integrated Transit System of the South Moravian Region (IDS JMK)"
        }
    ],
    "options": {}
}

pipeline.ingest_file('./data', 'parquet', config, True)

pipeline.ingest_file('s3://your-bucket-path/', 'csv', config, True)

pipeline.ingest_duckdb(duckdb_path, config, True)

pipeline.ingest_motherduck("md:motherduck-token", config, True)

```

---
