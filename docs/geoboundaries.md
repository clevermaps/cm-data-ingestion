# Geoboundaries Source

This source provides geographic boundary data for various administrative levels.

## Example Usage

```python

config = {
    "provider": "geoboundaries",
    "items": [
        {"admin_level": "ADM0"},
        {"admin_level": "ADM1"},
        {"admin_level": "ADM2"},
        {"admin_level": "ADM3"},
        {"admin_level": "ADM4"}
    ],
    "options": {
        "country_codes": ["cz"]
    }
}

pipeline.ingest_file('./data', 'parquet', config, True)

pipeline.ingest_file('s3://your-bucket-path/', 'csv', config, True)

pipeline.ingest_duckdb(duckdb_path, config, True)

pipeline.ingest_motherduck("md:motherduck-token", config, True)
```

