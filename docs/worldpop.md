# WorldPop Source

This source provides population data by theme and country.

## Example Usage

```python

config = {
    "provider": "worldpop",
    "items": [
        {
            "theme": "population"
        }
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

