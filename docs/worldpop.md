# WorldPop Source

This source provides population data by theme and country.

## Example Usage

```python
config = {
    "provider": "worldpop",
    "items": [
        {
            "theme": "population",
            "country": "cze"
        }
    ]
}

# pipeline.ingest_duckdb(duckdb_path, config, True)
```

