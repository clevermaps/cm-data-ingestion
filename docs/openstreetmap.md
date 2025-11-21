# OpenStreetMap Source

This source provides OpenStreetMap data filtered by theme and country code.

## Example Usage

```python
config = {
    "provider": "openstreetmap",
    "items": [
        {
            "theme": "amenity",
            "country_code": "cz"
        }
    ]
}

# pipeline.ingest_duckdb(duckdb_path, config, True)
```

