# OpenStreetMap Source

This source provides OpenStreetMap data filtered by theme and country code.

## Example Usage

```python

config = {
    "provider": "openstreetmap",
    "items": [
        {"theme": "amenity"},
        {"theme": "aerialway"},
        {"theme": "aeroway"},
        {"theme": "barrier"},
        {"theme": "boundary"},
        {"theme": "building"},
        {"theme": "craft"},
        {"theme": "emergency"},
        {"theme": "geological"},
        {"theme": "highway"},
        {"theme": "historic"},
        {"theme": "landuse"},
        {"theme": "leisure"},
        {"theme": "man_made"},
        {"theme": "military"},
        {"theme": "natural"},
        {"theme": "office"},
        {"theme": "place"},
        {"theme": "power"},
        {"theme": "public_transport"},
        {"theme": "railway"},
        {"theme": "route"},
        {"theme": "shop"},
        {"theme": "telecom"},
        {"theme": "tourism"},
        {"theme": "waterway"}
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

