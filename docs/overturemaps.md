# OvertureMaps (OVM) Source

This source provides data for various themes and types such as places, buildings, addresses, transportation, and divisions.

## Example Usage

```python

config = {
    "provider": "overturemaps",
    "items": [
        {"theme": "places", "type": "place", "table_name": "places__place"},
        {"theme": "buildings", "type": "building", "table_name": "buildings__building"},
        {"theme": "addresses", "type": "address", "table_name": "addresses__address"},
        {"theme": "transportation", "type": "segment", "table_name": "transportation__segment"},
        {"theme": "transportation", "type": "connector", "table_name": "transportation__connector"},
        {"theme": "divisions", "type": "division", "table_name": "divisions__division"},
        {"theme": "divisions", "type": "division_area", "table_name": "divisions__area"},
        {"theme": "base", "type": "land_use", "table_name": "base__land_use"}
    ],
    "options": {
        "release": "2025-09-24.0",
        "bbox": [15.54242618063797, 48.61653930468355, 17.646931589819502, 49.63325475249341]
    }
}

pipeline.ingest_file('./data', 'parquet', config, True)

pipeline.ingest_file('s3://your-bucket-path/', 'csv', config, True)

pipeline.ingest_duckdb(duckdb_path, config, True)

pipeline.ingest_motherduck("md:motherduck-token", config, True)
```

---
