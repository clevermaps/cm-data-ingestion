# OvertureMaps (OVM) Source

This source provides data for various themes and types such as places, buildings, addresses, transportation, and divisions.

## Example Usage

```python
config = {
    "provider": "overturemaps",
    "items": [
        {"theme": "places", "type": "place", "table_name": "places__place"},
        # Additional items can be added here
    ],
    "options": {
        "release": "2025-09-24.0",
        "bbox": [15.54242618063797, 48.61653930468355, 17.646931589819502, 49.63325475249341]
    }
}

# pipeline.ingest_duckdb(duckdb_path, config, True)
```

---
