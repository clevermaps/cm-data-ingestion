# Geoboundaries Source

This source provides geographic boundary data for various administrative levels.

## Example Usage

```python
config = {
    "provider": "geoboundaries",
    "items": [
        {"country_code": "CZE", "admin_level": "ADM0"},
        {"country_code": "CZE", "admin_level": "ADM1"},
        {"country_code": "CZE", "admin_level": "ADM2"},
        {"country_code": "CZE", "admin_level": "ADM3"},
        {"country_code": "CZE", "admin_level": "ADM4"}
    ]
}

pipeline.ingest_duckdb(duckdb_path, config, True)
```

