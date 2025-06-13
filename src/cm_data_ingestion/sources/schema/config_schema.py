config_schema = {
    "type": "object",
    "properties": {
        "database_path": {"type": "string"},
        "downloads": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "country_code": {"type": "string"},
                    "tag": {"type": ["string", "null"]},
                    "value": {"type": ["string", "null"]},
                    "element_type": {"type": ["string", "null"]},
                    "target_date_range": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                        "minItems": 2,
                        "maxItems": 2
                    },
                    "target_date_tolerance_days": {
                        "type": ["integer", "null"],
                        "minimum": 0
                    },
                    "table_name": {"type": "string"}
                },
                "required": ["country_code", "table_name"]
            }
        }
    },
    "required": ["database_path", "downloads"]
}