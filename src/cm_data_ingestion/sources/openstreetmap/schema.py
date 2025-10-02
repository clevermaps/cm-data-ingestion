config_schema = {
    "type": "object",
    "properties": {
        "database_path": {"type": "string"},
        "downloads": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "country_codes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
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
                    "target_date_prefer_older": {"type": ["boolean", "null"]},
                    "table_name": {"type": "string"}
                },
                "required": ["country_codes", "table_name"]
            }
        }
    },
    "required": ["database_path", "downloads"]
}