from pathlib import Path
import json

def load_source_config(config_path: str) -> dict:
    """
    Načte cestu ke source configu ze standardního dlt configu a vrátí obsah YAML.
    """
    
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file {config_path} not found.")
    with open(path, "r") as f:
        return json.load(f)
