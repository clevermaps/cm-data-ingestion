
## Running Worldpop pipeline

Configure download settings in `config_worldpop.json`.

Init virtual env, install all deps and run OSM pipeline.
```
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/worldpop/requirements.txt
python pipelines/worldpop_pipeline.py -c config_worldpop.json
```

DuckDB with downloaded data is located at `worldpop.duckdb`.