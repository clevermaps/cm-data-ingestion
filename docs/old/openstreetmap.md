## Running OSM pipeline

Configure download settings in `config.json`.

Init virtual env, install all deps and run OSM pipeline.

```bash
python -m venv venv
source venv/bin/activate
pip install .
pip install -r src/cm_data_ingestion/sources/openstreetmap/requirements.txt
python pipelines/osm_pipeline.py -c config.json
```

DuckDB with downloaded data is located at `openstreetmap.db`. Check it with DuckDB CLI:

```bash
duckdb openstreetmap.db
```

or using DuckDB UI:

```bash
duckdb --ui openstreetmap.db
```
DuckDB with downloaded data is located at `openstreetmap.db`.