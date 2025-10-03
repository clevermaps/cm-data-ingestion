import dlt
import uuid


def run_dbt(destination, dataset_name, dbt_dir):

    pipeline = dlt.pipeline(
        pipeline_name=str(uuid.uuid4()),
        destination=destination,
        dataset_name=dataset_name
    )

    venv = dlt.dbt.get_venv(pipeline)

    # Get runner, optionally pass the venv
    dbt = dlt.dbt.package(
        pipeline,
        dbt_dir,
        venv=venv
    )

    # Run the models and collect any info
    # If running fails, the error will be raised with a full stack trace
    models = dbt.run_all()

    for m in models:
        print(
            f"Model {m.model_name} materialized" +
            f" in {m.time}" +
            f" with status {m.status}" +
            f" and message {m.message}"
        )


def run_dlt(dlt_resource, destination, schema):

    pipeline = dlt.pipeline(
        pipeline_name=str(uuid.uuid4()),
        destination=destination, 
        dataset_name=schema
    )

    result = pipeline.run(dlt_resource)
    print(result)