import dlt
import uuid
import yaml


def run_dbt(destination, dataset_name, dbt_dir, dbt_model_config):

    if dbt_model_config:
        create_models_yml(
            input_data=dbt_model_config,
            template_file="{}/models/stg/models.yml.template".format(dbt_dir),
            output_file="{}/models/stg/models.yml".format(dbt_dir)
        )

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


def create_models_yml(input_data, template_file, output_file):
    
    with open(template_file, "r", encoding="utf-8") as f:
        template_models = yaml.safe_load(f)

    input_dict = {d["name"]: d["alias"] for d in input_data}

    # Aktualizace šablony
    for model in template_models.get("models", []):
        model_name = model["name"]
        if model_name in input_dict:
            # Aktualizace aliasu a odstranění enabled
            model["config"]["alias"] = input_dict[model_name]
            model["config"].pop("enabled", None)
        else:
            # Pokud model není ve vstupu, ponecháme nebo doplníme enabled: false
            if "enabled" not in model["config"]:
                model["config"]["enabled"] = False

    with open(output_file, "w", encoding="utf-8") as f:
        yaml.dump(template_models, f, sort_keys=False, default_flow_style=False, allow_unicode=True)


# Dat nekam jinam
def get_worldpop_url(country, theme):

    if theme == 'population':
        url = 'https://data.worldpop.org/GIS/Mastergrid/Global_2000_2020/{}/L0/{}_level0_100m_2000_2020.tif'.format(country.upper(), country)
    else:
        raise ValueError('Theme {} is not suported.'.format(theme))

    return url