import dlt
import uuid
import yaml
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    return models

    # for m in models:
    #     print(
    #         f"Model {m.model_name} materialized" +
    #         f" in {m.time}" +
    #         f" with status {m.status}" +
    #         f" and message {m.message}"
    #     )


def run_dlt(dlt_resource, destination, schema):

    logger.info("Starting run_dlt with destination: %s, schema: %s", destination, schema)

    pipeline_name = str(uuid.uuid4())
    logger.info("Creating pipeline with name: %s", pipeline_name)

    pipeline = dlt.pipeline(
        pipeline_name=str(uuid.uuid4()),
        destination=destination, 
        dataset_name=schema
    )

    logger.info("Pipeline created successfully")

    logger.info("Running pipeline with resource: %s", dlt_resource.name)
    result = pipeline.run(dlt_resource, write_disposition='replace')
    logger.info("Pipeline run completed with result: %s", result)
    
    return result


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


# TODO dat to source
def get_worldpop_url(country, theme):

    if theme == 'population':
        #url = 'https://data.worldpop.org/GIS/Mastergrid/Global_2000_2020/{}/L0/{}_level0_100m_2000_2020.tif'.format(country.upper(), country)
        url = 'https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2025/{}/v1/100m/constrained/{}_pop_2025_CN_100m_R2025A_v1.tif'.format(country.upper(), country)
    else:
        raise ValueError('Theme {} is not suported.'.format(theme))

    return url