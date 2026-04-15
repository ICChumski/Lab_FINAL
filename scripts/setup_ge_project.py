import os
from pathlib import Path

from dotenv import load_dotenv
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")

GE_DIR = Path("great_expectations").resolve()

DATASOURCE_NAME = "postgres_raw_datasource"
SUITE_NAME = "raw_cycle_trips_suite"
CHECKPOINT_NAME = "raw_cycle_trips_checkpoint"
DATA_CONNECTOR_NAME = "default_inferred_data_connector_name"
DATA_ASSET_NAME = "raw.raw_cycle_trips"


def main():
    connection_string = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    print(f"Usando projeto GE em: {GE_DIR}")

    context = gx.get_context(context_root_dir=str(GE_DIR))

    datasource_config = {
        "name": DATASOURCE_NAME,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetSqlDataConnector",
                "include_schema_name": True,
                "introspection_directives": {
                    "schema_name": "raw",
                },
            },
        },
    }

    print("Criando ou atualizando datasource...")
    context.add_datasource(**datasource_config)

    print("Criando ou atualizando expectation suite...")
    context.add_or_update_expectation_suite(expectation_suite_name=SUITE_NAME)

    batch_request = BatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name=DATA_CONNECTOR_NAME,
        data_asset_name=DATA_ASSET_NAME,
    )

    print("Criando validator...")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME,
    )

    print("Adicionando expectativas...")
    validator.expect_column_values_to_not_be_null("trip_id")
    validator.expect_column_values_to_not_be_null("start_date")
    validator.expect_column_values_to_be_between(
        "total_duration_ms",
        min_value=1,
        mostly=0.999,
    )

    validator.save_expectation_suite(discard_failed_expectations=False)

    print("Criando ou atualizando checkpoint...")
    context.add_or_update_checkpoint(
        name=CHECKPOINT_NAME,
        config_version=1.0,
        class_name="SimpleCheckpoint",
        validations=[
            {
                "batch_request": batch_request.to_json_dict(),
                "expectation_suite_name": SUITE_NAME,
            }
        ],
    )

    print("\nConfiguração concluída com sucesso.")
    print(f"Datasource: {DATASOURCE_NAME}")
    print(f"Suite: {SUITE_NAME}")
    print(f"Checkpoint: {CHECKPOINT_NAME}")


if __name__ == "__main__":
    main()