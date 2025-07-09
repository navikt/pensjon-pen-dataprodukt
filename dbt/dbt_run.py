import requests
import os
import time
import json
import logging
from pathlib import Path
import shlex
from google.cloud import secretmanager

from dbt.cli.main import dbtRunner, dbtRunnerResult

logging.basicConfig(level=logging.INFO)

DBT_BASE_COMMAND = ["--no-use-colors", "--log-format-file", "json"]
SCHEMA = "pen_dataprodukt"
DBT_DOCS_URL = "https://dbt.intern.nav.no/docs/wendelboe/pensjon-pen-dataprodukt"

print("Tester om print går til loggen i Airflow")
logging.info("Tester om logging går til loggen i Airflow")


def set_secrets_as_envs(secret_name: str):
    secrets = secretmanager.SecretManagerServiceClient()
    secret = secrets.access_secret_version(name=secret_name)
    secret_str = secret.payload.data.decode("UTF-8")
    secrets = json.loads(secret_str)
    os.environ["DBT_ENV_SECRET_HOST"] = secrets["DB_HOST"]
    os.environ["DBT_ENV_SERVICE_NAME"] = secrets["DB_SERVICE_NAME"]
    os.environ["DBT_ENV_SECRET_USER"] = secrets["DB_USER"]
    os.environ["DBT_ENV_SECRET_PASS"] = secrets["DB_PASSWORD"]
    logging.info("DBT miljøvariabler er lastet inn")


def publish_docs(dbt_docs_url: str = DBT_DOCS_URL):
    # Connection informasjon fo å pushe dbt docs
    files = [
        "target/manifest.json",
        "target/catalog.json",
        "target/index.html",
    ]
    multipart_form_data = {}
    for file_path in files:
        file_name = os.path.basename(file_path)
        with open(file_path, "rb") as file:
            file_contents = file.read()
            multipart_form_data[file_name] = (file_name, file_contents)

    res = requests.put(dbt_docs_url, files=multipart_form_data)
    res.raise_for_status()


if __name__ == "__main__":
    if secret_name := os.getenv("TEAM_GCP_SECRET_PATH"):
        set_secrets_as_envs(secret_name=secret_name)
    else:
        raise KeyError("Environment variable TEAM_GCP_SECRET_PATH")

    valid_db_targets = ["pen_q2", "pen_q1", "pen_prod_lesekopi", "pen_prod"]
    if dbt_target := os.getenv("DBT_DB_TARGET"):
        if dbt_target not in valid_db_targets:
            raise ValueError(f"Ugyldig DBT_DB_TARGET: {dbt_target}. Velg mellom: {valid_db_targets}")

    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()

    # default dbt kommando er build
    command = shlex.split(os.getenv("DBT_COMMAND", "build"))
    if dbt_models := os.getenv("DBT_MODELS", None):
        command = command + ["--select", dbt_models]

    dbt = dbtRunner()
    dbt_deps = dbt.invoke(DBT_BASE_COMMAND + ["deps"])
    output: dbtRunnerResult = dbt.invoke(DBT_BASE_COMMAND + command)

    # Exit code 2, feil utenfor DBT
    if output.exception:
        raise output.exception
    # Exit code 1, feil i dbt (test eller under kjøring)
    if not output.success:
        raise Exception(output.result)

    if "docs" in command:
        logging.info("publiserer dbt docs")
        publish_docs()
