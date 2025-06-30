import os
import requests
from dbt.cli.main import dbtRunner, dbtRunnerResult

# publiserer statiske filer fra dbt docs generate til Nada sin side
DBT_BASE_COMMAND = ["--no-use-colors", "--log-format-file", "json"]
URL_SUFFIX = "pensjon-pen-dataprodukt"


def generate_docs():
    dbt = dbtRunner()
    dbt_deps = dbt.invoke(DBT_BASE_COMMAND + ["deps"])
    output: dbtRunnerResult = dbt.invoke(DBT_BASE_COMMAND + ["docs", "generate"])

    # Exit code 2, feil utenfor DBT
    if output.exception:
        raise output.exception
    # Exit code 1, feil i dbt (test eller under kjøring)
    if not output.success:
        raise Exception(output.result)


def publish_docs():
    # fra Nada på https://github.com/navikt/dbt-docs#publisering
    complete_url = "https://dbt.intern.nav.no/docs/wendelboe/" + URL_SUFFIX
    files = ["target/manifest.json", "target/catalog.json", "target/index.html"]
    multipart_form_data = {}
    for file_path in files:
        file_name = os.path.basename(file_path)
        with open(file_path, "rb") as file:
            file_contents = file.read()
            print(f"Gathering {file_path} ({len(file_contents)/1024:.0f} kB)")
            multipart_form_data[file_name] = (file_name, file_contents)
    res = requests.put(complete_url, files=multipart_form_data)
    res.raise_for_status()
    print("HTTP PUT status: ", res.status_code, res.text)


def main():
    generate_docs()
    publish_docs()


if __name__ == "__main__":
    main()
