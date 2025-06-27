import os
import requests

# publiserer statiske filer fra dbt docs generate til Nada sin side


def publish_docs(url_suffix="pensjon-pen-dataprodukt"):
    # fra Nada på https://github.com/navikt/dbt-docs#publisering
    complete_url = "https://dbt.intern.nav.no/docs/wendelboe/" + url_suffix
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


if __name__ == "__main__":
    publish_docs()
