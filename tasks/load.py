from prefect import task
import json
import sqlite3


@task
def cargar_en_json(lista_datos: list[dict], path: str = "./raw"):
    json.dumps(lista_datos, indent=4, sort_keys=True, default=str, open(f"{path}/data.json", "w")))
    return "archivo guardado"
