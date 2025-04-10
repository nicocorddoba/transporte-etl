from prefect import task
import json
import sqlite3


@task
def cargar_en_json(lista_datos: list[dict], path: str = "./raw"):
    json.dump(lista_datos, open(path, "w"), indent=4)
    return "archivo guardado"