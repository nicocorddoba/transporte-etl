from prefect import task
import json
import sqlite3
import datetime
import os

@task
def cargar_en_json(lista_datos: list[dict], path: str = "./raw"):
    now_arg = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = now_arg + ".json"
    path = os.path.join(os.path.dirname(__file__),'raw', filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(lista_datos, f, indent=4, default=str)
    return "archivo guardado"
