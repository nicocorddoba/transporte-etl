from prefect import task
import json
import sqlite3
import datetime
import os

@task
def cargar_en_json(lista_datos: list[dict], path: str = "./raw"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(lista_datos, f, indent=4, default=str)
    return "archivo guardado"
