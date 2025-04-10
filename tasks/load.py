from prefect import task
import json
import sqlite3
from datetime import datetime
import os

@task
def cargar_en_json(lista_datos: list[dict], path: str = "./raw"):
    filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".json"
    with open(f'{path}\\{filename}', "w", encoding="utf-8") as f:
        json.dump(lista_datos, f, indent=4, default=str)
    return "archivo guardado"
