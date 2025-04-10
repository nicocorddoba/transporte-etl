from prefect import task
import json
import sqlite3
import datetime


@task
def cargar_en_json(lista_datos: list[dict], path: str = "./raw"):
    now_arg = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(f'{path}/{now_arg}.json', "w", encoding="utf-8") as f:
        json.dump(lista_datos, f, indent=4, default=str)
    return "archivo guardado"
