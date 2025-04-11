from prefect import task
from datetime import datetime
from zoneinfo import ZoneInfo
import json


@task
def transformar_datos(data, path: str = "./raw"):
    argentina_time = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires"))
    line_dict = {}
    # data_list =[]
    line_dict["time"] = argentina_time
    line_dict["Linea 1"] = []
    transformed_data = {}
    
    for item in data:
        transformed_data['idInterno'] = (item['interno'])
        transformed_data['latitud'] = (item['latitud'])
        transformed_data['longitud'] = (item['longitud'])
        transformed_data['orientacion'] = (item['orientacion'])
        transformed_data['proximaParada'] = (item['proximaParada'])
        transformed_data['vehiculoRampa'] = (item['vehiculoRampa'])
        transformed_data['vehiculoNoVisibles'] = (item['vehiculoNoVisibles'])
        transformed_data['time'] = argentina_time
        line_dict["Linea 1"].append(transformed_data)
    
    # Store data in {path}
    filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".json"
    with open(f'{path}\\{filename}', "w", encoding="utf-8") as f:
        json.dump(line_dict, f, indent=4, default=str)
    return "archivo guardado"
