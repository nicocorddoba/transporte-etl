from prefect import task
from datetime import datetime
from zoneinfo import ZoneInfo


@task
def transformar_datos(data):
    argentina_time = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires"))
    transformed_data = {}
    data_list =[]
    
    for item in data:
        transformed_data['idInterno'] = (item['interno'])
        transformed_data['latitud'] = (item['latitud'])
        transformed_data['longitud'] = (item['longitud'])
        transformed_data['orientacion'] = (item['orientacion'])
        transformed_data['proximaParada'] = (item['proximaParada'])
        transformed_data['vehiculoRampa'] = (item['vehiculoRampa'])
        transformed_data['vehiculoNoVisibles'] = (item['vehiculoNoVisibles'])
        transformed_data['time'] = argentina_time
        data_list.append(transformed_data)
    return data_list