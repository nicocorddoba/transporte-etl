from prefect import task
from prefect_aws.s3 import S3Bucket
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

today = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires")).strftime("%Y-%m-%d")

@task
def cargar(path: str = "./raw"):
    check = os.scandir(path)
    file_list = [i.name for i in check]
    if len(file_list) == 0:
        return "No hay archivos para cargar"
    stored_data = []

    for i in file_list:
        file_path = f'{path}\\{i}'
        data = json.load(open(file_path, 'r'))
        stored_data.append(data)

    try: 
        json.dump(stored_data, open(f'{path}\\{today}.json', 'w'), indent=4, default=str)
        s3 = S3Bucket.load("s3block")
        s3.upload_from_path(f'{path}/{today}.json', f'./raw/{today}.json')
    except Exception as e:
        print("Error al guardar el archvio", e)

    for i in file_list:
        file_path = f'{path}\\{i}'
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error al remover el archivo {file_path}: {e}")