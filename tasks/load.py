from prefect import task
import json
import os

@task
def cargar(path: str = "./raw"):
    check = os.scandir(path)
    file_list = [i.name for i in check]
    try:
        stored_data = []
        for i in file_list:
            file_path = f'{path}\\{i}'
            data = json.load(open(file_path, 'r'))
            stored_data.extend(data)
            os.remove(file_path)
        json.dump(stored_data, open('./ola.json', 'w'), indent=4, default=str)
    except Exception as e:
        print(f"Error: {e}")