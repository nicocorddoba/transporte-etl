from prefect import task
import json
import os

@task
def cargar(path: str = "./raw"):
    check = os.scandir(path)
    file_list = [i.name for i in check]
    if len(file_list) == 0:
        # print("No hay archivos para cargar")
        return "No hay archivos para cargar"
    try:
        stored_data = []
        for i in file_list:
            file_path = f'{path}\\{i}'
            data = json.load(open(file_path, 'r'))
            stored_data.append(data)
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error al remover el archivo {file_path}: {e}")
        try: 
            json.dump(stored_data, open(f'{path}/ola.json', 'w'), indent=4, default=str)
        except Exception as e:
            print("Error al guardar el archvio", e)
    except Exception as e:
        print(f"Error: {e}")