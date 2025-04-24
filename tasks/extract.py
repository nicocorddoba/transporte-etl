from prefect import task
import requests

@task
def extraer_datos(url:str, lines_dict: dict):
    for key, values in lines_dict.items():
        new_d = {}
        nurl = f"{url}/{values}"
        try:
            response = requests.get(nurl, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data['error'] == 0 and len(data['posiciones']) > 0:
                new_d[key] = data['posiciones']
            elif len(data['posiciones']) == 0:
                new_d[key] = None
            else:
                print('error en la respuesta:', data['error'])
                return data['error']
        except requests.RequestException as e:
            print(f"Error al hacer la solicitud: {e}")
            return None
    return new_d