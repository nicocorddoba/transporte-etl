from prefect import task
import requests

@task
def extraer_datos(url: str):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data['error'] == 0 and len(data['posiciones']) > 0:
            return data['posiciones']
        elif len(data['posiciones']) == 0:
            return None
        else:
            print('error en la respuesta:', data['error'])
            return None
    
    except requests.RequestException as e:
        print(f"Error al hacer la solicitud: {e}")
        return None