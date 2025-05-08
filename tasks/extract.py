from prefect import task
import requests


class NoDataError(Exception):
    """Error personalizado para cuando no se extraen datos."""
    pass


@task
def extraer_datos(url:str):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if len(data) > 0:
            return data
        elif len(data) == 0:
            raise NoDataError("No se encontraron datos en la respuesta.")
    except requests.RequestException as e:
        raise requests.RequestException(f"Error al realizar la solicitud: {e}")