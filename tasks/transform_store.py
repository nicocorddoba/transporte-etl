from prefect import task
from datetime import datetime
from zoneinfo import ZoneInfo
import json


@task
def transformar_datos(data, path: str = "./raw"):
    if data is None:
        return "Error al transformar: No hay datos"
    else:
        # data = data['posiciones']
        argentina_time = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires"))
        # Store data in {path}
        filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".json"
        try:
            with open(f'{path}\\{filename}', "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, default=str, ensure_ascii=False)
            return "archivo guardado"
        except Exception as e:
            return f"Error al guardar el archivo: {e}"