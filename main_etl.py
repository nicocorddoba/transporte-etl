from prefect import flow, get_run_logger

from tasks.extract import extraer_datos
from tasks.transform import transformar_datos
from tasks.load import cargar_en_json

@flow
def flujo_transporte(url: str, path: str):
    logger = get_run_logger()
    logger.info("🌱 Iniciando el flujo ETL de posiciones...")
    raw = extraer_datos(url)
    logger.info("Extracted data", raw)
    limpio = transformar_datos(raw)
    logger.info("data cleaned")
    cej = cargar_en_json(limpio, path)
    logger.info(cej)
