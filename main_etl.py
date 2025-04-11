from prefect import flow, get_run_logger

from tasks.extract import extraer_datos
from tasks.transform_store import transformar_datos
from tasks.load import cargar

@flow
def flujo_transporte(url: str, path: str):
    logger = get_run_logger()
    logger.info("🌱 Iniciando el flujo ETL de posiciones...")
    raw = extraer_datos(url)
    logger.info("Extracted data", raw)
    limpio = transformar_datos(raw)
    logger.info("data stored")
    # cej = cargar_en_json(limpio, path)
    # logger.info(cej)


@flow
def load(path: str):
    logger = get_run_logger()
    logger.info("🌱 Iniciando carga de datos")
    cargar(path)
    logger.info("Data loaded")