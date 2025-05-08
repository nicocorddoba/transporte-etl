
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
    transformed = transformar_datos(raw, path)
    if type(transformed) is str:
        logger.info(transformed)
    logger.info("data stored")
    # cej = cargar_en_json(limpio, path)
    # logger.info(cej)


@flow
def flujo_carga(path: str):
    logger = get_run_logger()
    logger.info("🌱 Iniciando carga de datos")
    is_error=cargar(path)
    logger.info(is_error)