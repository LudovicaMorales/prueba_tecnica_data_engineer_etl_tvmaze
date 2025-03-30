import logging
import os
import sys
from datetime import date, timedelta
from typing import List

from extraction import fetch_tvmaze_schedule, save_json_response

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def main():
    """
    Ejecuta el pipeline ETL para extraer, transformar y cargar información de episodios emitidos 
    en plataformas web/streaming, utilizando la API de TVMaze para el mes de enero de 2024.
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando proceso ETL...")

    # 1. Parámetros
    year = 2024
    month = 1

    # 2. Definición de rutas
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_folder = os.path.join(project_root, "json")
    os.makedirs(json_folder, exist_ok=True)

    # 3. Extraer datos para todos los días de enero del 2024
    all_dates_jan_2024 = get_all_dates_for_month(year, month)
    for day in all_dates_jan_2024:
        logger.info(f"Obteniendo data para fecha: {day}")
        response_json = fetch_tvmaze_schedule(day)
        save_json_response(response_json, json_folder, day)

    logger.info("Proceso ETL finalizado exitosamente.")

def get_all_dates_for_month(year: int, month: int) -> List[date]:
    """
    Calcula y retorna una lista con todas las fechas (objetos datetime.date) de un mes y año dados.
    """

    # Se define la fecha de inicio con los parámetros recibidos
    start_date = date(year, month, 1)

    # Se calcula la fecha final, que corresponde al primer día del mes siguiente
    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)

    # Se define un delta de un día (una diferencia de tiempo de un día) para iterar sobre el rango de fechas
    delta = timedelta(days=1)
    dates_range = []
    current = start_date

    while current < end_date:
        dates_range.append(current)
        current += delta
    return dates_range

if __name__ == "__main__":
    main()
