import logging
import os
import sys
from datetime import date, timedelta
from typing import List
import pandas as pd

from extraction import fetch_tvmaze_schedule, save_json_response
from transform import create_dataframe_from_json, perform_data_cleaning
from analysis import generate_profiling_report, run_aggregations
from load import save_as_parquet, create_database_tables, insert_data_to_db

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
    database_name = "tvmaze_data.db"

    # 2. Definición de rutas
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_folder = os.path.join(project_root, "json")
    profiling_folder = os.path.join(project_root, "profiling")
    data_folder = os.path.join(project_root, "data")
    db_folder = os.path.join(project_root, "db")

    # 3. Extraer datos para todos los días de enero del 2024
    all_dates_jan_2024 = get_all_dates_for_month(year, month)
    for day in all_dates_jan_2024:
        logger.info(f"Obteniendo data para fecha: {day}")
        response_json = fetch_tvmaze_schedule(day)
        save_json_response(response_json, json_folder, day)
    
    # 4. Transformar datos (generar Dataframe de Pandas)
    logger.info("Creando DataFrames desde JSON...")
    df = create_dataframe_from_json(json_folder)

    # 5. Generar profiling
    logger.info("Generando reporte de profiling...")
    profile_file = os.path.join(profiling_folder, "profiling_report.html")
    generate_profiling_report(df, profile_file)

    # 6. Limpieza / transformaciones
    logger.info("Limpieza y transformaciones en los datos...")
    df_clean = perform_data_cleaning(df)

    # 7. Almacenar en Parquet (snappy)
    logger.info("Guardando DataFrame limpio en formato Parquet snappy...")
    parquet_file_path = os.path.join(data_folder, "clean_data_tvmaze_january_2024.parquet")
    save_as_parquet(df_clean, parquet_file_path)

    # 8. Cargar la información en DB (SQLite) del archivo .parquet
    logger.info("Cargando datos en base de datos SQLite desde archivo .parquet...")

    df_data_parquet = pd.read_parquet(parquet_file_path)
    db_path = os.path.join(db_folder, database_name)
    create_database_tables(db_path)
    insert_data_to_db(df_data_parquet, db_path)

    # 8. Operaciones de agregación
    logger.info("Realizando consultas de agregación...")
    run_aggregations(df_data_parquet)

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
