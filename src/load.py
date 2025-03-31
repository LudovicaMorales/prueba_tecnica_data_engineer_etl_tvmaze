import os
import logging
import pandas as pd
import sqlite3

logger = logging.getLogger(__name__)

def save_as_parquet(df: pd.DataFrame, parquet_file_path: str):
    """
    Guarda un DataFrame en formato Parquet con compresión snappy.
    """
    if df.empty:
        logger.warning("DataFrame vacío. No se guardará en Parquet.")
        return
    df.to_parquet(parquet_file_path, compression='snappy')
    logger.info(f"Archivo Parquet guardado en: {parquet_file_path}")

