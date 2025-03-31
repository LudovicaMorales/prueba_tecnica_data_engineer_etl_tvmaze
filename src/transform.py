import os
import json
import pandas as pd
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def create_dataframe_from_json(json_folder: str) -> pd.DataFrame:
    """
    Lee todos los archivos JSON (raw_data) que se extrajeron de la API de TVMaze y,
    los concatena en un Dataframe de Pandas
    """
    all_files = [os.path.join(json_folder, file) for file in os.listdir(json_folder) if file.endswith(".json")]
    all_data = []
    for file in all_files:
        with open(file, 'r') as f:
            data = json.load(f)
            all_data.extend(data)

    df = pd.json_normalize(all_data, sep='.')
    return df

def safe_to_datetime(column):
    """
    Convierte una columna a formato datetime de forma segura.
    Si no es posible convertir, retorna NaT.
    """
    try:
        return pd.to_datetime(column, errors='raise')
    except Exception:
        return pd.NaT

def perform_data_cleaning(df):
    # Copia del DataFrame original
    df_clean = df.copy()

    # Estandarizar nombres de columnas
    df_clean.columns = [col.strip().lower() for col in df_clean.columns]

    # Estandarizar fechas
    date_columns = ['airdate', 'airtime', 'airstamp', '_embedded.show.premiered', '_embedded.show.ended']
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = safe_to_datetime(df_clean[col])

    # Limpiar texto HTML (para facilitar el análisis de texto)
    df_clean['summary'] = df_clean['summary'].apply(
        lambda x: BeautifulSoup(x, "html.parser").get_text() if isinstance(x, str) else x)

    # Eliminar columnas con más del 85% de datos faltantes
    total_rows = len(df_clean)
    missing_threshold = 0.85
    cols_to_drop = [col for col in df_clean.columns
                    if df_clean[col].isnull().sum() / total_rows >= missing_threshold]
    df_clean = df_clean.drop(columns=cols_to_drop)

    # Filtrar registros con 'season' 2024
    if 'season' in df_clean.columns:
        df_clean = df_clean[df_clean['season'] != 2024]

    # Rellenar valores numéricos con la mediana
    numeric_columns = ['runtime', '_embedded.show.averageruntime']
    for column in numeric_columns:
        if column in df_clean.columns and df_clean[column].isnull().any():
            df_clean[column] = df_clean[column].fillna(df_clean[column].median())

    # Rellenar valores categóricos con la moda
    categorical_cols = df_clean.select_dtypes(include=['object']).columns
    for column in categorical_cols:
        if df_clean[column].isnull().any():
            mode_value = df_clean[column].mode().iloc[0] if not df_clean[column].mode().empty else 'unknown'
            df_clean[column] = df_clean[column].fillna(mode_value)

    # Convertir listas en cadenas separadas por comas
    list_columns = ['_embedded.show.genres', '_embedded.show.schedule.days']
    for column in list_columns:
        if column in df_clean.columns:
            df_clean[column] = df_clean[column].apply(
                lambda x: ', '.join(x) if isinstance(x, list) else
                str(x) if pd.notna(x) else '')

    # Mapear días de la semana a números
    if 'embedded.show.schedule.days' in df_clean.columns:
        days_mapping = {
            'monday': '1', 'tuesday': '2', 'wednesday': '3', 'thursday': '4',
            'friday': '5', 'saturday': '6', 'sunday': '7'
        }

        df_clean['embedded.show.schedule.days'] = df_clean['embedded.show.schedule.days'].apply(
            lambda day_str: '' if pd.isna(day_str) or day_str == '' else
            ', '.join([days_mapping.get(d.strip().lower(), d) for d in day_str.split(',')])
        )

    # Normalizar categorías poco frecuentes en 'type'
    if 'type' in df_clean.columns:
        threshold = 10
        type_counts = df_clean['type'].value_counts()
        df_clean['type'] = df_clean['type'].apply(
            lambda x: 'other' if type_counts.get(x, 0) <= threshold else x
        )

    # Eliminar filas con menos del 25% de datos
    min_non_null = int(len(df_clean.columns) * 0.25)
    df_clean = df_clean.dropna(thresh=min_non_null)

    # Eliminar duplicados y reiniciar índice
    df_clean.drop_duplicates(inplace=True)
    df_clean.reset_index(drop=True, inplace=True)

    return df_clean
