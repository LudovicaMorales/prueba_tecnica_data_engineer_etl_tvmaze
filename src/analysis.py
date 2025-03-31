import logging
import pandas as pd
from ydata_profiling import ProfileReport
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def generate_profiling_report(df: pd.DataFrame, output_file: str):
    """
    Genera un reporte de profiling usando ydata_profiling y lo guarda en formato HTML.
    """
    if df.empty:
        logger.warning("DataFrame vacío. No se puede generar el reporte de profiling.")
        return
    profile = ProfileReport(df, title="TV Shows Profiling Report", explorative=True)
    profile.to_file(output_file)
    logger.info(f"Reporte de profiling generado: {output_file}")


def run_aggregations(df_data_parquet):
    """
    Realizar operaciones de agregación sobre los datos de la API de TVMaze
    """

    # a. Calcular el runtime promedio (averageRuntime)
    calculate_average_runtime(df_data_parquet)

    # b. Conteo de shows de TV por género
    count_shows_by_genre(df_data_parquet)

    # c. Listar los dominios únicos del sitio oficial
    list_unique_domains(df_data_parquet)


def calculate_average_runtime(df):
    """Calcular el runtime promedio de todos los shows"""

    # Usamos averageruntime que es más consistente que runtime individual
    avg_runtime = df['_embedded.show.averageruntime'].mean()

    logger.info(f"Runtime promedio de todos los shows: {avg_runtime:.2f} minutos")

    return avg_runtime


def count_shows_by_genre(df):
    """Contar shows de TV por género"""

    # Crear un DataFrame para almacenar los conteos por género
    genre_counts = {}

    # Iterar sobre cada fila y procesar los géneros
    for _, row in df.iterrows():
        genres = row.get('_embedded.show.genres')
        if isinstance(genres, str):
            for genre in genres.split(', '):
                if genre:
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1

    # Convertir a DataFrame para mejor visualización
    genre_df = pd.DataFrame({
        'Género': list(genre_counts.keys()),
        'Cantidad': list(genre_counts.values())
    }).sort_values('Cantidad', ascending=False)

    genre_df.reset_index(drop=True, inplace=True)

    logger.info(f"Distribución de shows por género (Total de {len(genre_df)} géneros):")
    logger.info(genre_df.to_string(index=False))

def list_unique_domains(df):
    """Listar los dominios únicos del sitio oficial de los shows"""

    # Extraer dominios de los sitios oficiales
    domains = []
    for url in df['_embedded.show.officialsite'].dropna():
        try:
            domain = urlparse(url).netloc
            if domain:
                domains.append(domain)
        except:
            # Ignorar URLs inválidas
            pass

    # Obtener dominios únicos
    unique_domains = sorted(list(set(domains)))

    logger.info(f"Dominios únicos encontrados ({len(unique_domains)}):")
    logger.info(unique_domains)
