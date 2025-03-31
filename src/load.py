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
        logger.warning("El DataFrame está vacío. No se podrá guardar en formato Parquet.")
        return
    df.to_parquet(parquet_file_path, compression='snappy')
    logger.info(f"Archivo Parquet guardado en: {parquet_file_path}")


# Función para crear las tablas en sqlite
def create_database_tables(db_path):
    """
    Crea las tablas en la base de datos SQLite
    """

    # Asegurar que el directorio existe
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    logger.info(f"Conectando a la base de datos para la creación de las tablas en {db_path}")

    # Conectar a la base de datos
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Crear tabla shows
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shows (
        id INTEGER PRIMARY KEY,
        url TEXT,
        name TEXT,
        type TEXT,
        language TEXT,
        status TEXT,
        runtime INTEGER,
        average_runtime INTEGER,
        premiered TEXT,
        ended TEXT,
        official_site TEXT,
        weight INTEGER,
        web_channel_id INTEGER,
        image_medium TEXT,
        image_original TEXT,
        summary TEXT,
        days TEXT,
        FOREIGN KEY (web_channel_id) REFERENCES web_channels (id)
    )
    ''')

    # Crear tabla episodes
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS episodes (
        id INTEGER PRIMARY KEY,
        show_id INTEGER,
        url TEXT,
        name TEXT,
        season INTEGER,
        number INTEGER,
        type TEXT,
        airdate TEXT,
        airtime TEXT,
        airstamp TEXT,
        runtime INTEGER,
        summary TEXT,
        FOREIGN KEY (show_id) REFERENCES shows (id)
    )
    ''')

    # Crear tabla web_channels
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS web_channels (
        id INTEGER PRIMARY KEY,
        name TEXT,
        official_site TEXT,
        country_code TEXT,
        FOREIGN KEY (country_code) REFERENCES country (code)
    )
    ''')

    # Crear tabla country
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS country (
        code TEXT PRIMARY KEY,
        name TEXT,
        timezone TEXT
    )
    ''')

    # Crear tabla genres
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS genres (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    ''')

    # Crear tabla show_genre (relación muchos a muchos)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS show_genre (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER,
        genre_id INTEGER,
        FOREIGN KEY (show_id) REFERENCES shows (id),
        FOREIGN KEY (genre_id) REFERENCES genres (id),
        UNIQUE(show_id, genre_id)
    )
    ''')

    # Guardar cambios y cerrar conexión
    conn.commit()
    conn.close()


def insert_data_to_db(df_clean, db_path):
    """
    Inserta los datos limpios del DataFrame en la base de datos SQLite
    """

    logger.info(f"Conectando a la base de datos para la inserción de los datos en {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    def safe_value(value):
        """Convierte los NaN en None, formatea las fechas y preserva el tipo original para otros valores"""
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d')  # Format as YYYY-MM-DD
        return value  # Mantiene el tipo original del valor

    for _, row in df_clean.iterrows():
        try:

            # Manejar valores nulos o NaN
            code = safe_value(row.get('_embedded.show.webchannel.country.code'))
            name = safe_value(row.get('_embedded.show.webchannel.country.name'))
            timezone = safe_value(row.get('_embedded.show.webchannel.country.timezone'))

            # Insertar datos en country
            if code and name and timezone:
                cursor.execute('''
                    INSERT OR IGNORE INTO country (code, name, timezone) VALUES (?, ?, ?)
                ''', (code, name, timezone))

            # Insertar datos en web_channels
            web_channel_id = safe_value(row.get('_embedded.show.webchannel.id'))
            web_channel_name = safe_value(row.get('_embedded.show.webchannel.name'))
            web_channel_site = safe_value(row.get('_embedded.show.webchannel.officialsite'))

            if web_channel_id:
                cursor.execute('''
                    INSERT OR IGNORE INTO web_channels (id, name, official_site, country_code) VALUES (?, ?, ?, ?)
                ''', (
                    web_channel_id,
                    web_channel_name,
                    web_channel_site,
                    code
                ))

            # Insertar datos en shows
            show_id = safe_value(row.get('_embedded.show.id'))
            show_url = safe_value(row.get('_embedded.show.url'))
            show_name = safe_value(row.get('_embedded.show.name'))
            show_type = safe_value(row.get('_embedded.show.type'))
            show_language = safe_value(row.get('_embedded.show.language'))
            show_status = safe_value(row.get('_embedded.show.status'))
            show_runtime = safe_value(row.get('_embedded.show.runtime'))
            show_avgruntime = safe_value(row.get('_embedded.show.averageruntime'))
            show_premiered = safe_value(row.get('_embedded.show.premiered'))
            show_ended = safe_value(row.get('_embedded.show.ended'))
            show_site = safe_value(row.get('_embedded.show.officialsite'))
            show_weight = safe_value(row.get('_embedded.show.weight'))
            show_img_med = safe_value(row.get('_embedded.show.image.medium'))
            show_img_orig = safe_value(row.get('_embedded.show.image.original'))
            show_summary = safe_value(row.get('_embedded.show.summary'))
            show_days = safe_value(row.get('_embedded.show.schedule.days'))

            cursor.execute('''
                INSERT OR IGNORE INTO shows (
                    id, url, name, type, language, status, runtime, average_runtime, premiered, ended,
                    official_site, weight, web_channel_id, image_medium, image_original, summary, days
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                show_id, show_url, show_name, show_type, show_language, show_status,
                show_runtime, show_avgruntime, show_premiered, show_ended, show_site,
                show_weight, web_channel_id, show_img_med, show_img_orig, show_summary, show_days
            ))

            # Procesar datos de episodios con safe_value para todos los campos
            episode_id = safe_value(row.get('id'))
            episode_url = safe_value(row.get('url'))
            episode_name = safe_value(row.get('name'))
            episode_season = safe_value(row.get('season'))
            episode_number = safe_value(row.get('number'))
            episode_type = safe_value(row.get('type'))
            episode_airdate = safe_value(row.get('airdate'))
            episode_airtime = safe_value(row.get('airtime'))
            episode_airstamp = safe_value(row.get('airstamp'))
            episode_runtime = safe_value(row.get('runtime'))
            episode_summary = safe_value(row.get('summary'))

            # Insertar en episodes con valores seguros
            cursor.execute('''
                INSERT OR IGNORE INTO episodes (
                    id, show_id, url, name, season, number, type, airdate, airtime, airstamp,
                    runtime, summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                episode_id, show_id, episode_url, episode_name, episode_season,
                episode_number, episode_type, episode_airdate, episode_airtime, episode_airstamp,
                episode_runtime, episode_summary
            ))

            # Insertar géneros asociados con el show
            genres = row.get('_embedded.show.genres', '')
            if isinstance(genres, str):
                genres = genres.split(', ')

            for genre in genres:
                genre = safe_value(genre)
                if genre:
                    genre_id = abs(hash(genre)) % 1000000
                    cursor.execute('''
                        INSERT OR IGNORE INTO genres (id, name) VALUES (?, ?)
                    ''', (genre_id, genre))

                    if show_id:
                        cursor.execute('''
                            INSERT OR IGNORE INTO show_genre (show_id, genre_id) VALUES (?, ?)
                        ''', (show_id, genre_id))

        except sqlite3.Error as e:
            logger.error(f"Error al insertar los datos en la Base de Datos: {e}")
            logger.error(f"Detalle del error para ID {row.get('id')}")
            import traceback
            logger.error(traceback.format_exc())

    conn.commit()
    conn.close()
    logger.info("Datos insertados correctamente.")