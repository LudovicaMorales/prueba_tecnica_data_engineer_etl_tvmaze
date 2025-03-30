import os
import requests
import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

BASE_URL = "http://api.tvmaze.com/schedule/web"

def fetch_tvmaze_schedule(day: date):
    """
    Realiza una petición GET a la API de TVMaze para obtener los episodios que se emiten 
    en los canales web/streaming en una fecha determinada.
    """
    url = f"{BASE_URL}?date={day.isoformat()}"
    logger.debug(f"Llamando a URL: {url}")
    try:
        response = requests.get(url, timeout=50)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error al llamar a la API: {e}")
        return []

def save_json_response(data, folder_path: str, day: date):
    """
    Guarda la respuesta (JSON) de la API en un archivo .json en la carpeta recibida.
    """
    filename = f"data_tvmaze_{day.isoformat()}.json"
    full_path = os.path.join(folder_path, filename)

    with open(full_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    logger.info(f"Archivo JSON guardado: {full_path}")
