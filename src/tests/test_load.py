import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
import os
import sys

# Se sube dos niveles desde la ubicación actual (tests/) hasta llegar a la raíz del proyecto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.load import save_as_parquet

# Construye la ruta al archivo de mock que contiene la respuesta de ejemplo
data_path = os.path.join(os.path.dirname(__file__), 'mock_response.json')

# Se abre y carga el contenido del archivo JSON en la variable SAMPLE_JSON.
with open(data_path, 'r', encoding='utf-8') as f:
    SAMPLE_JSON = json.load(f)

class TestSaveAsParquet(unittest.TestCase):

    @patch("src.load.logger")  # Patch directo al logger del módulo
    @patch("pandas.DataFrame.to_parquet")
    def test_save_as_parquet_success(self, mock_to_parquet, mock_logger):
        # Crear un DataFrame de ejemplo
        df = pd.json_normalize(SAMPLE_JSON, sep='.')

        parquet_file_path = "test.parquet"

        # Llamar a la función save_as_parquet
        save_as_parquet(df, parquet_file_path)

        # Verificar que to_parquet se haya llamado con el archivo correcto y la compresión correcta
        mock_to_parquet.assert_called_once_with(parquet_file_path, compression='snappy')

        # Verificar que el mensaje de log se haya registrado correctamente
        mock_logger.info.assert_called_once_with(f"Archivo Parquet guardado en: {parquet_file_path}")

if __name__ == "__main__":
    unittest.main()
