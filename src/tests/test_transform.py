import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
import os
import sys
import json

# Se sube dos niveles desde la ubicación actual (tests/) hasta llegar a la raíz del proyecto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.transform import create_dataframe_from_json, safe_to_datetime, perform_data_cleaning

# Construye la ruta al archivo de mock que contiene la respuesta de ejemplo
data_path = os.path.join(os.path.dirname(__file__), 'mock_response.json')

# Se abre y carga el contenido del archivo JSON en la variable SAMPLE_JSON.
with open(data_path, 'r', encoding='utf-8') as f:
    SAMPLE_JSON = json.load(f)

class TestDataTransformation(unittest.TestCase):

    @patch("os.listdir")
    @patch("builtins.open", new_callable=MagicMock)
    def test_create_dataframe_from_json(self, mock_open_func, mock_listdir):
        # Simula la estructura del directorio con un archivo JSON
        mock_listdir.return_value = ['mock_response.json']

        # Configurar el mock para que devuelva un objeto file-like
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = json.dumps(SAMPLE_JSON)
        mock_open_func.return_value = mock_file

        # Usar un directorio ficticio para la prueba
        test_dir = "/path/to/test/dir"
        df = create_dataframe_from_json(test_dir)

        # Verificar que el DataFrame se ha creado
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.shape[0], 0)  # Asegurar que hay datos en el DataFrame
        self.assertIn('id', df.columns)  # Verificar que la columna 'id' está en el DataFrame

        # Verificar que se llamó a open con la ruta correcta
        full_path = os.path.join(test_dir, 'mock_response.json')
        mock_open_func.assert_called_with(full_path, 'r')

    def test_safe_to_datetime(self):
        # Caso de fecha válida
        valid_date = '2024-01-02'
        result = safe_to_datetime(valid_date)
        self.assertTrue(pd.to_datetime(valid_date, errors='raise') == result)

        # Caso de fecha no válida
        invalid_date = 'invalid_date'
        result = safe_to_datetime(invalid_date)
        self.assertTrue(pd.isna(result))  # Esperamos NaT

    def test_perform_data_cleaning(self):
        # Normalizar el JSON para evitar problemas con tipos no hashables
        df = pd.json_normalize(SAMPLE_JSON, sep='.')

        # Limpiar los datos
        df_clean = perform_data_cleaning(df)

        # Asegurarse que la columna 'airdate' es de tipo datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_clean['airdate']))

        # Verificar que las columnas con muchos datos faltantes hayan sido eliminadas
        self.assertNotIn('rating.average', df_clean.columns)

        # Verificar que las columnas de listas hayan sido convertidas a strings separados por comas
        self.assertEqual(df_clean['_embedded.show.genres'].iloc[0], 'Drama, Comedy, Supernatural')

        # Verificar que los valores de tipo 'season' no sean 2024
        self.assertFalse((df_clean['season'] == 2024).any())

        # Verificar que se haya rellenado el valor numérico en 'runtime'
        self.assertEqual(df_clean['runtime'].iloc[0], 25)

if __name__ == "__main__":
    unittest.main()