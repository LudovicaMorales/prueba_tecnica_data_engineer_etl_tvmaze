import unittest 
from unittest.mock import patch, MagicMock
import os
import sys
import json
import tempfile
from datetime import date 
from requests import RequestException
import logging

# Se sube dos niveles desde la ubicación actual (tests/) hasta llegar a la raíz del proyecto
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.extraction import fetch_tvmaze_schedule, save_json_response

# Construye la ruta al archivo de mock que contiene la respuesta de ejemplo
data_path = os.path.join(os.path.dirname(__file__), 'mock_response.json')

# Se abre y carga el contenido del archivo JSON en la variable SAMPLE_JSON.
with open(data_path, 'r', encoding='utf-8') as f:
    SAMPLE_JSON = json.load(f)

class TestExtraction(unittest.TestCase):

    @patch('src.extraction.requests.get')
    def test_fetch_tvmaze_success(self, mock_get):
        """
        Test que verifica que la función fetch_tvmaze_schedule funcione correctamente
        cuando la API devuelve una respuesta exitosa (HTTP 200) utilizando el JSON de ejemplo
        """
        # Se crea un objeto mock que simula la respuesta de la API
        mock_response = MagicMock()

        mock_response.json.return_value = SAMPLE_JSON
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        test_date = date(2024, 1, 2) # Fecha de prueba

        result = fetch_tvmaze_schedule(test_date)

        # Se verifica que los valores retornados sean los esperados según el JSON de ejemplo
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 2719122)
        self.assertEqual(result[0]["name"], "Серия 23")

        # Se verifca que la petición se haya realizado con los datos exactos
        expected_url = f"http://api.tvmaze.com/schedule/web?date=2024-01-02"
        mock_get.assert_called_once_with(expected_url, timeout=50)
    
    @patch('src.extraction.requests.get')
    def test_fetch_tvmaze_error(self, mock_get):
        """
        Test que verifica que la función fetch_tvmaze_schedule maneje correctamente una excepción de petición
        """
        mock_get.side_effect = RequestException("Connection Error Test")

        test_date = date(2024, 1, 3) # Fecha de prueba

        result = fetch_tvmaze_schedule(test_date)

        # Se verifica que el resultado sea una lista vacía
        self.assertEqual(result, [])

    def test_save_json_response(self):
        """
        Test que verifica que la función save_json_response guarde correctamente el archivo JSON
        en el directorio especificado.
        """
        # Se crea un directorio temporal para que la prueba no afecte el sistema de archivos real
        with tempfile.TemporaryDirectory() as tmpdir:
            # Datos de ejemplo
            test_data = SAMPLE_JSON
            test_day = date(2024, 1, 2)

            # Se llama a la función para guardar el JSON en el directorio temporal
            save_json_response(test_data, tmpdir, test_day)

            expected_filename = os.path.join(tmpdir, "data_tvmaze_2024-01-02.json")
            self.assertTrue(os.path.exists(expected_filename))

            with open(expected_filename, 'r', encoding='utf-8') as f:
                content = json.load(f)
            # Comprobamos que el contenido del archivo sea idéntico a los datos de prueba
            self.assertEqual(content, test_data)

if __name__ == '__main__':
    unittest.main()
