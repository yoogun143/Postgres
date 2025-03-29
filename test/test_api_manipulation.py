import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from helper.api_manipulation import api_to_pandas

class TestApiToPandas(unittest.TestCase):
    def setUp(self):
        self.base_url = "https://api.example.com"
        self.endpoint = "/data"
        self.headers = {"Authorization": "Bearer test-token"}
        self.params = {"filter": {"key": "value"}}

    @patch('requests.get')
    def test_successful_single_page(self, mock_get):
        # Mock successful response with single page
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [{'id': 1, 'name': 'test'}],
            'totalPages': 1
        }
        mock_get.return_value = mock_response

        df = api_to_pandas(self.base_url, self.endpoint, self.params, self.headers)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'test')

    @patch('requests.get')
    def test_successful_multiple_pages(self, mock_get):
        # Mock successful response with multiple pages
        responses = [
            {'data': [{'id': 1, 'name': 'test1'}], 'totalPages': 2},
            {'data': [{'id': 2, 'name': 'test2'}], 'totalPages': 2}
        ]
        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: resp) 
            for resp in responses
        ]

        df = api_to_pandas(self.base_url, self.endpoint, self.params, self.headers)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[1]['name'], 'test2')

    @patch('requests.get')
    def test_empty_response(self, mock_get):
        # Mock empty response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response

        with self.assertRaises(ValueError):
            api_to_pandas(self.base_url, self.endpoint, self.params, self.headers)

    @patch('requests.get')
    def test_with_proxy(self, mock_get):
        # Mock successful response with proxy
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [{'id': 1, 'name': 'test'}],
            'totalPages': 1
        }
        mock_get.return_value = mock_response

        with patch('builtins.open', unittest.mock.mock_open(read_data='proxy1.com\nproxy2.com')):
            with patch('subprocess.run') as mock_run:
                df = api_to_pandas(
                    self.base_url, 
                    self.endpoint, 
                    self.params, 
                    self.headers,
                    use_proxy=True
                )
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

if __name__ == '__main__':
    unittest.main()