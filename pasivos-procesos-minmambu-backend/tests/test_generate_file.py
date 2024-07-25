import os
import json
import  logging

import pytest
import unittest
from unittest import TestCase
from unittest.mock import patch
from unittest import  mock
from requests.exceptions import HTTPError

from src.sap import generate_file

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MockResponse:
    def __init__(self, status_code: int, content: dict) -> None:
        self.status_code = status_code
        self.content = content
    
    def json(self):
        return self.content

@mock.patch("src.sap.generate_file.call_generete_file_to_sap")
def test_convert_generete_to_json(mock_gen):
    mock_gen.return_value = MockResponse("200", {"test": "ok"})
    resp = generate_file.convert_generete_to_json("a", "b")
    assert resp == {"test": "ok"}

@mock.patch("src.sap.generate_file.call_download_file_sap")
def test_convert_download(mock_dow):
    mock_dow.return_value = MockResponse("200", {"test": "ok"})
    resp = generate_file.convert_download_to_json("a", "b")
    assert resp == {"test": "ok"}

class TestGenerateFile(TestCase):

    @patch("src.sap.generate_file.response_data")
    @patch("src.sap.generate_file.convert_download_to_json")
    @patch("src.sap.generate_file.convert_generete_to_json")
    @patch("src.sap.generate_file.get_access_token")
    def test_ok_lambda_handler(self,mock_acces_token,mock_generete_file,mock_download,mock_response_data):
        event = {
            'queryStringParameters':{
                'from':'2021-10-01',
                'to':'2021-10-01',
                'user_name':'test_user'
            }
        }
        mock_acces_token.return_value = "token-de-prueba"

        expected_rp = {
            'detail': 'plano generado correctamente',
            'filename': 'archivo_prueba_123.txt'
        }
        mock_generete_file.return_value = expected_rp

        dict_download = {
            'information': "information_test"
        }
        
        mock_download.return_value = dict_download

        dic_respon = {
            'detail': "plano generado correctamente",
            'filename': "archivo_prueba_123.txt",
            'information': 'information_test'
        }
        rtaObj = {
            "statusCode":200,
            "body": json.dumps(dic_respon),
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS"                
            },
            "isBase64Encoded": False
        }
        mock_response_data.return_value = rtaObj

        response_test = generate_file.lambda_handler(event,"")
        self.assertEqual(json.dumps(rtaObj), json.dumps(response_test))

    @patch("src.sap.generate_file.response_data")
    @patch("src.sap.generate_file.convert_generete_to_json")
    @patch("src.sap.generate_file.get_access_token")
    def test_without_filename_lambda_handler(self,mock_acces_token,mock_generete_file,mock_response_data):
        event = {
            'queryStringParameters':{
                'from':'2021-10-01',
                'to':'2021-10-01',
                'user_name':'test_user'
            }
        }
        mock_acces_token.return_value = "token-de-prueba"

        expected_rp = {
            'detail': 'plano generado correctamente',
            'filename': ''
        }
        mock_generete_file.return_value = expected_rp

        
        dic_respon = {
            'detail': "plano generado correctamente",
            'filename': "",
            'information': ''
        }
        rtaObj = {
            "statusCode":200,
            "body": json.dumps(dic_respon),
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS"                
            },
            "isBase64Encoded": False
        }
        mock_response_data.return_value = rtaObj

        response_test = generate_file.lambda_handler(event,"")
        self.assertEqual(json.dumps(rtaObj), json.dumps(response_test))

    @patch.dict(os.environ, {"baseURL": "xxxxx"})
    @patch.dict(os.environ, {"stage": "dev"})
    @mock.patch('src.sap.generate_file.requests.request')  # Mock 'requests' module 'get' method.
    def test_ok_service_download_file_sap(self,mock_get):
        my_mock_response = mock.Mock(status_code=200)     
        my_mock_response.json.return_value = {            
            "data": 
                {
                    "information": "information test."
                }
            
        }
        mock_get.return_value = my_mock_response
        token = '123token'
        object_key = 'filename_test.txt'
        response_test = generate_file.service_download_file_sap(token,object_key)
        self.assertEqual(response_test.status_code, 200)

    @patch("src.sap.generate_file.service_download_file_sap")
    def test_error_status_code_call_download_file_sap(self,mock_response):
        mock_response.return_value.status_code = 400
        self.assertRaises(HTTPError, generate_file.call_download_file_sap,"token","information_test.txt")
    
    @patch("src.sap.generate_file.service_download_file_sap")
    def test_not_content_information_call_download_file_sap(self,mock_response):
        expected_rp = {
            "error": ""
        }
        mock_response.return_value.status_code = 200
        mock_response.return_value.json.return_value = expected_rp
        self.assertRaises(HTTPError, generate_file.call_download_file_sap,"token","information_test.txt")

    def test_response_data(self):
        body = {"message":"test"}
        rtaObj = {
            "statusCode":200,
            "body": json.dumps(body),
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS"                
            },
            "isBase64Encoded": False
        }
        response = generate_file.response_data(200,body)        
        self.assertEqual(json.dumps(rtaObj), json.dumps(response))

    def test_response_error(self):
        body = {'message':"error"}
        rtaObj = {
            "statusCode":400,
            "body": json.dumps(body),
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS"                
            },
            "isBase64Encoded": False
        }
        response = generate_file.process_error_message("error")        
        self.assertEqual(json.dumps(rtaObj), json.dumps(response))

    @patch.dict(os.environ,{"baseURL":"http://lcl","secret_token_mambu":"arn:secret:token_mambu"},clear=True)
    @patch("src.common.util.service_access_token")
    def test_error_access_token(self,mock_response):
        expected_token = {
                        "error": "unauthorized_client",
                        "error_description": "AADSTS700016: Application with identifier 'e1918055-4f55-47c3-8b9c-74b58eed40712' was not found in the directory 'Banco BTG Pactual S.A '. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant.\r\nTrace ID: 9ea70718-591d-48b1-ac6b-272e4ecccb00\r\nCorrelation ID: b6aa4be4-33bc-4f5b-a75b-789bec812b39\r\nTimestamp: 2021-10-19 13:39:59Z",
                        "error_codes": [
                            700016
                        ],
                        "timestamp": "2021-10-19 13:39:59Z",
                        "trace_id": "9ea70718-591d-48b1-ac6b-272e4ecccb00",
                        "correlation_id": "b6aa4be4-33bc-4f5b-a75b-789bec812b39",
                        "error_uri": "https://login.microsoftonline.com/error?code=700016"
                    }
        mock_response.return_value.status_code = 400
        mock_response.return_value.json.return_value = expected_token
        self.assertRaises(HTTPError, generate_file.get_access_token)

    @patch.dict(os.environ,{"baseURL":"http://lcl","secret_token_mambu":"arn:secret:token_mambu"},clear=True)
    @patch("src.common.util.service_access_token")
    def test_ok_access_token(self,mock_response):
        expected_token = {
                            "token_type": "Bearer",
                            "expires_in": "3599",
                            "ext_expires_in": "3599",
                            "expires_on": "1634680830",
                            "not_before": "1634676930",
                            "resource": "api://dde47416-93ec-483e-8a2e-49745d93d87a",
                            "access_token": "token-de-prueba"
                        }
        mock_response.return_value.status_code = 200
        mock_response.return_value.json.return_value = expected_token
        token = generate_file.get_access_token()
        self.assertEqual("token-de-prueba", token)

    @patch("src.sap.generate_file.service_generete_file_to_sap")
    def test_fail_response_call_generete_file_to_sap(self,mock_response):
        mock_response.return_value.status_code = 400
        self.assertRaises(HTTPError, generate_file.call_generete_file_to_sap,"token",{"to":"2021-09-20","from":"2021-09-16","user_name":"test_user"})

@mock.patch("src.sap.generate_file.get_access_token")
class TestLambdaExceptions:
    
    def test_key_err(self, mocked_tok):
        mocked_tok.side_effect = KeyError("Test KeyError!")
        result = generate_file.lambda_handler({"queryStringParameters": "1"},"")
        assert result["body"] == '{"message": "Test KeyError!"}'
        
        
    def test_http_err(self, mocked_tok):
        mocked_tok.side_effect = HTTPError("Test HTTPError!")
        result = generate_file.lambda_handler({"queryStringParameters": "1"},"")
        assert result["body"] == '{"message": "Test HTTPError!"}'
        
        
    def test_exc_err(self, mocked_tok):
        mocked_tok.side_effect = Exception("Test Exception!")
        result = generate_file.lambda_handler({"queryStringParameters": "1"},"")
        assert result["body"] == '{"message": "Test Exception!"}'

@patch.dict(os.environ, {"baseURL": "http://test.com"})
def test_service_generete_file_to_sap(requests_mock):
    requests_mock.post("http://test.com/mambu-sap/api/v1/accounting-SAP", 
                       json={"test": "prueba"}, 
                       status_code = 200)
    resp = generate_file.service_generete_file_to_sap(
        token="test-token",
        params_query={"from": "2022","to": "2023","user_name": "Cris"}
        )
    assert resp.json() == {'test': 'prueba'}

@patch.dict(os.environ, {"baseURL": "/super"})
def test_service_generete_file_to_sap_invalid():
    with pytest.raises(HTTPError) as er:
        generate_file.service_generete_file_to_sap(
            token="123",
            params_query={"from": "@",
                          "to": "@",
                          "user_name": "@"})

if __name__ == '__main__':
    unittest.main()