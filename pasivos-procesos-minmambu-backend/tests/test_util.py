import os
import json

import unittest
from unittest import mock
from unittest import TestCase
from unittest.mock import patch
from pandas import DataFrame

from src.common import util

class TestUtil(TestCase):    

    def test_validate_key(self):
        key="test"
        data_frame ={
            "Nro":["test"],
        }

        result = util.validate_key(key,DataFrame(data_frame))

        self.assertEqual(result,"test")

 
    def test_format_common_is_correct_correct(self):
        data_frame ={
            "test":["test"],
        }
        columns=["test"]

        data = util.format_common_is_correct(columns,DataFrame(data_frame))

        self.assertEqual(data,[])
    
    def test_format_common_is_correct_None(self):
        data_frame ={

        }
        columns=["test"]

        data = util.format_common_is_correct(columns,DataFrame(data_frame))

        self.assertEqual(data,["test"])

    @patch("src.common.util.dowload_file_s3")
    def test_loadfile_excel(self,mock_response):
        working_directory = os.path.dirname(os.path.abspath(__file__))
        mock_response.return_value = f"{working_directory}/resources/carguecuentasdepositocdt.xlsx"
        data = util.read_excel_file('','','Plantilla')
        self.assertEqual(data.columns.size,16)

    @patch("boto3.s3.transfer.S3Transfer.download_file")
    @patch("boto3.session.Session")
    def test_dowload_file_s3(self,mock_session_class,mock_download):
        mock_session_object = mock.Mock()
        mock_download.return_value = ''        
        mock_session_object.client.return_value = mock_download
        mock_session_class.return_value = mock_session_object
        localfile = util.dowload_file_s3('/prueba','excel.xslx')
        self.assertEqual(localfile,"/tmp/excel.xslx")

    @patch("src.common.util.boto3.client")
    def test_sns(self,mock_session_class):
        mock_client = mock.Mock()
        mock_client.publish.return_value = {
            'ResponseMetadata':
                {
                     'HTTPStatusCode':200
                }
        }
        
        mock_session_class.return_value = mock_client
        status = util.send_message_to_sns({'id':1},'colaprueba')
        self.assertEqual(status,200)

    @patch.dict(os.environ,{"baseURL":"http://lcl"},clear=True)
    @patch("src.common.util.boto3.client")
    def test_ok_service_secret(self,mock):
        payloadValues = {
            'clientId': '1234',
            'clientSecret': '1234',
            'resource': '1234',
            'grantType': '1234',
            'token_type': '1234'
        }       
        mock.return_value = self.SecretTest()
        response = util.service_secret("SecretString", True)
        self.assertEqual(payloadValues, response)

    class SecretTest:
        def get_secret_value(self, SecretId):
            payloadValues = {
            'clientId': '1234',
            'clientSecret': '1234',
            'resource': '1234',
            'grantType': '1234',
            'token_type': '1234'
        }       

            rta = {"SecretString":json.dumps(payloadValues)}
            return rta

    @patch.dict(os.environ, {"baseURL": "xxxxx","secret_token_mambu":"1234"})
    @mock.patch('src.sap.generate_file.requests.request')  # Mock 'requests' module 'get' method.
    @patch("src.common.util.service_secret")
    def test_ok_service_access_token(self,mock_service_secret,mock_get):
        response_service_secret = {
           'client_id':'123',
           'client_secret':'client_secret_test',
           'resource':'resource_test', 
           'grant_type':'grant_type_test',
           'token_type':'token_type_test'
        }
        mock_service_secret.return_value = response_service_secret

        my_mock_response = mock.Mock(status_code=200)     
        my_mock_response.json.return_value = {            
            "data": 
                {
                    "access_token": "access ok"
                }
            
        }
        mock_get.return_value = my_mock_response

        response_test = util.service_access_token("arn:secretid")
        self.assertEqual(response_test.status_code, 200)
        
    def test_round_decimals(self):
        flt_decimal = 3.9222820440170114
        str_decimal = "3.9222820440170114"
        int_decimal = 922
        
        flt_round = util.round_decimals(flt_decimal)
        str_round = util.round_decimals(str_decimal)
        int_round = util.round_decimals(int_decimal)
        
        self.assertEqual(flt_round, 3.92228204401701)
        self.assertEqual(str_round, "3.92228204401701")
        self.assertEqual(int_round, 922)

    def test_round_decimals(self):
        flt_decimal = 3.9222820440170114
        str_decimal = "3.9222820440170114"
        int_decimal = 922
        
        flt_round = util.round_decimals(flt_decimal)
        str_round = util.round_decimals(str_decimal)
        int_round = util.round_decimals(int_decimal)
        
        self.assertEqual(flt_round, 3.92228204401701)
        self.assertEqual(str_round, "3.92228204401701")
        self.assertEqual(int_round, 922)

if __name__ == '__main__':
    unittest.main()