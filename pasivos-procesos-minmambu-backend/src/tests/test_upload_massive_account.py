import os
import json
import base64

import unittest
from unittest import  mock
from unittest import TestCase

from src.sap import upload_massive_account

class TestUploadMassiveAccount(TestCase):

    @mock.patch('src.sap.upload_massive_account.generate_consecutive')
    def test_get_information_file_no_content(self, mock_consecutive):
        mock_consecutive.return_value = '20220216172107383'

        body = {
            'file_name' : 'file_name_test.xlsx',
            'product' : 'CDT',
            'file_content' : '',
            'user_upload':'user_test'
        }

        consecutive, file_name, original_file_name, upload_type, file_content,user_upload = upload_massive_account.get_information_file(body)

        self.assertEqual(consecutive,'20220216172107383')
        self.assertEqual(file_name,'cargue_CDT-20220216172107383.xlsx')
        self.assertEqual(original_file_name,'file_name_test.xlsx')
        self.assertEqual(upload_type,'CDT')
        self.assertEqual(file_content,'No content')
        self.assertEqual(user_upload,'user_test')

    @mock.patch('src.sap.upload_massive_account.generate_consecutive')
    def test_get_information_file_invalid_user_name(self, mock_consecutive):
        mock_consecutive.return_value = '20220216172107383'

        body = {
            'file_name' : 'file_name_test.xlsx',
            'product' : 'CDT',
            'file_content' : 'test',
            'user_upload':'Atehourta<script>||</script@#${}>'
        }

        consecutive, file_name, original_file_name, upload_type, file_content,user_upload = upload_massive_account.get_information_file(body)

        self.assertEqual(consecutive,'20220216172107383')
        self.assertEqual(file_name,'cargue_CDT-20220216172107383.xlsx')
        self.assertEqual(original_file_name,'file_name_test.xlsx')
        self.assertEqual(upload_type,'CDT')
        self.assertEqual(file_content,'Invalid User Name')
        self.assertEqual(user_upload,'Invalid User Name')

    @mock.patch('src.sap.upload_massive_account.generate_consecutive')
    def test_get_information_file_invalid_file_name(self, mock_consecutive):
        mock_consecutive.return_value = '20220216172107383'

        body = {
            'file_name' : 'file_name_test?=1+2;.xlsx',
            'product' : 'CDT',
            'file_content' : 'content_ok',
            'user_upload':'user_test'
        }

        consecutive, file_name, original_file_name, upload_type, file_content,user_upload = upload_massive_account.get_information_file(body)

        self.assertEqual(consecutive,'')
        self.assertEqual(file_name,'')
        self.assertEqual(original_file_name,'')
        self.assertEqual(upload_type,'')
        self.assertEqual(file_content,'Invalid File Name')
        self.assertEqual(user_upload,'')

    @mock.patch.dict(os.environ,{"bucketMassiveAccount":"s3-minmambu-dev-massive-accounts"})
    @mock.patch("boto3.client")
    def test_put_object_file_ok(self,mock_client):

        mock_put = mock.Mock()
        mock_put.put_object.return_value = 'Upload ok!'
  
        mock_client.return_value = mock_put

        file_name = 'file_name_test-20220216172107383.xlsx'
        upload_type = 'CDT'

        working_directory = os.path.dirname(os.path.abspath(__file__))
        with open(f"{working_directory}/test.xlsx", "rb") as test_file:
            encoded = base64.b64encode(test_file.read())
        file_content = base64.b64decode(encoded)
        consecutivo="1234"

        response_put = upload_massive_account.put_object_file(file_name, upload_type, file_content,consecutivo)

        self.assertEqual(response_put , "ok")

    @mock.patch.dict(os.environ,{"bucketMassiveAccount":"s3-minmambu-dev-massive-accounts"})
    def test_put_object_file_error(self):

        file_name = 'file_name_test-20220216172107383.xlsx'
        upload_type = 'CDT'
        file_content = 'bits_content_test'
        consecutivo="1234"
        response_put = upload_massive_account.put_object_file(file_name, upload_type, file_content,consecutivo)

        self.assertEqual(response_put , "Error en put_object_file: [Errno 2] No such file or directory: 'bits_content_test'")

    @mock.patch.dict(os.environ,{"stage":"dev"})
    @mock.patch("src.common.common_dynamodb.put_items")
    def test_put_database_ok(self, mock_common_database):
        consecutive = '20220216172107383'
        original_file_name = 'file_name_test.xlsx'
        file_name = 'file_name_test-20220216172107383.xlsx'
        file_content = 'base64_content_test'
        upload_type = 'CDT'
        user_upload = ''

        mock_common_database.return_value = 'ok'

        response_database = upload_massive_account.put_database(consecutive, file_name, original_file_name, upload_type, user_upload)

        self.assertEqual(response_database , 'ok')

    @mock.patch('src.sap.upload_massive_account.put_database')
    @mock.patch('src.sap.upload_massive_account.put_object_file')
    @mock.patch('src.sap.upload_massive_account.get_information_file')
    def test_process_upload_file_content_ok(self, mock_information, mock_put, mock_database):
        consecutive = '20220216172107383'
        original_file_name = 'file_name_test.xlsx'
        file_name = 'file_name_test-20220216172107383.xlsx'
        file_content = 'base64_content_test'
        upload_type = 'CDT'    
        user_upload = 'user_test'    

        mock_information.return_value = consecutive, file_name, original_file_name, upload_type, file_content, user_upload

        mock_put.return_value = "ok"

        mock_database.return_value = "ok"


        dic_response = {
            'description': 'ok'
        }

        response_expected  = {
            "statusCode": 200,
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS",
                "Access-Control-Allow-Headers":"*"
            },
            "body": json.dumps(dic_response)
        }

        

        body = """{
            "file_name" : "file_name_test.xlsx",
            "product" : "CDT",
            "file_content" : "base64_test",
            "user_upload" : "user_test"
        }"""

        response_process = upload_massive_account.process_upload_file(body)

        self.assertEqual(response_process , response_expected)

    @mock.patch('src.sap.upload_massive_account.put_object_file')
    @mock.patch('src.sap.upload_massive_account.get_information_file')
    def test_process_upload_file_content_esc_puts3_error(self, mock_information, mock_put):
        consecutive = '20220216172107383'
        original_file_name = 'file_name_test.xlsx'
        file_name = 'file_name_test-20220216172107383.xlsx'
        file_content = 'base64_content_test'
        upload_type = 'CDT'        
        user_upload = 'user_test'

        mock_information.return_value = consecutive, file_name, original_file_name, upload_type, file_content, user_upload

        mock_put.return_value = "Error en put_object"


        dic_response = {
            'description': 'Error en put_object'
        }

        response_expected  = {
            "statusCode": 400,
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS",
                "Access-Control-Allow-Headers":"*"
            },
            "body": json.dumps(dic_response)
        }

        body = """{
            "file_name" : "file_name_test.xlsx",
            "product" : "CDT",
            "file_content" : "base64_test",
            "user_upload" : "user_test"
        }"""

        response_process = upload_massive_account.process_upload_file(body)

        self.assertEqual(response_process , response_expected)

    @mock.patch('src.sap.upload_massive_account.put_database')
    @mock.patch('src.sap.upload_massive_account.put_object_file')
    @mock.patch('src.sap.upload_massive_account.get_information_file')
    def test_process_upload_file_content_esc_database_error(self, mock_information, mock_put, mock_database):
        consecutive = '20220216172107383'
        original_file_name = 'file_name_test.xlsx'
        file_name = 'file_name_test-20220216172107383.xlsx'
        file_content = 'base64_content_test'
        upload_type = 'CDT'      
        user_upload = 'user_test'  

        mock_information.return_value = consecutive, file_name, original_file_name, upload_type, file_content, user_upload

        mock_put.return_value = "ok"

        mock_database.return_value = 'Error no controlado en BD'


        dic_response = {
            'description': 'Error no controlado en BD'
        }

        response_expected  = {
            "statusCode": 400,
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS",
                "Access-Control-Allow-Headers":"*"
            },
            "body": json.dumps(dic_response)
        }

        body = """{
            "file_name" : "file_name_test.xlsx",
            "product" : "CDT",
            "file_content" : "base64_test",
            "user_upload" : "user_test"
        }"""

        response_process = upload_massive_account.process_upload_file(body)

        self.assertEqual(response_process , response_expected)

    @mock.patch('src.sap.upload_massive_account.get_information_file')
    def test_process_upload_file_no_content(self, mock_information):
        consecutive = '20220216172107383'
        original_file_name = 'file_name_test.xlsx'
        file_name = 'file_name_test-20220216172107383.xlsx'
        file_content = 'No content'
        upload_type = 'CDT'        
        user_upload = 'user_test'

        mock_information.return_value = consecutive, file_name, original_file_name, upload_type, file_content, user_upload

        body = """{
            "file_name" : "file_name_test.xlsx",
            "product" : "CDT",
            "file_content" : "",
            "user_upload": "user_test"
        }"""

        response_process = upload_massive_account.process_upload_file(body)

        dic_respon = {
            'description': file_content
        }
        response_expected = {
            "statusCode": 400,
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS",
                "Access-Control-Allow-Headers":"*"
            },
            "body": json.dumps(dic_respon)
        }

        self.assertEqual(response_process , response_expected)

    @mock.patch('src.sap.upload_massive_account.process_upload_file')
    def test_lambda_handler(self, mock_process):
        
        dic_respon = {
            'description': 'ok'
        }

        response_process  = {
            "statusCode": 200,
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS",
                "Access-Control-Allow-Headers":"*"
            },
            "body": json.dumps(dic_respon)
        }

        mock_process.return_value = response_process

        body = """{
            'file_name' : 'file_name_test.xlsx',
            'product' : 'CDT',
            'file_content' : 'base64_test',
            'user_upload' : 'user_test'
        }"""
        event = {
            'body' : body
        }

        response_lambda = upload_massive_account.lambda_handler(event,'')

        self.assertEqual(response_lambda , response_process)

if __name__ == '__main__':
    unittest.main()