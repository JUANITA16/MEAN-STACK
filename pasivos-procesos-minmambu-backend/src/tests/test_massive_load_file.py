import os

from unittest import TestCase
from unittest.mock import patch

from src.sap import massive_load_file as mlfc

class TestMasiveLoadFile(TestCase):    

    @patch.dict(os.environ, {"stage": "test"})

    @patch("src.common.common_dynamodb.update_item")
    def test_add_error_message_to_dynamo(self,mock_update):
        columns_fail=["test","test1"]
        key="123"
        response ={}
        mock_update.return_value=response
        mlfc.add_error_message_to_dynamo(key,columns_fail)

    @patch.dict(os.environ, {"snsLoadFile": "topicdt"})
    @patch("src.common.util.format_common_is_correct")
    @patch("src.common.util.send_message_to_sns")
    @patch("src.common.util.dowload_file_s3")
    def test_loadfile_excel_cdt(self,mock_response_s3,mock_response_sns,mock_format):
        working_directory = os.path.dirname(os.path.abspath(__file__))
        mock_response_s3.return_value = f"{working_directory}/resources/carguecuentasdepositocdt.xlsx"
        mock_response_sns.return_value = 200
        mock_format.return_value=([])
        event = {
            'Records': [
                {'s3':
                    {
                        'bucket':{
                            'name':'buckets3'
                        },
                        'object':{
                            'key':'CDT/2022-01-21/123/Prueba_Flujo_CDT_004-202201211704687.xlsx',
                        }
                    }
                }
            ]
        }
        
        mlfc.lambda_handler(event,{})

    @patch.dict(os.environ, {"snsLoadFile": "topiccc"})
    @patch("src.common.util.format_common_is_correct")
    @patch("src.common.util.send_message_to_sns")
    @patch("src.common.util.dowload_file_s3")
    def test_loadfile_excel_cc(self,mock_response_s3,mock_response_sns,mock_format):
        working_directory = os.path.dirname(os.path.abspath(__file__))
        mock_response_s3.return_value = f"{working_directory}/resources/carguecuentasdepositocc.xlsx"
        mock_response_sns.return_value = 200
        mock_format.return_value=([])
        event = {
            'Records': [
                {'s3':
                    {
                        'bucket':{
                            'name':'buckets3'
                        },
                        'object':{
                            'key':'CC/2022-01-28/123/Prueba_Flujo_CC_dasdaadd_ASDdsa.xlsx',
                        }
                    }
                }
            ]
        }
        
        mlfc.lambda_handler(event,{})