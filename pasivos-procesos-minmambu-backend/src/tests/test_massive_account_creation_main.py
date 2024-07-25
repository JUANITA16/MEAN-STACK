import os
import json

from unittest import  mock
from unittest.mock import patch
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from src.massive_account_creation import  main
from src.tests.mocks import mock_massive_account_creation

def import_class_app() -> TestClient:
    from src.massive_account_creation.main import app
    test_client = TestClient(app)
    return test_client

class TestMassiveAccountCreationMain:
    @mock.patch('src.massive_account_creation.main.common_dynamodb.execute_statement',MagicMock(return_value={'Items':json.dumps(mock_massive_account_creation)}))
    @mock.patch('src.massive_account_creation.main.common_dynamodb.from_dynamodb_to_json_list',MagicMock(return_value=mock_massive_account_creation))
    @patch.dict(os.environ,{"stage":"dev"})
    def test_get_results_by_id(self):
        result = main.get_results_by_id("123456789")
        assert result == mock_massive_account_creation

@mock.patch('src.massive_account_creation.main.common_dynamodb.execute_statement',
            return_value={'Items':json.dumps(mock_massive_account_creation)})
@mock.patch('src.massive_account_creation.main.common_dynamodb.from_dynamodb_to_json_list',
            return_value=mock_massive_account_creation)
def test_main_all_results(mock_json, mock_dynamo):
    test_client = import_class_app()
    resp = test_client.get("/minmambu/api/v1/mambu/massiveAccounts/results",
                           params={"start_date": "2021-12-01",
                                   "end_date": "2021-12-31"})
    
    assert resp.json() == mock_massive_account_creation

@mock.patch('src.massive_account_creation.main.common_dynamodb.execute_statement',
            side_effect=Exception("Fatal Error!"))
class TestExceptions:
    
    client = import_class_app()
    
    def test_all_results_ex(self, mock_dyn):
        resp = self.client.get("/minmambu/api/v1/mambu/massiveAccounts/results",
                               params={"start_date": "2021-12-01",
                                       "end_date": "2021-12-31"})
        assert resp.status_code == 500
        
        
    def test_id_results_ex(self, mock_dyn):
        resp = self.client.get("/minmambu/api/v1/mambu/massiveAccounts/results/15")
        assert resp.status_code == 500