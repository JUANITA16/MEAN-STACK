import os
import json

import pytest
from unittest import mock
from unittest.mock import patch

from src.massive_account_creation import send_request_mambu_creation as srmc

@mock.patch("src.massive_account_creation.send_request_mambu_creation.util.get_access_token")
@mock.patch.dict(os.environ,{"baseURL":"http://prueba.com", "sc_token_accounts":"arn:secret:xxxxx"}, clear=True)
def test_create_cdt(gat_mock,requests_mock):
    gat_mock.return_value = "token test"
    body = {
        "destinatario": "prueba",
        "producto": "CdPrueba",
        "nombre_mostrar": "Prueba Prueba",
        "tasa_interes": "1231",
        "exento_iva": "1231",
        "fecha_emision": "2021-11-08",
        "fecha_vencimiento": "2022-01-19",
        "isin": "A83453453",
        "nemotecnico": "12312312",
        "periodicidad": "2",
        "tasa_nominal": "2342",
        "tipo_tasa_referencia": "PV",
        "tasa_referencia": "12",
        "spread": "2",
        "notas": "sadasdaSFASD"
    }
    
    requests_mock.post("http://prueba.com/mambu-accounts/api/v1/deposits/createDepositCDT", json={"test": "prueba"}, status_code = 200)
    
    assert srmc.create_cdt(body) == (200, '{"test": "prueba"}')

@mock.patch("src.massive_account_creation.send_request_mambu_creation.util.get_access_token")
@mock.patch.dict(os.environ,{"baseURL":"http://prueba.com", "sc_token_accounts":"arn:secret:xxxxx"}, clear=True)
def test_create_cc(gat_mock,requests_mock):
    gat_mock.return_value = "token test"

    body = {
        "destinatario":"prueba",
        "producto":"prueba_test",
        "tasa_interes":"4",
        "nombre_mostrar":"prueba_nombre",
        "notas":"obs",
        "limite_sobregiro":"5.4123",
        "fecha_vencimiento":"fecha-test",
        "exento_iva":"SI",
        "exento_gmf":"NO",
        "consecutivo_less":"5000",
        "monto_maximo_retiro":"4000000",
        "tasa_sobregiro":"345345",
        "fuente_impuestos": "test"
    }

    requests_mock.post("http://prueba.com/mambu-accounts/api/v1/deposits/createCurrentAccount", json={"test": "prueba"}, status_code = 200)

    assert srmc.create_cc(body) == (200, '{"test": "prueba"}')

def test_run_array():
    array = [{"errorSource":"test_source", "errorReason": "test_reason"}]
    
    expected = "test_source: test_reason | "
    
    srmc.run_array(array) == expected

def test_error_response():
  # Average
  code = 500
  message = "Prueba del error_response"

  expected = {
    "statusCode": code,
    "body": json.dumps({ "error":  message } ),
    "headers": {"content-type": "application/json"},
    "isBase64Encoded": False,
  }
  # Act
  result = srmc.error_response(code, message)
  # Assert
  assert result == expected

@pytest.mark.parametrize(
    "event",
    [
        {
            'Records':[{
                'Sns':{
                    'Message':json.dumps({"nombre_archivo":"CDT/2022-01-24/Prueba_Flujo_CDT_038-202201242019402.xlsx","fila":1,"tipo_carga":"cdt"})
                }
            }]
        }
    ]
)
@patch.dict(os.environ,{"stage":"dev", "baseURL":"http:prueba.com"},clear=True)
@mock.patch("src.massive_account_creation.send_request_mambu_creation.create_cdt")
@mock.patch("src.massive_account_creation.send_request_mambu_creation.common_dynamodb.update_item")
def test_lambda_handler(update_mock, ccdt_mock, event):
    
    ccdt_mock.return_value = 201, json.dumps({"id": "test", "name": "test", "encodedKey": "sdasdfaew23edASD", "_set_info_adicional": {"_isin_cdt": "SADFASD", "_nemotecnico_cdt": "dasdf"}})
    
    expected= {
            "statusCode": 200,
            "body": json.dumps({"message": "Ejecución Finalizada"}),
            "headers": {
                "content-type": "application/json", 
            },
            "isBase64Encoded": False,
    }
    
    assert srmc.lambda_handler(event, "") == expected
    
    ccdt_mock.return_value = 400, json.dumps({"errors": [{"errorSource": "test", "errorReason": "asdfasdf"}]})
    
    expected= {
            "statusCode": 200,
            "body": json.dumps({"message": "Ejecución Finalizada"}),
            "headers": {
                "content-type": "application/json", 
            },
            "isBase64Encoded": False,
    }
    
    assert srmc.lambda_handler(event, "") == expected
    
    ccdt_mock.return_value = 400, json.dumps({"message": "testtt"})
    
    expected= {
            "statusCode": 200,
            "body": json.dumps({"message": "Ejecución Finalizada"}),
            "headers": {
                "content-type": "application/json", 
            },
            "isBase64Encoded": False,
    }
    
    assert srmc.lambda_handler(event, "") == expected

@patch.dict(os.environ,{"stage":"dev"},clear=True)
def test_lambda_handler_err():
    
    expected = {
        "statusCode": 500,
        "body": json.dumps({"error": "string indices must be integers"}),
        "headers": {
            "content-type": "application/json"
        },
        "isBase64Encoded": False,
    }
    
    assert srmc.lambda_handler("", "") == expected