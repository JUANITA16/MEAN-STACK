import os

import pytest
from unittest import mock
from unittest.mock import patch
from fastapi.testclient import TestClient

from src.tests.mocks import mock_typeproduct_schema
from src.tests.fixtures import upload_cc_fixture

class Response:
    def __init__(self, status_code: int, resp: dict) -> None:
        self.status_code = status_code
        self.resp = resp

@patch.dict(os.environ, {"basePath": "/super"})
def import_class_app() -> TestClient:
    from src.petitions.mambu_contabilidad import app
    test_client = TestClient(app)
    return test_client

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_all_results_taxaprodt(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/taxaprodt")
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_taxaprodt_item(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.put("/super/taxaprodt/123/No Aplica",
        json={
            'debittaxaccount': 123,
            'credittaxaccount': 123,
            'debittaxaccountinterest': 123,
            'credittaxaccountinterest': 123,
            'producttypemaestrosunicos': 123,
            'producttypedescription': '123'  
        })
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_all_sap_files(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/files/",
                               params={"from_date": "11", "to_date": "31"})
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_sap_url(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/files/download/testName.txt")
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_cosif_item(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.put("/super/tblCosifAccounting/123",
        json={
            "accounting_account": "210486",
            "cosif": "4310520301", 
            "costcenteraccounting": "000000000001",
            "producttype": "BONO"
        })
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_cosif_item_not_numeric(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = [{'message':'Los valores enviados no son numéricos'}]

    response = test_client.put("/super/tblCosifAccounting/123",
        json={
            "accounting_account": "<img src=1 onerror=alert(0)>",
            "cosif": "<img src=1 onerror=alert(0)>", 
            "costcenteraccounting": "<img src=1 onerror=alert(0)>",
            "producttype": "CDT"
        })
    assert response.status_code == 405
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_insert_cosif_item_not_numeric(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.post("/super/tblCosifAccounting",
        json={
            "accounting_account": "123456",
            "cosif": "7890", 
            "costcenteraccounting": "98685120000",
            "producttype": "BONO"
        })
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_insert_cosif_item_not_numeric(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = [{'message':'Los valores enviados no son numéricos'}]

    response = test_client.post("/super/tblCosifAccounting",
        json={
            "accounting_account": "<img src=1 onerror=alert(0)>",
            "cosif": "<img src=1 onerror=alert(0)>", 
            "costcenteraccounting": "<img src=1 onerror=alert(0)>",
            "producttype": "CDT"
        })
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_all_results_tblCosifAccountung(mock_cont):
    test_client = import_class_app()
   
    mock_cont.return_value = {}
    
    expect = {}

    response = test_client.get("/super/tblCosifAccounting")
    assert response.status_code == 200
    assert response.json() == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_all_results_tblCosifAccountung(mock_cont):
    test_client = import_class_app()
   
    mock_cont.return_value = {}
    
    expect = {}

    response = test_client.get("/super/tblCosifAccounting")
    assert response.status_code == 200
    assert response.json() == expect

@patch.dict(os.environ, {"secretApiKeyMambuContabilidad":"test","baseURL": "/superr"})
@mock.patch("src.petitions.mambu_contabilidad.consume_service")
@mock.patch("src.petitions.mambu_contabilidad.get_secret_value")
def test_consume_mambu_contabilidad(mock_secre,mock_request):
    from src.petitions.mambu_contabilidad  import consume_mambu_contabilidad

   
    mock_request.return_value.json.return_value = {}
    
    mock_secre.return_value="test"
    expect = {}

    response = consume_mambu_contabilidad("GET","test")
 
    assert response == expect

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_mass_interest_events(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/rates", 
                               params={"initial_date": "a", 
                                       "final_date": "b",
                                       "consecutive": "c"})
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_rates(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.post("/super/rates", params={"update_date": "2021", "user": "user-test"})
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_create_reprocess(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = []
    expect = {'mambu_response': [], 'message': 'Ejecución contabilidad constitucion exitosa'}

    response = test_client.post("/super/reprocess", 
        json={
            "date": "2022-02-02",
            "user": "user-test", 
            "enddate": "2022-02-02",
            "event_type": "constitucion"
        })
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_create_reprocess_no_movements(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = "No hay movimientos para el día de hoy"
    expect = {'mambu_response': "No hay movimientos para el día de hoy", 'message': 'Ejecución contabilidad constitucion exitosa'}

    response = test_client.post("/super/reprocess", 
        json={
            "date": "2022-02-02",
            "user": "user-test", 
            "enddate": "2022-02-02",
            "event_type": "constitucion"
        })
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_create_reprocess_no_movements_no_user(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = "No hay movimientos para el día de hoy"
    expect = {'mambu_response': "No hay movimientos para el día de hoy", 'message': 'Ejecución contabilidad constitucion exitosa'}

    response = test_client.post("/super/reprocess", 
        json={
            "date": "2022-02-02",
            "enddate": "2022-02-02",
            "event_type": "constitucion"
        })
    assert response.json() == expect
    assert response.status_code == 200

def test_create_reprocess_fail():
    test_client = import_class_app()
    response = test_client.post("/super/reprocess", 
        json={
            "date": "2022-02-02",
            "user": "user-test", 
            "enddate": "2022-02-02",
            "event_type": "pepe"
        })
    assert response.status_code == 422

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_reprocess_table(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/reprocess", 
                               params={"initial_date": "a", "final_date": "b"})
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_typeprod_table(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/typeproduct", 
                               params={"producttypedescription": "pepe", 
                                       "producttypemaestrosunicos": "nombre-test",
                                       "producttype": "CDT",
                                       })
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_insert_typeproduct(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value ={}
    expect ={}
    typeproduct = mock_typeproduct_schema
    response = test_client.post("/super/typeproduct", json=typeproduct)
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_insert_typeproduct_with_values(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value ={"test":"ok"}
    expect =  {'message':'typeproduct-exist'}
    typeproduct = mock_typeproduct_schema
    response = test_client.post("/super/typeproduct", json=typeproduct)
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_insert_typeproduct_exception(mock_cont):
    test_client = import_class_app()

    mock_cont.side_effect = Exception("Test-error")
    typeproduct = mock_typeproduct_schema
    response = test_client.post("/super/typeproduct", json=typeproduct)
    assert response.status_code == 500

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_typeproduct_ok(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value ={}
    expect ={}
    typeproduct = mock_typeproduct_schema
    response = test_client.put("/super/typeproduct/456/TEST", json=typeproduct)
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_typeproduct_change_producttypedescription(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value ={}
    expect ={}
    typeproduct = mock_typeproduct_schema
    response = test_client.put("/super/typeproduct/456/TEST2", json=typeproduct)
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_typeproduct_exist(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value ={"test":"ok"}
    expect = {'message':'typeproduct-exist'}
    typeproduct = mock_typeproduct_schema
    response = test_client.put("/super/typeproduct/456/TEST2", json=typeproduct)
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_update_typeproduct_except(mock_cont):
    test_client = import_class_app()

    mock_cont.side_effect = Exception("Test-error")
    typeproduct = mock_typeproduct_schema
    response = test_client.put("/super/typeproduct/456/TEST", json=typeproduct)
    assert response.status_code == 500

@patch.dict(os.environ, {"bucketCC":"bucket-test"})
@pytest.mark.parametrize("upload_cc_fixture", [{"mock_contabilidad_param" : {"url":"XXXXXXXXX"}, "mock_base64_param": "decoding-test", "mock_consume_param": None}], indirect=True)
def test_upload_cc_none(upload_cc_fixture):
    test_client = import_class_app()
    expect = {"message": "Ocurrió un error al cargar archivo"}

    body_request = {
        "file_content":"content_test",
        "user_upload":"test_user"
    }
    response = test_client.post("/super/upload-cc", json=body_request)
    assert response.json() == expect
    assert response.status_code == 500

@patch.dict(os.environ, {"bucketCC":"bucket-test"})
@pytest.mark.parametrize("upload_cc_fixture", [{"mock_contabilidad_param" : {"url":"XXXXXXXXX"}, "mock_base64_param": "decoding-test", "mock_consume_param": Response(400, {"ok": "ok"})}], indirect=True)
def test_upload_cc_err(upload_cc_fixture):
    test_client = import_class_app()
    expect = {'message': "Ocurrió un error al cargar archivo" }
    
    body_request = {
        "file_content":"content_test",
        "user_upload":"test_user"
    }
    response = test_client.post("/super/upload-cc", json=body_request)
    assert response.json() == expect
    assert response.status_code == 400  

@patch.dict(os.environ, {"bucketCC":"bucket-test"})
@pytest.mark.parametrize("upload_cc_fixture", [{"mock_contabilidad_param" : {"url":"XXXXXXXXX"}, "mock_base64_param": "decoding-test", "mock_consume_param": Response(200, {"ok": "ok"})}], indirect=True)
def test_upload_cc_ok(upload_cc_fixture):
    test_client = import_class_app()
    expect = {'message': 'Carga de archivo exitosa, consulte el estado de la ejecución en unos segundos'}
    
    body_request = {
        "file_content":"content_test",
        "user_upload":"test_user"
    }
    response = test_client.post("/super/upload-cc", json=body_request)
    assert response.json() == expect
    assert response.status_code == 200  

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_rates_update_detail(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/rates-update", 
                               params={"process_date":"test_date","file_id": "id_test"})
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad")
def test_get_rates_update_ppal(mock_cont):
    test_client = import_class_app()

    mock_cont.return_value = {}
    expect = {}

    response = test_client.get("/super/rates-update", 
                               params={"initial_date":"date_init","final_date": "date_final"})
    assert response.json() == expect
    assert response.status_code == 200

@mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad",
            side_effect=Exception("Test Error!"))
class TestExceptions:
    client = import_class_app()

    def test_get_all_cosif_err(self, mock_cont):
        response = self.client.get("/super/tblCosifAccounting", 
                                params={"initial_date":"date_init","final_date": "date_final"})
        assert response.status_code == 500
        assert response.json() == [{'message':'Ocurrió un error: Test Error!'}]

    def test_put_cosif_err(self, mock_cont):
        response = self.client.put(
            "/super/tblCosifAccounting/12", 
            json={
                "accounting_account": "123",
                "cosif": "2312", 
                "costcenteraccounting": "33",
                "producttype": "CDT"
                })
        assert response.json() == [{'message':'Ocurrió un error: Test Error!'}] 
        assert response.status_code == 500

    def test_post_cosif_err(self, mock_cont):
        response = self.client.post(
            "/super/tblCosifAccounting", 
            json={
                "accounting_account": "123",
                "cosif": "2312", 
                "costcenteraccounting": "33",
                "producttype": "CDT"
                })
        assert response.json() == [{'message':'Ocurrió un error: Test Error!'}] 
        assert response.status_code == 500

    def test_get_all_tax_err(self, mock_cont):
        response = self.client.get("/super/taxaprodt")
        assert response.status_code == 500
        assert response.json() == [{'message':'Ocurrió un error: Test Error!'}]

    def test_put_tax_err(self, mock_cont):
        response = self.client.put(
            "/super/taxaprodt/12/No", 
            json={
                'debittaxaccount': 123,
                'credittaxaccount': 123,
                'debittaxaccountinterest': 123,
                'credittaxaccountinterest': 123,
                'producttypemaestrosunicos': 123,
                'producttypedescription': '123'  
            })
        assert response.json() == [{'message':'Ocurrió un error: Test Error!'}] 
        assert response.status_code == 500

    def test_post_tax_err(self, mock_cont):
        response = self.client.post(
            "/super/taxaprodt", 
            json={
                'debittaxaccount': 123,
                'producttypedescription': '123'
            })
        assert response.json() == [{'message':'Ocurrió un error: Test Error!'}] 
        assert response.status_code == 500

    def test_get_all_files_err(self, mock_cont):
        response = self.client.get("/super/files/",
                                   params={"from_date": "2023-05-05",
                                           "to_date": "2023-05-05"})
        assert response.status_code == 500
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}

    def test_get_url_err(self, mock_cont):
        response = self.client.get("/super/files/download/test.xlsx",
                                   params={"from_date": "2023-05-05",
                                           "to_date": "2023-05-05"})
        assert response.status_code == 500
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}

    def test_upd_rates_err(self, mock_cont):
        response = self.client.post("/super/rates",
                                   params={"update_date": "2023-05-05",
                                         "user": "Cris"})
        assert response.json() == {'message':'Error a la hora de realizar la petición: Test Error!'}
        assert response.status_code == 500

    def test_mass_int_err(self, mock_cont):
        response = self.client.get("/super/rates",
                                   params={"initial_date": "2023-05-05",
                                           "final_date": "Cris"})
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}
        assert response.status_code == 500

    def test_acc_reprs_err(self, mock_cont):
        response = self.client.post(
            "/super/reprocess",                 
            json={
                "date": "2022-02-02",
                "user": "user-test", 
                "enddate": "2022-02-02",
                "event_type": "constitucion"
                })
        assert response.json() == {'message':'Ejecución contabilidad constitucion fallida. Error 500'}
        assert response.status_code == 500

    def test_get_repr_err(self, mock_cont):
        response = self.client.get("/super/reprocess",
                                   params={"initial_date": "2023-05-05",
                                           "final_date": "2024-04-04"})
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}
        assert response.status_code == 500

    def test_get_typeproduct_err(self, mock_cont):
        response = self.client.get("/super/typeproduct")
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}
        assert response.status_code == 500

    def test_post_typeproduct(self, mock_cont):
        response = self.client.post(
            "/super/typeproduct",                 
            json={
                "producttypedescription": "super",
                "producttypemaestrosunicos": "duper",
                "producttype": "CDT",
                "user": "user-test"
                })
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}
        assert response.status_code == 500

    @patch.dict(os.environ, {"bucketCC":"bucket-test"})    
    def test_post_uploadcc(self, mock_cont):
        response = self.client.post(
            "/super/upload-cc",                 
            json={
                "file_content": "super",
                "user_upload": "duper"
                })
        assert response.json() == {'message':'Ocurrió un error: Test Error!'}
        assert response.status_code == 500

    def test_get_rates_update(self, mock_cont):
        response = self.client.get("/super/rates-update")
        assert response.json() == {'message':'Error consumiendo el servicio mambu: Test Error!'}
        assert response.status_code == 500