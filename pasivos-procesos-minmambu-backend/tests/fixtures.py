import pytest
from unittest import mock

@pytest.fixture
def upload_cc_fixture(request):
    mock_contabilidad_param = request.param.get("mock_contabilidad_param", {})
    mock_base64_param = request.param.get("mock_base64_param", "")
    mock_consume_param = request.param.get("mock_consume_param", "")

    with mock.patch("src.petitions.mambu_contabilidad.consume_mambu_contabilidad") as mock_cont, \
         mock.patch("src.petitions.mambu_contabilidad.base64.b64decode") as mock_base64, \
         mock.patch("src.petitions.mambu_contabilidad.consume_service") as mock_consume:
        
        mock_cont.json.return_value = mock_contabilidad_param
        mock_base64.return_value = mock_base64_param
        mock_consume.return_value = mock_consume_param

        yield mock_cont, mock_base64, mock_consume