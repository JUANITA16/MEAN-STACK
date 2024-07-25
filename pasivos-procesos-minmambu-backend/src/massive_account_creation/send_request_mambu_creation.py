import logging
import json
import os
import traceback

from src.common import common_dynamodb, util
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_cdt(dict):
    secret = os.environ['sc_token_accounts']
    token = util.get_access_token(secret)
    logger.info("Obtuve el Token correctamente")
    
    body = {
        'destinatario':str(dict['destinatario']),
        'producto':dict['producto'],
        'nombre':dict['nombre_mostrar'],
        'tasa_interes':str(dict['tasa_interes']),
        'exento_iva':dict['exento_iva'],
        'fecha_emision':dict['fecha_emision'],
        'fecha_vencimiento':dict['fecha_vencimiento'],
        'isin':str(dict['isin']),
        'nemotecnico':dict['nemotecnico'],
        'periodicidad':dict['periodicidad'],
        'tasa_nominal':str(dict['tasa_nominal']),
        'tipo_tasa_referencia':dict['tipo_tasa_referencia'],
        'tasa_referencia':dict['tasa_referencia'],
        'spread':str(dict['spread']),
        'notas':dict['notas']
	}
	#headers
    headers = {
	    'Authorization': token
	}
    url = os.environ['baseURL'] + "/mambu-accounts/api/v1/deposits/createDepositCDT"
    response = requests.post(url, headers = headers, json=body)
    logger.info("Petición enviada a lambda de Integración de CD") 
    return response.status_code, response.text


def create_cc(dic):
    
    """
        Función encargada de ejecutar el Create CC (Cuentas Corrientes) Mambu
        
        Parámetros: 
            dic = Diccionario con la información
    """
    
    secret = os.environ['sc_token_accounts']
    token = util.get_access_token(secret)
    logger.info("Obtuve el token con exito")

    body = {
        "destinatario":str(dic['destinatario']),
        "producto":dic['producto'],
        "tasa_interes":str(dic['tasa_interes']),
        "nombre":dic['nombre_mostrar'],
        "notas":dic['notas'],
        "limite_sobregiro":str(dic['limite_sobregiro']),
        "fecha_vencimiento":str(dic['fecha_vencimiento']),
        "exento_iva":dic['exento_iva'],
        "exento_gmf":dic['exento_gmf'],
        "consecutivo_less":str(dic['consecutivo_less']),
        "monto_maximo_retiro":str(dic['monto_maximo_retiro']),
        "tasa_interes_sobregiro":str(dic['tasa_sobregiro']),
        "fuente_impuestos": str(dic['fuente_impuestos'])
    }

    headers = {
	    'Authorization': token
	}
    
    url = os.environ['baseURL'] + "/mambu-accounts/api/v1/deposits/createCurrentAccount"
    response = requests.post(url, headers = headers, json=body) 
    logger.info("Petición enviada a la Lambda de Integración CC")
    return response.status_code, response.text


def error_response(code, message):

    """
        Función encargada de generar el formato de errores en el body de respuesta

        Parámetros:
            codeResponse : código de respuesta
            message : mensaje de respuesta
    """

    body = { "error": message }
    response = {
        "statusCode": code,
        "body": json.dumps(body),
        "headers": {
            "content-type": "application/json"
        },
        "isBase64Encoded": False,
    }
    return response


def run_array(array): 
    
    details = ""
    for x in range(len(array)):
        details += ("" if 'errorSource' not in array[x] else f"{array[x]['errorSource']}: ")
        details += f"{array[x]['errorReason']} | "
    
    return details


def lambda_handler(event, context):
    try:
        dynamo_table = 'ddb-pasivos-minmambu-backend-'+os.environ["stage"]+'-tblResultMassiveAccounts'

        logger.info("Inicio del Proceso")
        
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        logger.info(f"Obtención del Evento: {message}")
        # tipo/yyyy-MM-dd/archivo-key.xlsx
        key = message['nombre_archivo'].split("-")[3].split(".")[0]
        fila = message['fila']
        product = message['tipo_carga']
        
        if "CDT" in product.upper():
            logger.info("Ingresar a Crear un CDT")
            status_code, text = create_cdt(message)
            logger.info(f"Code de la creación del CDT: {status_code}")
        elif "CC" in product.upper():
            logger.info("Ingresar a crear Cuenta Corriente")
            status_code, text = create_cc(message)
            logger.info(f"Code de la creación de CC: {status_code}")       
                 
        resp = json.loads(text)
        
        if status_code == 201:
            details = f"Id: {resp['id']} | Name: {resp['name']} | EncodedKey: {resp['encodedKey']} "
            if "CDT" in product.upper():
                details += f"| Isin: {resp['_set_info_adicional']['_isin_cdt']} | Nemotecnico: {resp['_set_info_adicional']['_nemotecnico_cdt']}"
        else:
            if "message" in resp:
                details = str(resp["message"])
            else:
                details = run_array(resp["errors"])
        
        result = {
            "rowId": str(fila),
            "status": ("OK" if status_code == 201 else "Error"),
            "codeStatus":str(status_code),
            "detail": details
        }
        
        common_dynamodb.update_item(dynamo_table, key, {"#attrName": "results_per_row"}, "SET #attrName = list_append(#attrName, :attrValue)", {":attrValue": [result]})
        logger.info("Actualización completada")
        

        response = {
            "statusCode": 200,
            "body": json.dumps({"message": "Ejecución Finalizada"}),
            "headers": {
                "content-type": "application/json", 
            },
            "isBase64Encoded": False,
        }
        
        return response
        
    except Exception as e:
        message = e.args[0]
        code_response = 500
        logger.info(f"Error: {message}")
        logger.info(traceback.format_exc())
        return error_response(code_response, str(message))
        
    