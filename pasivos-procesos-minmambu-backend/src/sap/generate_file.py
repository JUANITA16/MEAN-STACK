import os, re
import logging
import requests
import json
from requests.exceptions import HTTPError
from botocore.exceptions import ClientError
from src.common import util

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def convert_generete_to_json(token,paramsQuery):
    response = call_generete_file_to_sap(token, paramsQuery)
    return response.json()

def convert_download_to_json(token,object_key):
    response_download = call_download_file_sap(token,object_key)
    return response_download.json()

def lambda_handler(event,context):
    try:
        logger.info('Inicio funcion lambda_handler')
        params_query = event['queryStringParameters']
        token  = get_access_token()
        response_json = convert_generete_to_json(token,params_query)
        logger.info("Se obtuvo response de generate_file_to_sap "+str(response_json))
        
        dic_respon = {
                'message': 'La creación del plano ha iniciado exitosamente'
        }
        return response_data(200, dic_respon)

    except KeyError as e:
        logger.exception("Error en los parametros enviados")
        return process_error_message(e)
    except ClientError as e:
        logger.exception("Error client aws: "+e.response['Error']['Code'])
        return process_error_message(e)
    except HTTPError as e:
        logger.exception("Se ha presentado un error intentando consumir un api")
        return process_error_message(e)
    except Exception as e:
        logger.exception("Se ha presentado un error no controlado")
        return process_error_message(e)


def service_download_file_sap(token,object_key): 
    url_download = os.environ['baseURL']+"/mambu-sap/api/v1/share-presigned-url-s3"

    stage = os.environ["stage"]

    bucket_name = os.getenv("sapBucket", "s3-test-bucket")

    params_bucket = {
        'bucket_name' : bucket_name,
        'object_key' : object_key
    }

    headers_download = {
        'Authorization': token
    }
    response_api = requests.request("GET", url_download, headers=headers_download, params=params_bucket)
    logger.info("Finaliza servicio share-presigned-url-s3, status code "+str(response_api.status_code))
    return response_api

def call_download_file_sap(token,object_key):
    response_call = service_download_file_sap(token, object_key)
    if response_call.status_code != 200 :
        logger.error("Error consumiendo api de descarga sap, "+response_call.text)
        raise HTTPError(response_call.reason)
    
    if 'information' not in response_call.json() or not response_call.json()['information']:
        logger.error("El archivo sap no se descargó correctamente, "+response_call.text)
        raise HTTPError("El archivo sap no se descargó")
    
    return response_call


def service_generete_file_to_sap(token,params_query):
    logger.info('Inicio funcion service_generete_file_to_sap')
    if 'to' not in params_query or 'from' not in params_query:
        raise KeyError("params 'to' and 'from' are required")
    
    url = os.environ['baseURL']+"/mambu-sap/api/v1/accounting-SAP"
    logger.info(f'api a consumir mambu: {url}')
    logger.info(f'Parametros a enviar al api de mambu: {params_query}. Tipo dato params: {type(params_query)}')
 
    #headers
    headers = {
        'Authorization': token
    }
    #response = requests.request(method="GET",url=url, headers=headers, params=params_query)
    if (re.match(r'\W', params_query['from'])
        or re.match(r'\W', params_query['to'])
        or re.match(r'\W', params_query['user_name'])):
        logger.error("Los caractéres enviados en los parámetros son invalidos.")
        raise HTTPError("Invalid request. The parameters can't start with "
                        + "specials characters.")
        
    
    body = {
        "execution_date": {
            "from": params_query['from'],
            "to": params_query['to']
        },
        "user_name": params_query['user_name']
    }

    logger.info(f'body of POST request : {body}')
    response = requests.post(url, headers = headers, json=body) 
    logger.info("Finaliza servicio accounting-SAP, status code "+str(response.status_code))
    return response


def call_generete_file_to_sap(token,paramsQuery):
    '''Process sap file service response'''
    logger.info('Inicio funcion call_generete_file_to_sap')
    response = service_generete_file_to_sap(token, paramsQuery)
    logger.info('response status code  from service_generate_file_to_sap{}'.format(response.status_code))
    
    if response.status_code != 200 :
        logger.error("Error consumiendo api mambu sap, "+response.text)
        raise HTTPError(response.reason)

    logger.info('response json from service_generate_file_to_sap{}'.format(response.json()))
    
    return response

     
def get_access_token():
    '''Process token service response'''
    secret_id = os.environ['secret_token_mambu']
    response = util.service_access_token(secret_id)
    if response.status_code != 200 :        
        logger.error("Error consumiendo token integracion: "+response.text)
        raise HTTPError(response.reason)    
        
    return response.json()['access_token']


def response_data(http_status: int, body):
    '''return standard response data'''
    return {
            "statusCode":http_status,
            "body": json.dumps(body),
            "headers": {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS"
            },
            "isBase64Encoded": False
        }


def process_error_message(exception):
    '''process and return standard error response data.'''
    error_body = {
            'message': str(exception).replace('\'','')
        }
    return response_data(400, error_body)  
    