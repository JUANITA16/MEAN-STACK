import os, logging, json
import pathlib
import boto3
from pandas import read_excel
import requests
from requests.exceptions import HTTPError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def dowload_file_s3(bucket, key):
    '''download from s3 and read file xlsx'''
    s3_client = boto3.client('s3')
    local_filename = '/tmp/' + key
    pathlib.Path(local_filename).parent.mkdir(parents=True, exist_ok=True) # Crear ruta del archivo si no existe
    s3_client.download_file(bucket, key, local_filename)
    return local_filename

def read_excel_file(bucket, key, sheet ):
    '''download from s3 and read file xlsx'''
    local_filename = dowload_file_s3(bucket,key)    
    df = read_excel(local_filename, sheet_name = sheet)
    return df

def validate_key(key,columns):
    if not key in columns:
        return key



def format_common_is_correct(columns,data_frame):
    logger.info("### starting format_common_is_correct")

    key_errors=[validate_key(column,data_frame.columns) for column in columns]
    key_errors= list(filter(None, key_errors))

    logger.info(f"key_errors: {key_errors}")
    logger.info("### exit format_common_is_correct")

    return key_errors

def round_decimals(interest_rate):
    if isinstance(interest_rate, (float, int)):
        return round(interest_rate, 14)
    else:
        return interest_rate[0:16]  

def send_message_to_sns(message:dict,arn_ans):
    logger.info(f'mensaje a enviar a sns: {message}')
    client = boto3.client('sns')

    response = client.publish(
        TargetArn=arn_ans,
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )

    logger.info(f'Id transaccion enviado a SNS: {message["id"]}. Response: {response["ResponseMetadata"]["HTTPStatusCode"]}')
    return response['ResponseMetadata']['HTTPStatusCode']

def service_secret(secret_id:str,json_return:bool):
    logger.info("Entré a Service Secret")
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(
        SecretId = secret_id
    )
    secret_string = response['SecretString']

    if json_return :
        return json.loads(secret_string)

    return secret_string

def response_data(http_status: int(),body):
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


def get_access_token(secret_id):
    '''Process token service response'''
    response = service_access_token(secret_id)
    if response.status_code != 200 :        
        logger.error("Error consumiendo api token "+response.text)
        raise HTTPError(response.reason)    
        
    return response.json()['access_token']

def service_access_token(secret_id):
    logger.info("Inicio funcion service_access_token")
    base_url = os.environ['baseURL']
    url = base_url + "/auth/token" #url
    payload_values = service_secret(secret_id, True)
    payload = f"client_id={payload_values['client_id']}&client_secret={payload_values['client_secret']}&resource={payload_values['resource']}&grant_type={payload_values['grant_type']}&token_type={payload_values['token_type']}"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    #call token service
    response = requests.request("GET", url, headers=headers, data=payload)
    logger.info("Se realiza consulta de token, status code "+str(response.status_code))
    return response


def consume_service(method:str,url:str,headers:dict={},params:dict={},body:str=''):
    logger.info('# Start consume_service')
    try:
        #Ejecucion de Api Mambu
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params= params,
            data=body,
            timeout=25
        )

        return response
    
    except BaseException as e:
        logger.error('Ocurrió un error en la funcion consume_service.Error:' + e.__str__())
        return None


def get_secret_value(secret_id):
    session = boto3.session.Session()
    client = session.client('secretsmanager')    
    
    response = client.get_secret_value(
        SecretId = secret_id
    )
    secret_value = response['SecretString']

    logger.info('Se obtiene correctamente el valor del secreto')
    return secret_value
