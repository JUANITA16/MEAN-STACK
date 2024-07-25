import logging, base64, os, datetime, json, random, re
import boto3
import pytz
import pandas as pd
from src.common import common_dynamodb


logger = logging.getLogger()
logger.setLevel(logging.INFO)


#Método creado con el obetivo de generar el consecutivo
def generate_consecutive():
    my_date = datetime.datetime.now(pytz.timezone('America/Bogota')).strftime('%Y%m%d%H%M%S')
    
    logger.info("my_date: "+str(my_date))

    #Encuentra el numero random
    num_random = random.randrange(0,999) 

    #Encuentra la cantidad de ceros que restan
    str_random = "0" * (3 - len(str(num_random))) 

    #Concatena los ceros con el numero random
    str_random = str_random + str(num_random) 
    logger.info('str_random: '+str(str_random))
    
    return str(my_date) + str(str_random)


#Método creado con el objetivo de obtener la información del archivo de carga
def get_information_file(body):
    #Se obtiene el nombre del archivo cargado   
    if re.match(r'^[a-zA-Z0-9_\-\ ]*\.(xlsx|xls|csv)$',body['file_name']):
        original_file_name = body['file_name']

        #Invoca la función de generacion del consecutivo
        consecutive = generate_consecutive()

        #Se obtiene el nombre del producto
        upload_type = body['product']

        #Se cambia el nombre del producto en el caso de ser cuenta corriente para eliminar el espacio
        upload_type = 'CC' if upload_type == 'Cuentas Corrientes' else upload_type

        #Se concatena el consecutivo con el nombre original
        file_name = f"cargue_{upload_type}-{str(consecutive)}.xlsx"

        user_upload = body['user_upload']
        
        if body['file_content']=="" :
            file_content = 'No content'
        # Validamos que no se inserten fórmulas csv
        elif re.match(r'^[A-Za-z, ]+$', user_upload):
            file_content = base64.b64decode(body['file_content'])
        else:
            user_upload = "Invalid User Name"
            file_content = "Invalid User Name"
    
    else:
        file_content = 'Invalid File Name'
        file_name=''
        consecutive = ''
        original_file_name=''
        upload_type=''
        user_upload=''
    return consecutive, file_name, original_file_name, upload_type, file_content, user_upload


#Método creado con el objetivo de subir el archivo a S3
def put_object_file(file_name, upload_type, file_content,consecutive):
    try:
        BUCKET_NAME = os.environ['bucketMassiveAccount']
        s3_client = boto3.client('s3')

        #Se obtiene la fecha de carga
        date_upload = datetime.datetime.now(pytz.timezone('America/Bogota')).strftime('%Y-%m-%d')
        logger.info('date_upload: '+str(date_upload))

        excel_file = pd.ExcelFile(file_content)
        rute = str(upload_type)+'/' + str(date_upload) + '/'+ consecutive +'/' + str(file_name)
        logger.info('rute'+str(rute))

        s3_response = s3_client.put_object(Bucket=BUCKET_NAME, Key=rute, Body=file_content)   
        logger.info('S3 Response: {}'.format(s3_response))
        return "ok"
    except Exception as e:
        messge = 'Error en put_object_file: '+ e.__str__()
        logger.error(messge)
        return messge


#Método creado con el objetivo de insertar el resultado en la BD
def put_database(consecutive, file_name, original_file_name, upload_type, user_upload):
    stage = os.environ["stage"]

    dynamo_table = 'ddb-pasivos-minmambu-backend-'+str(stage)+'-tblResultMassiveAccounts'
    logger.info('dynamo_table: '+ str(dynamo_table))

    #Se obtiene la fecha de carga
    date_upload = datetime.datetime.now(pytz.timezone('America/Bogota')).strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info('date_upload: '+str(date_upload))


    #Armar json
    dict_input = {
		'file_id': consecutive,
		'date_upload': date_upload,
		'filename': file_name,
		'original_filename': original_file_name,
        'results_per_row':[],
		'upload_type':upload_type,
		'user_upload':user_upload
	}

    result = common_dynamodb.put_items(dynamo_table,dict_input)

    return result


#Método creado con el objetivo de realizar todo el proceso de subir un archivo
def process_upload_file(body):
    body_json = json.loads(body)

    #Se obtiene los atributos necesarios para subir a S3 y para insertar en la BD
    consecutive, file_name, original_file_name, upload_type, file_content,user_upload = get_information_file(body_json)
    
    response  = {
        "headers": {
            "content-type":"application/json",
            "Access-Control-Allow-Origin":"*",
            "Access-Control-Allow-Methods":"GET,OPTIONS",
            "Access-Control-Allow-Headers":"*"
        }
    }

    status_code = 200
    description = 'ok'
    
    if file_content not in ('No content', 'Invalid User Name','Invalid File Name'):

        response_put =put_object_file(file_name, upload_type, file_content,consecutive)

        if response_put=='ok' :

            response_db = put_database(consecutive, file_name, original_file_name, upload_type, user_upload)

            description = response_db

            if description!='ok' :
                status_code = 400
            
        else: 
            status_code = 400
            description = response_put

    else:
        description = file_content
        status_code = 400

        
    dic_response = {
        'description': description
    }
    response["body"] =  json.dumps(dic_response)
    response["statusCode"] =  status_code

    logger.info('response: '+str(response))
    return response


#Método main
def lambda_handler(event, context):
    body = event['body']
    return process_upload_file(body)


    