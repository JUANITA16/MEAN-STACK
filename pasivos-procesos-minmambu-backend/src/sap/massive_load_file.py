import logging, uuid, os
from src.common import util, common_dynamodb
import pytz
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    '''main function'''
    arn_sns = os.environ['snsLoadFile']
    # For each record
    logger.info("event"+ str(event['Records']))
    for record in event['Records']:
        
        # Get Bucket and Key
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        last_modified = datetime.datetime.now(pytz.timezone('America/Bogota')).strftime('%Y-%m-%d %H:%M')
        logger.info("Ingreso a Procesar Excel")
        procesar_excel(bucket, key, arn_sns, last_modified)        

def add_error_message_to_dynamo(key, columns_fail):
    logger.info('### entrando a add_error_message_to_dynamo') 
    stage = os.environ["stage"]
    
    message_error="Formato de archivo inválido, columnas no especificadas: | "
    for column in columns_fail:
        message_error=message_error+ column+" | "

    result={
        "detail":message_error,
        "rowId":"0",
        "status":"Error",
        "codeStatus":"400"
    }
    logger.info(f'resultado para insertar en la tabla: {result}') 
    dynamo_table = 'ddb-pasivos-minmambu-backend-'+str(stage)+'-tblResultMassiveAccounts'
    logger.info(f'tabla de dynamo: {dynamo_table}') 
    response=common_dynamodb.update_item(dynamo_table, key, {"#attrName": "results_per_row"}, "SET #attrName = list_append(#attrName, :attrValue)", {":attrValue": [result]})
    logger.info(f'datos insertados correctamente: {response}') 
    logger.info('### saliendo a add_error_message_to_dynamo') 

def procesar_excel(bucket,key,arn_sns,last_modified):
    df = util.read_excel_file(bucket,key,'Plantilla')
    logger.info('columnas cargadas excel '+ key + ': ' + df.columns) 
    id_file = str(uuid.uuid4())
    upload_type = key.split("/")[0]
    logger.info(f'Tipo de carga: {upload_type}' )

    if upload_type == 'CDT':
        columns = ['Nro','Destinatario','Producto','Nombre a mostrar','Tasa de interés','Nemotécnico','ISIN','Fecha emisión','Fecha de vencimiento','Tasa nominal','Tipo tasa referencia','Spread','Exento iva','Notas']
        key_errors =util.format_common_is_correct(columns,df)
        if len(key_errors)==0:
            
            df['Tasa de interés'].fillna(0, inplace=True)
            df = df.fillna('')
            for indice_fila, fila in df.iterrows():        
                if fila['Producto']:
                    dicc = {
                        'id':id_file,
                        'fila':fila['Nro'],
                        'destinatario':fila['Destinatario'],
                        'producto':fila['Producto'],
                        'nombre_mostrar':fila['Nombre a mostrar'],
                        'tasa_interes': util.round_decimals(fila['Tasa de interés']),
                        'nemotecnico':fila['Nemotécnico'],
                        'isin':fila['ISIN'],
                        'fecha_emision':str(fila['Fecha emisión']).split(" ")[0],
                        'fecha_vencimiento':str(fila['Fecha de vencimiento']).split(" ")[0],
                        'tasa_nominal':fila['Tasa nominal'],
                        'tipo_tasa_referencia':fila['Tipo tasa referencia'],
                        'tasa_referencia':fila['Tasa de referencia'],
                        'periodicidad':fila['Periodicidad'],
                        'spread':fila['Spread'],
                        'exento_iva':("TRUE" if str(fila['Exento iva']).upper()=="SI" else "FALSE" if str(fila['Exento iva']).upper()=="NO" else fila['Exento iva']),
                        'notas':fila['Notas'],
                        'tipo_carga':upload_type,
                        'nombre_archivo':key,
                        'fecha':last_modified
                    }
                    status = util.send_message_to_sns(dicc, arn_sns)
                    logger.debug("Status sns: "+ str(status) +", fila:" + str(indice_fila) + ", archivo: " + key)
            else:
                logger.info(f'Fila vacia: {indice_fila}')
        else:
            logger.info('Error con el formato del archivo, faltan columnas') 
            add_error_message_to_dynamo(key.split("/")[2],key_errors)
    elif upload_type == 'CC':
        columns = ['Nro','Destinatario','Producto','Nombre a mostrar','Tasa de interés','Monto Maximo Retiro','Fuente de impuestos','Tasa de interés sobregiro','Límite del Sobregiro','Consecutivo LESS','Exento GMF','Fecha Vencimiento','Exento IVA','Notas']
        key_errors =util.format_common_is_correct(columns,df)
        if len(key_errors)==0:
            for columna in ('Tasa de interés sobregiro',
                            'Límite del Sobregiro',
                            'Tasa de interés'):
                df[columna].fillna(0, inplace=True)
            df.fillna('', inplace=True)
            for indice_fila, fila in df.iterrows():
                fecha_vencimiento = ""
                if isinstance(fila["Fecha Vencimiento"], datetime.datetime):
                    fecha_vencimiento = fila["Fecha Vencimiento"].strftime("%Y-%m-%dT%X-05:00")
                elif str(fila["Fecha Vencimiento"]) != "0":
                    fecha_vencimiento = fila["Fecha Vencimiento"]
                
                dicc = {
                        'id': id_file,
                        'fila': fila['Nro'],
                        'destinatario': fila['Destinatario'],
                        'producto': fila['Producto'],
                        'nombre_mostrar': fila['Nombre a mostrar'],
                        'tasa_interes': util.round_decimals(fila['Tasa de interés']),
                        'monto_maximo_retiro': fila['Monto Maximo Retiro'],
                        'fuente_impuestos': fila['Fuente de impuestos'],
                        'tasa_sobregiro': util.round_decimals(fila['Tasa de interés sobregiro']),
                        'limite_sobregiro': fila['Límite del Sobregiro'],
                        'fecha_vencimiento': fecha_vencimiento,
                        'exento_iva':("TRUE" if str(fila['Exento IVA']).upper()=="SI" else "FALSE" if str(fila['Exento IVA']).upper()=="NO" else fila['Exento IVA']),
                        'exento_gmf': fila['Exento GMF'],
                        'consecutivo_less': str(fila['Consecutivo LESS']),
                        'notas': fila['Notas'],
                        'tipo_carga':upload_type,
                        'nombre_archivo':key,
                        'fecha':last_modified
                    }
                status = util.send_message_to_sns(dicc, arn_sns)
                logger.debug("Status sns: "+ str(status) +", fila:" + str(indice_fila) + ", archivo: " + key)
        else:
            logger.info('Error con el formato del archivo, faltan columnas')
            add_error_message_to_dynamo(key.split("/")[2],key_errors)
            

