from hashlib import new
import boto3
from boto3.dynamodb.types import TypeDeserializer
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def execute_statement(query:str,parameters: list):
    try:
        logger.info(f'Inicio funcion execute_statement. Lista de parametros recibidos: {parameters}')
        dynamodb = boto3.client('dynamodb')
        response = dynamodb.execute_statement(Statement = query,Parameters = parameters)
        logger.info('Fin funcion execute_statement')
        return response
    except BaseException as e:
        logger.info('Ocurrió un error en la funcion execute_statement')
        logger.info(str(e))
        return []

    


def update_item(table:str, key:str, name_expression:dict, u_expression:str, value_expression:dict):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)

    response = table.update_item(
        Key={
            "file_id":key
        },
        UpdateExpression=u_expression,
        ExpressionAttributeNames=name_expression,
        ExpressionAttributeValues=value_expression
    )
        
    return response

def put_items(table:str, input:dict):
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table(table)

        result = table.put_item(Item=input)
        response_metadata =  result['ResponseMetadata']
        logger.error('response_metadata: '+ str(response_metadata))
        status = response_metadata['HTTPStatusCode']
        logger.info('HTTPStatusCode de put_items: '+ str(status))
        if status == 200 :
            result_desciption= 'ok'
        else:
            result_desciption = 'Error en HTTPStatusCode'
    except Exception as e:
        logger.error('Error en put_items: '+ str(e))
        result_desciption = 'Error no controlado en BD'
    return result_desciption



def from_dynamodb_to_json_list(list_dynamo:list):
    try:
        logger.info('Inicio funcion from_dynamodb_to_json_list')
        d = TypeDeserializer()
        new_list = []
        for i in list_dynamo:
            new_list.append({k: d.deserialize(value=v) for k, v in i.items()})

        logger.info('Fin funcion from_dynamodb_to_json_list')
        return new_list
    except BaseException as e:
        logger.info('Ocurrió un error en la funcion from_dynamodb_to_json_list')
        logger.info(str(e))
        return []
