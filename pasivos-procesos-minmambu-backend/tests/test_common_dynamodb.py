from decimal import Decimal

import boto3
import pytest
from moto import mock_aws
from botocore.stub import Stubber

from src.common import common_dynamodb

table_creation_args = {
    "TableName":'TestTable',
    "KeySchema":[
        {
            'AttributeName': 'test_hash',
            'KeyType': 'HASH'  # Partition key
        },
        {
            'AttributeName': 'test_range',
            'KeyType': 'RANGE'  # Sort key
        }
    ],
    "AttributeDefinitions":[
        {
            'AttributeName': 'test_hash',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'test_range',
            'AttributeType': 'S'
        }

    ],
    "ProvisionedThroughput": {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }}

@pytest.fixture(scope='function')
def dynamodb():
    with mock_aws():
        db_client =  boto3.client('dynamodb', region_name='us-east-1')
        db_client.create_table(**table_creation_args)
        yield db_client

def test_execute_statement():
    # Set up test
    stubber = Stubber(boto3.client('dynamodb', region_name='us-east-1'))
    expected_response =  {
        "Items": [{'test_hash': {'S': 'test_val1'}, 
                   'test_range': {'S': 'test_val2'}}]
        }
    stubber.add_response("execute_statement", expected_response)
    
    # Testing
    with stubber:
        
        statement_input = "Select * From TestTable"
        resp = common_dynamodb.execute_statement(statement_input, [{"S": "aa"}])
    assert resp == []

def test_put_items(dynamodb):
    res =common_dynamodb.put_items(
        "TestTable", 
        {'test_hash': 'test_val1', 
        'test_range': 'test_val2'})
    assert res == 'ok' 

def test_from_dynamodb_to():
    items = [
        {"TestField": {"S": "TestString"}}, 
        {"TestField2": {"N": "0"}}]
    result = common_dynamodb.from_dynamodb_to_json_list(items)
    
    assert result == [{"TestField": "TestString"},
                      {"TestField2": Decimal(0)}]

def test_from_dynamodb_to():
    items = "pepe"
    result = common_dynamodb.from_dynamodb_to_json_list(items)
    
    assert result == []