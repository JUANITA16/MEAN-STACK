#python
from typing import List
import logging
import os

#FastAPI
from fastapi import FastAPI
from fastapi import status
from fastapi import Query, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from src.massive_account_creation import  models
from src.common import common_dynamodb
from mangum import Mangum

# -------------------------------------------------------------------------------------------------------------
# GLOBAL VARIABLES
# -------------------------------------------------------------------------------------------------------------

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = FastAPI()

app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["access-control-allow-methods"]
)
stage = os.getenv("stage", "dev")

# -------------------------------------------------------------------------------------------------------------
# PATH OPERATIONS
# -------------------------------------------------------------------------------------------------------------

base_path = '/minmambu/api/v1'

@app.get(path=base_path + "/mambu/massiveAccounts/results",
        response_model=List[models.ResultsFile],
        status_code=status.HTTP_200_OK,
        summary="Get the files loaded between two dates")
def get_all_results(
                start_date:str = Query(
                                    None,
                                    min_length=10,
                                    max_length=20,
                                    title="Start date",
                                    description="indicates the start date to find",
                                    example="2021-12-01"
                                ),
                end_date:str = Query(
                                    None,
                                    min_length=10,
                                    max_length=20,
                                    title="End date",
                                    description="indicates the end date to find",
                                    example="2021-12-31"
                                )
        ):
        headers = {
                "content-type":"application/json",
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS"
        }

        try:
                logger.info(f'Inicio funcion get_all_results. Fecha inicial: {start_date}, fecha final: {end_date}')
                query = f'SELECT * FROM "ddb-pasivos-minmambu-backend-{stage}-tblResultMassiveAccounts" WHERE date_upload BETWEEN ? AND ?'
                parameters = [{"S":start_date},{"S":end_date}]
                response = common_dynamodb.execute_statement(query,parameters)
                logger.info(f'El response obtenido fue: {response}. El tipo de dato de response es {type(response)}')
                items = response["Items"]
                items = common_dynamodb.from_dynamodb_to_json_list(items)
                return JSONResponse(content = items, headers=headers)
        except BaseException as e:
                logger.info('Ocurrió un error en la funcion get_all_results')
                logger.info(str(e))
                return JSONResponse(content = [{'message':'No existen datos a retornar'}], 
                                    headers=headers,
                                    status_code=500) 
        


@app.get(path=base_path + "/mambu/massiveAccounts/results/{file_id}",
        response_model=List[models.ResultsFile],
        status_code=status.HTTP_200_OK,
        summary="Get the files loaded by id")
def get_results_by_id(
                file_id:str = Path(
                                    title="Id of the file loaded",
                                    description="indicates the id of the file loaded",
                                    example="20210119123032999"
                                )
        ):
        try:
                logger.info(f'Inicio funcion get_results_by_id. File id: {file_id}')
                query = f'SELECT * FROM "ddb-pasivos-minmambu-backend-{stage}-tblResultMassiveAccounts" WHERE file_id = ?'
                parameters = [{"S":file_id}]
                response = common_dynamodb.execute_statement(query,parameters)
                logger.info(f'El response obtenido fue: {response}. El tipo de dato de response es {type(response)}')
                items = response["Items"]
                items = common_dynamodb.from_dynamodb_to_json_list(items)
                return items
        except BaseException as e:
                logger.info('Ocurrió un error en la funcion get_results_by_id')
                logger.info(str(e))
                
                return JSONResponse(content=[{'message':'No existen datos a retornar'}],
                                    status_code=500)
        


logger.info(f'#Inicio de ejecucion lambda. Stage: {stage}#')
handler = Mangum(app)