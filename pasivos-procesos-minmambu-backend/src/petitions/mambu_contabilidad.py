import logging, uuid, os, json, base64, re
from typing import Optional
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from mangum import Mangum
from pydantic  import BaseModel, root_validator
from fastapi import FastAPI
from src.common.util import consume_service, get_secret_value
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = FastAPI()


app.add_middleware(
        CORSMiddleware,
        allow_origins=[os.getenv("origURL")] ,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["access-control-allow-methods"]
)

base_path =  os.getenv('basePath')
ERROR_BASE = 'Ocurrió un error: '
MAMBU_ERROR = 'Error consumiendo el servicio mambu: '

class ProductType(str, Enum):
    CDT = "CDT"
    BONO = "BONO"

class UploadCc(BaseModel):
    file_content: str
    user_upload: str

class Updatetaxaprodt(BaseModel):
    producttypemaestrosunicos: Optional[int] = None
    producttypedescription: Optional[str] = None
    producttype: Optional[ProductType] = None
    credittaxaccountinterest: Optional[int] = None
    credittaxaccount: Optional[int] = None
    debittaxaccountinterest: Optional[int] = None
    debittaxaccount: Optional[int] = None
    debittaxaccountemission: Optional[int] = None
    credittaxaccountemission: Optional[int] = None
    debittaxaccountinterestpaymet: Optional[int] = None
    credittaxaccountinterestpaymet: Optional[int] = None
    debittaxaccountcapitalpaymet: Optional[int] = None
    credittaxaccountcapitalpaymet: Optional[int] = None
    debittaxaccountgmf: Optional[int] = None
    credittaxaccountgmf: Optional[int] = None
    user: Optional[str] = None
    
class Cosif(BaseModel):
    accounting_account: Optional[str] = None
    cosif: Optional[str] = None
    costcenteraccounting: Optional[str] = None
    producttype: Optional[ProductType] = None
    isValid: Optional[bool] = None
    @root_validator(pre=True)
    def is_valid_numeric(cls, values):
        """Valida que los atributos accounting_account, cosif y costcenteraccounting sean numéricos

        Args:
            values (dict): evento con todos los valores recibidos, 

        Returns:
            bool: True o False si cumplió la validación
        """
        expression_only_numeric = r'^[0-9]+$'
        if "accounting_account" in values and not re.match(expression_only_numeric, values["accounting_account"]):
                values["isValid"] = False
                return values
        
        if "cosif" in values and not re.match(expression_only_numeric, values["cosif"]):
                values["isValid"] = False
                return values

        if "costcenteraccounting" in values and not re.match(expression_only_numeric, values["costcenteraccounting"]):
                values["isValid"] = False
                return values
        
        values["isValid"] = True
        return values

class TypeProduct(BaseModel):
    producttypedescription: str
    producttypemaestrosunicos: str
    producttype: ProductType
    user: str
    
class Reprocess(BaseModel):
    date: Optional[str] = None
    user: Optional[str] = None
    enddate: Optional[str] = None
    event_type: str
    event_path: Optional[str] = None
    
    @root_validator(pre=True)
    def event_in_list(cls, values):
        """Valida que el evento sea válido, si lo es procede a devolver alguno de los endpoints
        en el lugar donde antes estaba event_type, tiene que ser alguno de la lista valid_events, en
                caso de añadir otro evento modificar la lista y el diccionario path_list

        Args:
            values (dict): evento con todos los valores recibidos, 

        Raises:
            ValueError: Error que especifica el valor del parámetro event_type

        Returns:
            dict: endpoint que consumirá el backend
        """
        
        # Modificar si se requiere otro evento
        valid_events = ["constitucion", "interes", "vencimientos_capital", 
                        "vencimientos_gmf","pago_rendimientos"]
        
        event_type = values["event_type"]
        if event_type not in valid_events:
            raise ValueError("event_type not valid, must be constitucion, interes or vencimientos")
        #Añadir el path asociado al evento
        path_list = {
            "constitucion": "/mambu-accounting/api/v1/movements/constitutionDominus",
            "interes": "/mambu-accounting/api/v1/transactions/dailyInterestDominus",
            "vencimientos_capital": "/mambu-accounting/api/v1/movements/capitalPayDominus",
            "vencimientos_gmf": "/mambu-accounting/api/v1/transactions/taxesGMFCapitalDominus",
            "pago_rendimientos": "/mambu-accounting/api/v1/movements/yieldPayDominus"
            }
        values["event_path"] = path_list[event_type]
        
        return values


def consume_mambu_contabilidad(method,path,bodyData="", params={}):
    logger.info('#start  consume_mambu_contabilidad')


    secret=get_secret_value(os.environ['secretApiKeyMambuContabilidad'])

    base_url_integration =  os.environ['baseURL']

    header={
        "Authorization":secret
    }

    url=base_url_integration+path
    result = consume_service(method, url,header,body=json.dumps(bodyData), params=params)
    logger.info("peticion exitosa")

    logger.info('#exit  consume_mambu_contabilidad')
    return result.json()


@app.get(path=base_path + "/tblCosifAccounting")
def get_all_results_tbl_cosif_accounting():
    logger.info('#start  get_all_results_tblCosifAccounting')
    try:
        logger.info('#exit  get_all_results_tblCosifAccounting')
        return consume_mambu_contabilidad("GET","/mambu-masters/api/v1/cosifaccounting")
    except Exception as e:
            logger.error('Ocurrió un error en la funcion get_all_results_tblCosifAccountung')
            logger.error(str(e))
            return JSONResponse(
                status_code=500,
                content = [{'message':ERROR_BASE + e.__str__()}]
                )

@app.put(path=base_path + "/tblCosifAccounting/{accountid}")
def update_cosif_item(accountid, cosif: Cosif ):
    logger.info('#start  update_cosif_item')
    body= jsonable_encoder(cosif)
    try:
        if cosif.isValid:
            return consume_mambu_contabilidad("PUT","/mambu-masters/api/v1/cosifaccounting/"+str(accountid), body)
        else:
            return JSONResponse(
                status_code=405,
                content = [{'message':'Los valores enviados no son numéricos'}])

    except Exception as e:
            logger.error('Ocurrió un error en la funcion update_cosif_item')
            logger.error(str(e))
            return JSONResponse(
                status_code=500,
                content = [{'message':ERROR_BASE + e.__str__()}]
                )

@app.post(path=base_path + "/tblCosifAccounting")
def insert_cosif_item(cosif: Cosif):
    logger.info('#start  insert_cosif_item')
    body=jsonable_encoder(cosif)
    
    try:
        if cosif.isValid:
            return consume_mambu_contabilidad("POST","/mambu-masters/api/v1/cosifaccounting",body)
        else:
            return JSONResponse(content = [{'message':'Los valores enviados no son numéricos'}])

    except Exception as e:
            logger.error('Ocurrió un error en la funcion insert_cosif_item')
            logger.error(str(e))
            return JSONResponse(
                status_code=500,
                content = [{'message':ERROR_BASE + e.__str__()}]
                )

@app.get(path=base_path + "/taxaprodt")
def get_all_results_taxaprodt():
    logger.info('#start  get_all_results_taxaprodt')
    try:

        logger.info('#exit  get_all_results_taxaprodt')
        return consume_mambu_contabilidad("GET","/mambu-masters/api/v1/taxaprodt/all")
    except Exception as e:
            logger.error('Ocurrió un error en la funcion get_all_results_taxaprodt')
            logger.error(str(e))
            return JSONResponse(
                status_code=500,
                content = [{'message':ERROR_BASE + e.__str__()}]
                )

# El item no puede tener otra emisión que ya se encuentre registrada
#
# Por lo tanto se agrega el parámetro producttypedescription para realizar la validación
#
# El parámetro producttypedescription contiene el valor del producto que ya se encuentra
# registrado para el ítem
#
# El objeto tax_account contiene la emisión seleccionada en la edición, si coincide 
# con la emisión que se encontraba registrada no se realiza la validación, en el caso que 
# no sea la misma emisión se realizaría la validación de la emisión.
@app.put(path=base_path + "/taxaprodt/{taxaccountid}/{producttypedescription}")
def update_taxaprodt_item(taxaccountid,producttypedescription,tax_account: Updatetaxaprodt):
    logger.info('#start  update_taxaprodt_item')
    try:
        json_get_product = []

        if producttypedescription != tax_account.producttypedescription:
            logger.info(f'Búsqueda de la emisión: {tax_account.producttypedescription}')
            json_get_product = consume_mambu_contabilidad("GET","/mambu-masters/api/v1/taxaprodt/productdescription/"+tax_account.producttypedescription)

        if json_get_product:
            logger.info('La emisión seleccionada ya se encuentra registrada')
            return JSONResponse(content = {'message':'emision-exist'}, status_code=200) 
        else:
            return consume_mambu_contabilidad("PUT","/mambu-masters/api/v1/taxaprodt/"+taxaccountid,jsonable_encoder(tax_account))
    except Exception as e:
        logger.error('Ocurrió un error en la funcion update_taxaprodt_item')
        logger.error(str(e))
        return JSONResponse(
                status_code=500,
                content = [{'message':ERROR_BASE + e.__str__()}]
                )


@app.post(path=base_path + "/taxaprodt")
def insert_taxaprodt_item(tax_account: Updatetaxaprodt):
    logger.info('#start  insert_taxaprodt_item')

    try:
        logger.info(f'Búsqueda de la emisión: {tax_account.producttypedescription}')
        json_get_product = consume_mambu_contabilidad("GET","/mambu-masters/api/v1/taxaprodt/productdescription/"+tax_account.producttypedescription)
        if json_get_product:
            logger.info('La emisión seleccionada ya se encuentra registrada')
            return JSONResponse(content = {'message':'emision-exist'},status_code= 200) 
        else:
            return consume_mambu_contabilidad("POST","/mambu-masters/api/v1/taxaprodt",jsonable_encoder(tax_account))
    except Exception as e:
        logger.error('Ocurrió un error en la funcion insert_taxaprodt_item')
        logger.error(str(e))
        
        return JSONResponse(
                status_code=500,
                content = [{'message':ERROR_BASE + e.__str__()}]
                )
        
        
@app.get(base_path + "/files/")
def get_all_sap_files(from_date: str, to_date: str):
    try:
        resp = consume_mambu_contabilidad(
            "GET", "/mambu-sap/api/v1/files/dates/", 
            params={"from_date": from_date, "to_date": to_date}
            )
        return resp
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, 
                            status_code=500)
  
    
@app.get(base_path + "/files/download/{filename}")
def get_sap_url(filename: str):
    try:
        resp = consume_mambu_contabilidad(
            "GET", "/mambu-sap/api/v1/share-presigned-url-s3", 
            params={"bucket_name": os.environ["sapBucket"], "object_key": filename}
            )
        return resp
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, 
                            status_code=500)
 
    
@app.post(base_path + "/rates")
def update_rates(update_date: str, user: str):
    logger.info("### Start Update Request ###")
    try:
        resp = consume_mambu_contabilidad(
            "POST", "/mambu-rates/api/v1/taxes-rates-sqs",
            bodyData={"execution_date": update_date, "user":user}
        )
        return resp
    except Exception as e:
        message = "Error a la hora de realizar la petición: " + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, 
                            status_code=500)
    
@app.get(base_path + "/rates")
def get_mass_interest_events(initial_date: str, 
                             final_date: str, 
                             consecutive: Optional[str] = ""):
    logger.info("## Start get_all_sap_files")
    try:
        resp = consume_mambu_contabilidad(
            "GET", "/mambu-acc-mass/api/v1/mass-interest-acc", 
            params={"initial_date": initial_date, 
                    "final_date": final_date,
                    "consecutive": consecutive}
            )
        return resp
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, status_code=500)

@app.post(base_path + "/reprocess")
def accounting_reprocess(reprocess: Reprocess):
    logger.info("### Start accounting_reprocess ###")
    event_type = reprocess.event_type
    try:
        body = jsonable_encoder(reprocess, exclude_none=True)
        body.pop("event_path")
        body.pop("event_type")
        resp = consume_mambu_contabilidad(method="POST",
                                          path=reprocess.event_path,
                                          bodyData=body)

        if str(resp) =="No hay movimientos para el día de hoy":
            now_server = datetime.utcnow() #Obtencion hora UTC
            now_colombia = now_server - timedelta(hours=5) #Resta de 5 horas para Colombia
            user = reprocess.user

            user_list = {
                "constitucion": "API_Constitucion",
                "interes":"API_Interes",
                "vencimientos_capital":"API_Pago_Capital",
                "vencimientos_gmf":"API_GMF_Capital",
                "pago_rendimientos":"API_Pago_Rendimientos"
            }
            if not user:
                user = user_list[event_type]

            body_table = {
                'date_process': now_colombia.strftime("%Y-%m-%d"),
                'user': user,
                'date_event': now_colombia.strftime("%Y-%m-%dT%H:%M:%S-05:00") ,
                'detailed': "No existe información en el sistema origen que cumpla con los criterios de consulta",
                'value': "0",
                'type_process':"Normal",        
                'status_code': "200",
                'status': resp,
                'data_group': "NULL"
            }
            consume_mambu_contabilidad(
                method="POST",
                path="/mambu-acc-mass/api/v1/mass-acc-events",
                bodyData=body_table
            )

        content = {
            "mambu_response": resp,
            "message": f"Ejecución contabilidad {event_type} exitosa"
        }
        status_code = 200
    except Exception as e:
        message = "Error consumiendo api que lleva a Mambu: " + e.__str__()
        logger.error(message)
        content = {"message": f"Ejecución contabilidad {event_type} fallida. Error 500"}
        status_code = 500
    
    return JSONResponse(content=content, status_code=status_code)
  

@app.get(base_path + "/reprocess")
def get_reprocess_table(initial_date: str, final_date: str):
    logger.info("## Start get_reprocess_table")
    try:
        resp = consume_mambu_contabilidad(
            "GET", "/mambu-acc-mass/api/v1/mass-acc-events", 
            params={"initial_date": initial_date, "final_date": final_date}
            )
        return resp
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, 
                            status_code=500)

def get_typeproduct(filters):
    resp = consume_mambu_contabilidad("GET", "/mambu-masters/api/v1/typeproduct", params=filters)
    return resp

@app.get(base_path + "/typeproduct")
def get_typeproduct_all_filter(producttypedescription:str="",producttypemaestrosunicos:str="", producttype: ProductType = ""):
    logger.info("## Start get_typeproduct_all_filter")
    try:
        filters = {}
        if producttypedescription:
            filters["producttypedescription"] = producttypedescription
        if producttypemaestrosunicos:
            filters["producttypemaestrosunicos"] = producttypemaestrosunicos
        if producttype:
            filters["producttype"] = producttype.value
        resp = get_typeproduct(filters)
        return resp
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, status_code=500)

@app.post(base_path + "/typeproduct")
def insert_typeproduct(typeproduct: TypeProduct):
    logger.info("## Start insert_typeproduct")
    try:
        filters = {"producttypedescription": typeproduct.producttypedescription}
        items_products = get_typeproduct(filters)
        if items_products:
            logger.info('El tipo de emisión ya se encuentra registrada')
            return JSONResponse(content = {'message':'typeproduct-exist'},status_code= 200)
        else:
            body=jsonable_encoder(typeproduct)
            return consume_mambu_contabilidad("POST", "/mambu-masters/api/v1/typeproduct",bodyData=body)
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, status_code=500)


@app.put(base_path + "/typeproduct/{typeid}/{producttypedescription}")
def update_typeproduct(typeid,producttypedescription,typeproduct: TypeProduct):
    logger.info("## Start insert_typeproduct")
    try:
        items_products = []
        if producttypedescription != typeproduct.producttypedescription:
            filters = {"producttypedescription": typeproduct.producttypedescription}
            items_products = get_typeproduct(filters)
        if items_products:
            logger.info('El tipo de emisión ya se encuentra registrada')
            return JSONResponse(content = {'message':'typeproduct-exist'},status_code= 200)
        else:
            body=jsonable_encoder(typeproduct)
            return consume_mambu_contabilidad("PUT", "/mambu-masters/api/v1/typeproduct/"+typeid,bodyData=body)
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, status_code=500)


@app.post(base_path + "/upload-cc")
def upload_cc(body_upload_cc: UploadCc):
    logger.info("### Start Upload CC ###")
    status_code = 102
    message="Procesando archivo"
    try:
        bucket_key = f"update_cc_rate/{uuid.uuid1().time}-{body_upload_cc.user_upload}.xlsx" 
        response_url = consume_mambu_contabilidad(
            "GET", "/mambu-rates/api/v1/generate-url-s3",
            params={"bucket_name": os.environ['bucketCC'], "bucket_key":bucket_key}
        )
        url = response_url["url"]
        file_content = base64.b64decode(body_upload_cc.file_content)
        resp = consume_service(method="PUT",url = url,body =file_content)
        if resp is not None:
            status_code = resp.status_code
            if status_code == 200:
                message = "Carga de archivo exitosa, consulte el estado de la ejecución en unos segundos"
            else:
                message = "Ocurrió un error al cargar archivo"
        else:
            status_code = 500
            message = "Ocurrió un error al cargar archivo" 
    except Exception as e:
        status_code = 500
        message = ERROR_BASE + e.__str__()
        logger.error(message)
    return JSONResponse(content={"message": message}, status_code=status_code)

@app.get(base_path + "/rates-update")
def get_rates_update(process_date:str="", file_id:str="", initial_date: str="", final_date: str=""):
    logger.info("## Start get_reprocess_table")
    try:
        filters = {}
        if process_date and file_id:
            filters["process_date"] = process_date
            filters["file_id"] = file_id
        elif initial_date and final_date:
            filters["initial_date"] = initial_date
            filters["final_date"] = final_date

        resp = consume_mambu_contabilidad(
            "GET", "/mambu-rates/api/v1/updates", 
            params=filters
            )
        return resp
    except Exception as e:
        message = MAMBU_ERROR + e.__str__()
        logger.error(message)
        return JSONResponse(content={"message": message}, status_code=500)


logger.info('#Inicio de ejecucion lambda.')
handler = Mangum(app)