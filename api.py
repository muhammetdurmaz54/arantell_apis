from fastapi import FastAPI,File, UploadFile, HTTPException, Security,status,APIRouter
from fastapi.security.api_key import APIKeyHeader
import datetime
import pandas as pd
from enum import Enum
from src.processors.dd_extractor.extractor import Extractor
from src.processors.config_extractor.extract_config import ConfigExtractor
from src.processors.dd_processor.processor import Processor
from src.processors.stats_generator.stats_generator import StatsGenerator
from src.processors.historical_data_extractor.extract_historicaldd import Historical
import os
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
load_dotenv()


app = FastAPI()

router = APIRouter()

api_key_header_auth = APIKeyHeader(name=os.getenv('API_KEY_NAME'), auto_error=True)
class dd_type(str, Enum):
    fuel = "fuel"
    engine = "engine"


async def get_api_key(api_key_header: str= Security(api_key_header_auth)):
    if api_key_header != os.getenv('API_KEY'):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid API Key")



@router.get("/status")
def get_status():
    """
    Returns API Status
    """
    return {"status": "Yesss"}



@router.post("/extract_shipconfigs")
def extract_ship_configs(ship_imo: int,
                         override: bool,
                         file: UploadFile = File(...)):
    """
        ## Extract ship configs  for particular ship
        - **ship_imo** : IMO number 7 digits
        - **override**: Override Existing records in database
        - **file**: Excel file (xls or xlsx)
        """
    if file.filename.endswith('.xls') or file.filename.endswith('.xlsx'):
        df = pd.read_excel(file.file)
    else:
        raise HTTPException(status_code=400,
                            detail="Only XLS/XLSX files allowed.")

    extract = ConfigExtractor(ship_imo=ship_imo,file=df)
    result = extract.do_steps()

    return {"filename": file.filename}



@router.post("/extract_dd",)
def extract_daily_data(ship_imo: int,
                       date: datetime.date,
                       type: dd_type,
                       override: bool,
                       file: UploadFile = File(...)):
    """
    ## Extract daily data  for particular ship
    - **ship_imo** : IMO number 7 digits
    - **type** : "fuel"/"engine" data
    - **date**: Date in format YYYY-MM-DD
    - **override**: Override Existing records in database
    - **file**: CSV file
    """
    if file.filename.endswith('csv'):
        df = pd.read_csv(file.file)
    elif file.filename.endswith('xlsx'):
        df = pd.read_excel(file.file)
    else:
        raise HTTPException(status_code=400,detail="Only CSV/XLSX files allowed.")

    extract = Extractor(ship_imo=ship_imo,
                        date=date,
                        type=type,
                        file=df)

    return {"filename": file.filename}


@router.post("/generate_stats")
def extract_stats(ship_imo: int,
                       from_date: datetime.date,
                       to_date: datetime.date,
                       override : bool,
                       all: bool,
                       file: UploadFile = File(...)):
    """
    ## Generate Stats for particular ship
    - **ship_imo** : IMO number 7 digits
    - **from_date**: Date in format YYYY-MM-DD
    - **to_date**: Date in format YYYY-MM-DD
    - **override**: Override Existing records in database
    - **all**: Ignore from and to dates and builds stats for all the dates
    """
    if file.filename.endswith('.csv'):
        df = pd.read_csv(file.file)
    else:
        raise HTTPException(status_code=400,detail="Only CSV files allowed.")

    extract = StatsGenerator(ship_imo=ship_imo,
                             from_date=from_date,
                             to_date=to_date,
                             override=override,
                             all=all)

    return {"filename": file.filename}



@router.post("/process_dd")
def process_daily_data(ship_imo: int,
                       override: bool,
                       date: datetime.date):
    """
        ## Process daily data for particular ship
        - **ship_imo** : IMO number 7 digits
        - **date**: Date in format YYYY-MM-DD
        - **override**: Override Existing records in database
        """
    process = Processor(ship_imo,
                        date,
                        override)
    result, response = process.do_steps()
    return {"details":response,"result":result}


@router.post("/extract_historical_dd")
def extract_historical_dd(ship_imo: int,
                       type: dd_type,
                       override: bool,
                       file: UploadFile = File(...)):
    """
    ## Extract Historical Data for particular ship
    - **ship_imo** : IMO number 7 digits
    - **type** : "fuel"/"engine" data
    - **override**: Override Existing records in database
    - **file**: CSV file
    """

    if file.filename.endswith('.csv'):
        df = pd.read_csv(file.file)
    else:
        raise HTTPException(status_code=400,detail="Only CSV files allowed.")
    process = Historical(ship_imo,
                         type,
                         override,
                         file)

app.include_router(
    router,
    prefix='/api',
    dependencies=[Security(get_api_key, scopes=['openid'])]
)