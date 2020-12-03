from fastapi import FastAPI,File, UploadFile, HTTPException
import datetime
import pandas as pd
from enum import Enum
from src.processors.dd_extractor import Extractor
from src.processors.config_extractor.extract_config import ConfigExtractor
from src.processors.stats_generator import StatsGenerator
from src.routers import process_dd

app = FastAPI()
app.include_router(process_dd.router)

X_API_KEY = APIKeyHeader(name='X-API-Key')


def check_authentication_header(x_api_key: str = Depends(X_API_KEY)):
    """ takes the X-API-Key header and converts it into the matching user object from the database """

    # this is where the SQL query for converting the API key into a user_id will go
    if x_api_key == "1234567890":
        # if passes validation check, return user data for API Key
        # future DB query will go here
        return {
            "id": 1234567890,
            "companies": [1, ],
            "sites": [],
        }
    # else raise 401
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )


@app.get("/")
def read_root():
    return {"status": "Ok"}


@app.get("/status")
def read_status():
    return {"status": "Ok"}



@app.post("/extract_shipconfigs")
def extract_ship_configs(ship_imo: int,
                         override: bool,
                         file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        df = pd.read_csv(file.file)
    else:
        raise HTTPException(status_code=400,
                            detail="Only CSV files allowed.")

    extract = ConfigExtractor(ship_imo=ship_imo,
                              file=df)
    return {"filename": file.filename}


class dd_type(str, Enum):
    fuel = "fuel"
    engine = "engine"

@app.post("/extract_dd")
def extract_daily_data(ship_imo: int,
                       date: datetime.date,
                       type: dd_type,
                       file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        df = pd.read_csv(file.file)
    else:
        raise HTTPException(status_code=400,detail="Only CSV files allowed.")

    extract = Extractor(ship_imo=ship_imo,
                        date=date,
                        type=type,
                        file=df)

    return {"filename": file.filename}


@app.post("/generate_stats")
def extract_daily_data(ship_imo: int,
                       from_date: datetime.date,
                       to_date: datetime.date,
                       override : bool,
                       all: bool):
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