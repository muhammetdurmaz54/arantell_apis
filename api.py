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