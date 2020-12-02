from typing import Optional

from fastapi import FastAPI,File, UploadFile,Query,HTTPException
import datetime
import pandas as pd
from enum import Enum
from src.dd_processor.processor import Processor
from src.dd_extractor.extractor import Extractor
from src.config_extractor.extract_config import ConfigExtractor

app = FastAPI()

def error_response():
    pass

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/status")
def read_root():
    return {"status": "Ok"}


@app.post("/process_dd")
def process_daily_data(ship_imo: int,
                       override: bool,
                       date: datetime.date):
    process = Processor(ship_imo,
                        date,
                        override)
    result, response = process.do_steps()
    return {"details":response,"result":result}

@app.post("/extract_shipconfigs")
def extract_ship_configs(ship_imo: int,
                         override: bool,
                         file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        df = pd.read_csv(file.file)
    else:
        raise HTTPException(status_code=400, detail="Only CSV files allowed.")

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