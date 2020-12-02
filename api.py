from typing import Optional

from fastapi import FastAPI
import datetime
from src.dd_processor.processor import Processor
from src.dd_extractor.extractor import Extractor

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/status")
def read_root():
    return {"status": "Ok"}

@app.post("/process_dd")
def process_daily_data(ship_imo: int, date: datetime.date):
    process = Processor(ship_imo,date)
    result, response = process.do_steps()
    return {"details":response,"result":result}

@app.post("/extract_dd")
def process_daily_data(ship_imo: int, date: datetime.date):
    extract = Extractor(ship_imo,date)