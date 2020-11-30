from typing import Optional

from fastapi import FastAPI
import datetime
from processor.processors import Processor

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/process")
def process_daily_data(ship_imo: int, date:datetime.date):
    proc = Processor(ship_imo,date)
    result, response = proc.do_steps()
    return {"details":response,"result":result}