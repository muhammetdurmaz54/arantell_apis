from fastapi import APIRouter
import datetime
from src.processors.dd_processor import Processor

router = APIRouter()

@router.post("/process_dd")
def process_daily_data(ship_imo: int,
                       override: bool,
                       date: datetime.date):
    process = Processor(ship_imo,
                        date,
                        override)
    result, response = process.do_steps()
    return {"details":response,"result":result}