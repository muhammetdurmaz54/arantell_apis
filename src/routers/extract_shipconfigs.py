
from fastapi import File, UploadFile, HTTPException,APIRouter
import pandas as pd
from src.processors.config_extractor.extract_config import ConfigExtractor

router = APIRouter()


@router.post("/extract_shipconfigs")
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