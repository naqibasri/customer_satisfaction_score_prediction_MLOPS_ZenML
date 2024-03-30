import logging

import pandas as pd
from zenml import step

class IngestData:
    def __init__(self,data_path:str):
        self.data_path = data_path
    
    def get_data(self):
        logging.info("Ingesting data from {self.data_path}")
        return pd.read_csv(self.data_path)
    
@step
def ingest_data(data_path:str) -> pd.DataFrame:
    try:
        data_ingester = IngestData(data_path)
        df = data_ingester.get_data()
        return df
    except Exception as e:
        logging.error(f"Error while ingesting data: {e}")
        raise e
    