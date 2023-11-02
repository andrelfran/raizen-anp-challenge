from airflow.hooks.base import BaseHook

import logging
import pandas as pd

class DataExtractionFuelSalesHook(BaseHook):
    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        
    def extract_sheet(
        self,
        path,
        sheet_name
    ):
        logging.info(path)

        df = pd.read_excel(
                    path, 
                    sheet_name
                )
        
        return df
