from airflow.models import BaseOperator
from data_extraction.hooks.de_hook import DataExtractionFuelSalesHook


class DataExtractionFuelSalesOperator(BaseOperator):
    def __init__(
        self, 
        path,
        sheet_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        
        self.path = path
        self.sheet_name = sheet_name

    def execute(
        self,
        **kwargs
    ):
        hook = DataExtractionFuelSalesHook()

        df = hook.extract_sheet(
                    self.path, 
                    self.sheet_name
                )

        print(df)