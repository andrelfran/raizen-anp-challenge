from airflow.models import BaseOperator

class DataExtractionFuelSalesOperator(BaseOperator):
    def __init__(
        self, 
        my_path
    ):
        self.path = my_path