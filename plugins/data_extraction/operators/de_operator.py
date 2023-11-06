from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from data_extraction.hooks.de_hook import DataExtractionFuelSalesHook

import logging 
from tabulate import tabulate


class DataExtractionFuelSalesOperator(BaseOperator):
    """
    This Operator is responsible for calling the DataExtractionFuelSalesHook, processing the data, 
    and persisting it on the AWS S3 bucket.

    Params:
    path (str): .XLS file
    sheet_name (str): Sheet name in .XlS file
    bucket (str): AWS S3 bucket destination
    """
    def __init__(
        self, 
        path,
        sheet_name,
        bucket,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        
        self.path = path
        self.sheet_name = sheet_name
        self.bucket = bucket

    def execute(
        self,
        **kwargs
    ):
        hook = DataExtractionFuelSalesHook()

        logging.info("Starting Task...") 

        if self.path \
            and self.sheet_name:
                df = hook.extract_sheet(
                            self.path, 
                            self.sheet_name
                        )
        else:
            raise AirflowException("Path and/or Sheet Name not valid!")
        
        transformed_df = hook.transform_data(
                    df
                )
                
        logging.info("\n"+tabulate(transformed_df, headers='keys', tablefmt='psql', numalign="right", floatfmt=".3f"))

        validated_df = hook.validate_schema(
                    transformed_df
                )   

        if not validated_df.empty:
            hook.upload_to_s3(
                bucket=self.bucket,
                file_name=self.sheet_name,
                df=validated_df
            )

        logging.info("Ending Task...")