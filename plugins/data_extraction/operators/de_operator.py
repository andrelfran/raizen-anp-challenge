from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

import logging 

from data_extraction.hooks.de_hook import DataExtractionFuelSalesHook

from tabulate import tabulate

class DataExtractionFuelSalesOperator(BaseOperator):
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
        
        validated_df = hook.validate_schema(
                    transformed_df
                )

        if not validated_df.empty:
                logging.info("\n"+tabulate(validated_df, headers='keys', tablefmt='psql', numalign="right", floatfmt=".3f"))

                hook.upload_to_s3(
                    bucket=self.bucket,
                    file_name=self.sheet_name,
                    df=validated_df
                )

        logging.info("Ending Task...")