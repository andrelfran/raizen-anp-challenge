from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException, AirflowException

import logging
import pandas as pd
import uuid

import awswrangler as wr

import pandera as pa
from pandera.engines import pandas_engine

from datetime import datetime

from tabulate import tabulate

class DataExtractionFuelSalesHook(BaseHook):
    """
    This Hook is responsible for obtaining the data, transforming it, validating the schema, 
    and sending the final results to the AWS S3 bucket.

    Params:
    """
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
        """
        This method gets the data in an XLS file.

        Params:
        path (str): .XLS file
        sheet_name: Sheet name in .XlS file

        Returns:
        object:Returning a DataFrame object
        """
        logging.info("Extracting data...")

        df = pd.read_excel(
                    path, 
                    sheet_name,
                    decimal=',',
                    engine='xlrd'
                    )
        
        if not df.empty:
            logging.info(f"The total rows that were extracted: {len(df)}")
            return df
        else:
            raise AirflowSkipException("DataFrame is empty!")
        
    def translate_columns(
        self,
        df
    ):
        """
        This method translates the columns to English.

        Params:
        df (object): DataFrame object

        Returns:
        object:Returning a DataFrame object
        """
        columns = ["COMBUSTÍVEL","ANO","ANO"]

        for column in columns:
            if column not in df.columns:
                raise AirflowException(f"The following column is not present within the DataFrame: {column}")

        df.rename(columns={
                "COMBUSTÍVEL": "product",
                "ANO": "year", 
                "ESTADO": "uf", 
                },
                inplace=True
            )
        
        return df
    
    def translate_col_months(
        self,
        df
    ):
        """
        This method translates the Portuguese month columns to numbers.

        Params:
        df (object): DataFrame object

        Returns:
        object:Returning a DataFrame object
        """
        months_br = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

        for month in months_br:
            if month not in df.columns:
                raise AirflowException(f"The following month is not present within the DataFrame: {month}")

        df.rename(columns={
                "Jan": "01",
                "Fev": "02", 
                "Mar": "03", 
                "Abr": "04", 
                "Mai": "05", 
                "Jun": "06", 
                "Jul": "07", 
                "Ago": "08", 
                "Set": "09", 
                "Out": "10", 
                "Nov": "11", 
                "Dez": "12"
                },
                inplace=True
            )
        
        return df
    
    def transform_data(
        self,
        df
    ):
        """
        This method transforms the data and prepares it to the schema validation.

        Params:
        df (object): DataFrame object

        Returns:
        object:Returning a DataFrame object
        """
        logging.info("Transforming data...")

        validated_col_df = self.translate_columns(df)
        validated_month_df = self.translate_col_months(validated_col_df)

        indexes = ['product','year','uf']
        months = ['01','02','03','04','05','06','07','08','09','10','11','12']

        melt_df = pd.melt(
                validated_month_df, 
                id_vars=indexes, 
                value_vars=months, 
                var_name='month', 
                value_name='volume'
            )
        
        melt_df['volume'].fillna(
                            value='0', 
                            inplace=True
                        )
        melt_df['volume'] = melt_df['volume'].apply(lambda x: format(float(x),".3f"))
        
        melt_df['year_month'] = melt_df['year'].astype(str) + '_' + melt_df['month'].astype(str).str.lower()
        # melt_df['year_month'] = pd.to_datetime(melt_df['year_month'], format='%Y_%m')

        melt_df['unit'] = melt_df['product'].str.extract('.*\((.*)\).*')
        melt_df['product'] = melt_df['product'].str.replace('\(.*?\)', '', regex=True).str.strip()
        
        melt_df['created_at'] = pd.Timestamp.utcnow()
        
        setindex_df = melt_df.set_index(['year_month','uf','product'], drop=False)
        setindex_df.drop(['year','month'], axis=1, inplace=True)

        if not setindex_df.empty:
            logging.info(f"The total rows that were transformed: {len(setindex_df)}")
            return setindex_df
        else:
            raise AirflowException("Review the DataFrame transformation!")
    
    def validate_schema(
        self,
        df
    ):
        """
        This method validates the DataFrame schema.

        Params:
        df (object): DataFrame object

        Returns:
        object:Returning a DataFrame object

        Side Notes:
        Data type date is not supported by Schema validation. The date must be validated as a datetime type 
        or a string type; if it is a string type, then checking must be implemented as a year and a month
        """
        logging.info("Validating schema...")

        schema = pa.DataFrameSchema(
            columns={
                "year_month": pa.Column(pandas_engine.DateTime(
                                        to_datetime_kwargs = {"format":"%Y_%m"}
                                    )
                ),
                "uf": pa.Column(str),
                "product": pa.Column(str),
                "unit": pa.Column(str),
                "volume": pa.Column(float),
                "created_at": pa.Column(pd.Timestamp)
            },
            index=pa.MultiIndex([
                pa.Index(pandas_engine.DateTime(
                                        to_datetime_kwargs = {"format":"%Y_%m"}
                                    ), name="year_month"),
                pa.Index(str, name='uf'),
                pa.Index(str, name='product')
            ]),
            coerce=True
        )
        try:
            validated_df = schema.validate(df, lazy=True)
            logging.info("Schema has been validated successfully!")
            return validated_df
        except pa.errors.SchemaErrors as e:
            logging.info(e.failure_cases)

    def upload_to_s3(
        self,
        bucket,
        file_name,
        df
    ):
        """
        This method sends the processed data to the AWS S3 bucket.

        Params:
        bucket (str): AWS S3 bucket destination
        file_name (str): File name that will be placed on AWS S3 bucket
        """
        logging.info("Uploading data to S3 bucket...")

        dt = datetime.utcnow()
        dt_timestamp = dt.strftime("%Y%m%d%H%M%S")
        dt_date = dt.strftime("%Y-%m-%d")

        path = f"s3://{bucket}/processed_files/{dt_date}/{file_name.lower()}_{uuid.uuid4()}_{dt_timestamp}.parquet"

        wr.s3.to_parquet(
            df=df,
            path=path,
            index=True
        )

        logging.info("Data has been uploaded successfully!")

