from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException, AirflowException

import logging
import pandas as pd
import uuid

import awswrangler as wr

from datetime import datetime

import pandera as pa
from pandera.engines import pandas_engine


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
        
    def validate_columns(
        self,
        df
    ):
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
    
    def validate_months(
        self,
        df
    ):
        months_br = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

        for month in months_br:
            if month not in df.columns:
                raise AirflowException(f"The following month is not present within the DataFrame: {month}")

        df.rename(columns={
                "Jan": "Jan",
                "Fev": "Feb", 
                "Mar": "Mar", 
                "Abr": "Apr", 
                "Mai": "May", 
                "Jun": "Jun", 
                "Jul": "Jul", 
                "Ago": "Aug", 
                "Set": "Sep", 
                "Out": "Oct", 
                "Nov": "Nov", 
                "Dez": "Dec"
                },
                inplace=True
            )
        
        return df
    
    def transform_data(
        self,
        df
    ):
        logging.info("Transforming data...")

        validated_col_df = self.validate_columns(df)
        validated_month_df = self.validate_months(validated_col_df)

        indexes = ['product','year','uf']
        months_us = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

        melt_df = pd.melt(
                validated_month_df, 
                id_vars=indexes, 
                value_vars=months_us, 
                var_name='month', 
                value_name='volume'
            )
        
        melt_df['volume'].fillna(
                            value='0', 
                            inplace=True
                        )

        melt_df['volume'] = melt_df['volume'].apply(lambda x: format(float(x),".3f"))
        
        melt_df['year_month'] = melt_df['year'].astype(str) + '_' + melt_df['month'].astype(str).str.lower()

        melt_df['unit'] = melt_df['product'].str.extract('.*\((.*)\).*')
        melt_df['product'] = melt_df['product'].str.replace('\(.*?\)', '', regex=True)
        
        melt_df['created_at'] = pd.Timestamp.utcnow()

        columns = ['year_month','uf','product','unit','volume','created_at']
        
        reindexed_df = melt_df.reindex(
                                columns, 
                                axis ='columns'
                            )

        if not reindexed_df.empty:
            logging.info(f"The total rows that were transformed: {len(reindexed_df)}")
            return reindexed_df
        else:
            raise AirflowException("Review the DataFrame transformation!")
    
    def validate_schema(
        self,
        df
    ):
        logging.info("Validating schema...")

        schema = pa.DataFrameSchema(
            {
                "year_month": pa.Column(pandas_engine.DateTime(
                                        to_datetime_kwargs = {"format":"%Y_%b"}
                                    )
                ),
                "uf": pa.Column(str),
                "product": pa.Column(str),
                "unit": pa.Column(str),
                "volume": pa.Column(float),
                "created_at": pa.Column(pd.Timestamp)
            },
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
        logging.info("Uploading data to S3 bucket...")

        dt = datetime.utcnow()
        dt_timestamp = dt.strftime("%Y%m%d%H%M%S")
        dt_date = dt.strftime("%Y-%m-%d")

        path = f"s3://{bucket}/processed_files/{dt_date}/{file_name.lower()}_{uuid.uuid4()}_{dt_timestamp}.parquet"

        wr.s3.to_parquet(
            df=df,
            path=path
        )

        logging.info("Data has been uploaded successfully!")