from airflow.hooks.base import BaseHook

import logging
import pandas as pd

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
        df = pd.read_excel(
                    path, 
                    sheet_name
                )
        
        return df
    
    def transform_data(
        self,
        df
    ):
        df.rename(columns={
                "COMBUSTÍVEL": "product",
                "ANO": "year", 
                "ESTADO": "uf", 
                },
                inplace=True
            )
        
        indexes = ['product','year','uf']

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
        
        months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

        melt_df = pd.melt(
                df, 
                id_vars=indexes, 
                value_vars=months, 
                var_name='month', 
                value_name='volume'
            )
        
        melt_df['volume'].fillna(
                            value='0', 
                            inplace=True
                        )

        melt_df['year_month'] = melt_df['year'].astype(str) + '_' + melt_df['month'].astype(str).str.lower()

        melt_df['unit'] = melt_df['product'].str.extract('.*\((.*)\).*')
        melt_df['product'] = melt_df['product'].str.replace('\(.*?\)', '', regex=True)

        melt_df['created_at'] = pd.Timestamp.utcnow()

        columns = ['year_month','uf','product','unit','volume','created_at']
        
        reindexed_df = melt_df.reindex(
                                columns, 
                                axis ='columns'
                            )

        return reindexed_df
    
    def validate_schema(
        self,
        df
    ):
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
            return schema.validate(df, lazy=True)
        except pa.errors.SchemaErrors as err:
            logging.info(err.failure_cases)