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
        months = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

        melt_df = pd.melt(
                df, 
                id_vars=indexes, 
                value_vars=months, 
                var_name='month', 
                value_name='volume'
            )

        melt_df['year_month'] = melt_df['year'].astype(str) + '_' + melt_df['month'].str.lower()

        melt_df.drop(
                columns=['year','month'], 
                inplace = True
            )
        
        melt_df['unit'] = melt_df['product'].str.extract('.*\((.*)\).*')

        columns = ['year_month','uf','product','unit','volume']
        
        reindexed_df = melt_df.reindex(
                                columns, 
                                axis ='columns'
                            )

        # grouped_df = reindexed_df.groupby(['year_month','uf','product']).sum()

        return reindexed_df