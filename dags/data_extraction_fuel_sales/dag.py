import datetime
import os

from os.path import dirname, abspath

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from data_extraction.operators.de_operator import DataExtractionFuelSalesOperator

DAG_NAME = "data_extraction_fuel_sales"

with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 3},
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None,
    tags=["fuel","sales"],
) as dag:
    
    xls_path = dirname(abspath(__file__))
    xls_fullpath = xls_path + "/anp_file/vendas-combustiveis-m3.xls"

    sheets = ['DPCache_m3','DPCache_m3_2']

    for sheet in sheets:
        with TaskGroup(group_id=sheet) as task_tg:
            extract = DataExtractionFuelSalesOperator(
                task_id="extract",
                path=xls_fullpath,
                sheet_name=sheet
            )
            extract