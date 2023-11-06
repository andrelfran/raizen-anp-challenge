import datetime
from os.path import dirname, abspath

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from data_extraction.operators.de_operator import DataExtractionFuelSalesOperator

from airflow.models import Variable

DAG_NAME = "data_extraction_fuel_sales"

with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 3},
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None, #TBD
    tags=["fuel","sales"],
) as dag:
    
    dag_config = Variable.get(
                        'data_extraction_config',
                        deserialize_json=True,
                    )

    assert 'url' in dag_config

    if dag_config.get('url'):
        xls_fullpath = dag_config.get('url')
    else:   
        xls_path = dirname(abspath(__file__))
        xls_fullpath = xls_path + "/anp_file/vendas-combustiveis-m3.xls"

    assert 'sheets' in dag_config

    if dag_config.get("sheets"):
        sheets = dag_config.get("sheets")

    assert 'bucket' in dag_config

    if dag_config.get("bucket"):
        bucket = dag_config.get("bucket")

    for sheet in sheets:
        with TaskGroup(group_id=sheet) as task_tg:
            process_sheet = DataExtractionFuelSalesOperator(
                task_id="process_sheet",
                path=xls_fullpath,
                sheet_name=sheet,
                bucket=bucket
            )
            
            process_sheet