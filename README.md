# Raízen ANP Challenge
---

[[_TOC_]]

This DAG obtains data from the ANP Fuel Sales.XLS file, transforms it, and sends it to an AWS S3 bucket.

## Sources:

* #### ANP Fuels Sales ETL test
    * https://github.com/raizen-analytics/data-engineering-test/blob/master/TEST.md

* #### Excel File
    * https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls

## Variables

A variable must be set up as "data_extraction_config". Follow a sample.

``` jsonc
{
    "url": "<.XLS url or .XLS local file path>",
    "sheets": "<[Sheet names that will be processed]>",
    "bucket": "<AWS S3 bucket destination>"
}

```

## Destination

The processed data will be sent to an AWS S3 bucket as in the previous config.

S3 bucket:
s3://data-extraction-fuel-sales/processed_files/

## Layers UML Diagram

```mermaid
classDiagram
    DataExtractionFuelSalesOperator <|-- DAG : data_extraction_fuel_sales
    DataExtractionFuelSalesHook <|-- DataExtractionFuelSalesOperator

    class DAG{ 
        + Variables
    } 
    
    class DataExtractionFuelSalesOperator{
        + Variables
        + execute()
    }

    class DataExtractionFuelSalesHook{
        + extract_sheet()
        + translate_columns()
        + translate_col_months()
        + transform_data()
        + validate_schema()
        + upload_to_s3()
    }
 ```