# adventure-work-data-engineer-project | End-to-End ETL Pipeline with Medallion Architecture using Azure Data Factory, Databricks, and Synapse Analytics

This project demonstrates the design and implementation of a dynamic, scalable ETL pipeline using Azure Data Factory (ADF), Databricks, and Synapse Analytics. The pipeline is built following the medallion architecture pattern, enabling structured data refinement through bronze, silver, and gold layers. It integrates dynamic control flows, such as lookup and foreach activities in ADF, to manage the orchestration of multiple data sources. The transformed data is used to create external tables in Synapse for efficient analytical querying and reporting.

## Project Description:

### ETL Orchestration with Azure Data Factory:
Developed a dynamic ETL pipeline using lookup and foreach activities in ADF to ingest and process data from multiple sources. The pipeline is designed to be reusable and data-driven, reducing the need for repetitive hardcoding.

### Medallion Architecture Implementation:
Implemented the bronze, silver, and gold layer structure to streamline data processing:

- Bronze Layer: Raw ingestion of data.
- Silver Layer: Cleaned and conformed data using Databricks notebooks.
- Gold Layer: Aggregated and business-level data ready for analytics.

### Databricks Integration:
Triggered Databricks notebooks from ADF pipelines to perform data transformation and enrichment tasks in the silver layer.

### Synapse Analytics Integration:
Created SQL scripts in Synapse workspace to read curated data from the silver layer and generate external tables in the gold layer using Azure Data Lake as the backing store. These tables were used to serve dashboards and analytical queries.

### Key Features:
- Reusable and scalable ETL orchestration.
- Modular notebook design in Databricks for transformation logic.
- Efficient data consumption using external tables in Synapse.
- Designed with reusability, maintainability, and performance in mind.



# Techstack
- PySpark
- Azure Data Factory, Data Bricks, App Registrations, Storage Account, Synapse Analytics etc
- Github

## Architecture
  
![Project Architecture](etl-tokyo-olympic-flow-chart.png)

## Project Demo
