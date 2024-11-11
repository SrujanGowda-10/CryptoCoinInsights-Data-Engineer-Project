# CryptoCoinInsights Data Pipeline | AWS-Apache Airflow Data Engineering Project


## Introduction
CoinInsights is a robust ETL (Extract, Transform, Load) pipeline designed to process cryptocurrency market data. The pipeline automatically fetches real-time cryptocurrency data from the CoinGecko API, transforms it into a structured format, and loads it into Amazon Redshift for analysis. The entire workflow is orchestrated using Apache Airflow, ensuring reliable, scheduled data updates with proper error handling and monitoring.

## Architecture
![image](https://github.com/user-attachments/assets/ecb3d1e6-f6e6-40f2-84f3-fd3aef61abea)

## Table Structure
![image](https://github.com/user-attachments/assets/bf1d64fa-6564-4f4d-ba1e-6c1429624958)

## Tasks flow in DAG
![image](https://github.com/user-attachments/assets/db5701ab-add6-4b14-ac1c-54a18fd42ef2)

## Technology Stack
- **Amazon S3**: Data lake storage for raw and transformed data
- **AWS Lambda**: Serverless compute for data extraction and transformation
- **Amazon Redshift**: Cloud data warehouse for analytics
- **Apache Airflow**: Workflow orchestration and scheduling

## Data Source Used
[CoinGeckoAPI](https://docs.coingecko.com/reference/coins-markets) - A comprehensive cryptocurrency data API



