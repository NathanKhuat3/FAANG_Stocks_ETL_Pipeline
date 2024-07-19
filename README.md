# FAANG Stocks ETL Pipeline
![image](https://github.com/user-attachments/assets/6ce93fd8-fc09-4b85-91e0-30e9e58be8e9)

### Pipeline's Structure
![image](https://github.com/user-attachments/assets/4c03b24b-7546-48e7-8230-3f9d1ab6f33f)

### Tools:
* AWS EC2
* AWS S3
* AWS RDS PostgreSQL
* Apache Airflow

### Overview

This ETL parallel pipeline extracts and transforms historical monthly stock data of FAANG companies from the Alpha Avantage API, the load it into the PostgreSQL database. This data is joined with a CSV file pulled from the Data Lake into a PostgreSQL table, which contains additional information about the stocks and the companies. The joined data table is then loaded back into the Data Lake in a CSV format. This pipeline runs daily and provides real time data for the purpose of stock analysis.

### EC2 and RDS instances
* EC2 t2.medium (2 vCPU, 4 GiB Memory)
* RDS db.t3.micro (2 vCPU, 1 GiB RAM)
  
### DAG View
![image](https://github.com/user-attachments/assets/c32a51c0-d392-448e-a2a6-0e95779b29a6)
