# ETL_Airflow_Spark
My first ETL learning experiment Using Airflow, Spark and docker

This repository demonstrates a pipeline with Spark, Airflow and docker as a 
container. The pipeline process involves the ingestion of different csv data table of an hotel booking record

#### - start_session:
A dummy operator that kick-starts the pipeline session

#### - Spark process:
Ingest only the booking record data to filter out the top 5 most
booked room type,this can be set aside for recommending room types for clients.

#### - Transform process: 
This session aggregates all the information including the hotel data, client data, spark process result, and booking data, which transforms these data
and saves them as a single record.

#### - Load data:
The load data process creates the database engine (SQlite3. You can use any database of your choice here) where all the processed records can be saved .

#### - End_session:
Just another dag dummy operator that ends the entire process
