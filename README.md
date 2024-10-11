Amazon Sales Data Warehouse Project

This project focuses on building a data warehouse star schema for Amazon sales data using DBT (Data Build Tool). 
The goal is to transform raw sales data into meaningful fact and dimension tables that support efficient querying and analysis. 

Project Overview
This project involves creating a star schema for Amazon sales data to support data analysis and business intelligence. The schema includes:

Fact table: daily_sales_fact ,city_sales_fact 
Dimension tables: address_dim,date_dim,order_dim,product_dim,shipment_dim,value_dim
Incremental Cleaned Data : sales_data_clean_inc

Tech Stack
DBT (Data Build Tool): For transforming raw data into the star schema.
MySQL: As the database to host the data warehouse.
