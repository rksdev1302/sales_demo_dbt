Amazon Sales Data Warehouse Project

This project focuses on building a data warehouse star schema for Amazon sales data using DBT (Data Build Tool). 
The goal is to transform raw sales data into meaningful fact and dimension tables that support efficient querying and analysis. 

Project Overview
This project involves creating a star schema for Amazon sales data to support data analysis and business intelligence. The schema includes:

ER Diagram - https://lucid.app/lucidchart/53c89d96-5d67-4a04-9039-d84df2393740/edit?viewport_loc=494%2C-33%2C3072%2C1341%2C0_0&invitationId=inv_b1217286-df26-413c-922b-068bee99ed20
Fact table: 
1.daily_sales_fact
2.city_sales_fact 
3.sales_fact

Dimension tables: 
1.address_dim
2.date_dim
3.order_dim
4.product_dim
5.shipment_dim

Incremental Cleaned Data : sales_data_clean_inc

Tech Stack
DBT (Data Build Tool): For transforming raw data into the star schema.
MySQL: As the database to host the data warehouse.
