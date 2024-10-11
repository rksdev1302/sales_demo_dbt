# Amazon Sales Data Warehouse Project

This project focuses on building a data warehouse star schema for Amazon sales data using **DBT (Data Build Tool)**. 
The goal is to transform raw sales data into meaningful fact and dimension tables that support efficient querying and analysis.

## Project Overview

The purpose of this project is to create a star schema for Amazon sales data to support data analysis and business intelligence needs.
The schema includes fact and dimension tables that can be queried efficiently.

### ER Diagram
You can view the ER Diagram for the schema [here](https://lucid.app/lucidchart/53c89d96-5d67-4a04-9039-d84df2393740/edit?viewport_loc=494%2C-33%2C3072%2C1341%2C0_0&invitationId=inv_b1217286-df26-413c-922b-068bee99ed20).

### Fact Tables
1. **daily_sales_fact**: Captures daily sales metrics.
2. **city_sales_fact**: Stores sales data categorized by city.
3. **sales_fact**: Contains overall sales data across multiple dimensions.

### Dimension Tables
1. **address_dim**: Stores customer or delivery addresses.
2. **date_dim**: Stores date-related information for transactions.
3. **order_dim**: Contains information related to customer orders.
4. **product_dim**: Holds product details including category and SKU.
5. **shipment_dim**: Tracks shipment information for each order.

### Incremental Cleaned Data
- **sales_data_clean_inc**: Stores the incremental cleaned version of the sales data, ready for transformation into the star schema.

## Tech Stack

- **DBT (Data Build Tool)**: Used for transforming raw sales data into the star schema.
- **MySQL**: Database used to host the data warehouse.

## Setup Instructions

1. Clone the repository:
    ```bash
    git clone https://github.com/rksdev1302/sales_demo_dbt.git
    ```
## Contact
For any questions or suggestions, feel free to reach out at [rksolanke1997@gmail.com].
