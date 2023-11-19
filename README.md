# Data Centralization and Analysis for Global Sales

## Project Description

- **What it does**: This project brings together all the different sales information of a large company from around the world into one place. Instead of having sales data spread out in many areas, we put it all in one easy-to-access spot. This makes it simpler to look at and understand the sales information. The second part of this project looks at querying the database that we set up to answer business queries that are affecting the business. Part of this involves cleaning up the database and setting it primary and foreign keys - ensuring the database is correct in terms of formatting. 
- **Aim of the project**: The primary objective is to create a unified and reliable source of sales data, enhancing data-driven decision-making capabilities within the company.
- **What you learned**: Through this project, I was able to test and expand my knowledge on data extraction, cleaning, and database management. I was able to extract data from various different data sources and gained proficiency in cleaning through these differing datasets. During the SQL query part of the project, I learnt a variety of things, I learnt how to clean data that was not appropriately cleaned using python through using SQL. I learnt how to utilise the lead function but mainly how to query a database to find solutions to answer business problems. I gained further proficiency on using SQL to perform basic and advanced proficiency. I also learnt how to navigate and set up a database, including the use and need of primary and foreign keys. 


## Installation Instructions

To set up the project:
1. Ensure Python is installed on your system.
2. Install necessary libraries: `pandas`, `sqlalchemy`, `numpy` (add any other required libraries).
3. Clone the repository/download the project files to your local machine.

## Usage Instructions

To use the system:
1. Run `data_extraction.py` to extract data from various sources.
2. Execute `data_cleaning.py` to clean and preprocess the data.
3. Use `database_utils.py` to load the cleaned data into the database.
4. Use `Query_practice.sql` to format the database that was just created.
5. Use `Sales_Data_Queries.sql` to query the database to answer key business questions.

## File Structure

- `data_cleaning.py`: Cleans and preprocesses the raw data for database insertion.
- `database_utils.py`: Handles the operations related to database management, including data insertion and querying.
- `data_extraction.py`: Extracts data from multiple sources, preparing it for cleaning and database integration.
- `sales_data_queries.sql`: Queries the database created to find solutions to common business questions.
- `query_practice.sql`: Outlines the structure of the database including table definitions, primary and foreign keys and relationships between tables.

## License
