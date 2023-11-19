
-- Retrieve the total number of stores and their countries 

SELECT 
    country_code AS country, -- Renaming country_code to 'country' 
    COUNT(*) AS total_no_stores -- Counting the number of stores in each country
FROM 
    dim_store_details -- Source table containing store details
WHERE 
    address != 'N/A' -- Excluding online stores
GROUP BY 
    country_code -- Grouping results by country
ORDER BY 
    total_no_stores DESC; -- Ordering the results by the total number of stores in descending order


-- Retrieve which locations have the most stores 

SELECT 
    locality, -- Selecting the locality column
    COUNT(*) AS total_no_stores -- Counting the number of stores in each locality
FROM 
    dim_store_details -- From the store details table
GROUP BY 
    locality -- Grouping the results by locality
ORDER BY 
    total_no_stores DESC -- Ordering the results by total number of stores in descending order 
LIMIT 7; -- Limiting the results top the top 7 localtiies

-- Retrieve the months with the largest amount of  total sales 
SELECT 
    dim_date_times.month as sale_month, -- Selecting the month of sale 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales -- Calculating total sales 
FROM 
    orders_table -- From the orders table 
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code -- Joining with products table on product code
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid -- Joining with date_times table on date UUID

GROUP BY 
    sale_month -- Grouping the results by sale month
ORDER BY 
    total_sales DESC -- Ordering the results by total sales in descending order
LIMIT 6; -- Limiting the results to the top 6 months


-- Retrieve the number of sales between the two different platforms 
SELECT 
    COUNT (orders_table.product_code) AS numbers_of_sales, -- Counting the number of sales 
    SUM(orders_table.product_quantity) AS product_quantity_count, -- Summing up the quantities of products sold
    CASE
        WHEN dim_store_details.store_code = 'WEB-1388012W' THEN 'Web' -- Identifying and labelling 'Web' store
        ELSE 'Offline' -- Identifiyng all other stores as 'Offline'
    END AS location -- Renaming the case statement result as 'location'
FROM 
    orders_table -- From the orders table.
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code -- Joining with store details table on store code
GROUP BY 
    location -- Grouping the results by location
ORDER BY 
    numbers_of_sales ASC; -- Ordering the results by the number of sales in ascending order.


-- Calculate the percentage of total sales for each store type
WITH TotalSales AS (
    -- Subquery to calculate total sales for each store type
    SELECT
        dim_store_details.store_type AS store_type, -- Selecting store type
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales -- Calculating total sales
    FROM
        orders_table -- From the orders table
    INNER JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code -- Joining with store details on store_code
    INNER JOIN
        dim_products ON orders_table.product_code = dim_products.product_code -- Joining with product information on product code
    GROUP BY
        store_type -- Grouping by store typ
),
TotalSalesSum AS (
    -- Subquery to calculate the sum of all sales across store types
    SELECT
        SUM(total_sales) AS total_sales_sum -- Summing all total sales
    FROM
        TotalSales
)
-- Query to calculate sales percentage for each store type
SELECT
    TS.store_type, -- Selecting store type
    TS.total_sales, -- Selecting total sales for each store type
    (TS.total_sales / TSS.total_sales_sum) * 100 AS sales_percentage -- Calculating the sales percentage
FROM
    TotalSales TS
CROSS JOIN
    TotalSalesSum TSS -- Cross joining with the total sales to calculate percentages
ORDER BY
    TS.total_sales DESC; -- Ordering by total sales in descending order


-- Retrieve which months in each year had the highest amount of sales
SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales, -- Calculating total sales
    dim_date_times.month AS sale_month, -- Selecting the sale month 
    dim_date_times.year AS sale_year -- Selecting the sale year
FROM orders_table
LEFT JOIN
    dim_products ON orders_table.product_code = dim_products.product_code -- Joining with products table to get product prices
LEFT join
    dim_date_times  ON orders_table.date_uuid = dim_date_times.date_uuid -- Joining with date_times table to get sale dates
GROUP BY
    sale_year, -- Grouping results by sale year
    sale_month -- Grouping results by sale month
ORDER BY
    total_sales DESC -- Ordering the results by total sales in descending order
LIMIT 10; -- Limiting the results to the top 10 months. 


-- Retrieve total staff headcount by country
SELECT
    dim_store_details.country_code AS country_code, -- Selecting the country code
    sum(dim_store_details.staff_numbers) as number_of_staff -- Calculating total number of staff
FROM dim_store_details -- From store details table
GROUP BY
    country_code -- Group by country code
ORDER BY 
    number_of_staff DESC; -- Order by number of staff in descending order


--  Retrieve total number of sales by store type in Germany 
SELECT
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC ), 2) AS total_sales, -- Calculating and rounding to 2 decimal places total sales
    dim_store_details.store_type as store_type, -- Selecting the store type
    dim_store_details.country_code AS country_code -- Selecting the country code
FROM orders_table
JOIN
    dim_store_details on orders_table.store_code = dim_store_details.store_code -- Joining with store details to get store types and country codes on 'store_code'
JOIN 
    dim_products on orders_table.product_code = dim_products.product_code -- Joining with product details to get product prices
WHERE 
    dim_store_details.country_code = 'DE' -- Filtering for stores only in Germany
GROUP BY
    country_code, -- Grouping results by country code
    store_type -- Grouping results by store_type
ORDER BY
    total_sales ASC; -- Ordering results by total sales in ascending order

-- How quickly is the company making sales? 

WITH TimeDifferences AS (
    -- Subquery to construct full timestamps for each sale and group them by date and time
    SELECT
        dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day AS sale_date, -- Creating a sale date string by combining all parts of a date
        dim_date_times.timestamp AS time_of_sale, -- Selecting the time of the sale
        (dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp AS full_sale_timestamp -- Creating the full timestamp with all necessary details
        
    FROM 
        orders_table
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid -- Joining orders with date_times to get the sale date and time on date uuid 
    GROUP BY 
        dim_date_times.year, dim_date_times.month, dim_date_times.day, dim_date_times.timestamp -- Group by  year, month, day and then timestamp
),
Next_sale AS (
    -- Subquery to calculate the time to the next sale for each sale
    SELECT
        EXTRACT(YEAR FROM full_sale_timestamp) AS sale_year, -- Extracting the year from the full timestamp and identifiyng it as sale year
        full_sale_timestamp, -- Selecting the full sale timestamp
        LEAD(full_sale_timestamp) OVER (ORDER BY full_sale_timestamp) AS time_to_next_sale -- Obtaining the timestamp of the next sale using LEAD
    FROM 
        TimeDifferences
)
SELECT
-- Queery to calculate the average time difference between sales for each year
    sale_year, -- Selecting the sale year
    AVG(time_to_next_sale - full_sale_timestamp) AS average_time_diff -- Calculating the average time difference between sales 
FROM
    Next_sale
WHERE
    time_to_next_sale IS NOT NULL -- Filtering out the last sale of each year 
GROUP BY
    sale_year -- Group by sale year
ORDER BY
    average_time_diff DESC -- Order by average time difference between sales over the course of the year
LIMIT 5; -- Top 5 years with the longest years only

