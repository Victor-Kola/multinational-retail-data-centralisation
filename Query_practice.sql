

SELECT * FROM orders_table;

SELECT user_uuid
FROM orders_table
WHERE NOT user_uuid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';

-- Milestone 3 task 1 

SELECT MAX(LENGTH(card_number)) FROM orders_table;
SELECT MAX(LENGTH(store_code)) FROM orders_table;
SELECT MAX(LENGTH(product_code)) FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::uuid);

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID using (date_uuid::uuid);

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19) USING (card_number::varchar(19));

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12) USING (store_code::varchar(12));

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11) USING (product_code::varchar(11));

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT USING (product_quantity::smallint);

SELECT 
    date_uuid, 
    data_type 
FROM 
    information_schema.columns 
WHERE 
    table_name = 'orders_table';

-- Milestone 3 Task 2

SELECT MAX(LENGTH(country_code)) FROM dim_users;

DELETE FROM dim_users
WHERE NOT (user_uuid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$');


ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(10) USING (country_code::varchar(10));

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255) USING (first_name::varchar(255));

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255) USING (last_name::varchar(255));

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING (date_of_birth::date);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::uuid);

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE using (join_date::date);

-- Note here for the fact that we learnt how to deal with data that wasnt properly cleaned.


SELECT user_uuid FROM dim_users

SELECT * FROM dim_store_details

SELECT lat FROM dim_store_details;

SELECT * FROM dim_store_details WHERE lat IS NOT NULL;


ALTER TABLE dim_store_details
DROP COLUMN lat;


SELECT * from dim_store_details;

-- Milestone 3 - Task 3
UPDATE dim_store_details
SET lat = '0'
WHERE lat = 'N/A';

UPDATE dim_store_details
SET latitude = COALESCE(CAST(lat AS float), latitude)
WHERE lat IS NOT NULL AND lat <> '';

ALTER TABLE dim_store_details
DROP COLUMN lat;


SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

SELECT store_type FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING (longitude::float),
ALTER COLUMN locality TYPE VARCHAR(255) USING (locality::varchar(255)),
ALTER COLUMN store_code TYPE VARCHAR(12) USING (store_code::varchar(12)),
ALTER COLUMN staff_numbers TYPE smallint USING (staff_numbers::smallint),
ALTER COLUMN opening_date TYPE DATE USING (opening_date::date),
ALTER COLUMN store_type TYPE VARCHAR(255) USING (store_type::varchar(255)),
ALTER COLUMN latitude TYPE FLOAT USING (latitude::float),
ALTER COLUMN country_code TYPE VARCHAR(2) USING (country_code::varchar(2)),
ALTER COLUMN continent TYPE VARCHAR(255) USING (continent::varchar(255));


-- Milestone 3 task 4
-- Check for £
SELECT product_price from dim_products;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255) NOT NULL DEFAULT 'N/A';


UPDATE dim_products
SET weight_class = CASE 
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;


-- Milestone 3 Task 5
SELECT MAX(LENGTH('weight')) FROM dim_products;

SELECT MAX(LENGTH(product_code)),
    MAX(LENGTH(weight_class))
    FROM dim_products;


ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING (product_price::float),
ALTER COLUMN "EAN" TYPE VARCHAR(13) USING ("EAN"::varchar(13)),
ALTER COLUMN product_code TYPE VARCHAR(11) USING (product_code::varchar(11)),
ALTER COLUMN date_added TYPE DATE USING (date_added::date),
ALTER COLUMN uuid TYPE UUID USING (uuid::uuid),
ALTER COLUMN weight_class TYPE VARCHAR(14) USING (weight_class::varchar(14));

ALTER TABLE dim_products
ALTER COLUMN "weight" TYPE FLOAT USING ("weight"::float)


ALTER TABLE dim_products
ADD COLUMN still_available BOOLEAN;

UPDATE dim_products
SET still_available = CASE
WHEN removed = 'Still_available' THEN TRUE
ELSE FALSE
END;

ALTER TABLE dim_products
DROP COLUMN removed; 

-- Milestone 3 Task 6
SELECT * from dim_date_times;

SELECT MAX(LENGTH(time_period))
FROM dim_date_times;

SELECT LENGTH(MAX(CAST(month AS VARCHAR))) FROM dim_date_times;


ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::uuid),
ALTER COLUMN month TYPE VARCHAR(2) USING (month::varchar(2)),
ALTER COLUMN year TYPE VARCHAR(4) USING (year::varchar(4)),
ALTER COLUMN day TYPE VARCHAR(2) USING (day::varchar(2)),
ALTER COLUMN time_period TYPE VARCHAR(10) USING (time_period::varchar(10));





-- Milestone 3 Task 7 

select * from dim_card_details;

SELECT MAX(LENGTH(card_number)) FROM dim_card_details;

SELECT MAX(LENGTH('expiry_date')) FROM dim_card_details;

SELECT MAX(LENGTH('card_provider')) FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22) USING (card_number::varchar(22)),
ALTER COLUMN expiry_date TYPE VARCHAR(11) USING (expiry_date::varchar(11)),
ALTER COLUMN date_payment_confirmed TYPE DATE USING (date_payment_confirmed::date),
ALTER COLUMN card_provider TYPE VARCHAR(13) USING (card_provider::varchar(13));

-- we found a row that wasn't cleaned properly so we will drop it.

SELECT * FROM dim_card_details
WHERE NOT (date_payment_confirmed ~ '^\d{4}-\d{2}-\d{2}$');

DELETE FROM dim_card_details
WHERE NOT (date_payment_confirmed ~ '^\d{4}-\d{2}-\d{2}$');

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22) USING (card_number::varchar(22)),
ALTER COLUMN expiry_date TYPE VARCHAR(11) USING (expiry_date::varchar(11)),
ALTER COLUMN date_payment_confirmed TYPE DATE USING (date_payment_confirmed::date),
ALTER COLUMN card_provider TYPE VARCHAR(13) USING (card_provider::varchar(13));

-- Check for non-null values
SELECT COUNT(*) FROM dim_card_details WHERE card_number IS NULL;

-- Check for unique values
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT card_number) AS unique_card_numbers 
FROM dim_card_details;

-- Assuming 'card_number' in 'orders_table' references 'card_number' in 'dim_card_details'
SELECT o.*
FROM orders_table o
LEFT JOIN dim_card_details d ON o.card_number = d.card_number
WHERE d.card_number IS NULL;

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_date_times_date_uuid
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_users_user_uuid
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);


ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

select card_number
FROM dim_card_details;

DELETE FROM dim_card_details
WHERE card_number IS NULL;

SELECT DISTINCT card_number
FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

UPDATE orders_table
SET card_number = NULL
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_card_details_card_number
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);


select card_number FROM orders_table;
select card_number from dim_card_details;

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_products_product_code
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_store_details_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);






-- 


SELECT date_uuid from orders_table;



SELECT
    EXTRACT (MONTH FROM dim_date_times.date) as sale_month,
    SUM(orders_table.product_quantity) AS total_sales
FROM
    orders_table
join
    dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    sale_month
ORDER BY
    total_sales DESC;

SELECT 
    EXTRACT(MONTH FROM CAST(dim_date_times.date AS DATE)) AS sale_month, 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM 
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid

GROUP BY 
    sale_month
ORDER BY 
    total_sales DESC;

EXTRACT(MONTH FROM CAST(dd.date AS DATE)) AS sale_month


SELECT COUNT(*) FROM orders_table WHERE date_uuid IS NULL;

SELECT COUNT(*) 
FROM orders_table ot 
WHERE NOT EXISTS (
  SELECT 1 FROM dim_date_times dd 
  WHERE ot.date_uuid = dd.date_uuid
);


select locality from dim_store_details;

SELECT DISTINCT store_code AS unique_store_codes
FROM dim_store_details;


SELECT DISTINCT store_type
FROM dim_store_details;

DROP TABLE dim_store_details;

SELECT conname
FROM pg_constraint
WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'orders_table')
  AND contype = 'f';


ALTER TABLE orders_table
DROP CONSTRAINT fk_dim_store_details_store_code;


SELECT 
    SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE
        WHEN dim_store_details.store_code = 'WEB-1388012W' THEN 'WEB'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
    location;

select * from dim_store_details;

Select *
FROM orders_table
WHERE store_code = 'WEB-1388012W' ;

SELECT *
FROM orders_table
WHERE NOT EXISTS (
    SELECT 1
    FROM cleaned_orders_table
    WHERE cleaned_orders_table.store_code = orders_table.store_code
);

SELECT o.*
FROM orders_table o
LEFT JOIN cleaned_orders_table c ON o.store_code = c.store_code
WHERE c.store_code IS NULL AND o.store_code IS NOT NULL;

SELECT c.*
FROM cleaned_orders_table c
LEFT JOIN orders_table o ON c.store_code = o.store_code
WHERE o.store_code IS NULL;

ALTER TABLE cleaned_orders_table
ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::uuid);

ALTER TABLE cleaned_orders_table
ALTER COLUMN date_uuid TYPE UUID using (date_uuid::uuid);

ALTER TABLE cleaned_orders_table
ALTER COLUMN card_number TYPE VARCHAR(19) USING (card_number::varchar(19));

ALTER TABLE cleaned_orders_table
ALTER COLUMN store_code TYPE VARCHAR(12) USING (store_code::varchar(12));

ALTER TABLE cleaned_orders_table
ALTER COLUMN product_code TYPE VARCHAR(11) USING (product_code::varchar(11));

ALTER TABLE cleaned_orders_table
ALTER COLUMN product_quantity TYPE SMALLINT USING (product_quantity::smallint);

INSERT INTO orders_table (date_uuid, user_uuid, card_number, store_code, product_code, product_quantity)
SELECT c.date_uuid, c.user_uuid, c.card_number, c.store_code, c.product_code, c.product_quantity
FROM cleaned_orders_table c
LEFT JOIN orders_table o ON c.store_code = o.store_code
WHERE o.store_code IS NULL;


ALTER TABLE orders_table
DROP CONSTRAINT fk_dim_card_details_card_number;


select * from orders_table;


SELECT 
    COUNT (orders_table.product_code) AS numbers_of_sales,
    SUM(orders_table.product_quantity) AS product_quantity_count, 
    CASE
        WHEN dim_store_details.store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
    location
ORDER BY
    location DESC;

SELECT 
    dim_store_details.store_type AS store_type,
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM 
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    store_type
ORDER BY 
    total_sales DESC;



WITH TotalSales AS (
    SELECT
        dim_store_details.store_type AS store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
    FROM
        orders_table
    INNER JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    INNER JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY
        store_type
),
TotalSalesSum AS (
    SELECT
        SUM(total_sales) AS total_sales_sum
    FROM
        TotalSales
)

SELECT
    TS.store_type,
    TS.total_sales,
    (TS.total_sales / TSS.total_sales_sum) * 100 AS sales_percentage
FROM
    TotalSales TS
CROSS JOIN
    TotalSalesSum TSS
ORDER BY
    TS.total_sales DESC;


SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    EXTRACT(MONTH FROM CAST(dim_date_times.date AS DATE)) AS sale_month,
    EXTRACT(YEAR FROM CAST(dim_date_times.date AS DATE)) AS sale_year
FROM orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER join
    dim_date_times  ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    sale_year,
    sale_month
ORDER BY
    total_sales DESC;

SELECT 
    SUM(orders_table.product_quantity * COALESCE(dim_products.product_price, 0)) AS total_sales,
    EXTRACT(MONTH FROM CAST(COALESCE(dim_date_times.date) AS DATE)) AS sale_month,
    EXTRACT(YEAR FROM CAST(COALESCE(dim_date_times.date) AS DATE)) AS sale_year
FROM orders_table
LEFT JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
LEFT JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    sale_year, sale_month
ORDER BY
    total_sales DESC;
    
    

SELECT
    dim_store_details.country_code AS country_code,
    sum(dim_store_details.staff_numbers) as number_of_staff
FROM dim_store_details
GROUP BY
    country_code

SELECT *
FROM dim_store_details
WHERE country_code = 'DE';



SELECT
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC ), 2) AS total_sales,
    dim_store_details.store_type as store_type,
    dim_store_details.country_code AS country_code
FROM orders_table
JOIN
    dim_store_details on orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products on orders_table.product_code = dim_products.product_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY
    country_code, store_type
ORDER BY
    total_sales ASC;

ALTER COLUMN card_provider TYPE VARCHAR(13) USING (card_provider::varchar(13));

WITH TotalSales AS (
    SELECT
        dim_store_details.store_type AS store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
    FROM
        orders_table
    INNER JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    INNER JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY
        store_type
),
TotalSalesSum AS (
    SELECT
        SUM(total_sales) AS total_sales_sum
    FROM
        TotalSales
)

SELECT
    TS.store_type,
    TS.total_sales,
    (TS.total_sales / TSS.total_sales_sum) * 100 AS sales_percentage
FROM
    TotalSales TS
CROSS JOIN
    TotalSalesSum TSS
ORDER BY
    TS.total_sales DESC;


SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    EXTRACT(MONTH FROM CAST(dim_date_times.date AS DATE)) AS sale_month,
    EXTRACT(YEAR FROM CAST(dim_date_times.date AS DATE)) AS sale_year
FROM orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER join
    dim_date_times  ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    sale_year,
    sale_month
ORDER BY
    total_sales DESC;

SELECT     
    EXTRACT(MONTH FROM CAST(dim_date_times.date AS DATE)) AS sale_month,
    EXTRACT(YEAR FROM CAST(dim_date_times.date AS DATE)) AS sale_year,
    timestamp,
    LEAD (timestamp) OVER (ORDER BY time_period) AS times
FROM dim_date_times
ORDER BY times;

-- We are going to order by sale, so what is a sale? 
-- we need year
-- we need to get times split into hours minutes and seconds
-- product code is a sale 

SELECT
    EXTRACT( YEAR FROM CAST(dim_date_times.date AS DATE)) as year,
    

WITH TimeDifferences AS (
    SELECT
        EXTRACT (YEAR FROM CAST(dim_date_times.date AS DATE)) as sale_year,
        sale_date,
        LEAD(sale_date) OVER (PARTITION BY EXTRACT (YEAR FROM sale_date) ORDER BY sale_date) AS next_sale_date
    FROM
    orders_table
)
SELECT 
    sale_year



    next_sale_date - sale_date AS sale_interval
FROM TimeDifferences
WHERE next_Sale_date IS NOT NULL;

WITH time_of_sale AS(
    SELECT
    dim_date_times.timestamp as time_of_sale
    FROM
    orders_table
    JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid 
)

SELECT * FROM dim_date_times;

-- Add new columns for day, month, and year
ALTER TABLE dim_date_times
ADD COLUMN month VARCHAR(2),
ADD COLUMN year VARCHAR(4),
ADD COLUMN day VARCHAR(2);

-- Update the new columns with values from the date column
UPDATE dim_date_times
SET year = EXTRACT(YEAR FROM CAST(date AS DATE)),
    month = EXTRACT(MONTH FROM CAST(date AS DATE)),
    day = EXTRACT(DAY FROM CAST(date AS DATE));

-- After confirming that the data is correct, drop the original date column
ALTER TABLE dim_date_times
DROP COLUMN date;



SELECT
    EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
    EXTRACT(MONTH FROM CAST(date AS DATE)) AS month,
    EXTRACT(DAY FROM CAST(date AS DATE)) AS day
FROM
    dim_date_times;


SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_date_times.month AS sale_month,
    dim_date_times.year AS sale_year
FROM orders_table
LEFT JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
LEFT join
    dim_date_times  ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    sale_year,
    sale_month
ORDER BY
    total_sales DESC;