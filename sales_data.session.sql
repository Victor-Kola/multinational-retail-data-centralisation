
SELECT MAX(LENGTH(card_number)) FROM orders_table;
SELECT MAX(LENGTH(store_code)) FROM orders_table;
SELECT MAX(LENGTH(product_code)) FROM orders_table;


SELECT * FROM orders_table;

SELECT user_uuid
FROM orders_table
WHERE NOT user_uuid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';


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

SELECT MAX(LENGTH(country_code)) FROM dim_users;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(10) USING (country_code::varchar(10));

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255) USING (first_name::varchar(255));

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255) USING (last_name::varchar(255));

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING (date_of_birth::date);

DELETE FROM dim_users
WHERE NOT (user_uuid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$');

-- Note here for the fact that we learnt how to deal with data that wasnt properly cleaned.

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::uuid);

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE using (join_date::date);

SELECT user_uuid FROM dim_users

SELECT * FROM dim_store_details

SELECT lat FROM dim_store_details;

ALTER TABLE dim_store_details
DROP COLUMN lat;


SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

SELECT store_type FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING (longitude::float),
ALTER COLUMN locality TYPE VARCHAR(255) USING (locality::varchar(255)),
ALTER COLUMN store_code TYPE VARCHAR(11) USING (store_code::varchar(11)),
ALTER COLUMN staff_numbers TYPE smallint USING (staff_numbers::smallint),
ALTER COLUMN opening_date TYPE DATE USING (opening_date::date),
ALTER COLUMN store_type TYPE VARCHAR(255) USING (store_type::varchar(255)),
ALTER COLUMN latitude TYPE FLOAT USING (latitude::float),
ALTER COLUMN country_code TYPE VARCHAR(2) USING (country_code::varchar(2)),
ALTER COLUMN continent TYPE VARCHAR(255) USING (continent::varchar(255));

SELECT product_price from dim_products;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255) NOT NULL DEFAULT 'N/A';

SELECT * FROM dim_products;

UPDATE dim_products
SET weight_class = CASE 
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

select * from dim_products;

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


SELECT * from dim_date_times;


ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::uuid),
ALTER COLUMN time_period TYPE VARCHAR(10) USING (time_period::varchar(10)),
ALTER COLUMN timestamp TYPE TIME USING (timestamp::time),
ALTER COLUMN date TYPE VARCHAR(10) USING (date::varchar(10));




ALTER TABLE dim_date_times
ALTER COLUMN date TYPE date
using date::date;

SELECT MAX(LENGTH(time_period)) FROM dim_date_times;

select * from dim_card_details;


ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22) USING (card_number::varchar(22)),
ALTER COLUMN expiry_date TYPE VARCHAR(11) USING (expiry_date::varchar(11)),
ALTER COLUMN date_payment_confirmed TYPE DATE USING (date_payment_confirmed::date),
ALTER COLUMN card_provider TYPE VARCHAR(13) USING (card_provider::varchar(13));


SELECT MAX(LENGTH(card_number)) FROM dim_card_details;

SELECT MAX(LENGTH('expiry_date')) FROM dim_card_details;

SELECT MAX(LENGTH('card_provider')) FROM dim_card_details;

SELECT * FROM dim_card_details WHERE date_payment_confirmed = 'NULL';

UPDATE dim_card_details SET date_payment_confirmed = NULL WHERE date_payment_confirmed = 'NULL';

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


SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);


select store_code from dim_store_details;
select store_code from orders_table;


SELECT card_number, COUNT(*)
FROM dim_card_details
GROUP BY card_number
HAVING COUNT(*) > 1;

DELETE FROM dim_card_details
WHERE card_number, COUNT(*) IS >1; 


ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

SELECT * FROM dim_card_details
WHERE card_number = 'NULL';

UPDATE dim_card_details
SET card_number = NULL
WHERE card_number = 'NULL';


SELECT *, COUNT(*)
FROM dim_card_details
GROUP BY card_number
HAVING COUNT(*) > 1;


SHOW CREATE TABLE dim_card_details;

SHOW TRIGGERS

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

UPDATE orders_table
SET store_code = NULL
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_store_details_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);u