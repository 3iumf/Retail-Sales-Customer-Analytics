CREATE DATABASE retail_bi;
USE retail_bi;

CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50)
);

CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(50),
    sub_category VARCHAR(50)
);

CREATE TABLE geography (
    postal_code VARCHAR(20) PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_details (
    row_id INT PRIMARY KEY,
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    postal_code VARCHAR(20),
    sales DECIMAL(10,2),
    quantity INT,
    discount DECIMAL(4,2),
    profit DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (postal_code) REFERENCES geography(postal_code)
);

CREATE TABLE staging_superstore (
    row_id INT,
    order_id VARCHAR(20),
    order_date VARCHAR(20),
    ship_date VARCHAR(20),
    ship_mode VARCHAR(50),
    customer_id VARCHAR(20),
    customer_name VARCHAR(150),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    product_id VARCHAR(20),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255),
    sales VARCHAR(50),
    quantity VARCHAR(20),
    discount VARCHAR(20),
    profit VARCHAR(50)
);

USE retail_bi;
drop table staging_superstore;

SELECT COUNT(*) FROM staging_superstore;

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/superstore_clean.csv'
INTO TABLE staging_superstore
CHARACTER SET latin1
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE order_details;
TRUNCATE TABLE orders;
TRUNCATE TABLE customers;
TRUNCATE TABLE products;
TRUNCATE TABLE geography;

SET FOREIGN_KEY_CHECKS = 1;

/*populate customer*/
INSERT INTO customers (customer_id, customer_name, segment)
SELECT DISTINCT 
    customer_id,
    customer_name,
    segment
FROM staging_superstore;
SELECT COUNT(*) FROM customers;

/*Populate Products*/
INSERT INTO products (product_id, product_name, category, sub_category)
SELECT 
    product_id,
    MIN(product_name),
    MIN(category),
    MIN(sub_category)
FROM staging_superstore
GROUP BY product_id;
SELECT COUNT(*) FROM products;

/*Populate geography*/
INSERT INTO geography (postal_code, city, state, region, country)
SELECT 
    postal_code,
    MIN(city),
    MIN(state),
    MIN(region),
    MIN(country)
FROM staging_superstore
GROUP BY postal_code;
SELECT COUNT(*) FROM geography;

/*Populate orders*/
INSERT INTO orders (order_id, order_date, ship_date, ship_mode, customer_id)
SELECT 
    order_id,
    STR_TO_DATE(MIN(order_date), '%d/%m/%Y'),
    STR_TO_DATE(MIN(ship_date), '%d/%m/%Y'),
    MIN(ship_mode),
    MIN(customer_id)
FROM staging_superstore
GROUP BY order_id;
select count(*) from orders;

/*Populate order_details*/
INSERT INTO order_details 
(row_id, order_id, product_id, postal_code, sales, quantity, discount, profit)
SELECT
    row_id,
    order_id,
    product_id,
    postal_code,
    CAST(sales AS DECIMAL(10,2)),
    CAST(quantity AS SIGNED),
    CAST(discount AS DECIMAL(4,2)),
    CAST(profit AS DECIMAL(10,2))
FROM staging_superstore;
select count(*) from order_details;

SELECT order_id, COUNT(*) AS items_in_order
FROM order_details
GROUP BY order_id
ORDER BY items_in_order DESC;

/*Regional Margin*/
SELECT 
    g.region,
    ROUND(SUM(od.profit) / SUM(od.sales) * 100, 2) AS margin_percentage
FROM order_details od
JOIN geography g ON od.postal_code = g.postal_code
GROUP BY g.region
ORDER BY margin_percentage DESC;

/*sub-category losing money*/
SELECT 
    p.sub_category,
    ROUND(SUM(od.profit), 2) AS total_profit
FROM order_details od
JOIN products p ON od.product_id = p.product_id
GROUP BY p.sub_category
ORDER BY total_profit ASC;



/*RFM Customer Analysis*/

SELECT 
    c.customer_id,
    c.customer_name,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date)) AS recency_days,
    COUNT(DISTINCT o.order_id) AS frequency,
    ROUND(SUM(od.sales), 2) AS monetary_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_details od ON o.order_id = od.order_id
GROUP BY c.customer_id, c.customer_name
ORDER BY monetary_value DESC;

/*What This Does
Recency → How many days since last purchase
Frequency → How many orders they made
Monetary → Total revenue they generated
This is how businesses rank customers.*/

/*Who is the highest revenue customer? Sean Miller
Who has the highest frequency? Emily Phan*/


/*Add RFM Scores
We’ll rank customers into 5 groups for each metric*/

WITH rfm_base AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        DATEDIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date)) AS recency,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(od.sales) AS monetary
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.customer_id, c.customer_name
)

SELECT *,
    NTILE(5) OVER (ORDER BY recency ASC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
FROM rfm_base;

/*What This Does
NTILE(5) splits customers into 5 equal groups:
For Recency:
1 = very recent buyers
5 = long time inactive
For Frequency:
1 = very frequent
5 = rare buyers
For Monetary:
1 = high spenders
5 = low spenders*/

/*What are Sean Miller’s RFM scores? 3 4 1
What are Emily Phan’s scores? 1 1 1
Who looks more “valuable” overall? Sean Miller*/


/*We’ll convert RFM scores into segments.
🔥 Step 1 — Build Segmentation Query*/

WITH rfm_base AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        DATEDIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date)) AS recency,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(od.sales) AS monetary
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.customer_id, c.customer_name
),

rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency ASC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
    FROM rfm_base
)

SELECT *,
    CASE
        WHEN r_score = 1 AND f_score = 1 AND m_score = 1 THEN 'Champion'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Loyal Customer'
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Lost Customer'
        WHEN r_score >= 4 THEN 'At Risk'
        WHEN m_score = 1 THEN 'Big Spender'
        ELSE 'Regular'
    END AS customer_segment
FROM rfm_scores
ORDER BY customer_segment;

/*What This Does
We classify customers:
Champion → Best in all 3
Loyal → Recent + Frequent
Lost → Not recent + Not frequent
At Risk → Haven’t purchased recently
Big Spender → High monetary but not frequent
Regular → Everyone else*/

/*COUNTING THE SEGMENTS*/
WITH rfm_base AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        DATEDIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date)) AS recency,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(od.sales) AS monetary
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.customer_id, c.customer_name
),

rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency ASC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
    FROM rfm_base
),

rfm_segments AS (
    SELECT *,
        CASE
            WHEN r_score = 1 AND f_score = 1 AND m_score = 1 THEN 'Champion'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Loyal Customer'
            WHEN r_score >= 4 AND f_score >= 4 THEN 'Lost Customer'
            WHEN r_score >= 4 THEN 'At Risk'
            WHEN m_score = 1 THEN 'Big Spender'
            ELSE 'Regular'
        END AS customer_segment
    FROM rfm_scores
)

SELECT 
    customer_segment,
    COUNT(*) AS number_of_customers
FROM rfm_segments
GROUP BY customer_segment
ORDER BY number_of_customers DESC;
