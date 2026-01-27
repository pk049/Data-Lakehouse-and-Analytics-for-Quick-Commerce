-- ============================================
--          CREATNIG TABLES
-- ===========================================
CREATE EXTERNAL TABLE items (
    item_id STRING,
    category STRING,
    price INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/pratik/project/dimensions/item_schema/';




DROP TABLE IF EXISTS orders_silver;

CREATE EXTERNAL TABLE orders_silver (
    event_id STRING,
    event_type STRING,
    event_time STRING,
    order_id STRING,
    user_id STRING,
    city STRING,
    zone STRING,
    items_parsed ARRAY<STRUCT<
        item_id: STRING,
        quantity: INT,
        line_total: DOUBLE
    >>,
    item_count INT,
    order_amount INT,
    payment_method STRING,
    platform STRING
)
STORED AS PARQUET
LOCATION '/user/pratik/project/orders/orders_silver/';


-- ============================================
--          CATEGORYWISE COUNT
-- ===========================================
SELECT
    order_id,
    item.item_id,
    item.quantity,
    item.line_total
FROM orders_silver
LATERAL VIEW explode(items_parsed) exploded_items AS item;


WITH cte AS (
    SELECT
        item.item_id,
        item.quantity,
        item.line_total
    FROM orders_silver
    LATERAL VIEW explode(items_parsed) exploded_items AS item
)
SELECT
    items.category,
    COUNT(*) AS item_order_count
FROM cte
INNER JOIN items
ON cte.item_id=items.item_id
GROUP BY category;



-- ============================================
--          CATEGORYWISE COUNT
-- ===========================================
