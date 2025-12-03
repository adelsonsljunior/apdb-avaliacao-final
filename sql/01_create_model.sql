--- CREATE TABLES

CREATE TABLE IF NOT EXISTS olist_seller
(
    seller_id              VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix CHAR(5),
    seller_city            VARCHAR(64),
    seller_state           CHAR(2)
);


CREATE TABLE IF NOT EXISTS product_category_name_translation
(
    product_category_name         VARCHAR(64),
    product_category_name_english VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS olist_product
(
    product_id                 VARCHAR(32) PRIMARY KEY,
    product_category_name      VARCHAR(64),
    product_name_lenght        INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty         INTEGER,
    product_weight_g           INTEGER,
    product_length_cm          INTEGER,
    product_height_cm          INTEGER,
    product_width_cm           INTEGER
);

CREATE TABLE IF NOT EXISTS olist_customer
(
    customer_id              VARCHAR(32) PRIMARY KEY,
    customer_unique_id       VARCHAR(32) UNIQUE,
    customer_zip_code_prefix CHAR(5),
    customer_city            VARCHAR(64),
    customer_state           CHAR(2)
);

CREATE TABLE IF NOT EXISTS olist_geolocation
(
    geolocation_zip_code_prefix CHAR(5),
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            VARCHAR(64),
    geolocation_state           CHAR(2)
);

CREATE TABLE IF NOT EXISTS olist_order
(
    order_id                      VARCHAR(32) PRIMARY KEY,
    customer_id                   VARCHAR(32),
    order_status                  VARCHAR(16),
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS olist_order_item
(
    order_id            VARCHAR(32),
    order_item_id       SMALLINT,
    product_id          VARCHAR(32),
    seller_id           VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(10, 2),
    freight_value       DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS olist_order_payment
(
    order_id             VARCHAR(32),
    payment_sequential   SMALLINT,
    payment_type         VARCHAR(16),
    payment_installments SMALLINT,
    payment_value        SMALLINT

);

CREATE TABLE IF NOT EXISTS olist_order_review(
    review_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32),
    review_score SMALLINT,
    review_comment_title VARCHAR(256),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);
