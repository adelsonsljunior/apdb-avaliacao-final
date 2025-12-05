-- Script SQL para adicionar Chaves Estrangeiras (FKs), Índices Únicos e Restrições de Integridade
-- Baseado no diagrama de relacionamento e nos scripts CREATE TABLE fornecidos.

-- 1. Adicionar Chaves Estrangeiras (FKs)

-- Tabela olist_order
ALTER TABLE olist_order
ADD CONSTRAINT fk_order_customer
FOREIGN KEY (customer_id)
REFERENCES olist_customer (customer_id)
ON UPDATE CASCADE
ON DELETE RESTRICT;

-- Tabela olist_order_item
ALTER TABLE olist_order_item
ADD CONSTRAINT fk_order_item_order
FOREIGN KEY (order_id)
REFERENCES olist_order (order_id)
ON UPDATE CASCADE
ON DELETE CASCADE,

ADD CONSTRAINT fk_order_item_product
FOREIGN KEY (product_id)
REFERENCES olist_product (product_id)
ON UPDATE CASCADE
ON DELETE RESTRICT,

ADD CONSTRAINT fk_order_item_seller
FOREIGN KEY (seller_id)
REFERENCES olist_seller (seller_id)
ON UPDATE CASCADE
ON DELETE RESTRICT;

-- Tabela olist_order_payment
ALTER TABLE olist_order_payment
ADD CONSTRAINT fk_order_payment_order
FOREIGN KEY (order_id)
REFERENCES olist_order (order_id)
ON UPDATE CASCADE
ON DELETE CASCADE;

-- Tabela olist_order_review
ALTER TABLE olist_order_review
ADD CONSTRAINT fk_order_review_order
FOREIGN KEY (order_id)
REFERENCES olist_order (order_id)
ON UPDATE CASCADE
ON DELETE CASCADE;

-- Tabela olist_product
ALTER TABLE olist_product
ADD CONSTRAINT fk_product_category
FOREIGN KEY (product_category_name)
REFERENCES product_category_name_translation (product_category_name)
ON UPDATE CASCADE
ON DELETE RESTRICT;

-- 2. Restrições para valores não negativos (CHECK Constraints)

-- Tabela olist_product
ALTER TABLE olist_product
ADD CONSTRAINT chk_product_name_lenght CHECK (product_name_lenght >= 0),
ADD CONSTRAINT chk_product_description_lenght CHECK (product_description_lenght >= 0),
ADD CONSTRAINT chk_product_photos_qty CHECK (product_photos_qty >= 0),
ADD CONSTRAINT chk_product_weight_g CHECK (product_weight_g >= 0),
ADD CONSTRAINT chk_product_length_cm CHECK (product_length_cm >= 0),
ADD CONSTRAINT chk_product_height_cm CHECK (product_height_cm >= 0),
ADD CONSTRAINT chk_product_width_cm CHECK (product_width_cm >= 0);

-- Tabela olist_order_item
ALTER TABLE olist_order_item
ADD CONSTRAINT chk_order_item_price CHECK (price >= 0),
ADD CONSTRAINT chk_order_item_freight_value CHECK (freight_value >= 0);

-- Tabela olist_order_payment
ALTER TABLE olist_order_payment
ADD CONSTRAINT chk_order_payment_installments CHECK (payment_installments >= 0),
ADD CONSTRAINT chk_order_payment_value CHECK (payment_value >= 0);

-- Tabela olist_order_review
ALTER TABLE olist_order_review
ADD CONSTRAINT chk_order_review_score CHECK (review_score >= 1 AND review_score <= 5);