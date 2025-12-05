--5.1
SELECT
    seller_id,
    SUM(price) AS total_faturado
FROM
    olist_order_item
GROUP BY
    seller_id
ORDER BY
    total_faturado DESC;


--5.2
SELECT
    t1.customer_unique_id,
    t1.customer_city,
    t1.customer_state,
    COUNT(DISTINCT t2.order_id) AS total_pedidos,
    SUM(t3.price + t3.freight_value) AS valor_total_gasto
FROM
    olist_customer AS t1
JOIN
    olist_order AS t2 ON t1.customer_id = t2.customer_id
JOIN
    olist_order_item AS t3 ON t2.order_id = t3.order_id
WHERE
    t2.order_status = 'delivered' 
    AND t2.order_purchase_timestamp >= '2017-01-01' 
    AND t2.order_purchase_timestamp < '2018-01-01'  
GROUP BY
    t1.customer_unique_id,
    t1.customer_city,
    t1.customer_state
ORDER BY
    valor_total_gasto DESC
LIMIT 10;

--5.3
SELECT
    t1.seller_id,
    t1.seller_city,
    t1.seller_state,
    COUNT(t3.review_score) AS total_avaliacoes,
    
    ROUND(AVG(t3.review_score)::NUMERIC, 2) AS media_avaliacao 
FROM
    olist_seller AS t1
JOIN
    olist_order_item AS t2 ON t1.seller_id = t2.seller_id
JOIN
    olist_order_review AS t3 ON t2.order_id = t3.order_id
GROUP BY
    t1.seller_id,
    t1.seller_city,
    t1.seller_state
ORDER BY
    media_avaliacao DESC;

    --5.4
SELECT
    t1.order_id,
    t1.order_status,
    t1.order_purchase_timestamp,
    t2.customer_unique_id,
    t2.customer_city,
    t2.customer_state,
    SUM(t3.payment_value) AS valor_total_pago
FROM
    olist_order AS t1
JOIN
    olist_customer AS t2 ON t1.customer_id = t2.customer_id
LEFT JOIN
    olist_order_payment AS t3 ON t1.order_id = t3.order_id
WHERE
    t1.order_purchase_timestamp >= '2018-01-01 00:00:00'  
    AND t1.order_purchase_timestamp <= '2018-01-31 23:59:59' 
GROUP BY
    t1.order_id,
    t1.order_status,
    t1.order_purchase_timestamp,
    t2.customer_unique_id,
    t2.customer_city,
    t2.customer_state
ORDER BY
    t1.order_purchase_timestamp;


--5.5
    SELECT
    t1.product_id,
    t2.product_category_name,
    COUNT(t1.order_id) AS total_unidades_vendidas
FROM
    olist_order_item AS t1
JOIN
    olist_product AS t2 ON t1.product_id = t2.product_id
JOIN
    olist_order AS t3 ON t1.order_id = t3.order_id 
WHERE
    t3.order_purchase_timestamp >= '2017-06-01' 
    AND t3.order_purchase_timestamp < '2017-07-01'    
GROUP BY
    t1.product_id,
    t2.product_category_name
ORDER BY
    total_unidades_vendidas DESC
LIMIT 5;

--5.6
SELECT
    t1.order_id,
    t1.order_purchase_timestamp AS data_da_compra,
    t1.order_estimated_delivery_date AS entrega_estimada,
    t1.order_delivered_customer_date AS entrega_real,
    
    EXTRACT(EPOCH FROM (t1.order_delivered_customer_date - t1.order_estimated_delivery_date)) / 86400 AS dias_de_atraso
FROM
    olist_order AS t1
WHERE
    t1.order_status = 'delivered' 
    AND t1.order_delivered_customer_date > t1.order_estimated_delivery_date 
    AND t1.order_purchase_timestamp >= '2017-01-01'
    AND t1.order_purchase_timestamp < '2018-01-01' 
ORDER BY
    dias_de_atraso DESC
LIMIT 10;

--5.7
SELECT
    t1.customer_unique_id,
    t1.customer_city,
    t1.customer_state,
    COUNT(DISTINCT t2.order_id) AS total_pedidos,
    ROUND(SUM(t3.payment_value), 2) AS valor_total_pago
FROM
    olist_customer AS t1
JOIN
    olist_order AS t2 ON t1.customer_id = t2.customer_id
JOIN
    olist_order_payment AS t3 ON t2.order_id = t3.order_id
GROUP BY
    t1.customer_unique_id,
    t1.customer_city,
    t1.customer_state
ORDER BY
    valor_total_pago DESC
LIMIT 10;

--5.8
SELECT
    t2.customer_state,
    COUNT(t1.order_id) AS total_pedidos_entregues,
    
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM (t1.order_delivered_customer_date - t1.order_delivered_carrier_date)) / 86400
        ), 
        2
    ) AS tempo_medio_entrega_dias
FROM
    olist_order AS t1
JOIN
    olist_customer AS t2 ON t1.customer_id = t2.customer_id
WHERE
    t1.order_status = 'delivered' 
    AND t1.order_delivered_carrier_date IS NOT NULL
    AND t1.order_delivered_customer_date IS NOT NULL 
GROUP BY
    t2.customer_state
ORDER BY
    tempo_medio_entrega_dias DESC;

    --5.9
   WITH cliente_localizacao AS (
   
    SELECT
        -23.545802 AS lat_cliente, 
        -46.639180 AS lng_cliente,
        50 AS raio_km              
),
vendedores_localizacao AS (
    SELECT
        t1.seller_id,
        t1.seller_zip_code_prefix,
        AVG(t2.geolocation_lat) AS lat_vendedor,
        AVG(t2.geolocation_lng) AS lng_vendedor
    FROM
        olist_seller AS t1
    JOIN
        olist_geolocation AS t2 
        ON t1.seller_zip_code_prefix = t2.geolocation_zip_code_prefix
    GROUP BY
        t1.seller_id,
        t1.seller_zip_code_prefix
),
calculo_distancia AS (
    SELECT
        vl.seller_id,
        (6371 * 2 * ASIN(
            SQRT(
                POWER(SIN((RADIANS(vl.lat_vendedor) - RADIANS(cl.lat_cliente)) / 2), 2) +
                COS(RADIANS(cl.lat_cliente)) * COS(RADIANS(vl.lat_vendedor)) *
                POWER(SIN((RADIANS(vl.lng_vendedor) - RADIANS(cl.lng_cliente)) / 2), 2)
            )
        )) AS distancia_nao_arredondada_km,
        cl.raio_km
    FROM
        vendedores_localizacao AS vl,
        cliente_localizacao AS cl
)
SELECT
    cd.seller_id,
    
    ROUND(cd.distancia_nao_arredondada_km::NUMERIC, 2) AS distancia_km
FROM
    calculo_distancia AS cd
WHERE
    cd.distancia_nao_arredondada_km <= cd.raio_km
ORDER BY
    distancia_km;