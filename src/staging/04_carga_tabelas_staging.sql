/* ========================================================================================
    Carregando tabelas STAGING
    
    Este arquivo documenta o processo de transferência dos dados das tabelas RAW para as
    tabelas STAGING.

    - Os dados são inseridos exatamente como estão nas tabelas RAW, sem transformações.
    - Este é um passo intermediário antes da transformação e carga nas tabelas finais.
    ======================================================================================== */

-- Carga da tabela customers_staging
INSERT INTO customers_staging (customer_id, customer_name, customer_city)
SELECT customer_id, customer_name, customer_city
FROM customers_raw;

-- Carga da tabela order_items_staging
INSERT INTO order_items_staging (order_id, product_id, price, freight_value)
SELECT order_id, product_id, price, freight_value
FROM order_items_raw;

-- Carga da tabela orders_staging
INSERT INTO orders_staging (order_id, customer_id, purchased_at, delivered_at)
SELECT order_id, customer_id, purchased_at, delivered_at
FROM orders_raw;

-- Carga da tabela payments_staging
INSERT INTO payments_staging (order_id, payment_type, payment_value)
SELECT order_id, payment_type, payment_value
FROM payments_raw;

-- Carga da tabela products_staging
INSERT INTO products_staging (product_id, product_name, category)
SELECT product_id, product_name, category
FROM products_raw;

