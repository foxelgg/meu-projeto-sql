/* ========================================================================================
    Criação e Carga das Tabelas CLEAN

    - Dados retirados das tabelas de staging após o data profiling, limpeza leve e
      tratamento dos problemas identificados. As tabelas clean servirão como base para a
      análise e para a consolidação das tabelas finais, prontas para consumo de BI.

    ======================================================================================== */

-- Criação da tabela customers_clean a partir da tabela customers_staging
CREATE TABLE IF NOT EXISTS customers_clean AS
SELECT
    customer_id::TEXT AS customer_id,
    customer_name::TEXT AS customer_name,
    customer_city::TEXT AS customer_city
FROM customers_staging
WHERE customer_id IS NOT NULL

-- Adicionando customer_id como chave primária
ALTER TABLE customers_clean
ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

-- Criação da tabela order_items_clean a partir da tabela order_items_staging
CREATE TABLE order_items_clean AS
SELECT
    oi.order_id::TEXT AS order_id,
    oi.product_id::TEXT AS product_id,
    oi.price::NUMERIC AS price,
    oi.freight_value::NUMERIC AS freight_value
FROM order_items_staging oi
JOIN orders_clean o
    ON oi.order_id = o.order_id

-- Adicionando order_id + product_id como chave primária composta
ALTER TABLE order_items_clean
ADD CONSTRAINT pk_order_items PRIMARY KEY (order_id, product_id);

-- Criação da tabela orders_clean a partir da tabela orders_staging
CREATE TABLE IF NOT EXISTS orders_clean AS
SELECT
    order_id::TEXT AS order_id,
    customer_id::TEXT AS customer_id,
    purchased_at::DATE AS purchased_at,
    delivered_at::DATE AS delivered_at
FROM orders_staging
WHERE order_id IS NOT NULL;

-- Adicionando order_id como chave primária
ALTER TABLE orders_clean
ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

-- Criação da tabela payments_clean a partir da tabela payments_staging e criando PK artificial
CREATE TABLE IF NOT EXISTS payments_clean AS
SELECT
    ROW_NUMBER() OVER () AS payment_id,
    p.order_id::TEXT AS order_id,
    p.payment_type::TEXT AS payment_type,
    p.payment_value::NUMERIC AS payment_value
FROM payments_staging p
JOIN orders_clean o
    ON p.order_id = o.order_id

-- Criação da tabela products_clean a partir da tabela products_staging
CREATE TABLE IF NOT EXISTS products_clean AS
SELECT
    product_id::TEXT AS product_id,
    product_name::TEXT AS product_name,
    category::TEXT AS category
FROM products_staging
WHERE product_id IS NOT NULL;

-- Adicionando product_id como chave primária
ALTER TABLE products_clean
ADD CONSTRAINT pk_products PRIMARY KEY (product_id);

-- Adicionando chaves estrangeiras da tabela order_items_clean
ALTER TABLE order_items_clean
ADD CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders_clean(order_id);

-- Adicionando chave estrangeira da tabela payments_clean
ALTER TABLE payments_clean
ADD CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders_clean(order_id);

-- Criando coluna de validação de dados na tabela orders_clean
ALTER TABLE orders_clean
ADD COLUMN is_valid_for_analysis BOOLEAN 

UPDATE orders_clean o
SET is_valid_for_analysis = NOT EXISTS (
    SELECT 1
    FROM issue_log il
    WHERE il.record_id = o.order_id
    AND status = 'OPEN'
)

-- Depois de executar diversas resoluções de erros que ficaram pendentes na área de staging, vamos recarregar as tabelas clean: payments, orders e order_items.

-- Tabela PAYMENTS
TRUNCATE TABLE payments_clean;

INSERT INTO payments_clean (payment_id, order_id, payment_type, payment_value)
SELECT
    ROW_NUMBER() OVER () AS payment_id,
    order_id::TEXT AS order_id,
    payment_type::TEXT AS payment_type,
    payment_value::NUMERIC AS payment_value
FROM payments_staging;

-- Tabela ORDER_ITEMS
TRUNCATE TABLE order_items_clean;

INSERT INTO order_items_clean (order_id, product_id, price, freight_value)
SELECT
    order_id::TEXT AS order_id,
    product_id::TEXT AS product_id,
    price::NUMERIC AS price,
    freight_value::NUMERIC AS freight_value
FROM order_items_staging;

-- Tabela ORDERS
DELETE FROM orders_clean oc
WHERE NOT EXISTS (
    SELECT 1
    FROM orders_staging os
    WHERE os.order_id = oc.order_id
); -- Usei essa técnica pra limpar a tabela orders_clean por que deveria ter dado TRUNCATE primeiro nela - dar TRUNCATE agora retorna erro pois existem FKs dependentes
   -- que eu configurei lá em cima (add constraint)
