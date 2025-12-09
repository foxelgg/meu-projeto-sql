/* ========================================================================================
    Criando tabelas RAW
    - Dados crus, exatamente como chegam do dataset.
    - Todos definidos como TEXT para evitar erros na ingestão.
    ======================================================================================== */

CREATE TABLE customers_raw (
    customer_id TEXT,
    customer_name TEXT,
    customer_city TEXT
);

CREATE TABLE order_items_raw (
    order_id TEXT,
    product_id TEXT,
    price TEXT,
    freight_value TEXT
);

CREATE TABLE orders_raw (
    order_id TEXT,
    customer_id TEXT,
    purchased_at TEXT,
    delivered_at TEXT
);

CREATE TABLE payments_raw (
    order_id TEXT,
    payment_type TEXT,
    payment_value TEXT
);

CREATE TABLE products_raw (
    product_id TEXT,
    product_name TEXT,
    category TEXT
);

-- Queries para verificar se as tabelas foram criadas corretamente
SELECT * FROM customers_raw;
SELECT * FROM order_items_raw;
SELECT * FROM orders_raw;
SELECT * FROM payments_raw;
SELECT * FROM products_raw;
