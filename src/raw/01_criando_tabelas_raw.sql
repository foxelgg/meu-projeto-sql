/* ========================================================================================
    Criando tabelas RAW
    - Dados crus, exatamente como chegam do dataset.
    - Todos definidos como TEXT para evitar erros na ingestão.
    ======================================================================================== */

-- Tabela de clientes 
CREATE TABLE customers_raw (
    customer_id TEXT,
    customer_name TEXT,
    customer_city TEXT
);

-- Tabela de itens do pedido
CREATE TABLE order_items_raw (
    order_id TEXT,
    product_id TEXT,
    price TEXT,
    freight_value TEXT
);

-- Tabela de pedidos
CREATE TABLE orders_raw (
    order_id TEXT,
    customer_id TEXT,
    purchased_at TEXT,
    delivered_at TEXT
);

-- Tabela de pagamentos
CREATE TABLE payments_raw (
    order_id TEXT,
    payment_type TEXT,
    payment_value TEXT
);

-- Tabela de produtos
CREATE TABLE products_raw (
    product_id TEXT,
    product_name TEXT,
    category TEXT
);
