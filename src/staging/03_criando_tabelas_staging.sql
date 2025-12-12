/* ========================================================================================
    Criando tabelas STAGING
    - Tabelas que receberão os dados direto das tabelas RAW para posterior transformação.
    - Os tipos de dados ainda são TEXT para facilitar a carga inicial.
    ======================================================================================== */

-- Tabela de clientes 
CREATE TABLE customers_staging (
    customer_id TEXT,
    customer_name TEXT,
    customer_city TEXT
);

-- Tabela de itens do pedido
CREATE TABLE order_items_staging (
    order_id TEXT,
    product_id TEXT,
    price TEXT,
    freight_value TEXT
);

-- Tabela de pedidos
CREATE TABLE orders_staging (
    order_id TEXT,
    customer_id TEXT,
    purchased_at TEXT,
    delivered_at TEXT
);

-- Tabela de pagamentos
CREATE TABLE payments_staging (
    order_id TEXT,
    payment_type TEXT,
    payment_value TEXT
);

-- Tabela de produtos
CREATE TABLE products_staging (
    product_id TEXT,
    product_name TEXT,
    category TEXT
);
