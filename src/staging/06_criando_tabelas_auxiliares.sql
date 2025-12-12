/* ========================================================================================
    Criação de tabelas auxiliares

    Esse arquivo visa documentar o processo de criação de tabelas auxiliares que serão
    utilizadas durante o processo de ETL para suportar transformações, validações e
    enriquecimento dos dados. As seguintes tabelas serão criadas:

    - ISSUE_REFERENCE: Tabela para catalogar os tipos de problemas de qualidade de dados
      identificados durante o data profiling. As colunas da tabela ISSUE_REFERENCE são:
        - issue_code: Código único para cada tipo de problema (PK).
        - issue_type: Tipo de problema (UNIQUE para evitar duas issues referenciando mesmo erro).
        - issue_description: Descrição detalhada do problema.
        - severity: Nível de severidade do problema (low, medium, high, critical).
        - default_action: Ação padrão recomendada para resolver o problema.
        - created_at: Timestamp de quando o registro foi criado.

    - AUDIT_LOG: Tabela para registrar logs de auditoria das operações de ETL, incluindo
      timestamps, status e detalhes das execuções.

    - ORPHAN: Tabela para armazenar registros órfãos que não possuem correspondência
      nas tabelas principais, facilitando a análise e correção posterior.

    ======================================================================================== */

-- Criação da tabela ISSUE_REFERENCE
CREATE TABLE issue_reference (
    issue_code TEXT PRIMARY KEY,
    issue_type TEXT UNIQUE,
    issue_description TEXT,
    severity TEXT CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    default_action TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Criação da tabela AUDIT_LOG - Customers
CREATE TABLE audit_log_customers (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id TEXT,
    customer_name TEXT,
    customer_city TEXT,
    issue_code TEXT REFERENCES issue_reference(issue_code),
    issue_description TEXT,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution TEXT,
    resolved_at TIMESTAMP,
    detected_by TEXT
);

-- Criação da tabela AUDIT_LOG - Order Items
CREATE TABLE audit_log_order_items (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id TEXT,
    product_id TEXT,
    price TEXT,
    freight_value TEXT,
    issue_code TEXT REFERENCES issue_reference(issue_code),
    issue_description TEXT,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution TEXT,
    resolved_at TIMESTAMP,
    detected_by TEXT
);

-- Criação da tabela AUDIT_LOG - Orders
CREATE TABLE audit_log_orders (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id TEXT,
    customer_id TEXT,
    purchased_at TEXT,
    delivered_at TEXT,
    issue_code TEXT REFERENCES issue_reference(issue_code),
    issue_description TEXT,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution TEXT,
    resolved_at TIMESTAMP,
    detected_by TEXT
);

-- Criação da tabela AUDIT_LOG - Payments
CREATE TABLE audit_log_payments (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id TEXT,
    payment_type TEXT,
    payment_value TEXT,
    issue_code TEXT REFERENCES issue_reference(issue_code),
    issue_description TEXT,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution TEXT,
    resolved_at TIMESTAMP,
    detected_by TEXT
);

-- Criação da tabela AUDIT_LOG - Products
CREATE TABLE audit_log_products (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    issue_code TEXT REFERENCES issue_reference(issue_code),
    issue_description TEXT,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution TEXT,
    resolved_at TIMESTAMP,
    detected_by TEXT
);

-- Criação da tabela ORPHAN - Customers
CREATE TABLE orphan_customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    customer_city TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);

-- Criação da tabela ORPHAN - Order Items
CREATE TABLE orphan_order_items (
    order_id TEXT,
    product_id TEXT,
    price TEXT,
    freight_value TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);

-- Criação da tabela ORPHAN - Orders
CREATE TABLE orphan_orders (
    order_id TEXT,
    customer_id TEXT,
    purchased_at TEXT,
    delivered_at TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);

-- Criação da tabela ORPHAN - Payments
CREATE TABLE orphan_payments (
    order_id TEXT,
    payment_type TEXT,
    payment_value TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);

-- Criação da tabela ORPHAN - Products
CREATE TABLE orphan_products (
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);
