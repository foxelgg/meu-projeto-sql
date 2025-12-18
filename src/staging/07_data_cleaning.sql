/* ========================================================================================
    Data Cleaning Stage

    Este arquivo documenta o processo de limpeza e preparação dos dados nas tabelas STAGING.
    Essa etapa visa corrigir os problemas de qualidade dos dados identificados durante o
    data profiling, aplicando transformações e regras de negócio para garantir a
    integridade e consistência dos dados antes da carga final nas tabelas de destino.

    A etapa será dividida em duas principais classificações:

    - Limpeza leve: Normalizações automáticas seguras, que não serão registradas no issue_log.
      Exemplos incluem: Remoção de espaços em branco, padronização de textos, remoção de
      caracteres especiais, conversão de maiúsculas/minúsculas, padronização de datas, etc.

    - Tratamento semiautomático (regra de negócio aplicada - documentada em dq_rules.md):
        Transformações que envolvem regras de negócio específicas, que serão registradas no
        audit_log. Exemplos incluem: Correção de valores com base em listas de referência,
        preenchimento de valores ausentes com base em regras definidas, etc.

    ======================================================================================== */

-- **LIMPEZA LEVE NAS TABELAS STAGING**

-- Normalização de espaços em branco, padronização de textos e remoção de caracteres especiais.
-- Para isso, utilizaremos as funções TRIM, REGEXP_REPLACE (expressões regulares), e INITCAP com UNACCENT.


-- TABELA customers_staging
-- Normalização de textos nas colunas customer_name e customer_city
UPDATE customers_staging
SET customer_name = INITCAP(UNACCENT(TRIM(BOTH FROM REGEXP_REPLACE(
                        REGEXP_REPLACE(customer_name, '[^A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç'' ]', '', 'g'),
                        '\s+', ' ', 'g')))),
    customer_city = INITCAP(UNACCENT(TRIM(BOTH FROM REGEXP_REPLACE(
                        REGEXP_REPLACE(customer_city, '[^A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç'' ]', '', 'g'),
                        '\s+', ' ', 'g'))));

-- TABELA products_staging
-- Normalização de textos nas colunas product_name e category
UPDATE products_staging
SET product_name = INITCAP(UNACCENT(TRIM(BOTH FROM REGEXP_REPLACE(
                        REGEXP_REPLACE(product_name, '[^A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç''0-9 ]', '', 'g'),
                        '\s+', ' ', 'g')))),
    category = INITCAP(UNACCENT(TRIM(BOTH FROM REGEXP_REPLACE(
                        REGEXP_REPLACE(category, '[^A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç''0-9 ]', '', 'g'),
                        '\s+', ' ', 'g'))));

-- Correção de categorias comuns com erros de digitação
UPDATE products_staging
SET category = 'Eletronicos'
WHERE category IN ('Eletranicos', 'Eletronic')

-- **TRATAMENTO SEMIAUTOMÁTICO NAS TABELAS STAGING**

-- TABELA customers_staging
-- Marcar linhas com customer_id duplicado em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I002', 'customers_staging', 'customer_id', customer_id::TEXT, customer_id::TEXT,
       'Chave primária duplicada na tabela customers_staging',
       'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com customer_id duplicado detectado.'
FROM customers_staging
GROUP BY customer_id
HAVING COUNT(*) > 1
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I002'
      AND il.table_name = 'customers_staging'
      AND il.column_name = 'customer_id'
      AND il.record_id = customer_id::TEXT
);

-- Remoção de registros com customer_id duplicado (aplicando regra de negócio definida - manter o registro mais antigo)
WITH ranking_customers AS (
    SELECT ctid,
           customer_id,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn
    FROM customers_staging
) 
DELETE FROM customers_staging
WHERE ctid IN (
    SELECT ctid
    FROM ranking_customers
    WHERE rn > 1
);

-- Atualização do issue_log para marcar como RESOLVED os registros duplicados tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Duplicatas removidas, mantendo o registro mais antigo.')
WHERE issue_id = 'I002'
  AND table_name = 'customers_staging'
  AND column_name = 'customer_id'
  AND status = 'OPEN';

-- TABELA order_items_staging
-- Marcar linhas com price inválido (null, zero, negativo) em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I006', 'order_items_staging', 'price', order_id::TEXT, price::TEXT,
       'Valor de preço inválido: null, zero, negativo na tabela order_items_staging',
       'CRITICAL', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com preço inválido detectado.'
FROM order_items_staging
WHERE CAST(price AS NUMERIC) <= 0
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I006'
      AND il.table_name = 'order_items_staging'
      AND il.column_name = 'price'
      AND il.record_id = order_id::TEXT);

-- Tratamento de preços inválidos (aplicando regra de negócio definida - definir preço como NULL)
UPDATE order_items_staging
SET price = NULL
WHERE price IS NOT NULL AND CAST(price AS NUMERIC) <= 0;

-- Atualização do issue_log para marcar como RESOLVED os registros de preço inválido tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Valores de preço inválidos atualizados para NULL.')
WHERE issue_id = 'I006'
  AND table_name = 'order_items_staging'
  AND column_name = 'price'
  AND status = 'OPEN';

-- Marcar linhas com outlier em price em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I007', 'order_items_staging', 'price', order_id::TEXT, price::TEXT,
       'Valor estatisticamente muito acima do esperado na tabela order_items_staging',
       'MEDIUM', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com preço outlier detectado.'
FROM order_items_staging
WHERE price::numeric > (SELECT AVG(price::numeric) + 2 * STDDEV(price::numeric) FROM order_items_staging)
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I007'
      AND il.table_name = 'order_items_staging'
      AND il.column_name = 'price'
      AND il.record_id = order_id::TEXT);

-- Tratamento de outliers em price (aplicando regra de negócio definida - winsorizar para o valor máximo aceitável)
UPDATE order_items_staging
SET price = (
    SELECT ROUND(AVG(price::numeric) + 2 * STDDEV(price::numeric), 2)
    FROM order_items_staging
)
WHERE price::numeric > (SELECT AVG(price::numeric) + 2 * STDDEV(price::numeric) FROM order_items_staging);

-- Atualização do issue_log para marcar como RESOLVED os registros de preço outlier tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Valores de preço outlier ajustados para o valor máximo aceitável.')
WHERE issue_id = 'I007'
  AND table_name = 'order_items_staging'
  AND column_name = 'price'
  AND status = 'OPEN';

-- Marcar linhas com outlier em freight_value em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I007', 'order_items_staging', 'freight_value', order_id::TEXT, freight_value::TEXT,
       'Valor estatisticamente muito acima do esperado na tabela order_items_staging',
       'MEDIUM', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com freight_value outlier detectado.'
FROM order_items_staging
WHERE freight_value::numeric > (SELECT AVG(freight_value::numeric) + 2 * STDDEV(freight_value::numeric) FROM order_items_staging)
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I007'
      AND il.table_name = 'order_items_staging'
      AND il.column_name = 'freight_value'
      AND il.record_id = order_id::TEXT);

-- Tratamento de outliers em freight_value (aplicando regra de negócio definida - capar para o valor máximo aceitável)
UPDATE order_items_staging
SET freight_value = price::numeric * 2
WHERE freight_value::numeric > price::numeric * 2 AND price::numeric > 0;

-- Atualização do issue_log para marcar como RESOLVED os registros de freight_value outlier tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Valores de freight_value outlier ajustados para o valor máximo aceitável.')
WHERE issue_id = 'I007'
  AND table_name = 'order_items_staging'
  AND column_name = 'freight_value'
  AND status = 'OPEN';

-- Marcar linhas com product_id órfão em issue_log e registrar na tabela orphan_records
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I011', 'order_items_staging', 'product_id', oi.order_id::TEXT, oi.product_id::TEXT,
         'Chave estrangeira que não possui correspondência na tabela products_staging',
         'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com product_id órfão detectado.'
FROM order_items_staging oi
LEFT JOIN products_staging p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I011'
      AND il.table_name = 'order_items_staging'
      AND il.column_name = 'product_id'
      AND il.record_id = oi.order_id::TEXT);

-- Registrar órfãos na tabela orphan_records
INSERT INTO orphan_records (parent_table, parent_column, child_table, child_column, child_record_id, missing_parent_id, detected_at, pipeline_stage, status, analyst_note)
SELECT 'products_staging', 'product_id', 'order_items_staging', 'product_id',
       oi.order_id::TEXT, oi.product_id::TEXT, NOW(), 'DATA_CLEANING', 'OPEN',
       'Registro órfão de product_id detectado na tabela order_items_staging.'
FROM order_items_staging oi
LEFT JOIN products_staging p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM orphan_records orp
    WHERE orp.child_table = 'order_items_staging'
      AND orp.child_column = 'product_id'
      AND orp.child_record_id = oi.order_id::TEXT);

-- Remover registros órfãos de product_id na tabela order_items_staging
DELETE FROM order_items_staging oi
WHERE NOT EXISTS (
    SELECT 1 FROM products_staging p
    WHERE oi.product_id = p.product_id
);

-- Atualização do issue_log para marcar como RESOLVED os registros de product_id órfão tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Registros órfãos removidos da tabela order_items_staging.')
WHERE issue_id = 'I011'
  AND table_name = 'order_items_staging'
  AND column_name = 'product_id'
  AND status = 'OPEN';

-- Atualização da tabela orphan_records para marcar como RESOLVED os registros tratados
UPDATE orphan_records
SET status = 'RESOLVED'
WHERE parent_table = 'products_staging'
  AND child_table = 'order_items_staging'
  AND status = 'OPEN';

-- A remoção do product_id órfão fez com que a order_id O4 ficasse sem pedidos válidos, ou seja, registro sem dependentes. Vamos marcar esse caso em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I017', 'orders_staging', 'order_id', o.order_id::TEXT, o.order_id::TEXT,
       'Pedido sem itens associados na tabela orders_staging',
       'MEDIUM', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro de pedido sem itens detectado.'
FROM orders_staging o
LEFT JOIN order_items_staging oi ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I017'
      AND il.table_name = 'orders_staging'
      AND il.column_name = 'order_id'
      AND il.record_id = o.order_id::TEXT);


-- Marcar linhas com order_id órfão em issue_log e registrar na tabela orphan_records
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I011', 'order_items_staging', 'order_id', oi.order_id::TEXT, oi.order_id::TEXT,
        'Chave estrangeira que não possui correspondência na tabela orders_staging',
        'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com order_id órfão detectado na tabela order_items_staging.'
FROM order_items_staging oi
LEFT JOIN orders_staging o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I011'
      AND il.table_name = 'order_items_staging'
      AND il.column_name = 'order_id'
      AND il.record_id = oi.order_id::TEXT);
    
-- Registrar órfãos na tabela orphan_records
INSERT INTO orphan_records (parent_table, parent_column, child_table, child_column, child_record_id, missing_parent_id, detected_at, pipeline_stage, status, analyst_note)
SELECT 'orders_staging', 'order_id', 'order_items_staging', 'order_id',
       oi.order_id::TEXT, oi.order_id::TEXT, NOW(), 'DATA_CLEANING', 'OPEN',
       'Registro órfão de order_id detectado na tabela order_items_staging.'
FROM order_items_staging oi
LEFT JOIN orders_staging o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM orphan_records orp
    WHERE orp.child_table = 'order_items_staging'
      AND orp.child_column = 'order_id'
      AND orp.child_record_id = oi.order_id::TEXT);

-- Remover registros órfãos de order_id na tabela order_items_staging
DELETE FROM order_items_staging oi
WHERE NOT EXISTS (
    SELECT 1 FROM orders_staging o
    WHERE oi.order_id = o.order_id
)
AND oi.order_id NOT IN (
    SELECT il.record_id
    FROM issue_log il
    WHERE il.table_name = 'orders_staging'
    AND il.issue_id = 'I011'
    AND il.status = 'RESOLVED'
);

-- Atualização do issue_log para marcar como RESOLVED os registros de order_id órfão tratados
UPDATE issue_log il
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Registros órfãos removidos da tabela order_items_staging.')
WHERE il.table_name = 'order_items_staging'
  AND il.issue_id = 'I011'
  AND il.status = 'OPEN'
  AND il.record_id NOT IN (
    SELECT record_id
    FROM issue_log
    WHERE table_name = 'orders_staging'
      AND issue_id = 'I011'
      AND status = 'RESOLVED'
  );

-- Atualização da tabela orphan_records para marcar como RESOLVED os registros tratados
UPDATE orphan_records
SET status = 'RESOLVED'
WHERE parent_table = 'orders_staging'
  AND child_table = 'order_items_staging'
  AND status = 'OPEN'
  AND child_record_id NOT IN (
    SELECT record_id
    FROM issue_log
    WHERE table_name = 'order_items_staging'
      AND issue_id = 'I011'
      AND status = 'RESOLVED'
  );

-- TABELA orders_staging

-- Marcar linhas com datas inválidas em issue_log
INSERT INTO issue_log (
    issue_id,
    table_name,
    column_name,
    record_id,
    detected_value,
    detection_rule,
    severity,
    status,
    detected_at,
    pipeline_stage,
    analyst_note
)
SELECT
    'I009',
    'orders_staging',
    'purchased_at / delivered_at',
    order_id::TEXT,
    CONCAT(
        'purchased_at: ', purchased_at,
        ', delivered_at: ', delivered_at
    ),
    'Valor de data inválido ou fora do intervalo esperado',
    'MEDIUM',
    'OPEN',
    NOW(),
    'DATA_CLEANING',
    'Data inválida, formato inesperado ou valor fora do intervalo permitido.'
FROM orders_staging
WHERE
    -- formato básico inválido
    purchased_at IS NULL
    OR delivered_at IS NULL

    OR purchased_at !~ '^\d{4}-\d{2}-\d{2}$'
    OR delivered_at !~ '^\d{4}-\d{2}-\d{2}$'

    -- datas que não batem quando convertidas
    OR TO_DATE(purchased_at, 'YYYY-MM-DD')::TEXT <> purchased_at
    OR TO_DATE(delivered_at, 'YYYY-MM-DD')::TEXT <> delivered_at

    -- intervalo de negócio
    OR TO_DATE(purchased_at, 'YYYY-MM-DD') < DATE '2000-01-01'
    OR TO_DATE(delivered_at, 'YYYY-MM-DD') < DATE '2000-01-01'

    OR TO_DATE(purchased_at, 'YYYY-MM-DD') > CURRENT_DATE
    OR TO_DATE(delivered_at, 'YYYY-MM-DD') > CURRENT_DATE

AND NOT EXISTS (
    SELECT 1
    FROM issue_log il
    WHERE il.issue_id = 'I009'
      AND il.table_name = 'orders_staging'
      AND il.record_id = order_id::TEXT
);

-- Tratamento de datas inválidas (aplicando regra de negócio definida - converter para NULL)
-- coluna purchased_at
UPDATE orders_staging
SET purchased_at = NULL
WHERE purchased_at IS NOT NULL AND (
    purchased_at !~ '^\d{4}-\d{2}-\d{2}$'
    OR TO_DATE(purchased_at, 'YYYY-MM-DD')::TEXT <> purchased_at
    OR TO_DATE(purchased_at, 'YYYY-MM-DD') < DATE '2000-01-01'
    OR TO_DATE(purchased_at, 'YYYY-MM-DD') > CURRENT_DATE
);
-- coluna delivered_at
UPDATE orders_staging
SET delivered_at = NULL
WHERE delivered_at IS NOT NULL AND (
    delivered_at !~ '^\d{4}-\d{2}-\d{2}$'
    OR TO_DATE(delivered_at, 'YYYY-MM-DD')::TEXT <> delivered_at
    OR TO_DATE(delivered_at, 'YYYY-MM-DD') < DATE '2000-01-01'
    OR TO_DATE(delivered_at, 'YYYY-MM-DD') > CURRENT_DATE
);

-- Atualização do issue_log para marcar como RESOLVED os registros de datas inválidas tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Datas inválidas convertidas para NULL.')
WHERE issue_id = 'I009'
  AND table_name = 'orders_staging'
  AND status = 'OPEN';

-- Marcar linhas com inconsistência na sequência de datas em issue_log
INSERT INTO issue_log (
    issue_id,
    table_name,
    column_name,
    record_id,
    detected_value,
    detection_rule,
    severity,
    status,
    detected_at,
    pipeline_stage,
    analyst_note
)
SELECT
    'I010',
    'orders_staging',
    'purchased_at / delivered_at',
    order_id::TEXT,
    CONCAT(
        'purchased_at: ', purchased_at,
        ', delivered_at: ', delivered_at
    ),
    'Sequência de datas de compra e entrega inconsistente',
    'HIGH',
    'OPEN',
    NOW(),
    'DATA_CLEANING',
    'Data de entrega anterior à data de compra.'
FROM orders_staging
WHERE
    purchased_at IS NOT NULL
    AND delivered_at IS NOT NULL
    AND TO_DATE(delivered_at, 'YYYY-MM-DD') < TO_DATE(purchased_at, 'YYYY-MM-DD')
AND NOT EXISTS (
    SELECT 1
    FROM issue_log il
    WHERE il.issue_id = 'I010'
      AND il.table_name = 'orders_staging'
      AND il.record_id = order_id::TEXT
);

-- Tratamento de inconsistência na sequência de datas (aplicando regra de negócio definida - converter delivered_at para NULL, manter purchased_at)
UPDATE orders_staging
SET delivered_at = NULL
WHERE
    purchased_at IS NOT NULL
    AND delivered_at IS NOT NULL
    AND TO_DATE(delivered_at, 'YYYY-MM-DD') < TO_DATE(purchased_at, 'YYYY-MM-DD');

-- Atualização do issue_log para marcar como RESOLVED os registros de inconsistência na sequência de datas tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Inconsistência na sequência de datas corrigida (delivered_at convertido para NULL).')
WHERE issue_id = 'I010'
  AND table_name = 'orders_staging'
  AND status = 'OPEN';

-- Marcar linhas com customer_id órfão em issue_log e registrar na tabela orphan_records
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I011', 'orders_staging', 'customer_id', o.order_id::TEXT, o.customer_id::TEXT,
         'Chave estrangeira que não possui correspondência na tabela customers_staging',
         'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com customer_id órfão detectado.'
FROM orders_staging o
LEFT JOIN customers_staging c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM issue_log il
    WHERE il.issue_id = 'I011'
      AND il.table_name = 'orders_staging'
      AND il.record_id = o.order_id::TEXT
);

-- Registrar órfãos na tabela orphan_records
INSERT INTO orphan_records (parent_table, parent_column, child_table, child_column, child_record_id, missing_parent_id, detected_at, pipeline_stage, status, analyst_note)
SELECT 'customers_staging', 'customer_id', 'orders_staging', 'customer_id',
       o.order_id::TEXT, o.customer_id::TEXT, NOW(), 'DATA_CLEANING', 'OPEN',
       'Registro órfão de customer_id detectado na tabela orders_staging.'
FROM orders_staging o
LEFT JOIN customers_staging c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM orphan_records orp
    WHERE orp.child_table = 'orders_staging'
      AND orp.child_column = 'customer_id'
      AND orp.child_record_id = o.order_id::TEXT
);

-- Remover registros órfãos de customer_id na tabela orders_staging
DELETE FROM orders_staging o
WHERE NOT EXISTS (
    SELECT 1 FROM customers_staging c
    WHERE o.customer_id = c.customer_id
);

-- Atualização do issue_log para marcar como RESOLVED os registros de customer_id órfão tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Registros órfãos removidos da tabela orders_staging.')
WHERE issue_id = 'I011'
  AND table_name = 'orders_staging'
  AND status = 'OPEN';

-- Atualização da tabela orphan_records para marcar como RESOLVED os registros tratados
UPDATE orphan_records
SET status = 'RESOLVED'
WHERE parent_table = 'customers_staging'
  AND child_table = 'orders_staging'
  AND status = 'OPEN';

-- A remoção do órfão de customer_id fez com que a order O3 (PK) fosse removida, deixando órfãos induzidos nas tabelas payments e order_items. Vamos registrar ambos em issue_log e orphan_records.

-- Marcar linhas com órfãos induzidos da tabela order_items_staging em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I011', 'order_items_staging', 'order_id', oi.order_id::TEXT, oi.order_id::TEXT,
         'Chave estrangeira que não possui correspondência na tabela orders_staging',
         'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com order_id órfão detectado na tabela order_items_staging.'
FROM order_items_staging oi
LEFT JOIN orders_staging o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I011'
      AND il.table_name = 'order_items_staging'
      AND il.column_name = 'order_id'
      AND il.record_id = oi.order_id::TEXT);

-- Registrar órfãos induzidos da tabela order_items_staging na tabela orphan_records
INSERT INTO orphan_records (parent_table, parent_column, child_table, child_column, child_record_id, missing_parent_id, detected_at, pipeline_stage, status, analyst_note)
SELECT 'orders_staging', 'order_id', 'order_items_staging', 'order_id',
       oi.order_id::TEXT, oi.order_id::TEXT, NOW(), 'DATA_CLEANING', 'OPEN',
       'Registro órfão induzido de order_id detectado na tabela order_items_staging.'
FROM order_items_staging oi
LEFT JOIN orders_staging o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM orphan_records orp
    WHERE orp.child_table = 'order_items_staging'
      AND orp.child_column = 'order_id'
      AND orp.child_record_id = oi.order_id::TEXT);

-- Marcar linhas com órfãos induzidos da tabela payments_staging em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I011', 'payments_staging', 'order_id', p.order_id::TEXT, p.order_id::TEXT,
         'Chave estrangeira que não possui correspondência na tabela orders_staging',
         'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com order_id órfão detectado na tabela payments_staging.'
FROM payments_staging p
LEFT JOIN orders_staging o ON p.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I011'
      AND il.table_name = 'payments_staging'
      AND il.column_name = 'order_id'
      AND il.record_id = p.order_id::TEXT);

SELECT * FROM orphan_records;

-- Registrar órfãos induzidos da tabela payments_staging na tabela orphan_records
INSERT INTO orphan_records (parent_table, parent_column, child_table, child_column, child_record_id, missing_parent_id, detected_at, pipeline_stage, status, analyst_note)
SELECT 'orders_staging', 'order_id', 'payments_staging', 'order_id',
       p.order_id::TEXT, p.order_id::TEXT, NOW(), 'DATA_CLEANING', 'OPEN',
       'Registro órfão induzido de order_id detectado na tabela payments_staging.'
FROM payments_staging p
LEFT JOIN orders_staging o ON p.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM orphan_records orp
    WHERE orp.child_table = 'payments_staging'
      AND orp.child_column = 'order_id'
      AND orp.child_record_id = p.order_id::TEXT);

-- TABELA payments_staging

-- Marcar linhas com payment_value inválido (null, zero, negativo) em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I014', 'payments_staging', 'payment_value', p.order_id::TEXT, p.payment_value::TEXT,
       'Valor de pagamento inválido: null, zero, negativo na tabela payments_staging',
       'CRITICAL', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com valor de pagamento inválido detectado.'
FROM payments_staging p
WHERE CAST(payment_value AS NUMERIC) <= 0
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I014'
      AND il.table_name = 'payments_staging'
      AND il.column_name = 'payment_value'
      AND il.record_id = p.order_id::TEXT);

-- Tratamento de payment_value inválido (aplicando regra de negócio definida - definir payment_value como NULL)
UPDATE payments_staging
SET payment_value = NULL
WHERE payment_value IS NOT NULL AND CAST(payment_value AS NUMERIC) <= 0;

-- Atualização do issue_log para marcar como RESOLVED os registros de payment_value inválido tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Valores de payment_value inválidos atualizados para NULL.')
WHERE issue_id = 'I014'
  AND table_name = 'payments_staging'
  AND column_name = 'payment_value'
  AND status = 'OPEN';

-- Marcar linhas com payment_value outlier em issue_log
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I007', 'payments_staging', 'payment_value', p.order_id::TEXT, p.payment_value::TEXT,
       'Valor estatisticamente muito acima do esperado na tabela payments_staging',
       'MEDIUM', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com payment_value outlier detectado.'
FROM payments_staging p
WHERE CAST(p.payment_value AS NUMERIC) > (
    SELECT AVG(CAST(payment_value AS NUMERIC)) + 2 * STDDEV(CAST(payment_value AS NUMERIC))
    FROM payments_staging
)
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I007'
      AND il.table_name = 'payments_staging'
      AND il.column_name = 'payment_value'
      AND il.record_id = p.order_id::TEXT
);

-- Tratamento de outliers em payment_value (aplicando regra de negócio definida - winsorizar para o valor máximo aceitável)
WITH CTE AS (
    SELECT
        ROUND(AVG(CAST(payment_value AS NUMERIC)) + 2 * STDDEV(CAST(payment_value AS NUMERIC)), 2) AS max_payment_value
    FROM payments_staging
)
UPDATE payments_staging
SET payment_value = max_payment_value
FROM CTE c
WHERE CAST(payment_value AS NUMERIC) > c.max_payment_value;

-- Atualização do issue_log para marcar como RESOLVED os registros de payment_value outlier tratados
UPDATE issue_log
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Valores de payment_value outlier ajustados para o valor máximo aceitável.')
WHERE issue_id = 'I007'
  AND table_name = 'payments_staging'
  AND column_name = 'payment_value'
  AND status = 'OPEN';

-- Marcar linhas com order_id órfão em issue_log e registrar na tabela orphan_records
INSERT INTO issue_log (issue_id, table_name, column_name, record_id, detected_value, detection_rule, severity, status, detected_at, pipeline_stage, analyst_note)
SELECT 'I011', 'payments_staging', 'order_id', p.order_id::TEXT, p.order_id::TEXT,
         'Chave estrangeira que não possui correspondência na tabela orders_staging',
         'HIGH', 'OPEN', NOW(), 'DATA_CLEANING', 'Registro com order_id órfão detectado.'
FROM payments_staging p
LEFT JOIN orders_staging o ON p.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM issue_log il
    WHERE il.issue_id = 'I011'
      AND il.table_name = 'payments_staging'
      AND il.column_name = 'order_id'
      AND il.record_id = p.order_id::TEXT); -- Processo redundante (já foi adicionado anteriormente)

-- Registrar órfãos na tabela orphan_records
INSERT INTO orphan_records (parent_table, parent_column, child_table, child_column, child_record_id, missing_parent_id, detected_at, pipeline_stage, status, analyst_note)
SELECT 'orders_staging', 'order_id', 'payments_staging', 'order_id',
         p.order_id::TEXT, p.order_id::TEXT, NOW(), 'DATA_CLEANING', 'OPEN',
         'Registro órfão de order_id detectado na tabela payments_staging.'
FROM payments_staging p
LEFT JOIN orders_staging o ON p.order_id = o.order_id
WHERE o.order_id IS NULL
AND NOT EXISTS (
    SELECT 1 FROM orphan_records orp
    WHERE orp.child_table = 'payments_staging'
      AND orp.child_column = 'order_id'
      AND orp.child_record_id = p.order_id::TEXT); -- Processo redundante (já foi adicionado anteriormente)

-- Remover registros órfãos de order_id na tabela payments_staging
DELETE FROM payments_staging p
WHERE NOT EXISTS (
    SELECT 1 FROM orders_staging o
    WHERE p.order_id = o.order_id
) AND p.order_id NOT IN (
    SELECT il.record_id
    FROM issue_log il
    WHERE il.table_name = 'orders_staging'
    AND il.issue_id = 'I011'
    AND il.status = 'RESOLVED'
);

-- Atualização do issue_log para marcar como RESOLVED os registros de order_id órfão tratados
UPDATE issue_log il
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Registros órfãos removidos da tabela payments_staging.')
WHERE il.table_name = 'payments_staging'
  AND il.issue_id = 'I011'
  AND il.status = 'OPEN'
  AND il.record_id NOT IN (
    SELECT record_id
    FROM issue_log
    WHERE table_name = 'orders_staging'
      AND issue_id = 'I011'
      AND status = 'RESOLVED'
  );

-- Atualização da tabela orphan_records para marcar como RESOLVED os registros tratados
UPDATE orphan_records
SET status = 'RESOLVED'
WHERE parent_table = 'orders_staging'
  AND child_table = 'payments_staging'
  AND status = 'OPEN'
  AND child_record_id NOT IN (
    SELECT record_id
    FROM issue_log
    WHERE table_name = 'payments_staging'
      AND issue_id = 'I011'
      AND status = 'RESOLVED' -- erro de lógica nesse NOT IN
  );

-- A tabela products_staging não possui tratamentos semiautomáticos definidos além do já realizado no início deste script.

-- CORREÇÕES DE PROBLEMAS QUE FORAM ESQUECIDOS DURANTE A FASE STAGING

-- Remover order_id da tabela orders_staging que se refere a um pedido sem itens após exclusão de órfãos da base
DELETE FROM orders_staging o
WHERE NOT EXISTS (
  SELECT 1
  FROM order_items_staging oi
  WHERE o.order_id = oi.order_id
  );

-- Atualizando a issue_log para marcar o caso como 'RESOLVED'
UPDATE issue_log il
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Registro de pedido sem itens removido da tabela orders')
WHERE il.table_name = 'orders_staging'
    AND il.issue_id = 'I017'
    AND il.status = 'OPEN';

-- Marcar na tabela issue_log o registro que virou órfão induzido na tabela payments_staging
INSERT INTO issue_log (
  issue_id,
  table_name,
  column_name,
  record_id,
  detected_value,
  detection_rule,
  severity,
  status,
  detected_at,
  pipeline_stage,
  analyst_note
)
SELECT
  'I020',
  'payments_staging',
  'order_id',
  p.order_id::TEXT,
  p.order_id::TEXT,
  'Pagamento associado a pedido sem itens (order removida)',
  'MEDIUM',
  'OPEN',
  NOW(),
  'DATA_CLEANING',
  'Pagamento referente a pedido removido por ausência de itens.'
FROM payments_staging p
WHERE NOT EXISTS (
  SELECT 1
  FROM orders_staging o
  WHERE o.order_id = p.order_id
)
AND NOT EXISTS (
  SELECT 1
  FROM order_items_staging oi
  WHERE oi.order_id = p.order_id
)
AND NOT EXISTS (
  SELECT 1
  FROM issue_log il
  WHERE il.issue_id = 'I020'
    AND il.table_name = 'payments_staging'
    AND il.column_name = 'order_id'
    AND il.record_id = p.order_id::TEXT
);

-- Remover order_id que não existe mais na tabela order_items_staging
BEGIN;
DELETE FROM order_items_staging oi
WHERE NOT EXISTS (
  SELECT 1
  FROM orders_staging o
  WHERE o.order_id = oi.order_id
)

-- Remover order_id que não existe mais na tabela orders da tabela payments_staging
BEGIN;
DELETE FROM payments_staging p
WHERE NOT EXISTS (
  SELECT 1
  FROM orders_staging o
  WHERE o.order_id = p.order_id
);

-- Atualizar para 'RESOLVED' casos da orphan_records que já foram resolvidos anteriormente
UPDATE orphan_records o
SET status = 'RESOLVED'
FROM issue_log il
WHERE o.child_record_id = il.record_id
  AND o.child_table = il.table_name
  AND il.status = 'RESOLVED'
  AND o.status = 'OPEN';

-- Remover da orphan_records órfãos induzidos que foram erroneamente inseridos na tabela
DELETE FROM orphan_records o
WHERE o.child_table IN ('order_items_staging', 'payments_staging')
  AND NOT EXISTS (
    SELECT 1
    FROM issue_log il
    WHERE il.record_id = o.child_record_id
    AND il.table_name = o.child_table
    AND il.issue_id = 'I011'
    AND il.status = 'RESOLVED'
  );

-- Atualizar para 'RESOLVED' casos da issue_log que já foram resolvidos anteriormente
UPDATE issue_log il
SET status = 'RESOLVED',
    resolved_at = NOW(),
    analyst_note = CONCAT(analyst_note, ' - Registro removido da tabela orders')
WHERE il.table_name IN ('order_items_staging', 'payments_staging')
  AND il.column_name = 'order_id'
  AND il.issue_id IN ('I011', 'I020')
  AND il.status = 'OPEN';

