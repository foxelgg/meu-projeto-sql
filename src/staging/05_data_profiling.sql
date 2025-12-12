/* ========================================================================================
    Data Profiling nas tabelas STAGING
    
    Este arquivo documenta o processo de conferência e análise dos dados carregados nas
    tabelas STAGING. 

    - Essa etapa visa identificar problemas de qualidade dos dados, como valores nulos,
      inconsistências e formatos incorretos.
    - Os resultados deste profiling orientarão as transformações necessárias antes da
      carga final nas tabelas de destino.

      Os problemas encontrados nessa etapa serão registrados em uma planilha chamada de
      issues_found.xslx para posterior consolidação da tabela issue_reference.
    ======================================================================================== */

-- **1. PROFILING DA TABELA customers_staging**

-- Contagem total de registros da tabela
SELECT COUNT(*) AS total_registros FROM customers_staging;

-- Verificação de valores nulos em cada coluna
SELECT 
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS nulos_customer_id,
    COUNT(*) FILTER (WHERE customer_name IS NULL) AS nulos_customer_name,
    COUNT(*) FILTER (WHERE customer_city IS NULL) AS nulos_customer_city
FROM customers_staging;

-- Verificar se a coluna customer_id possui valores duplicados
SELECT customer_id, COUNT(*) AS ocorrencias
FROM customers_staging
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Verificar nomes de clientes com caracteres especiais ou numéricos
SELECT DISTINCT customer_id, customer_name
FROM customers_staging
WHERE customer_name !~ '^[A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç£'']+( [A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç£'']+)*$'
    
-- Verificar cidades com nomes inválidos
SELECT DISTINCT customer_id, customer_city
FROM customers_staging
WHERE customer_city !~ '^[A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç£'']+( [A-Za-zÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç£'']+)*$';

-- **CRIAÇÃO DA EXTENSÃO unaccent PARA AUXILIAR NA ANÁLISE E TRATAMENTO DE DADOS**
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Detectar diferenças no nome das cidades devido a acentuação ou case sensitivity
SELECT
	LOWER(unaccent(customer_city)) AS normalized,
	ARRAY_AGG(DISTINCT customer_city) AS versions,
COUNT(*)
FROM customers_staging
GROUP BY normalized
ORDER BY COUNT(*) DESC;

-- **2. PROFILING DA TABELA order_items_staging**

-- Verificar valores nulos em cada coluna
SELECT 
    COUNT(*) FILTER (WHERE order_id IS NULL) AS nulos_order_id,
    COUNT(*) FILTER (WHERE product_id IS NULL) AS nulos_product_id,
    COUNT(*) FILTER (WHERE price IS NULL) AS nulos_price,
    COUNT(*) FILTER (WHERE freight_value IS NULL) AS nulos_freight_value
FROM order_items_staging;

-- Verificar orders com product_id inexistente na tabela products_staging
SELECT oi.*
FROM order_items_staging oi
LEFT JOIN products_staging p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Verificar preços negativos ou zerados
SELECT *
FROM order_items_staging
WHERE CAST(price AS NUMERIC) <= 0;

-- Verificar frete negativo
SELECT *
FROM order_items_staging
WHERE CAST(freight_value AS NUMERIC) < 0;

-- Verificar outliers no preço (valores muito altos)
SELECT *
FROM order_items_staging
WHERE price::numeric > (SELECT AVG(price::numeric) + 2 * STDDEV(price::numeric) FROM order_items_staging);

-- Verificar fretes muito altos
SELECT *
FROM order_items_staging
WHERE freight_value::numeric > price::numeric * 2 AND price::numeric > 0;

-- **3. PROFILING DA TABELA orders_staging**

-- Verificar valores nulos em cada coluna
SELECT 
    COUNT(*) FILTER (WHERE order_id IS NULL) AS nulos_order_id,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS nulos_customer_id,
    COUNT(*) FILTER (WHERE purchased_at IS NULL) AS nulos_purchased_at,
    COUNT(*) FILTER (WHERE delivered_at IS NULL) AS nulos_delivered_at
FROM orders_staging;

-- Verificar se a coluna order_id possui valores duplicados
SELECT order_id, COUNT(*) AS ocorrencias
FROM orders_staging
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Verificar se existem textos em colunas de data ou separadores diferentes de hífen
SELECT purchased_at, COUNT(*) AS ocorrencias
FROM orders_staging
WHERE purchased_at !~ '^\d{4}-\d{2}-\d{2}$'
GROUP BY purchased_at
ORDER BY ocorrencias DESC;

SELECT delivered_at, COUNT(*) AS ocorrencias
FROM orders_staging
WHERE delivered_at !~ '^\d{4}-\d{2}-\d{2}$'
GROUP BY delivered_at
ORDER BY ocorrencias DESC;

-- Verificar se alguma data é irrealista (ex: anos muito antigos ou futuros)
SELECT *
FROM orders_staging
WHERE purchased_at < '2000-01-01' OR delivered_at < '2000-01-01'
   OR purchased_at::date > CURRENT_DATE OR delivered_at::date > CURRENT_DATE;

-- Verificar se a data de entrega é anterior à data de compra
SELECT *
FROM orders_staging
WHERE delivered_at < purchased_at;

-- Verificar se tem strings vazias nas colunas de data
SELECT *
FROM orders_staging
WHERE TRIM(purchased_at) = '' OR TRIM(delivered_at) = '';

-- Verificar se há caracteres inválidos nas colunas de data
SELECT *
FROM orders_staging
WHERE purchased_at ~ '[^0-9\-]' OR delivered_at ~ '[^0-9\-]';

-- Verificar datas com fuso horário incorreto (ex: timestamps com horas)
SELECT *
FROM orders_staging
WHERE purchased_at ~ 'T' OR delivered_at ~ 'T' OR purchased_at ~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}'
   OR delivered_at ~ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}';

-- Verificar datas corretas mas com mês ou dia inválidos (ex: mês > 12 ou dia > 31)
SELECT *
FROM orders_staging
WHERE purchased_at ~ '^\d{4}-\d{2}-\d{2}$' AND delivered_at ~ '^\d{4}-\d{2}-\d{2}$'
  AND purchased_at <> '0000-00-00' AND delivered_at <> '0000-00-00' AND (   
EXTRACT(MONTH FROM purchased_at::date) > 12
   OR EXTRACT(DAY FROM purchased_at::date) > 31
   OR EXTRACT(MONTH FROM delivered_at::date) > 12
   OR EXTRACT(DAY FROM delivered_at::date) > 31
  );

-- Verificar se algum registro da coluna customer_id não possui correspondência na tabela customers_staging (órfãos)
SELECT o.*
FROM orders_staging o
LEFT JOIN customers_staging c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- **4. PROFILING DA TABELA payments_staging**

-- Verificar valores nulos em cada coluna
SELECT 
    COUNT(*) FILTER (WHERE order_id IS NULL) AS nulos_order_id,
    COUNT(*) FILTER (WHERE payment_type IS NULL) AS nulos_payment_type,
    COUNT(*) FILTER (WHERE payment_value IS NULL) AS nulos_payment_value
FROM payments_staging;

-- Verificar payments com order_id inexistente na tabela orders_staging
SELECT p.*
FROM payments_staging p
LEFT JOIN orders_staging o ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Verificar valores negativos ou zerados em payment_value
SELECT *
FROM payments_staging
WHERE CAST(payment_value AS NUMERIC) <= 0;

-- Verificar pagamentos com valores muito altos (outliers)
SELECT *
FROM payments_staging
WHERE payment_value::numeric > (SELECT AVG(payment_value::numeric) + 2 * STDDEV(payment_value::numeric) FROM payments_staging);

-- **5. PROFILING DA TABELA products_staging**

-- Verificar valores nulos em cada coluna
SELECT
    COUNT(*) FILTER (WHERE product_id IS NULL) AS nulos_product_id,
    COUNT(*) FILTER (WHERE product_name IS NULL) AS nulos_product_name,
    COUNT(*) FILTER (WHERE category IS NULL) AS nulos_category
FROM products_staging;

-- Verificar se a coluna product_id possui valores duplicados
SELECT product_id, COUNT(*) AS ocorrencias
FROM products_staging
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Detectar diferenças na categoria devido a acentuação ou case sensitivity
SELECT
    LOWER(unaccent(category)) AS normalized,
    ARRAY_AGG(DISTINCT category) AS versions,
    COUNT(*) AS ocorrencias
FROM products_staging
GROUP BY normalized
ORDER BY COUNT(*) DESC;

-- Verificar nomes de produtos com caracteres especiais ou numéricos
SELECT DISTINCT product_id, product_name
FROM products_staging
WHERE product_name !~ '^[A-Za-z0-9ÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç£"''()\-.,&]+( [A-Za-z0-9ÁÉÍÓÚÂÊÔÃÕÀÜÇáéíóúâêôãõàüç£"''()\-.,&]+)*$';

