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
      A tabela ISSUE_REFERENCE será a única das 3 que será preenchida nessa etapa.
      Seus dados serão consolidados a partir da planilha issues_found.xlsx.
      Nem todas as issues são registradas nessa tabela, apenas as que demandam
      algum tratamento além da limpeza leve.

    - ISSUE_LOG: Tabela para registrar logs de auditoria das operações de ETL, incluindo
      timestamps, status e detalhes das execuções. As colunas da tabela ISSUE_LOG são:
        - issue_log_id: Identificador único do log (PK).
        - issue_id: Código do problema referenciado da tabela ISSUE_REFERENCE.
        - table_name: Nome da tabela onde o problema foi detectado.
        - column_name: Nome da coluna onde o problema foi detectado.
        - record_id: Identificador do registro afetado.
        - detected_value: Valor detectado que causou o problema.
        - detection_rule: Regra ou método utilizado para detectar o problema.
        - severity: Nível de severidade do problema.
        - status: Status atual do problema (OPEN, RESOLVED, PENDING).
        - detected_at: Timestamp de quando o problema foi detectado.
        - pipeline_stage: Etapa do pipeline onde o problema foi detectado.
        - analyst_note: Notas adicionais do analista sobre o problema.
        - resolved_at: Timestamp de quando o problema foi resolvido.

    - ORPHAN: Tabela para armazenar registros órfãos que não possuem correspondência
      nas tabelas principais, facilitando a análise e correção posterior. As colunas da
        tabela ORPHAN são:
            - orphan_id: Identificador único do registro órfão (PK).
            - parent_table: Nome da tabela pai onde o registro deveria existir.
            - parent_column: Nome da coluna pai onde o registro deveria existir.
            - child_table: Nome da tabela filho onde o registro órfão foi encontrado.
            - child_column: Nome da coluna filho onde o registro órfão foi encontrado.
            - child_record_id: Identificador do registro órfão.
            - missing_parent_id: Identificador do registro pai que está faltando.
            - detected_at: Timestamp de quando o registro órfão foi detectado.
            - pipeline_stage: Etapa do pipeline onde o registro órfão foi detectado.
            - status: Status atual do registro órfão (OPEN, RESOLVED).
            - analyst_note: Notas adicionais do analista sobre o registro órfão.

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

-- Populando a tabela ISSUE_REFERENCE com os problemas identificados no data profiling
INSERT INTO issue_reference (issue_code, issue_type, issue_description, severity, default_action)
VALUES
    ('I001', 'NULL_ANALYTICAL_FIELD', 'Campo analítico nulo', 'MEDIUM', 'Verificar origem dos dados e corrigir'),
    ('I002', 'DUPLICATE_PRIMARY_KEY', 'Chave primária duplicada', 'HIGH', 'Remover duplicatas ou consolidar registros'),
    ('I006', 'INVALID_PRICE_VALUE', 'Valor de preço inválido: null, zero, negativo', 'CRITICAL', 'Validar e corrigir valores de preço'),
    ('I007', 'OUTLIER_VALUE', 'Valor estatisticamente muito acima do esperado', 'MEDIUM', 'Aplicar regra de negócio para validação'),
    ('I009', 'INVALID_DATE_VALUE', 'Valor de data inválido: null, passado/futuro absurdo', 'MEDIUM', 'Converter para NULL'),
    ('I010', 'INCONSISTENT_DATE_SEQUENCE', 'Sequência de datas de compra e entrega inconsistente', 'HIGH', 'Converter inconsistência para NULL e manter a data base'),
    ('I011', 'ORPHAN_FOREIGN_KEY', 'Chave estrangeira que não possui correspondência na tabela referenciada', 'HIGH', 'Remover órfão e registrar na tabela ORPHAN'),
    ('I014', 'INVALID_PAYMENT_VALUE', 'Valor de pagamento inválido: null, zero, negativo', 'CRITICAL', 'Validar e corrigir valores de pagamento'),
    ('I016', 'NULL_MANDATORY_FIELD', 'Campo obrigatório nulo', 'MEDIUM', 'Aplicar regra de negócio para preenchimento ou remoção');

-- Adicionando nova issue para pedido que virou órfão em cascata na tabela order_items_staging
INSERT INTO issue_reference (issue_code, issue_type, issue_description, severity, default_action)
VALUES
    ('I017', 'ORDER_WITHOUT_ITEMS', 'Pedido sem itens associados', 'MEDIUM', 'Aplicar regra de negócio para tratamento');

-- Criação da tabela ISSUE_LOG
CREATE TABLE IF NOT EXISTS issue_log (
    issue_log_id BIGSERIAL PRIMARY KEY,
    issue_id VARCHAR(10) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    column_name VARCHAR(50) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    detected_value TEXT,
    detection_rule TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    pipeline_stage VARCHAR(50) NOT NULL,
    analyst_note TEXT,
    resolved_at TIMESTAMP
);

-- Criação da tabela ORPHAN
CREATE TABLE IF NOT EXISTS orphan_records (
    orphan_id BIGSERIAL PRIMARY KEY,
    parent_table VARCHAR(50) NOT NULL,
    parent_column VARCHAR(50) NOT NULL,
    child_table VARCHAR(50) NOT NULL,
    child_column VARCHAR(50) NOT NULL,
    child_record_id VARCHAR(100) NOT NULL,
    missing_parent_id VARCHAR(100) NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    pipeline_stage VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    analyst_note TEXT
);
