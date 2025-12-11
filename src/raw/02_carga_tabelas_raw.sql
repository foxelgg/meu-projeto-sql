/* ========================================================================================
    Carregando tabelas RAW
    Este arquivo documenta o processo de importação dos arquivos CSV para as tabelas RAW.

    IMPORTANTE:
    - Os comandos abaixo devem ser executados **DENTRO do TERMINAL PSQL**, pois \COPY é
        um comando específico do psql, e não funciona em scripts SQL padrão.
    - Execute a partir da raiz do projeto para que os caminhos relativos funcionem.


    Exemplo de execução:
    cd meu_projeto_sql
    psql -h localhost -U postgres -d teste_postgres
    ======================================================================================== */

-- Carga da tabela customers_raw
\COPY customers_raw FROM 'data/customers.csv' CSV HEADER;

-- Carga da tabela order_items_raw
\COPY order_items_raw FROM 'data/order_items.csv' CSV HEADER;

-- Carga da tabela orders_raw
\COPY orders_raw FROM 'data/orders.csv' CSV HEADER;

-- Carga da tabela payments_raw
\COPY payments_raw FROM 'data/payments.csv' CSV HEADER;

-- Carga da tabela products_raw
\COPY products_raw FROM 'data/products.csv' CSV HEADER;
