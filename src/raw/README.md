# Camada RAW (Bronze)
Essa camada contém os scripts necessários para estruturar e carregar os dados brutos no banco de dados.

Os dados originais (arquivos CSV) são mantidos fora dessa pasta, em 'data/raw/', e são importados para o PostgreSQL exatamente como foram baixados, sem qualquer alteração.

# Características
- Estrutura de tabelas padronizadas, com colunas do tipo TEXT, para garantir que não ocorram erros na ingestão.
- Nenhum tipo de diagnóstico, normalização ou limpeza é realizado nesta camada.
- Os comandos de carga utilizam \COPY, que é executado diretamente pelo terminal psql.
- Preserva a integridade dos dados brutos para permitir reprocessamentos futuros.

# Objetivo
Manter uma fonte única e imutável dos dados originais, servindo como base para todas as camadas posteriores do projeto (Staging, Clean e Final).