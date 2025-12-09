# Camada STAGING (Silver 1)

Essa camada recebe os dados diretamente da camada RAW. 
É aqui que são executados os primeiros diagnósticos, análises exploratórias, validações, buscando encontrar e reconhecer erros, assim como corrigí-los.
É nessa camada que são mantidas também as tabelas auxiliares "audit_*" e "issue_ref".

OBJETIVO: Corrigir erros e preparar os dados para modelagem.