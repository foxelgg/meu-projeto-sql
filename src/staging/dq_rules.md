# DATA QUALITY RULES


# DUPLICIDADE NA CHAVE PRIMÁRIA
Regra: DUPLICATE_PRIMARY_KEY
Descrição: Chaves primárias duplicadas são consideradas inválidas.
Ação: Deduplicar a chave primária, mantendo o registro mais antigo (com a ausência de created_at, mantem-se o registro de ctid menor), e registrar na tabela issue_log.
Justificativa: Duplicidade na chave primária compromete o modelo relacional e causa erro de ingestão na base de dados do tipo PRIMARY KEY. Além disso, a duplicidade impacta diretamente na análise de métricas de recorrência e volume total, por exemplo. 

# PREÇO INVÁLIDO
Regra: INVALID_PRICE_VALUE
Descrição: Valores de preço nulos, zerados ou negativos são considerados inválidos.
Ação: Converter price para NULL e registrar na tabela issue_log.
Justificativa: Valores inválidos podem distorcer métricas como faturamente e ticket médio. Converter para NULL evita distorções em análises por agregação numérica mas mantem o registro disponível para análises não financeiras, como volume de pedidos.

# OUTLIERS

## OUTLIER NO PREÇO
Regra: OUTLIER_VALUE
Descrição: Valores estatisticamente muito acima do esperado.
Ação: Winsorizar valores para o máximo aceitável: price = AVG(price) + 2 * STDDEV(price).
Justificativa: Outliers podem distorcer análises de desempenho de produtos. Winsorizar o preço do produto preserva a informação para análise e limita o impacto em análises por agregação numérica.

## OUTLIER NO FRETE
Regra: OUTLIER_VALUE
Descrição: Valores estatisticamente muito acima do esperado.
Ação: Capar valores para o máximo aceitável: freight_value = price * 2.
Justificativa: Outlier no frete comumente pode ser atrelado a erro de registro. Limitar o frete à duas vezes o valor do preço do pedido trata distorções e preserva a legitimidade do pedido.

## OUTLIER NO PAGAMENTO
Regra: OUTLIER_VALUE
Descrição: Valores estatisticamente muito acima do esperado.
Ação: Winsorizar valores para o máximo aceitável: payment_value = AVG(payment_value) + 2 * STDDEV(payment_value).
Justificativa: Outlier no pagamento dos pedidos pode gerar distorções em análises de receita. Winsorizar este valor, preserva o registro potencialmente legítimo e reduz distorções nas métricas.

# ÓRFÃOS

## ÓRFÃOS TÉCNICOS
Regra: ORPHAN_FOREIGN_KEY
Descrição: Chaves estrangeiras sem correspondência com uma chave primária que vieram diretamente do dataset original.
Ação: Remover órfãos e registrar na tabela issue_log e na tabela orphan_records.
Justificativa: Órfãos técnicos são provenientes do dataset e não representam eventos válidos e portanto são removidos.

## ÓRFÃOS INDUZIDOS
Regra: ORPHAN_FOREIGN_KEY
Descrição: Chaves estrangeiras sem correspondência com uma chave primária por causa de limpeza prévia do dataset.
Ação: Logar em issue_log e orphan_records e deixar 'OPEN'. Posteriormente dados serão filtrados para não poluírem análise.
Justificativa: Órfãos induzidos são consequências do próprio processo de limpeza dos dados. São mantidos para permitir rastreabilidade e são filtrados da base final para não interferirem em análises.

## REGISTROS SEM DEPENDENTES
Regra: ORDER_WITHOUT_ITEMS
Descrição: Com a remoção de órfãos, alguns registros podem perder seus dependentes (ex: pedido sem itens, pagamento de um pedido sem itens).
Ação: Marcar registros na tabela issue_log como 'OPEN' e posteriormente filtrar para não aparecerem em análises.
Justificativa: Preservar esses registros é importante para rastreabilidade, enquanto filtrá-los da base final evita distorções em análises. 

# DATAS INVÁLIDAS
Regra: INVALID_DATE_VALUE
Descrição: Datas impossíveis, datas com caracteres inválidos, nulls.
Ação: Definir datas como NULL e registrar na issue_log.
Justificativa: Datas inválidas comprometem todo e qualquer tipo de análise temporal, e por isso são convertidas para NULL, mantendo o registro.

# DATAS INCONSISTENTES
Regra: INCONSISTENT_DATE_SEQUENCE
Descrição: Datas com inconsistência na sequência: data da entrega < data da compra
Ação: Converter a data inconsistente para NULL e manter a data base, e logar em issue_log.
Justificativa: Evita distorções em métricas logísticas. Preserva-se a data de compra por ser o evento primário e menos sujeito a erros. 

# PAGAMENTOS INVÁLIDOS
REGRA: INVALID_PAYMENT_VALUE
Descrição: Valores de pagamento nulos, zerados ou negativos são considerados inválidos.
Ação: Converter campo para NULL e logar em issue_log.
Justificativa: Pagamentos inválidos podem gerar interpretações incorretas sobre receita.