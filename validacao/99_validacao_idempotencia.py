# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Validação de Idempotência do Pipeline
# MAGIC
# MAGIC **Objetivo:** provar que o pipeline pode ser reexecutado sem duplicar nem perder dados.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | **Camada** | Qualidade / testes |
# MAGIC | **Entrada** | Todas as tabelas de `{catalog}.bronze`, `{catalog}.silver` e `{catalog}.gold` |
# MAGIC | **Saída** | `{catalog}.validacao.idempotencia_log` |
# MAGIC | **Ordem no pipeline** | Após cada execução completa do pipeline |
# MAGIC
# MAGIC **Como usar:**
# MAGIC 1. Rode o pipeline completo e execute este notebook com `execucao = 1`.
# MAGIC 2. Rode o pipeline completo de novo e execute este notebook com `execucao = 2`.
# MAGIC 3. A última célula compara as execuções. Toda tabela deve aparecer como `IDEMPOTENTE`.

# COMMAND ----------

dbutils.widgets.text("catalog", "capgemini_academy", "Nome do catálogo no Unity Catalog")
dbutils.widgets.dropdown("execucao", "1", ["1", "2"], "Número da execução do pipeline")

catalog = dbutils.widgets.get("catalog")
execucao = int(dbutils.widgets.get("execucao"))
log_table = f"{catalog}.validacao.idempotencia_log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estrutura do log
# MAGIC Criada com `IF NOT EXISTS`, então pode rodar quantas vezes for preciso.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.validacao")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {log_table} (
        execucao      INT       COMMENT 'Número da execução do pipeline (1 ou 2)',
        tabela        STRING    COMMENT 'schema.tabela validada',
        linhas        BIGINT    COMMENT 'Contagem de linhas; NULL quando a leitura falhou',
        status        STRING    COMMENT 'OK ou mensagem de erro',
        verificado_em TIMESTAMP COMMENT 'Momento da verificação'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de todas as tabelas
# MAGIC Uma tabela com erro não interrompe a validação: o erro é registrado e a contagem continua.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

tabelas = spark.sql(f"""
    SELECT table_schema, table_name
    FROM {catalog}.information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
      AND table_type IN ('MANAGED', 'EXTERNAL')
""").collect()

resultados = []
for t in tabelas:
    nome = f"{t.table_schema}.{t.table_name}"
    try:
        linhas = spark.table(f"{catalog}.{nome}").count()
        status = "OK"
    except Exception as e:
        linhas = None
        status = str(e)[:200]
    resultados.append((execucao, nome, linhas, status))

df = (
    spark.createDataFrame(resultados, "execucao int, tabela string, linhas long, status string")
         .withColumn("verificado_em", current_timestamp())
)

# O próprio log é idempotente: substitui o registro desta execução, em vez de acumular.
spark.sql(f"DELETE FROM {log_table} WHERE execucao = {execucao}")
df.write.mode("append").saveAsTable(log_table)

print(f"Execução {execucao}: {df.count()} tabelas verificadas.")
display(df.filter("status != 'OK'"))  # vazio = nenhuma tabela com erro

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação entre as execuções
# MAGIC - `IDEMPOTENTE`: mesma contagem nas duas execuções.
# MAGIC - `DIVERGENTE`: a contagem mudou; investigar a estratégia de carga da tabela.
# MAGIC - `INCOMPLETO`: a tabela só existe em uma das execuções ou falhou na leitura.

# COMMAND ----------

comparacao = spark.sql(f"""
    SELECT
        COALESCE(e1.tabela, e2.tabela) AS tabela,
        e1.linhas AS execucao_1,
        e2.linhas AS execucao_2,
        CASE
            WHEN e1.linhas IS NULL OR e2.linhas IS NULL THEN 'INCOMPLETO'
            WHEN e1.linhas = e2.linhas                  THEN 'IDEMPOTENTE'
            ELSE 'DIVERGENTE'
        END AS resultado
    FROM      (SELECT tabela, linhas FROM {log_table} WHERE execucao = 1) e1
    FULL JOIN (SELECT tabela, linhas FROM {log_table} WHERE execucao = 2) e2
           ON e1.tabela = e2.tabela
    ORDER BY resultado, tabela
""")

display(comparacao)

resumo = {r.resultado: r["count"] for r in comparacao.groupBy("resultado").count().collect()}
print("Resumo:", resumo)
