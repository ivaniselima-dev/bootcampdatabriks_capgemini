# Lakehouse Medallion no Databricks — Bootcamp Capgemini

Pipelines de dados construídos no **Bootcamp Databricks da Capgemini (I&D Capgemini Brasil, setembro de 2026)**, organizados na arquitetura **Medallion (Bronze → Silver → Gold)** sobre **Unity Catalog** e **Delta Lake**.

O repositório reúne cinco domínios de dados, cada um usado para praticar um padrão diferente de engenharia: ingestão via API, carga em lote, ingestão incremental, CDC, modelagem dimensional e streaming.

---

## Arquitetura

```
                ┌──────────────────────────────────────────────────────────┐
  Fontes        │  API REST · CSV · JSON (arquivos gerados e enviados)     │
                └───────────────────────────┬──────────────────────────────┘
                                            ▼
  Landing       /Volumes/{catalog}/landing/files/...   (arquivos brutos)
                                            ▼
  Bronze        {catalog}.bronze.*    dado bruto + metadados de ingestão
                                            ▼
  Silver        {catalog}.silver.*    tipagem, limpeza, deduplicação, CDC aplicado
                                            ▼
  Gold          {catalog}.gold.*      agregações de negócio e modelo estrela
                                            ▼
  Governance    {catalog}.governance.*   views com controle de acesso (GRANT)
```

Todos os notebooks recebem o catálogo por **widget** (`catalog`, padrão `capgemini_academy`) e usam nomes totalmente qualificados `catalog.schema.tabela`. Para rodar em outro ambiente, basta trocar o valor do widget.

---

## Domínios e notebooks

A ordem abaixo é a **ordem de execução**. Os prefixos `Day`/`Dia` indicam o dia da aula, não a ordem do pipeline.

### 1. MovieLens — carga em lote e governança
| Ordem | Notebook | Camada | Saída |
|---|---|---|---|
| 1 | `Day2-CreateTablesMovieLens` | Bronze | `bronze.links`, `movies`, `ratings`, `tags` |
| 2 | `Day2-Movie-Rating-silver` | Silver | `silver.movie_ratings_summary` |
| 3 | `Dia3-Governanca-UnityCatalog` | Governance | `governance.vw_movie_ratings_public` + `GRANT` |

### 2. Exchange Rates — ingestão via API
| Ordem | Notebook | Camada | Saída |
|---|---|---|---|
| 1 | `Day1-Ingestao-Exchange-Rates` | Landing | JSON diário no volume |
| 2 | `Day2-CreateBronzeExchangeRates` | Bronze | `bronze.exchange_rates` |

A chave da API é lida de um **Secret Scope** com `dbutils.secrets.get`, nunca do código.

### 3. FIFA World Cup — carga de CSV
| Ordem | Notebook | Camada | Saída |
|---|---|---|---|
| 1 | `Dia3-Ingestao-Dados_Fifa` | Bronze | `bronze.fifa_world_cup_all_matches` |

### 4. Pedidos — padrões de ingestão, MERGE e CDC
| Ordem | Notebook | Camada | Saída |
|---|---|---|---|
| 1 | `DIA4_Gerador_Ingestao_Dados` | Bronze | `pedidos_full_load`, `pedidos_copy_into`, `pedidos_autoloader`, `pedidos_cdc_raw` |
| 2 | `Dia5-Pedidos-Bronze-Silver` | Silver | `silver.pedidos_autoloader`, `pedidos_autoloader_v2` |
| 3 | `Dia5-Pedidos-Silver-Gold` | Gold | `gold.receita_diaria` |
| 4 | `Dia5-Top-Produtos` | Gold | `gold.top_produtos` |
| 5 | `Day5-Demo-CDC` | Silver | `silver.pedidos_cdc` (estado final via `MERGE`) |

O gerador cria dados propositalmente sujos (ex.: `pedido_id = 'null'`, quantidade negativa, schema drift com coluna nova) para exercitar tratamento de qualidade na Silver.

### 5. Indian E-Commerce — modelagem dimensional (star schema)
| Ordem | Notebook | Camada | Saída |
|---|---|---|---|
| 1 | `Dia6a-Gerador-CSVs-Indian` | Landing | CSVs de clientes, produtos e vendas |
| 2 | `Dia5-Create-Tables-Indian` | Bronze + Gold | `bronze.customers/products/sales`, `gold.dim_datas`, `dim_horas`, `dim_produtos`, `dim_clientes` |
| 3 | `Dia6b-Fato-Vendas` | Silver + Gold | `gold.fat_vendas` |
| 4 | `Dia7-Spark Processing Examples` | Gold | `gold.daily_revenue`, `gold.revenue_by_category` |

As dimensões usam **chaves substitutas** (`IDENTITY`) e a fato é carregada com `MERGE`, resolvendo as SKs por join com as dimensões.

### 6. Streaming
| Notebook | Camada | Saída |
|---|---|---|
| `Dia8-Streaming` | Bronze | `bronze.events_stream` (Auto Loader com checkpoint) |

### Demos e validação
| Notebook | Objetivo |
|---|---|
| `Day1-Exemplo1` | Primeira leitura de CSV com a DataFrame API (exploratório) |
| `Day5-Demo-Merge` | Demonstração didática de `MERGE` e `CAST` vs `TRY_CAST`. Escreve nas mesmas tabelas do domínio Pedidos; rode antes do pipeline principal ou isoladamente. |
| `99_validacao_idempotencia` | Prova de idempotência do pipeline (ver abaixo) |

---

## Decisões técnicas

**Idempotência como requisito.** Todo notebook pode ser reexecutado sem duplicar dados. Cada camada usa a estratégia adequada ao tipo de fonte:

- **Bronze de fontes estáticas** (MovieLens, FIFA, Indian): recarga completa com `CREATE OR REPLACE TABLE AS SELECT`. O arquivo na Landing é a fonte de verdade, então recriar a tabela é seguro.
- **Bronze incremental**: `COPY INTO` e Auto Loader registram os arquivos já processados e ignoram reprocessamentos.
- **Silver e Gold**: `MERGE INTO` por chave de negócio.
- **`pedidos_cdc_raw`** mantém `overwrite` de forma intencional: o gerador produz sempre os mesmos eventos, e `append` duplicaria o histórico a cada execução.

**Tratamento de dado sujo sem quebrar o pipeline.** `TRY_CAST` no lugar de `CAST` converte valores inválidos em `NULL`, que são filtrados na Silver, em vez de interromper a carga.

**Governança por grupo, não por pessoa.** O `GRANT` é concedido a `account users` para fins didáticos. Em ambiente corporativo, o padrão seria um grupo específico (ex.: `data_analysts`) sincronizado via SCIM a partir do provedor de identidade.

**Segredos fora do código.** Credenciais ficam em Secret Scope e o notebook de configuração local não é versionado (`.gitignore`).

---

## Validação de idempotência

O notebook `validacao/99_validacao_idempotencia` conta as linhas de todas as tabelas Bronze, Silver e Gold e grava o resultado em `{catalog}.validacao.idempotencia_log`.

1. Execute o pipeline completo e rode a validação com o widget `execucao = 1`.
2. Execute o pipeline completo de novo e rode a validação com `execucao = 2`.
3. A última célula compara as duas execuções. Toda tabela deve aparecer como `IDEMPOTENTE`.

---

## Como executar

**Pré-requisitos:** workspace Databricks com Unity Catalog e permissão para criar catálogo, schemas e volumes.

1. Crie o catálogo e os schemas `landing`, `bronze`, `silver`, `gold` e `governance`, além dos volumes `landing.files` e `landing.metadata`. O notebook `DIA4_Gerador_Ingestao_Dados` cria parte dessa estrutura com `IF NOT EXISTS`.
2. Envie os arquivos de origem para a Landing:
   - MovieLens (`ml-latest-small`) em `/Volumes/{catalog}/landing/files/movielens/` — disponível em [grouplens.org](https://grouplens.org/datasets/movielens/). O dataset tem licença própria e por isso não é redistribuído aqui.
   - FIFA World Cup em `/Volumes/{catalog}/landing/files/fifa/`.
   - Os dados de Pedidos e Indian E-Commerce são **gerados pelos próprios notebooks**.
3. Para Exchange Rates, crie um Secret Scope `capgemini_academy` com a chave `exchangerates_api_key`.
4. Execute os notebooks na ordem das tabelas acima, ajustando o widget `catalog` se necessário.
5. Rode a validação de idempotência.

---

## Tecnologias

Databricks · Unity Catalog · Delta Lake · Spark SQL · PySpark · Auto Loader · COPY INTO · Structured Streaming · Databricks Secrets · Git Folders

---

## Aprendizados

- Separar código de dados: o repositório versiona a lógica que reconstrói o Lakehouse; os dados ficam no catálogo.
- Idempotência não é uma afirmação, é um teste: por isso o notebook de validação existe.
- Cada padrão de ingestão resolve um problema diferente, e a escolha depende de a fonte ser estática, incremental ou de eventos de mudança.
- Ferramentas de IA aceleram a refatoração, mas cada alteração precisa ser revisada e testada antes de ir para produção.

## Próximos passos

- Orquestrar o pipeline em um **Databricks Job** com dependências entre tarefas.
- Adicionar expectativas de qualidade de dados na Silver.
- Migrar os fluxos incrementais para **Lakeflow Declarative Pipelines**.

---

**Autora:** Ivanise Lima · Bootcamp Databricks — I&D Capgemini Brasil, 2026
