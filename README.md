# Crypto Pipeline — Dagster + CoinGecko + BigQuery

[![Python](https://img.shields.io/badge/python-3.11%2B-blue?logo=python&logoColor=white)](https://python.org)
[![Dagster](https://img.shields.io/badge/dagster-1.13-purple?logo=dagster&logoColor=white)](https://dagster.io)
[![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Tests](https://img.shields.io/badge/tests-10%2F10-brightgreen)](https://github.com/thallyssoares/financial-pipeline/actions)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Pipeline ELT de criptomoedas orquestrado pelo [Dagster](https://dagster.io), seguindo a arquitetura medalhao (Bronze -> Silver -> Gold). Extrai dados da API [CoinGecko](https://www.coingecko.com), aplica transformacoes e disponibiliza dados analiticos no [Google BigQuery](https://cloud.google.com/bigquery) para consumo em ferramentas de BI como Looker Studio.

---

## Recursos

- **Arquitetura Medalhao** — Dados organizados em camadas progressivas de qualidade
- **Pipeline idempotente** — Silver e Gold usam `WRITE_TRUNCATE`, podem ser reexecutadas sem duplicacao
- **Agendamento automatico** — Pipeline executa diariamente as 08:00 UTC
- **Qualidade de dados** — Asset Check valida a Gold apos cada materializacao
- **Testes unitarios** — 10 testes cobrindo todas as funcoes de transformacao
- **Containerizado** — Docker e Docker Compose para reproducibilidade
- **CI/CD** — GitHub Actions roda lint e testes a cada push

---

## Arquitetura

### Visao Geral

```
+-------------+     +------------------+     +-------------------+
|   Origem    | --> |   Bronze (Raw)   | --> |  Silver (Clean)   | --> Gold (Analytics) --> BI
| CoinGecko   |     |   JSON bruto     |     |  Dados tipados    |     Tabela pronta
| API         |     |   sem tratamento |     |  prontos p/ join  |     para dashboard
+-------------+     +------------------+     +-------------------+
```

### Fluxo de Dados Detalhado

```
                        COINGECKO API
                             |
              +--------------+--------------+
              |                             |
              v                             v
   +---------------------+    +------------------------+
   |   bronze.coin_data  |    |  bronze.trending_data  |
   |     3 registros     |    |     1 registro         |
   |   (BTC, ETH, SOL)   |    |   (lista de 15 coins)  |
   |   WRITE_APPEND      |    |   WRITE_APPEND         |
   +----------+----------+    +-----------+------------+
              |                             |
              | SELECT ingestion_timestamp, raw_payload
              v                             v
   +---------------------+    +------------------------+
   |    silver.coins     |    |   silver.trending      |
   |     3 linhas        |    |     15-30 linhas       |
   |   JSON parseado     |    |   desaninhado p/ linhas|
   |   WRITE_TRUNCATE    |    |   WRITE_TRUNCATE       |
   +----------+----------+    +-----------+------------+
              |                             |
              +---------> join <------------+
              |     (merge_asof por         |
              |      extracted_at,          |
              |      tolerancia 1h)         |
              v                             v
        +--------------------------------------+
        |      gold.trending_prices            |
        |      15-30 linhas enriquecidas       |
        |   coin_id, name, trending_rank,      |
        |   price_btc, btc_price_usd,          |
        |   price_usd_estimated, extracted_at  |
        |   WRITE_TRUNCATE + Asset Check       |
        +--------------------------------------+
                         |
                         v
                   LOOKER STUDIO
                (Dashboard / BI)
```

### Esquema das Tabelas

| Dataset | Tabela | Colunas Principais | Estrategia |
|---------|--------|-------------------|------------|
| `bronze` | `coin_data` | `ingestion_timestamp`, `raw_payload` | Append |
| `bronze` | `trending_data` | `ingestion_timestamp`, `raw_payload` | Append |
| `silver` | `coins` | `coin_id`, `price_usd`, `market_cap_usd`, `volume_usd`, `extracted_at` | Truncate |
| `silver` | `trending` | `coin_id`, `trending_rank`, `price_btc`, `extracted_at` | Truncate |
| `gold` | `trending_prices` | `coin_id`, `price_btc`, `btc_price_usd`, `price_usd_estimated`, `extracted_at` | Truncate + Check |

---

## Stack Tecnologica

| Categoria | Tecnologia | Versao |
|-----------|-----------|--------|
| Orquestracao | [Dagster](https://dagster.io) | 1.13 |
| Data Warehouse | [Google BigQuery](https://cloud.google.com/bigquery) | - |
| Fonte de Dados | [CoinGecko API](https://www.coingecko.com) | SDK 3.0 |
| Transformacao | [Pandas](https://pandas.pydata.org) | 3.0 |
| Linguagem | [Python](https://python.org) | 3.11+ |
| CI/CD | [GitHub Actions](https://github.com/features/actions) | - |
| Container | [Docker](https://docker.com) | - |

---

## Estrutura do Projeto

```
.
├── assets/                          # Assets do Dagster (pipeline ELT)
│   ├── __init__.py
│   ├── raw_coins_data.py            # Bronze: extracao de BTC, ETH, SOL da API
│   ├── raw_trending_data.py         # Bronze: extracao de trending coins da API
│   ├── silver_coins.py              # Silver: parse JSON -> dados estruturados
│   ├── silver_trending.py           # Silver: desaninha lista -> dados tabulares
│   └── gold_trending_prices.py      # Gold: join temporal + enriquecimento + Asset Check
├── tests/                           # Testes unitarios
│   ├── __init__.py
│   └── test_transforms.py           # 10 testes: parsing, transformacao, edge cases
├── .github/workflows/ci.yml         # CI: lint + pytest a cada push
├── definitions.py                   # Entry point do Dagster (assets, resources, schedules)
├── resources.py                     # Recursos customizados (BigQuery, CoinGecko)
├── Dockerfile                       # Imagem para containerizacao
├── docker-compose.yml               # Orquestracao do container
├── pyproject.toml                   # Configuracao do projeto e dependencias
└── README.md
```

---

## Pre-requisitos

- Python 3.11 ou superior
- Google Cloud Platform com BigQuery ativado
- Chave de API do CoinGecko (plano Demo, gratuita em [coingecko.com](https://www.coingecko.com))

---

## Configuracao

```bash
# Clone o repositorio
git clone https://github.com/thallyssoares/financial-pipeline.git
cd financial-pipeline

# Crie um ambiente virtual
python -m venv .venv

# Ative o ambiente
# Linux/Mac:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

# Instale as dependencias
pip install -e ".[dev]"

# Configure as variaveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais do GCP e CoinGecko
```

### Autenticacao GCP

O `BqResource` usa **Application Default Credentials (ADC)**. Apos instalar o [Google Cloud CLI](https://cloud.google.com/sdk/docs/install), autentique:

```bash
gcloud auth application-default login
```

---

## Execucao

### Local (desenvolvimento)

```bash
dagster dev
```

Acesse a UI em [http://localhost:3000](http://localhost:3000). Na interface:
1. Va para a aba **Assets**
2. Selecione todos os assets
3. Clique em **Materialize all**

### Docker

```bash
docker compose up --build
```

A UI estara disponivel em [http://localhost:3000](http://localhost:3000).

### CLI (sem UI)

```bash
# Materializar um asset especifico
dagster asset materialize --select raw_coins_data -f definitions.py

# Materializar toda a pipeline
dagster job execute -f definitions.py -j daily_refresh
```

---

## Testes

```bash
pytest tests/ -v
```

Saida esperada:

```
tests/test_transforms.py::TestParseCoinPayload::test_valid_coin PASSED
tests/test_transforms.py::TestParseCoinPayload::test_null_fields PASSED
tests/test_transforms.py::TestParseTrendingPayload::test_multiple_coins PASSED
tests/test_transforms.py::TestParseTrendingPayload::test_empty_coins PASSED
tests/test_transforms.py::TestTransformBronzeCoinsToSilver::test_single_row PASSED
tests/test_transforms.py::TestTransformBronzeCoinsToSilver::test_empty_dataframe PASSED
tests/test_transforms.py::TestTransformBronzeCoinsToSilver::test_skips_bad_rows PASSED
tests/test_transforms.py::TestTransformBronzeTrendingToSilver::test_multiple_snapshots PASSED
tests/test_transforms.py::TestEnrichTrendingWithBtcPrices::test_basic_enrichment PASSED
tests/test_transforms.py::TestEnrichTrendingWithBtcPrices::test_missing_btc_raises PASSED
```

### Cobertura dos Testes

| Funcao | Testes | O que valida |
|--------|--------|-------------|
| `parse_coin_payload` | 2 | Parsing valido e campos nulos |
| `parse_trending_payload` | 2 | Multiplas moedas e lista vazia |
| `transform_bronze_coins_to_silver` | 3 | Linha unica, vazio, erro por linha |
| `transform_bronze_trending_to_silver` | 1 | Multiplos snapshots |
| `enrich_trending_with_btc_prices` | 2 | Enriquecimento basico e erro sem BTC |

---

## Agendamento

O pipeline executa automaticamente via schedule do Dagster:

- **Job:** `daily_refresh`
- **Cron:** `0 8 * * *` (todos os dias as 08:00 UTC)
- **Selecao:** Todos os 5 assets na ordem de dependencia

---

## Exemplo de Query na Gold

Apos a materializacao, a tabela `gold.trending_prices` esta pronta para consumo no Looker Studio ou direto no BigQuery:

```sql
SELECT
  coin_id,
  name,
  trending_rank,
  ROUND(price_usd_estimated, 2) AS price_usd,
  extracted_at
FROM gold.trending_prices
ORDER BY trending_rank;
```

---

## Licenca

Distribuido sob a licenca MIT. Veja `LICENSE` para mais informacoes.
