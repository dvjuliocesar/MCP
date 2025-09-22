# 🌐 Aula 04 — MCP: Coleta, Armazenamento e Processamento

## 🧲 Exercício 1: Coleta de Dados (Guia + Código)

Este projeto demonstra **duas formas de coleta**:
- **Scraping web** de preços (site de treino: `Books to Scrape` — e-commerce fictício, seguro para estudos).
- **Cliente de API pública** (Open-Meteo) para séries horárias meteorológicas.

> Foco: conexões seguras, *timeouts*, *retries*, *backoff*, registro de logs, tratamento de erros e documentação dos **campos extraídos**.

### ▶️ 1) Como executar
```bash
python -m venv .venv
# Ative a venv (Windows: .venv\Scripts\activate | Linux/Mac: source .venv/bin/activate)
pip install -r requirements.txt
cp .env .env
python -m src.main
```
Saídas em `./data`.

### 2) Campos extraídos
#### 🗂️ Produtos (scraper)
`source, product_name, price_gbp, availability, rating_1to5, url, scraped_at`

#### 🗂️ Meteorologia (API)
`city, time, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m`

## 🗄️ Exercício 2: Armazenamento de Dados (PostgreSQL + MongoDB)

Este exercício complementa a Parte 1 (coleta) e mostra como persistir os CSVs em dois tipos de bancos:

- **PostgreSQL** → modelo estrela mínimo (dimensões + fatos)

- **MongoDB** → documentos aninhados (produtos com histórico de preços e clima horário)

### 1) 📋 Pré-requisitos:

- CSVs gerados no Exercício 1, na pasta `../MCP/data/`

- PostgreSQL e MongoDB rodando (local ou Docker)

- Python 3.10+ com pacotes de requirements instalados
```bash
pip install -r requirements.txt
```
### 2) ⚙️ Configuração (`.env`)
```ini
# Onde estão os CSVs do Exercício 1
DATA_DIR=../data

# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DB=mcp
PG_USER=postgres
PG_PASSWORD=postgres123

# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB=mcp
```
### 3) 🐘 PostgreSQL 

- Criar banco e tabelas
```bash
psql -h localhost -U postgres -d postgres -c "CREATE DATABASE mcp;"
psql -h localhost -U postgres -d aula04 -f sql/postgres/schema.sql
psql -h localhost -U postgres -d aula04 -f sql/postgres/indexes.sql
```
- ▶️ Como executar (ETL)
```bash
python -m src.load_to_postgres
```
### 4) 🍃 MongoDB

- ▶️ Como executar (ETL)
```bash
python -m src.load_to_mongo
```
## ⚙️ Exercício 3: Pipeline de Processamento (File + “Stream”)
Este exercício complementa as Partes 1 e 2 com um **pipeline de transformação** para gerar **dados curados, agregações, alertas e relatórios** a partir dos CSVs de produtos e clima.

- **File Processor (batch)** → lê CSV/JSON de uma pasta, aplica limpeza + normalização + agregações + detecção de anomalias, e salva as saídas.

- **Stream Processor (near real-time)** → observa uma pasta; quando entra/é alterado um CSV, reexecuta o pipeline automaticamente.

1) 📋 Pré-requisitos

- CSVs do Exercício 1 em ../aula04_mcp_coleta/data/ (ou outra pasta sua).

- Python 3.10+ e dependências instaladas.

2) 🧱 O que o pipeline faz (resumo)

**Produtos/Preços**

- Converte datas para UTC, tipa colunas numéricas.

- Regras de plausibilidade: `price_gbp >= 0` , `rating_1to5 ∈ [1,5]`.

- Normaliza `url` e deduplica por (`url,scraped_at`).

- Agregação diária por URL: `min/avg/max/last/n_obs`.

- Anomalias: z-score do delta de preço entre leituras consecutivas.

**Clima (hourly)**

- Datas UTC + tipagem numérica.

- Plausibilidade: `temperature [-80,80]`, `humidity [0,100]`, `precip >= 0`, `wind >= 0`.

- Dedup por (`city,time`).

- Agregação diária por cidade: `avg_temp/avg_rh/total_precip/avg_wind/n_obs`.

- Anomalias: z-score de temperatura e IQR para precipitação.

**Saídas geradas**
```lua
output/
├─ curated/
│  ├─ products_curated.parquet
│  └─ weather_hourly_curated.parquet
├─ agg/
│  ├─ daily_price_stats.csv
│  └─ weather_daily_stats.csv
├─ alerts/
│  ├─ price_anomalies.csv
│  └─ weather_anomalies.csv
└─ reports/
   ├─ chart_price_counts.png
   ├─ chart_temp_daily.png
   └─ report.md
```

3) ▶️ Execução em lote (File Processor)

```bash
python -m src.processor_file --data-dir ../aula04_mcp_coleta/data --out-dir ./output
```

4) 🔁 Execução “em tempo quase real” (Stream Processor)

```bash
python -m src.stream_processor --watch-dir ../aula04_mcp_coleta/data --out-dir ./output --poll-seconds 5
```