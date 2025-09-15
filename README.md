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
psql -h localhost -U postgres -d postgres -c "CREATE DATABASE aula04;"
psql -h localhost -U postgres -d aula04 -f sql/postgres/001_create_schema.sql
psql -h localhost -U postgres -d aula04 -f sql/postgres/002_indexes.sql
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



