# 🌐 Aula 04 — MCP: Coleta, Armazenamento e Processamento

## Exercício 1: Coleta de Dados (Guia + Código)

Este projeto demonstra **duas formas de coleta**:
1) **Scraping web** de preços (site de treino: `Books to Scrape` — e-commerce fictício, seguro para estudos).
2) **Cliente de API pública** (Open-Meteo) para séries horárias meteorológicas.

> Foco: conexões seguras, *timeouts*, *retries*, *backoff*, registro de logs, tratamento de erros e documentação dos **campos extraídos**.

## ▶️ Como executar
```bash
python -m venv .venv
# Ative a venv (Windows: .venv\Scripts\activate | Linux/Mac: source .venv/bin/activate)
pip install -r requirements.txt
cp .env .env
python -m src.main
```
Saídas em `./data`.

## Campos extraídos
### 🗂️ Produtos (scraper)
`source, product_name, price_gbp, availability, rating_1to5, url, scraped_at`

### 🗂️ Meteorologia (API)
`city, time, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m`
