-- =========================================================
-- Aula 04 - Exercício 2
-- 002_indexes.sql (índices auxiliares)
-- =========================================================

-- acelera análises por produto + tempo (últimos N dias/horas)
CREATE INDEX IF NOT EXISTS ix_fact_price_pid_time
  ON analytics.fact_price (product_id, scraped_at DESC);

-- acelera análises por cidade + tempo
CREATE INDEX IF NOT EXISTS ix_fact_weather_city_time
  ON analytics.fact_weather_hourly (city_id, time_at DESC);

-- lookup por URL (já existe um UNIQUE, mas mantemos explícito)
CREATE INDEX IF NOT EXISTS ix_dim_product_url
  ON analytics.dim_product (url);

-- (opcional) busca por nome de produto
-- CREATE INDEX IF NOT EXISTS ix_dim_product_name ON analytics.dim_product (product_name);
