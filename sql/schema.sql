-- =========================================================
-- Aula 04 - Exercício 2
-- 001_create_schema.sql  (criação de schema e tabelas)
-- Idempotente + normalizações necessárias
-- =========================================================

-- 1) Schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- 2) Dimensão de Produto
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id    BIGSERIAL PRIMARY KEY,
    url           TEXT NOT NULL UNIQUE,
    product_name  TEXT NOT NULL,
    source        TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3) Fato de Preço
CREATE TABLE IF NOT EXISTS analytics.fact_price (
    product_id   BIGINT       NOT NULL REFERENCES analytics.dim_product(product_id),
    scraped_at   TIMESTAMPTZ  NOT NULL,
    price_gbp    NUMERIC(10,2) CHECK (price_gbp IS NULL OR price_gbp >= 0),
    availability TEXT,
    rating_1to5  SMALLINT     CHECK (rating_1to5 IS NULL OR rating_1to5 BETWEEN 1 AND 5),
    PRIMARY KEY (product_id, scraped_at)
);

-- 4) Dimensão de Cidade (já com NOT NULL DEFAULT '' em country)
CREATE TABLE IF NOT EXISTS analytics.dim_city (
    city_id   BIGSERIAL PRIMARY KEY,
    city_name TEXT NOT NULL,
    country   TEXT NOT NULL DEFAULT '',
    latitude  NUMERIC(8,5),
    longitude NUMERIC(8,5)
);

-- 4.1) Caso a tabela já existisse com country NULL, normaliza:
--      (a) preenche NULL -> ''
UPDATE analytics.dim_city
SET country = ''
WHERE country IS NULL;

--      (b) garante DEFAULT e NOT NULL
ALTER TABLE analytics.dim_city
  ALTER COLUMN country SET DEFAULT '',
  ALTER COLUMN country SET NOT NULL;

--      (c) garante a unicidade (city_name, country) com constraint nomeada
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conrelid = 'analytics.dim_city'::regclass
    AND    conname  = 'uq_city'
  ) THEN
    ALTER TABLE analytics.dim_city
      ADD CONSTRAINT uq_city UNIQUE (city_name, country);
  END IF;
END $$;

-- 5) Fato de Clima por Hora
CREATE TABLE IF NOT EXISTS analytics.fact_weather_hourly (
    city_id               BIGINT       NOT NULL REFERENCES analytics.dim_city(city_id),
    time_at               TIMESTAMPTZ  NOT NULL,
    temperature_2m        NUMERIC(5,2)  CHECK (temperature_2m IS NULL OR (temperature_2m BETWEEN -80 AND 80)),
    relative_humidity_2m  NUMERIC(5,2)  CHECK (relative_humidity_2m IS NULL OR (relative_humidity_2m BETWEEN 0 AND 100)),
    precipitation         NUMERIC(6,2)  CHECK (precipitation IS NULL OR precipitation >= 0),
    wind_speed_10m        NUMERIC(6,2)  CHECK (wind_speed_10m IS NULL OR wind_speed_10m >= 0),
    PRIMARY KEY (city_id, time_at)
);
