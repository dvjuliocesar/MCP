-- Quantitativos básicos
SELECT COUNT(*) AS n_products FROM analytics.dim_product;
SELECT COUNT(*) AS n_prices   FROM analytics.fact_price;
SELECT COUNT(*) AS n_cities   FROM analytics.dim_city;
SELECT COUNT(*) AS n_weather  FROM analytics.fact_weather_hourly;

-- Amostra dos dados
TABLE analytics.dim_product LIMIT 5;
TABLE analytics.fact_price  LIMIT 5;
TABLE analytics.dim_city    LIMIT 5;
TABLE analytics.fact_weather_hourly LIMIT 5;

-- Último preço por produto (DISTINCT ON + ordenação)
SELECT DISTINCT ON (p.product_id)
       p.product_id, p.product_name, p.url,
       f.scraped_at, f.price_gbp, f.availability, f.rating_1to5
FROM analytics.dim_product p
JOIN analytics.fact_price f USING (product_id)
ORDER BY p.product_id, f.scraped_at DESC
LIMIT 20;

-- Clima — médias por cidade no dia corrente
SELECT c.city_name::text AS city,
       DATE_TRUNC('hour', f.time_at) AS hour_bucket,
       AVG(f.temperature_2m) AS avg_temp,
       AVG(f.relative_humidity_2m) AS avg_rh,
       SUM(f.precipitation) AS rain
FROM analytics.fact_weather_hourly f
JOIN analytics.dim_city c USING (city_id)
WHERE f.time_at::date = CURRENT_DATE
GROUP BY c.city_name, hour_bucket
ORDER BY city, hour_bucket;

-- Upsert de Produto (garante existência antes de preços)
WITH new_prod(url, product_name, source) AS (
  VALUES
  ('http://example.com/product/123', 'Produto Exemplo 123', 'example.com')
)
INSERT INTO analytics.dim_product (url, product_name, source, first_seen_at, last_seen_at)
SELECT url, product_name, source, NOW(), NOW()
FROM new_prod
ON CONFLICT (url) DO UPDATE
SET product_name = EXCLUDED.product_name,
    source       = EXCLUDED.source,
    last_seen_at = NOW();

-- Upsert de Preço (com chave (product_id, scraped_at))
WITH new_price(url, scraped_at, price_gbp, availability, rating_1to5) AS (
  VALUES
  ('http://example.com/product/123', NOW(), 99.90, 'In stock', 4)
),
pid AS (
  SELECT p.product_id, n.scraped_at, n.price_gbp, n.availability, n.rating_1to5
  FROM new_price n
  JOIN analytics.dim_product p ON p.url = n.url
)
INSERT INTO analytics.fact_price (product_id, scraped_at, price_gbp, availability, rating_1to5)
SELECT product_id, scraped_at, price_gbp, availability, rating_1to5 FROM pid
ON CONFLICT (product_id, scraped_at) DO UPDATE
SET price_gbp   = EXCLUDED.price_gbp,
    availability= EXCLUDED.availability,
    rating_1to5 = EXCLUDED.rating_1to5;

-- Upsert de Cidade (compatível com uq_city em (city_name, country))
INSERT INTO analytics.dim_city (city_name, country, latitude, longitude)
VALUES ('Brasília', '', NULL, NULL)
ON CONFLICT ON CONSTRAINT uq_city DO UPDATE
SET latitude  = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

-- Upsert de Clima horário (cidade + timestamp)
WITH city AS (
  SELECT city_id FROM analytics.dim_city WHERE city_name = 'Brasília' AND country = ''
)
INSERT INTO analytics.fact_weather_hourly
  (city_id, time_at, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m)
SELECT city_id, NOW(), 27.4, 55.0, 0.0, 2.1 FROM city
ON CONFLICT (city_id, time_at) DO UPDATE
SET temperature_2m       = EXCLUDED.temperature_2m,
    relative_humidity_2m = EXCLUDED.relative_humidity_2m,
    precipitation        = EXCLUDED.precipitation,
    wind_speed_10m       = EXCLUDED.wind_speed_10m;

-- Renomear produto e atualizar last_seen_at
UPDATE analytics.dim_product
SET product_name = '[AJUSTE] ' || product_name,
    last_seen_at = NOW()
WHERE url = 'http://example.com/product/123';

-- Remover leituras climáticas de uma data específica (cidade = Brasília)
DELETE FROM analytics.fact_weather_hourly f
USING analytics.dim_city c
WHERE f.city_id = c.city_id
  AND c.city_name = 'Brasília'
  AND f.time_at::date = CURRENT_DATE - 7;