# Bibliotecas
import os, glob, re
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "./data")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "mcp")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres123")

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DB", "mcp"),
)

engine = create_engine(url, future=True)

def latest(pattern):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None

def upsert_products(df):
    with engine.begin() as con:
        for _, r in df.iterrows():
            con.execute(text("""
                INSERT INTO analytics.dim_product (url, product_name, source, first_seen_at, last_seen_at)
                VALUES (:url, :product_name, :source, NOW(), NOW())
                ON CONFLICT (url) DO UPDATE
                SET product_name = EXCLUDED.product_name,
                    source = EXCLUDED.source,
                    last_seen_at = NOW();
            """), dict(url=r["url"], product_name=r["product_name"], source=r["source"]))

def insert_prices(df):
    with engine.begin() as con:
        urls = list(df["url"].unique())
        mapping = {row.url: row.product_id
                   for row in con.execute(text(
                       "SELECT url, product_id FROM analytics.dim_product WHERE url = ANY(:urls)"
                   ), {"urls": urls})}

        batch = []
        for _, r in df.iterrows():
            pid = mapping.get(r["url"])
            if pid is None:
                continue
            price = r["price_gbp"]
            if pd.notnull(price) and price < 0:
                continue
            rating = r["rating_1to5"]
            if pd.notnull(rating) and not (1 <= rating <= 5):
                rating = None

            batch.append(dict(
                product_id=pid,
                scraped_at=pd.to_datetime(r["scraped_at"], utc=True),
                price_gbp=price if pd.notnull(price) else None,
                availability=r.get("availability"),
                rating_1to5=int(rating) if pd.notnull(rating) else None
            ))
            if len(batch) >= 1000:
                con.execute(text("""
                    INSERT INTO analytics.fact_price
                    (product_id, scraped_at, price_gbp, availability, rating_1to5)
                    VALUES (:product_id, :scraped_at, :price_gbp, :availability, :rating_1to5)
                    ON CONFLICT (product_id, scraped_at) DO UPDATE
                    SET price_gbp = EXCLUDED.price_gbp,
                        availability = EXCLUDED.availability,
                        rating_1to5 = EXCLUDED.rating_1to5;
                """), batch); batch.clear()
        if batch:
            con.execute(text("""
                INSERT INTO analytics.fact_price
                (product_id, scraped_at, price_gbp, availability, rating_1to5)
                VALUES (:product_id, :scraped_at, :price_gbp, :availability, :rating_1to5)
                ON CONFLICT (product_id, scraped_at) DO UPDATE
                SET price_gbp = EXCLUDED.price_gbp,
                    availability = EXCLUDED.availability,
                    rating_1to5 = EXCLUDED.rating_1to5;
            """), batch)

def upsert_city(con, city_name, country=None, lat=None, lon=None):
    con.execute(text("""
        INSERT INTO analytics.dim_city (city_name, country, latitude, longitude)
        VALUES (:city_name, :country, :latitude, :longitude)
        ON CONFLICT ON CONSTRAINT uq_city DO UPDATE
        SET latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
    """), {
        "city_name": city_name,
        "country": (country or ""),  # compatível com NOT NULL DEFAULT ''
        "latitude": lat,
        "longitude": lon
    })

def insert_weather(df, city_name):
    with engine.begin() as con:
        upsert_city(con, city_name)
        city_id = con.execute(text(
            "SELECT city_id FROM analytics.dim_city WHERE city_name = :c"
        ), {"c": city_name}).scalar_one()

        batch = []
        for _, r in df.iterrows():
            batch.append(dict(
                city_id=city_id,
                time_at=pd.to_datetime(r["time"], utc=True),
                temperature_2m=r.get("temperature_2m"),
                relative_humidity_2m=r.get("relative_humidity_2m"),
                precipitation=r.get("precipitation"),
                wind_speed_10m=r.get("wind_speed_10m"),
            ))
            if len(batch) >= 1000:
                con.execute(text("""
                    INSERT INTO analytics.fact_weather_hourly
                    (city_id, time_at, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m)
                    VALUES (:city_id, :time_at, :temperature_2m, :relative_humidity_2m, :precipitation, :wind_speed_10m)
                    ON CONFLICT (city_id, time_at) DO UPDATE
                    SET temperature_2m = EXCLUDED.temperature_2m,
                        relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                        precipitation = EXCLUDED.precipitation,
                        wind_speed_10m = EXCLUDED.wind_speed_10m;
                """), batch); batch.clear()
        if batch:
            con.execute(text("""
                INSERT INTO analytics.fact_weather_hourly
                (city_id, time_at, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m)
                VALUES (:city_id, :time_at, :temperature_2m, :relative_humidity_2m, :precipitation, :wind_speed_10m)
                ON CONFLICT (city_id, time_at) DO UPDATE
                SET temperature_2m = EXCLUDED.temperature_2m,
                    relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                    precipitation = EXCLUDED.precipitation,
                    wind_speed_10m = EXCLUDED.wind_speed_10m;
            """), batch)

def main():
    # Produtos
    prod_csv = latest(os.path.join(DATA_DIR, "products_*.csv"))
    if prod_csv:
        dfp = pd.read_csv(prod_csv)
        upsert_products(dfp)
        insert_prices(dfp)
        logger.info(f"Postgres: produtos/preços carregados de {prod_csv}")
    else:
        logger.warning("CSV de produtos não encontrado.")

    # Clima (último arquivo)
    weather_csvs = sorted(glob.glob(os.path.join(DATA_DIR, "weather_hourly_*_*.csv")))
    if weather_csvs:
        latest_w = weather_csvs[-1]
        city = re.search(r"weather_hourly_(.+?)_\d{4}-\d{2}-\d{2}\.csv$", os.path.basename(latest_w)).group(1)
        dfw = pd.read_csv(latest_w)
        insert_weather(dfw, city)
        logger.info(f"Postgres: clima carregado de {latest_w} (cidade={city})")
    else:
        logger.warning("CSV de clima não encontrado.")

if __name__ == "__main__":
    main()
