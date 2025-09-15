# Bibliotecas
import os, glob
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, ASCENDING, UpdateOne
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "./data")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "aula04")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

def ensure_indexes():
    db.products.create_index([("url", ASCENDING)], unique=True)
    db.products.create_index([("prices.scraped_at", ASCENDING)])
    db.weather_hourly.create_index([("city", ASCENDING), ("time", ASCENDING)], unique=True)

def load_products(csv_path):
    df = pd.read_csv(csv_path)
    ops = []
    for _, r in df.iterrows():
        price = {
            "scraped_at": pd.to_datetime(r["scraped_at"]).to_pydatetime(),
            "price_gbp": float(r["price_gbp"]) if pd.notnull(r["price_gbp"]) else None,
            "availability": r.get("availability"),
            "rating_1to5": int(r["rating_1to5"]) if pd.notnull(r["rating_1to5"]) else None
        }
        ops.append(UpdateOne(
            {"url": r["url"]},
            {"$set": {"product_name": r["product_name"], "source": r["source"]},
             "$setOnInsert": {"created_at": datetime.utcnow()},
             "$push": {"prices": price}},
            upsert=True
        ))
    if ops: db.products.bulk_write(ops, ordered=False)

def load_weather(csv_path):
    df = pd.read_csv(csv_path)
    city = df["city"].iloc[0] if "city" in df.columns and len(df) else "Cidade"
    ops = []
    for _, r in df.iterrows():
        ops.append(UpdateOne(
            {"city": city, "time": pd.to_datetime(r["time"]).to_pydatetime()},
            {"$set": {
                "temperature_2m": float(r["temperature_2m"]) if pd.notnull(r["temperature_2m"]) else None,
                "relative_humidity_2m": float(r["relative_humidity_2m"]) if pd.notnull(r["relative_humidity_2m"]) else None,
                "precipitation": float(r["precipitation"]) if pd.notnull(r["precipitation"]) else None,
                "wind_speed_10m": float(r["wind_speed_10m"]) if pd.notnull(r["wind_speed_10m"]) else None,
            }},
            upsert=True
        ))
    if ops: db.weather_hourly.bulk_write(ops, ordered=False)

def main():
    ensure_indexes()
    prods = sorted(glob.glob(os.path.join(DATA_DIR, "products_*.csv")))
    if prods: load_products(prods[-1]); logger.info(f"Mongo: products {prods[-1]}")
    else: logger.warning("CSV de produtos não encontrado.")

    weathers = sorted(glob.glob(os.path.join(DATA_DIR, "weather_hourly_*_*.csv")))
    if weathers: load_weather(weathers[-1]); logger.info(f"Mongo: weather {weathers[-1]}")
    else: logger.warning("CSV de clima não encontrado.")

if __name__ == "__main__":
    main()
