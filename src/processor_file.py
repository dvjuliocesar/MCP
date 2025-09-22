import argparse
from pathlib import Path
import pandas as pd
from .transforms import (
    clean_products, agg_price_daily, detect_price_anomalies,
    clean_weather, agg_weather_daily, detect_weather_anomalies
)

def run(data_dir: Path, out_dir: Path):
    out_cur = out_dir / "curated"; out_agg = out_dir / "agg"; out_alerts = out_dir / "alerts"; out_rep = out_dir / "reports"
    for p in [out_cur, out_agg, out_alerts, out_rep]: p.mkdir(parents=True, exist_ok=True)
    # Produtos
    prod_files = sorted(list(Path(data_dir).glob("products_*.csv")))
    if prod_files:
        dfp = pd.concat([pd.read_csv(f) for f in prod_files], ignore_index=True)
        dfp = clean_products(dfp)
        dfp.to_parquet(out_cur / "products_curated.parquet", index=False)
        agg = agg_price_daily(dfp); agg.to_csv(out_agg / "daily_price_stats.csv", index=False)
        pa = detect_price_anomalies(dfp); pa.to_csv(out_alerts / "price_anomalies.csv", index=False)
    # Clima
    w_files = sorted(list(Path(data_dir).glob("weather_hourly_*_*.csv")))
    if w_files:
        dfw = pd.concat([pd.read_csv(f) for f in w_files], ignore_index=True)
        dfw = clean_weather(dfw)
        dfw.to_parquet(out_cur / "weather_hourly_curated.parquet", index=False)
        wag = agg_weather_daily(dfw); wag.to_csv(out_agg / "weather_daily_stats.csv", index=False)
        wa = detect_weather_anomalies(dfw); wa.to_csv(out_alerts / "weather_anomalies.csv", index=False)
    (out_rep / "report.md").write_text("# Relatório — Pipeline gerado\n", encoding="utf-8")
    print(f"[OK] Pipeline concluído. Saídas em: {out_dir}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    run(Path(args.data_dir), Path(args.out_dir))
