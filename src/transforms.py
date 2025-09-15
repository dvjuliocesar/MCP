import pandas as pd, numpy as np
def _to_datetime_utc(s): return pd.to_datetime(s, utc=True, errors="coerce")

def clean_products(df):
    df=df.copy(); df["scraped_at"]=_to_datetime_utc(df.get("scraped_at"))
    df["price_gbp"]=pd.to_numeric(df.get("price_gbp"), errors="coerce")
    if "rating_1to5" in df.columns:
        df["rating_1to5"]=pd.to_numeric(df["rating_1to5"], errors="coerce")
        df.loc[~df["rating_1to5"].between(1,5), "rating_1to5"]=np.nan
    df=df[(df["price_gbp"].isna()) | (df["price_gbp"]>=0)]
    if "url" in df.columns: df["url"]=df["url"].astype(str).str.strip()
    if {"url","scraped_at"}.issubset(df.columns):
        df=df.sort_values("scraped_at").drop_duplicates(["url","scraped_at"], keep="last")
    return df

def agg_price_daily(df):
    if df.empty: return df
    df=df.copy(); df["day"]=df["scraped_at"].dt.date
    return df.groupby(["url","day"]).agg(min_price=("price_gbp","min"),
        avg_price=("price_gbp","mean"), max_price=("price_gbp","max"),
        last_price=("price_gbp","last"), n_obs=("price_gbp","size")).reset_index()

def detect_price_anomalies(df, z_thresh=3.0):
    if df.empty: return df
    df=df.sort_values(["url","scraped_at"]).copy()
    df["price_prev"]=df.groupby("url")["price_gbp"].shift(1)
    df["delta"]=df["price_gbp"]-df["price_prev"]
    df["delta_z"]=df.groupby("url")["delta"].transform(lambda s:(s-s.mean())/s.std(ddof=0) if s.std(ddof=0) not in (0,np.nan) else 0)
    out=df[df["delta_z"].abs()>=z_thresh].dropna(subset=["delta"])
    return out[["url","scraped_at","price_gbp","price_prev","delta","delta_z"]]

def clean_weather(df):
    df=df.copy(); df["time"]=_to_datetime_utc(df.get("time"))
    for col in ["temperature_2m","relative_humidity_2m","precipitation","wind_speed_10m"]:
        if col in df.columns: df[col]=pd.to_numeric(df[col], errors="coerce")
    df.loc[~df["temperature_2m"].between(-80,80), "temperature_2m"]=np.nan
    df.loc[~df["relative_humidity_2m"].between(0,100), "relative_humidity_2m"]=np.nan
    df.loc[df["precipitation"]<0, "precipitation"]=np.nan
    df.loc[df["wind_speed_10m"]<0, "wind_speed_10m"]=np.nan
    if {"city","time"}.issubset(df.columns):
        df=df.sort_values("time").drop_duplicates(["city","time"], keep="last")
    return df

def agg_weather_daily(df):
    if df.empty: return df
    df=df.copy(); df["day"]=df["time"].dt.date
    return df.groupby(["city","day"]).agg(
        avg_temp=("temperature_2m","mean"),
        avg_rh=("relative_humidity_2m","mean"),
        total_precip=("precipitation","sum"),
        avg_wind=("wind_speed_10m","mean"),
        n_obs=("time","size")).reset_index()

def detect_weather_anomalies(df, z_thresh=3.0):
    if df.empty: return df
    df=df.sort_values(["city","time"]).copy()
    df["temp_z"]=df.groupby("city")["temperature_2m"].transform(lambda s:(s-s.mean())/s.std(ddof=0) if s.std(ddof=0) not in (0,np.nan) else 0)
    def iqr_flags(s):
        q1=s.quantile(0.25); q3=s.quantile(0.75); iqr=q3-q1; low,high=q1-1.5*iqr,q3+1.5*iqr
        return ~s.between(low,high)
    df["precip_outlier"]=df.groupby("city")["precipitation"].transform(iqr_flags)
    out=df[(df["temp_z"].abs()>=z_thresh) | (df["precip_outlier"]==True)]
    return out[["city","time","temperature_2m","temp_z","precipitation","precip_outlier","wind_speed_10m"]]
