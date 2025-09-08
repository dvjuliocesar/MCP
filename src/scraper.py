\
import os
import time
import datetime as dt
from urllib.parse import urljoin
import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
from loguru import logger

def _session(timeout: int = 15):
    # Return a requests.Session with retries and sane headers.
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    })
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        redirect=3,
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=0.5,
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.request_timeout = timeout  # custom attribute
    return s

def _parse_product(card, base_url):
    title_tag = card.select_one("h3 a")
    name = title_tag["title"].strip()
    url = urljoin(base_url, title_tag["href"])
    price_text = card.select_one(".price_color").get_text(strip=True).replace("£","").replace(",",".")
    try:
        price = float(price_text)
    except Exception:
        price = None
    availability = card.select_one(".availability").get_text(" ", strip=True)
    rating_cls = next((c for c in card.select_one(".star-rating")["class"] if c != "star-rating"), None)
    rating_map = {"One":1, "Two":2, "Three":3, "Four":4, "Five":5}
    rating = rating_map.get(rating_cls, None)
    return {
        "source": base_url,
        "product_name": name,
        "price_gbp": price,
        "availability": availability,
        "rating_1to5": rating,
        "url": url,
        "scraped_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }

def scrape_category(category_url: str, max_pages: int = 3, backoff_seconds: float = 1.0, timeout: int = 15) -> pd.DataFrame:
    # Scrapeia os produtos de uma categoria do site 'Books to Scrape' com paginação.
    logger.info(f"Iniciando scraping: {category_url}")
    s = _session(timeout=timeout)
    collected = []
    next_url = category_url
    pages = 0

    while next_url and pages < max_pages:
        pages += 1
        logger.info(f"Página {pages}: {next_url}")
        r = s.get(next_url, timeout=timeout)
        r.raise_for_status()
        soup = BS(r.text, "html.parser")
        base_url = next_url.rsplit("/", 1)[0] + "/"
        for card in soup.select("article.product_pod"):
            collected.append(_parse_product(card, base_url))

        # próxima página, se existir
        next_link = soup.select_one("li.next a")
        if next_link:
            next_url = urljoin(next_url, next_link["href"])
        else:
            next_url = None

        time.sleep(backoff_seconds)  # cortesia/limitação

    df = pd.DataFrame(collected)
    logger.info(f"Itens coletados: {len(df)}")
    return df

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    CATEGORY_URL = os.getenv("CATEGORY_URL")
    MAX_PAGES = int(os.getenv("MAX_PAGES", "3"))
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
    BACKOFF_SECONDS = float(os.getenv("BACKOFF_SECONDS", "1.0"))

    df = scrape_category(CATEGORY_URL, max_pages=MAX_PAGES, backoff_seconds=BACKOFF_SECONDS, timeout=REQUEST_TIMEOUT)
    out = os.path.join(os.path.dirname(__file__), "..", "data", f"products_{dt.date.today().isoformat()}.csv")
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"[OK] CSV salvo em: {out}")
