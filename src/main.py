\
import os
from dotenv import load_dotenv
from loguru import logger

def main():
    load_dotenv()
    logger.info("=== Coleta Aula 04 ===")
    # Scraping
    logger.info("Rodando scraper de preços...")
    os.system("python -m src.scraper")
    # API
    logger.info("Consumindo API meteorológica...")
    os.system("python -m src.api_client")
    logger.info("Concluído. Verifique a pasta ./data.")

if __name__ == "__main__":
    main()
