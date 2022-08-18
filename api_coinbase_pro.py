from pathlib import Path
import sys
import requests
import json

# third-party modules
from loguru import logger
import coinbasepro as cbp
from decimal import Decimal

# =====================================================
logger.remove()
# add console logger with formatting
logger.add(
    sys.stdout, level="DEBUG",
    format="<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> --- <level>{level}</level> | Thread {thread} <level>{message}</level>"
)

# =====================================================


class CoinbaseProAPI(cbp.AuthenticatedClient):
    def __init__(self):
        url = "https://api.pro.coinbase.com"
        secret_file = 'hide/apikey_coinbase_pro_full_permissions.properties'
        api_key, api_secret, api_passphrase = self.load_apikey_properties(secret_file)

        super().__init__(
            key=api_key,
            secret=api_secret,
            passphrase=api_passphrase,
            api_url=url
        )

    @staticmethod
    def load_apikey_properties(secret_file):
        """Get api key and secret from api secret file."""
        secret_file_exists = Path(f"{secret_file}").is_file()
        if secret_file_exists:
            try:
                with open(secret_file, 'r') as a:
                    data = a.read().split('\n')
                    for x in data:
                        if 'API_KEY' in x:
                            api_key = x.split()[-1]
                        elif 'API_PASSPHRASE' in x:
                            api_passphrase = x.split()[-1]
                        elif 'API_SECRET' in x:
                            api_secret = x.split()[-1]
            except Exception as e:
                logger.critical(e)
        else:
            logger.critical("API Secret file is missing!")
            logger.info(f"Ensure {secret_file} exists in base directory.")
            logger.info("Appropriate format is:\n# exchange\nAPI_KEY = X\nAPI_PASSPHRASE = XX\nAPI_SECRET = XXX")

        return api_key, api_secret, api_passphrase

    @staticmethod
    def save_to_json(obj, fname):
        with open(fname, 'w', encoding='UTF-8') as f:
            json.dump(obj, f, indent=4)

    @staticmethod
    def load_from_json(fname):
        with open(fname, 'r') as f:
            return json.load(f)

if __name__ == "__main__":
    cbp_api = CoinbaseProAPI()
    products = cbp_api.get_products()
    orderbook = cbp_api.get_product_order_book('MATIC-USD', level=3)
    cbp_api.save_to_json(orderbook, f"MATIC-USD_orderbook.json")



