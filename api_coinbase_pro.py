from pathlib import Path
import sys
import requests

# third-party modules
from loguru import logger
import coinbasepro as cbp
from decimal import Decimal

# =====================================================
# PARAMETERS
URL = "https://api.pro.coinbase.com"
SECRET_FILE = 'hide/apikey_coinbase_pro_full_permissions.properties'
ACCOUNTS_FILE = 'accounts.json'

# =====================================================
logger.remove()
# add console logger with formatting
logger.add(
    sys.stdout, level="DEBUG",
    format="<white>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</white> --- <level>{level}</level> | Thread {thread} <level>{message}</level>"
)

# =====================================================


def paginate_accounts(func):
    """Use coinbase's account pagination to do something."""

    def wrapper(self, *args, **kwargs):
        _next = None  # initialize next wallet id
        while True:  # this loop will run until the next_uri parameter is none (no pages left)
            accounts = self.client.get_accounts(starting_after=_next)
            _next = accounts.pagination.next_starting_after
            _uri = accounts.pagination.next_uri
            for account in accounts.data:
                func(self, account)
            if not _uri:
                print("================================")
                break

    return wrapper


class CoinbaseProAPI(cbp.AuthenticatedClient):
    def __init__(
            self, url=URL, secret_file=SECRET_FILE
    ):
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
        

if __name__ == "__main__":
    cbp_api = CoinbaseProAPI()
    currencies = ["MATIC"]
    # coinbase_api.parse_accounts(get_nonzero=False, curr_list=currencies)
    products = cbp_api.get_products()
    orderbook = cbp_api.get_product_order_book('MATIC-USD', level=3)
    print(len(orderbook))
    print(orderbook[0])


