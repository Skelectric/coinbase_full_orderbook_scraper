from coinbase.wallet.client import Client
from loguru import logger
from pathlib import Path

# =====================================================
# PARAMETERS
URL = "https://api.exchange.coinbase.com/"
SECRET_FILE = 'apikey.properties'

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


class CoinbaseAPI:
    def __init__(self, url=URL, secret_file=SECRET_FILE):
        # globals
        self.url = url
        self.secret_file = secret_file

        # api key
        self.api_key = None
        self.api_secret = None
        self.load_apikey_properties()

        # init coinbase client
        self.client = Client(self.api_key, self.api_secret)
        self.user = self.client.get_current_user()

        # self.accounts = []
        # self.get_accounts(nonzero=True)
        # # self.delete_zero_accounts()

    def load_apikey_properties(self):
        """Get api key and secret from api secret file."""
        secret_file_exists = Path(f"{self.secret_file}").is_file()
        if secret_file_exists:
            try:
                with open(self.secret_file, 'r') as a:
                    data = a.read().split('\n')
                    for x in data:
                        if 'API_KEY' in x:
                            self.api_key = x.split()[-1]
                        elif 'API_SECRET' in x:
                            self.api_secret = x.split()[-1]
            except Exception as e:
                logger.critical(e)
        else:
            logger.critical("API Secret file is missing!")
            logger.info("Ensure apikey.properties exists in appropriate directory.")
            logger.info("Appropriate format is:\n# coinbase\nAPI_KEY = X\nAPI_SECRET = XX")

    def get_accounts(self, nonzero=True):
        @paginate_accounts
        def _get(_self, _account):
            if nonzero:
                if float(_account.balance.amount) != 0:
                    print(_account.balance.currency, _account.balance.amount, _account.id, sep=' - ')
                    _self.accounts.append(_account)
            else:
                print(_account.balance.currency, _account.balance.amount, _account.id, sep=' - ')

        if nonzero:
            print("Coinbase accounts with nonzero balances:\n")
        else:
            print("Coinbase accounts:\n")

        _get(self)

    def delete_zero_accounts(self):
        @paginate_accounts
        def _delete(_account):
            if float(_account.balance.amount) == 0:
                print(f"Identified account {_account.id} for {_account.balance.currency} with 0 balance. Attempting delete...")
                self.client.delete_account(_account.id)

        confirm_del = input("Confirm delete all zero balance accounts? (Y/N): ")
        if confirm_del.lower() == 'y':
            print(f"Deleting...")
            _delete(self)

    def get_primary_account(self):
        primary_account = self.client.get_primary_account()
        print(primary_account)

    @property
    def get_user(self):
        return self.user


if __name__ == "__main__":
    coinbase_api = CoinbaseAPI()


# def get_spot_price(client):
#     """Use coinbase client to get spot price"""
#     base_currencies = ['BTC', 'ETH', 'SNX', 'UNI', 'AAVE', 'SOL']
#     quote_currencies = ['USD', 'BTC', 'USDC', 'USDT']
#     headers = {"Accept": "application/json"}
#     response = requests.get(url, headers=headers)
#     trading_pairs = json.loads(response.text)
#     # print(trading_pairs[0].keys())
#     pairs = [pair['id'] for pair in trading_pairs if pair['quote_currency'] in quote_currencies\
#              and pair['base_currency'] in base_currencies]
#     price = client.get_spot_price(currency_pair=pairs)
#     print(f"{datetime.utcnow().isoformat()} : {price}")




