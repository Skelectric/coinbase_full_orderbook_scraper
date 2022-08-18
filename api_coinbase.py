import coinbase.wallet.error
from coinbase.wallet.client import Client, new_api_object
from coinbase.wallet.model import Account
from loguru import logger
from pathlib import Path
from helper_tools import class_user_interface
import json
import sys

# =====================================================
# PARAMETERS
URL = "https://api.exchange.coinbase.com/"
SECRET_FILE = 'hide/apikey_coinbase_limited_permissions.properties'
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

        self.accounts = []
        self.transactions = {}  # account id : transaction data
        self.buys = {}  # account id : buys
        self.sells = {}  # account id : sells
        self.user_stats = None

    def user_interface(self):
        class_user_interface(self)

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
            logger.info("Ensure apikey_coinbase_limited_permissions.properties exists in appropriate directory.")
            logger.info("Appropriate format is:\n# exchange\nAPI_KEY = X\nAPI_SECRET = XX")

    def get_spot_price(self, currency, base):
        currency_pair = f"{currency}-{base}"
        try:
            exchange_rate = self.client.get_spot_price(currency_pair=currency_pair).amount
        except coinbase.wallet.error.NotFoundError as e:
            spot_price = None
        else:
            spot_price = float(exchange_rate)
        return spot_price

    def get_market_value(self, account, base="USD"):
        spot_price = self.get_spot_price(currency=account.balance.currency, base=base)
        amount = float(account.balance.amount)
        if spot_price is not None:
            return amount * spot_price
        else:
            return None

    def parse_accounts(self, get_nonzero=False, curr_list=None, load_local=False):

        @paginate_accounts
        def sync_accounts(_self, _account):
            _self.accounts.append(_account)

        if load_local:
            self.load_accounts()

        if len(self.accounts) == 0:
            sync_accounts(self)
            self.save_accounts()

        # parse accounts according to get_nonzero and curr_list params
        print("Coinbase accounts:")
        active_accounts = []

        for account in self.accounts:
            # include accounts passed in currencies param, irrespective of account balance
            # OR include accounts with get_nonzero balances (ignore dust accounts too)
            # OR include all accounts
            if (curr_list is not None and account.balance.currency in curr_list) or \
                    (get_nonzero and float(account.balance.amount) > 0.000001) or \
                    (not get_nonzero and curr_list is None):

                active_accounts.append(account)

                spot_price = self.get_spot_price(account.balance.currency, "USD")
                value = self.get_market_value(account)
                value_str = f"${value:.2f}" if value is not None else "No Spot Price"

                print(account.balance.currency, account.balance.amount, account.id, spot_price, value_str, sep=' - ')

        self.accounts = active_accounts

    def save_accounts(self):
        with open(ACCOUNTS_FILE, 'w', encoding='UTF-8') as f:
            json.dump(self.accounts, f, indent=4)
            logger.info(f"Saving new {ACCOUNTS_FILE}")

    def load_accounts(self):
        """Load accounts file into list of Account class instances.
        Faster account info retrieval
        Only useful for testing"""
        if Path(ACCOUNTS_FILE).is_file():
            logger.debug(f"Accounts file found. Loading into memory.")
            with open(ACCOUNTS_FILE, 'r') as f:
                json_data = json.load(f)
                for account in json_data:
                    account = new_api_object(self.client, account, Account)
                    self.accounts.append(account)
        else:
            logger.debug(f"No accounts file.")

    # def delete_zero_accounts(self):
    #     @paginate_accounts
    #     def _delete(_self, _account):
    #         if float(_account.balance.amount) == 0:
    #             print(f"Identified account {_account.id} for {_account.balance.currency} with 0 balance. Attempting delete...")
    #             try:
    #                 self.client.delete_account(_account.id)
    #             except coinbase.wallet.error.InvalidRequestError as e:
    #                 print(e)
    #
    #     confirm_del = input("Confirm delete all zero balance accounts? (Y/N): ")
    #     if confirm_del.lower() == 'y':
    #         print(f"Deleting...")
    #         _delete(self)

    def get_primary_account(self):
        primary_account = self.client.get_primary_account()
        print(f"Primary account: {primary_account}")

    def get_user(self):
        return self.user

    def get_auth_info(self):
        return self.client.get_auth_info()

    def get_transactions(self, account):
        logger.debug(f"Pulling transactions for {account.balance.currency} - {account.id}...")
        transactions = self.client.get_transactions(account_id=account.id)
        self.transactions[account.id] = transactions
        print(self.transactions[account.id])

    def get_buys(self, account, buy_id=None):
        logger.debug(f"Pulling buys for {account.balance.currency} - {account.id}...")
        if buy_id is None:
            self.buys[account.id] = self.client.get_buys(account_id=account.id)
        else:
            self.buys[account.id] = self.client.get_buy(account_id=account.id, buy_id=buy_id)
        print(self.buys[account.id])
        
    def get_sells(self, account, sell_id=None):
        logger.debug(f"Pulling sells for {account.balance.currency} - {account.id}...")
        if sell_id is None:
            self.sells[account.id] = self.client.get_sells(account_id=account.id)
        else:
            self.sells[account.id] = self.client.get_sell(account_id=account.id, sell_id=sell_id)
        print(self.sells[account.id])
        

if __name__ == "__main__":
    coinbase_api = CoinbaseAPI()
    currencies = ["MATIC"]
    # coinbase_api.parse_accounts(get_nonzero=False, curr_list=currencies)
    coinbase_api.parse_accounts(load_local=True)
    for acc in coinbase_api.accounts:
        coinbase_api.get_transactions(account=acc)




