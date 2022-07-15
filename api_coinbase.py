from datetime import datetime
import requests
from coinbase.wallet.client import Client
import logging
import json

# logging.disable(logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')

url = "https://api.exchange.coinbase.com/products"

def get_api_key():
    """Get api key and secret from API_KEY_FILE."""
    API_KEY_FILE = 'apikey.properties'
    with open(API_KEY_FILE, 'r') as a:
        data = a.read().split('\n')
        for x in data:
            if 'API_KEY' in x:
                api_key = x.split()[-1]
            elif 'API_SECRET' in x:
                api_secret = x.split()[-1]
    return api_key, api_secret

def get_spot_price(client):
    """Use coinbase client to get spot price"""
    base_currencies = ['BTC', 'ETH', 'SNX', 'UNI', 'AAVE', 'SOL']
    quote_currencies = ['USD', 'BTC', 'USDC', 'USDT']
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)
    trading_pairs = json.loads(response.text)
    # print(trading_pairs[0].keys())
    pairs = [pair['id'] for pair in trading_pairs if pair['quote_currency'] in quote_currencies\
             and pair['base_currency'] in base_currencies]
    price = client.get_spot_price(currency_pair=pairs)
    print(f"{datetime.utcnow().isoformat()} : {price}")

def get_user():
    api_key, api_secret = get_api_key()
    client = Client(api_key, api_secret)
    user = client.get_current_user()
    return user
