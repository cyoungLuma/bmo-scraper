from multiprocessing.connection import Client
import requests
import json
import os
from oauth_access_token import get_new_token
from call_product_api import call_luma_product_api

client_credentials = {
    'client_id':'$interService',
    'client_secret':'$CLIENTSECRET'
}
def lambda_handler():
    with open('product.json') as f:
        product_data = json.load(f)
    new_access_token = get_new_token(client_credentials['client_id'], client_credentials['client_secret'])
    call_luma_product_api(product_data, new_access_token)

lambda_handler()