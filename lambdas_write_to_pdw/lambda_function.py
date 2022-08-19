import requests
import json
import os
from oauth_access_token import get_new_token
from call_product_api import call_luma_product_api


def lambda_handler():
    with open('product.json') as f:
        product_data = json.load(f)
    new_access_token = get_new_token('1%4@119cXoqN', 'O0*t7SJ%6fza4D%z')
    call_luma_product_api(product_data, new_access_token)

lambda_handler()