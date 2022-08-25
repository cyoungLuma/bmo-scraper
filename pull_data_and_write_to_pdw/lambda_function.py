from multiprocessing.connection import Client
import requests
import json
import os
from oauth_access_token import get_new_token
from call_product_api import call_luma_product_api
from new_product_identifier import Driver 
from BmoScraper import BmoScraper


client_credentials = {
    'client_id':'CHANGEME',
    'client_secret':'CHANGEME'
}

def run_bmo_scraper(note_urls):
    bmo = BmoScraper(note_urls)
    bmo.run_all_rules()
    bmo.output_jsons()

    return bmo.result

def run_url_crawler():
    driver = Driver()

    # Get cusip and isin for all products added to pdw in the past week
    recent_pdw_products_dict = driver.get_recent_pdw_products()

    # Get new BMO products
    bmo_prods = driver.get_bmo_products()
    urls = driver.compare_site_to_pdw('bmo', bmo_prods, recent_pdw_products_dict)

    return urls

# %% Write to PDW & view status


def lambda_handler():
    # Get urls with crawler
    urls = run_url_crawler()
    # Get product info with scraper 
    products = run_bmo_scraper(urls)
    # Generates new token
    new_access_token = get_new_token(client_credentials['client_id'], client_credentials['client_secret'])
    product_list = []
    # Post to api
    for key in products:
        product_data = json.loads(products[key])
        call_luma_product_api(product_data, new_access_token)
        product_list.append(key)
    print(product_list)

lambda_handler()