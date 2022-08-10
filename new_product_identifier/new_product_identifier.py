import keyring
import pandas as pd
import datetime
import time
from selenium import webdriver
from urllib.request import urlopen
from pymongo import MongoClient


class Driver:

    def __init__(self):
        # Setup driver
        chrome_path = r"/opt/homebrew/bin/chromedriver"
        op = webdriver.ChromeOptions()
        op.add_argument('--headless')
        self.driver = webdriver.Chrome(chrome_path, options=op)

    def get_recent_pdw_products(self):
        ''''''
        # Define dates
        today = datetime.datetime.today()
        one_week_ago = today - datetime.timedelta(weeks=1)

        # Create connection string for DocDB
        user = "dbadmin"
        password = keyring.get_password('docdb_prod_dbadmin', 'dbadmin')
        host = "production-documentdb.cluster-cb6kajicuplh.us-east-1.docdb.amazonaws.com"
        port = "27017"
        options = "tls=true&tlsAllowInvalidCertificates=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        cxn_string_template= "mongodb://{}:{}@{}:{}/?{}"
        cxn_string_prod = cxn_string_template.format(user, password, host, port, options)

        # Get past week's new products
        client = MongoClient(cxn_string_prod)
        db = client['product-prod']
        results_ = list(db.PdwProductCore.find(
            {'createTimestamp': {'$gte': one_week_ago}}, 
            ('productGeneral.cusip', 'productGeneral.isin')
        ))
        id_dict = {}
        id_dict['cusip'] = [i['productGeneral']['cusip'] for i in results_ if 'cusip' in i['productGeneral']]
        id_dict['isin'] = [i['productGeneral']['isin'] for i in results_ if 'isin' in i['productGeneral']]

        return id_dict
    

    def get_bmo_products(self):
        ''''''
        # Setup for bmo
        bmo_act_dict = {}
        # Get first page
        url = 'https://www.bmonotes.com/Type/PPNs#active'
        page = self.driver.get(url)
        time.sleep(2)
        bmo_act_dict[0] = pd.read_html(self.driver.page_source)[1]
        # Get remaining 4 pages
        for num in range(2, 6):
            self.driver.find_element_by_id("DataTables_Table_1_next").click()
            time.sleep(2)
            bmo_act_dict[num] = pd.read_html(self.driver.page_source)[1]
        # Combine the dataframes
        bmo_active_products = pd.concat([bmo_act_dict[k] for k in bmo_act_dict.keys()], ignore_index=True)
        # Create pdw cusip for comparison
        bmo_active_products['pdwCusip'] = [
            'CA' + i if len(i) == 7 else 'C' + i for i in bmo_active_products['JHN Code / Cusip']
        ]

        return bmo_active_products


    def compare_bmo_pdw(self, bmo_prods, pdw_prods):
        ''''''
        # Filter out any products already present in pdw
        new_bmo_active_products = bmo_prods[
            (bmo_prods['JHN Code / Cusip'].isin(pdw_prods['cusip'])==False) &
            (bmo_prods['JHN Code / Cusip'].isin(pdw_prods['isin'])==False)
        ]
        # Create list of product urls to return
        urls = ['https://www.bmonotes.com/Note/' + i for i in new_bmo_active_products['JHN Code / Cusip']]

        return urls

if __name__ == '__main__':
    # Instantiate Driver
    driver = Driver()
    # Get cusip and isin for all products added to pdw in the past week
    recent_pdw_products_dict = driver.get_recent_pdw_products()
    # Get new BMO products
    bmo_prods = driver.get_bmo_products()
    driver.compare_bmo_pdw(bmo_prods, recent_pdw_products_dict)
