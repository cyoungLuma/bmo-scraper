import keyring
import pandas as pd
import datetime
import time
from selenium import webdriver
from urllib.request import urlopen
from pymongo import MongoClient


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
cusips = [i['productGeneral']['cusip'] for i in results_ if 'cusip' in i['productGeneral']]
isins = [i['productGeneral']['isin'] for i in results_ if 'isin' in i['productGeneral']]

# Setup for bmo
bmo_act_dict = {}
chrome_path = r"/opt/homebrew/bin/chromedriver"
op = webdriver.ChromeOptions()
op.add_argument('--headless')
driver = webdriver.Chrome(chrome_path, options=op)
url = 'https://www.bmonotes.com/Type/PPNs#active'

# Get first page
page = driver.get(url)
time.sleep(2)
bmo_act_dict[0] = pd.read_html(driver.page_source)[1]

# Get remaining 4 pages
for num in range(2, 6):
    driver.find_element_by_id("DataTables_Table_1_next").click()
    time.sleep(2)
    bmo_act_dict[num] = pd.read_html(driver.page_source)[1]
driver.close()

# Combine the dataframes
bmo_active_products = pd.concat([bmo_act_dict[k] for k in bmo_act_dict.keys()], ignore_index=True)

# Create pdw cusip for comparison
bmo_active_products['pdwCusip'] = [
    'CA' + i if len(i) == 7 else 'C' + i for i in bmo_active_products['JHN Code / Cusip']
]

# Filter out any products already present in pdw
new_bmo_active_products = bmo_active_products[
    ~bmo_active_products['JHN Code / Cusip'].isin(cusips) | 
    bmo_active_products['JHN Code / Cusip'].isin(isins)
]

print(len(new_bmo_active_products))
