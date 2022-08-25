import pandas as pd
import datetime
import time
from selenium import webdriver
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
        user = "dbuser"
        password = ']cF3X_TxD)}!pTvF'
        host = "dev-documentdb.cluster-cb6kajicuplh.us-east-1.docdb.amazonaws.com"
        port = "27017"
        options = "tls=true&tlsAllowInvalidCertificates=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        cxn_string_template= "mongodb://{}:{}@{}:{}/?{}"
        cxn_string_prod = cxn_string_template.format(user, password, host, port, options)

        # Get past week's new products
        client = MongoClient(cxn_string_prod)
        db = client['product-uat']
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
        chrome_path = r"/opt/homebrew/bin/chromedriver"
        op = webdriver.ChromeOptions()
        op.add_argument('--headless')
        urls = [
            # 'https://www.bmonotes.com/Type/PPNs#active', 
            'https://www.bmonotes.com/Type/Fixed-Income-Notes#active', 
            # 'https://www.bmonotes.com/Type/NPPNs#active'
        ]
        all_bmo_active_products = pd.DataFrame()
        for url in urls:
            print(url)
            # Get first page
            driver = self.driver
            page = driver.get(url)
            time.sleep(2)
            bmo_act_dict[0] = pd.read_html(driver.page_source)[1]
            # Get remaining pages
            flag = True
            num = 1
            while flag:
                if 'disabled' in driver.find_element_by_id("DataTables_Table_1_next").get_attribute('class'):
                    flag = 0
                else:
                    driver.execute_script("arguments[0].click();", driver.find_element_by_id("DataTables_Table_1_next"))
                    time.sleep(2)
                    bmo_act_dict[num] = pd.read_html(driver.page_source)[1]
                    num += 1
            # Combine the dataframes
            bmo_active_products = pd.concat([bmo_act_dict[k] for k in bmo_act_dict.keys()], ignore_index=True)
            all_bmo_active_products = pd.concat([all_bmo_active_products, bmo_active_products], ignore_index=True)
        # Remove junk
        all_bmo_active_products = all_bmo_active_products[(all_bmo_active_products['JHN Code / Cusip'].isna()==False) & (all_bmo_active_products['JHN Code / Cusip']!='Loading...')]
        # Create pdw cusip for comparison
        listy = []
        for i in all_bmo_active_products['JHN Code / Cusip']:
            if 'JHN' in str(i):
                if len(str(i)) == 7:
                    listy.append('CA' + str(i))
                elif len(str(i)) == 8:
                    listy.append('C' + str(i))
                elif len(str(i)) == 6:
                    listy.append('CAD' + str(i))
                else:
                    listy.append('Error')
            else:
                if len(str(i)) == 9:
                    listy.append(str(i))
                else:
                    listy.append('Error')
        all_bmo_active_products['pdwCusip'] = listy

        return all_bmo_active_products
    
    def get_nbcss_products(self):
        # Setup for nbc_ss
        nbcss_act_dict = {}
        # Get first page
        url = 'https://www.nbcstructuredsolutions.ca/listeProduits.aspx?mode=previous'
        page = self.driver.get(url)
        time.sleep(2)
        nbcss_act_dict[0] = pd.read_html(self.driver.page_source)[0]
        nbcss_act_dict[0]['urls'] = [
            self.driver.find_element_by_xpath('//*[@id="ctl00_cphMain_lvProducts_ctrl{}_lnkProduit"]'.format(i)).get_attribute('href') for i in range(len(nbcss_act_dict[0]))
        ]
        # Get remaining 4 pages
        for num in range(2, 6):
            self.driver.find_element_by_xpath('//*[@id="ctl00_cphMain_dpProductsHaut"]/a[{}]'.format(str(num))).click()
            time.sleep(2)
            nbcss_act_dict[num] = pd.read_html(self.driver.page_source)[0]
            nbcss_act_dict[num]['urls'] = [
                self.driver.find_element_by_xpath('//*[@id="ctl00_cphMain_lvProducts_ctrl{}_lnkProduit"]'.format(i)).get_attribute('href') for i in range(len(nbcss_act_dict[num]))
            ]
        # Combine the dataframes
        nbcss_active_products = pd.concat([nbcss_act_dict[k] for k in nbcss_act_dict.keys()], ignore_index=True)
        # Create pdw cusip for comparison
        nbcss_active_products['pdwCusip'] = [
            'CA' + i if len(i) == 7 else 'C' + i for i in nbcss_active_products['FundSERV']
        ]

        return nbcss_active_products
    
    def get_rbc_products(self):
        # Setup for rbc
        rbc_act_dict = {}
        chrome_path = r"/opt/homebrew/bin/chromedriver"
        op = webdriver.ChromeOptions()
        op.add_argument('--headless')
        driver = webdriver.Chrome(chrome_path, options=op)
        url = 'https://www.rbcnotes.com/Products'
        # Get first page
        page = driver.get(url)
        time.sleep(2)
        temp_table = pd.read_html(driver.page_source)
        temp_data = temp_table[1][(temp_table[1][7].isna()==False) & (temp_table[1][7].str.contains('Day')==False)]
        temp_data.columns = temp_table[0].columns
        rbc_act_dict[0] = temp_data
        rbc_act_dict[0]['urls'] = [driver.find_element_by_xpath('//*[@id="productGrid"]/div[2]/div[3]/table/tbody/tr[{}]/td[3]/a'.format(i)).get_attribute('href') 
                                        for i in range(1, len(rbc_act_dict[0])*2, 2)]
        # Get remaining 4 pages
        for num in range(2, 6):
            # Scroll to bottom of page to avoid cookie consent form
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            driver.find_element_by_xpath('//*[@id="productGrid"]/div[2]/div[4]/a[3]').click()
            time.sleep(2)
            temp_table = pd.read_html(driver.page_source)
            temp_data = temp_table[1][(temp_table[1][7].isna()==False) & (temp_table[1][7].str.contains('Day')==False)]
            temp_data.columns = temp_table[0].columns
            rbc_act_dict[num] = temp_data
            rbc_act_dict[num]['urls'] = [driver.find_element_by_xpath('//*[@id="productGrid"]/div[2]/div[3]/table/tbody/tr[{}]/td[3]/a'.format(i)).get_attribute('href') 
                                        for i in range(1, len(rbc_act_dict[0])*2, 2)]
        # Combine the dataframes
        rbc_active_products = pd.concat([rbc_act_dict[k] for k in rbc_act_dict.keys()], ignore_index=True)
        rbc_active_products = rbc_active_products[['Product Name', 'FundSERV Code', 'ADP Code',
            'CUSIP', 'End of Day Price', 'Current ETC', 'ETC End Date',
            'Issue Date', 'Maturity Date', 'Currency', 'urls']]
        # Create pdw cusip for comparison
        rbc_active_products['pdwCusip'] = [
            'CA' + i if len(i) == 7 else 'C' + i for i in rbc_active_products['FundSERV Code']
        ]

        return rbc_active_products


    def compare_site_to_pdw(self, site, site_prods, pdw_prods):
        ''''''
        # Filter out any products already present in pdw
        #### rbc has come cusips, too. Need to account for those
        new_active_products = site_prods[
            (site_prods['pdwCusip'].isin(pdw_prods['cusip'])==False) &
            (site_prods['pdwCusip'].isin(pdw_prods['isin'])==False)
        ]
        # Create list of product urls to return
        if site == 'bmo':
            urls = ['https://www.bmonotes.com/Note/' + i for i in new_active_products['JHN Code / Cusip']]
        elif site in ['nbcss', 'rbc']:
            urls = new_active_products['urls'].unique()
        
        print(urls)

        return urls

    def close_driver(self):
        self.driver.close()

if __name__ == '__main__':
    # Instantiate Driver
    driver = Driver()
    
    # Get cusip and isin for all products added to pdw in the past week
    recent_pdw_products_dict = driver.get_recent_pdw_products()
    
    # Get new BMO products
    bmo_prods = driver.get_bmo_products()
    print(driver.compare_site_to_pdw('bmo', bmo_prods, recent_pdw_products_dict))
    driver.close_driver()
    
    # # Get new NBCSS products
    # nbcss_prods = driver.get_nbcss_products()
    # print(driver.compare_site_to_pdw('nbcss', nbcss_prods, recent_pdw_products_dict))

    # # Get new rbc products
    # rbc_prods = driver.get_rbc_products()
    # print(driver.compare_site_to_pdw('rbc', rbc_prods, recent_pdw_products_dict))
    
