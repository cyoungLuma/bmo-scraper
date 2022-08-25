import requests
import json



result = {'errors':[],'success':[]}


def call_luma_product_api(product, new_access_token):
    """Post new product to pdw api and save the response in xls files."""
    print("Inside call_luma_product_api")
    url = "https://scg.buat.lumafintech.com/api/pdw-service/v2/products/"

    payload = json.dumps(product)
    headers = {
        'Authorization': 'Bearer ' + new_access_token,
        'Content-Type': 'application/json',}

    response = requests.post(url, headers=headers, data=payload)
    
    productIsin = product['productGeneral']['isin']


    if response.status_code == 200:
        print(f'Product {productIsin} posted with {response.content}')
        return True
    else:
        print(f'Product {productIsin} failed with error message: {response.content}')
        return False


