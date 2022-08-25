import requests
import json




def call_luma_product_api(products, new_access_token):
    """Post new product to pdw api, out of scope for PI's client credentials."""
    print("Inside call_luma_product_api")
    url = "https://scg.buat.lumafintech.com/api/pdw-service/v2/products/"

    payload = json.dumps(products)
    headers = {
        'Authorization': 'Bearer ' + new_access_token,
        'Content-Type': 'application/json',}

    response = requests.post(url, headers=headers, data=payload)

    print(response.content)

