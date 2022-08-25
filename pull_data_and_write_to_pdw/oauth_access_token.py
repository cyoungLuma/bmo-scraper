import requests
import json
import sys


def get_new_token(client_id, client_secret):
    """get new Oauth token, currently using PI's secrets"""
    auth_server_url = 'https://buat.lumafintech.com/api/auth-service/oauth/token'

    token_req_payload = {'grant_type': 'client_credentials', 'scope': 'serverclient'}


    token_response = requests.post(auth_server_url,
                                   data=token_req_payload, verify=False, allow_redirects=False,
                                   auth=(client_id, client_secret))

    if token_response.status_code != 200:
        print("Failed to obtain token from the OAuth 2.0 server", file=sys.stderr)
        sys.exit(1)

    print("Successfully obtained a new token")
    tokens = json.loads(token_response.text)
    print(tokens['access_token'])
    return tokens['access_token']

