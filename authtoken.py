import os
from dotenv import load_dotenv
load_dotenv()

print(f"SATX_USERNAME: {os.getenv('SATX_USERNAME')}")
print(f"SATX_PASSWORD: {os.getenv('SATX_PASSWORD')}")

import requests

def obter_token():
    auth_url = "https://integration.systemsatx.com.br/Login"
    params = {
        "Username": os.getenv("SATX_USERNAME"),
        "Password": os.getenv("SATX_PASSWORD")
    }
    auth_response = requests.post(auth_url, params=params)
    if auth_response.status_code == 200:
        auth_data = auth_response.json()
        token = auth_data.get("AccessToken")
        if token:
            print("Token obtido com sucesso!")
            return token
        else:
            print("Token não encontrado na resposta.")
            return None
    else:
        print("Erro na autenticação:", auth_response.status_code, auth_response.text)
        return None

if __name__ == '__main__':
    obter_token()