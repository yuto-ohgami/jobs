# api_jquants.py

import json
import tomli
import requests
from google.cloud import secretmanager

def load_secret_toml(secret_id="jquants-api-toml", project_id="project-1-465310"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_str = response.payload.data.decode("UTF-8")
    return tomli.loads(secret_str)

def build_headers_jquants():
    config = load_secret_toml()
    user_data = config["jquants-api-client"]

    api_url = "https://api.jquants.com"

    refresh_token = requests.post(
        f"{api_url}/v1/token/auth_user",
        data=json.dumps(user_data),
        headers={"Content-Type": "application/json"}
    ).json()["refreshToken"]

    id_token = requests.post(
        f"{api_url}/v1/token/auth_refresh?refreshtoken={refresh_token}"
    ).json()["idToken"]

    return {"Authorization": f"Bearer {id_token}"}
