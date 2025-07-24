import json
import gspread
from google.cloud import secretmanager
from oauth2client.service_account import ServiceAccountCredentials

def get_gspread_client():
    # Secret Manager からシークレットを取得
    secret_client = secretmanager.SecretManagerServiceClient()
    name = "projects/562344549270/secrets/api_sheet_1/versions/latest"
    response = secret_client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")

    # JSON文字列 → dict に変換
    service_account_info = json.loads(secret_payload)

    # 認証スコープを定義
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    # Credentials 生成
    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)

    # gspread クライアント生成
    client = gspread.authorize(creds)
    return client
