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

from gspread_dataframe import set_with_dataframe

def paste_dataframe_to_sheet(df, worksheet_name):
    """
    固定された Google Sheets URL に対して、指定タブへ DataFrame を貼り付ける関数

    Args:
        df (pd.DataFrame): 転記する DataFrame
        worksheet_name (str): 転記先のタブ名
    """
    # 固定の URL（必要ならここを書き換えるだけ）
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/1fy-1uq5jKGMy9S7AQwf-RyaDjWQ-K2xN0apVdSzo4Lw/edit#gid=0"

    client = get_gspread_client()
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(worksheet_name)

    worksheet.clear()
    set_with_dataframe(worksheet, df)

