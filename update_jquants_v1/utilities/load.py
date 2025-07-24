import pandas as pd
import json

# storage
from google.cloud import storage
bucket = storage.Client().bucket("bucket-project-2")

def load_json_flatten_level1(file_name):
    blob = bucket.blob(file_name)  # ← () は不要
    downloaded_data = blob.download_as_string().decode("utf-8")

    lines = downloaded_data.strip().split("\n")
    data = [json.loads(line) for line in lines if line.strip()]

    return pd.DataFrame(data)


import json
import pandas as pd

def load_json_flatten_level2(file_name, key):
    # GCSからJSON読み込み
    blob = bucket.blob(file_name)
    downloaded_data = blob.download_as_string()
    raw_data = json.loads(downloaded_data.decode("utf-8"))

    # 第二階層のkey（例："dividend"）のリストをDataFrameに変換
    flattened_data = {}
    for code, nested in raw_data.items():
        if nested and key in nested and isinstance(nested[key], list):
            flattened_data[code] = pd.DataFrame(nested[key])
        else:
            flattened_data[code] = pd.DataFrame()  # 空データ扱い

    return flattened_data

# sheet 接続
from utilities.api_sheet_1 import get_gspread_client
sheet_1 = get_gspread_client().open_by_url("https://docs.google.com/spreadsheets/d/1fy-1uq5jKGMy9S7AQwf-RyaDjWQ-K2xN0apVdSzo4Lw/edit?gid=0#gid=0")

def load_sheet_API(tab_name, filter_value):
    worksheet = sheet_1.worksheet(tab_name)
    data = worksheet.get_all_values()

    # ヘッダーとデータを分離
    header = data[0]
    records = data[1:]

    dict_list = [dict(zip(header, row)) for row in records]
    filtered = [row for row in dict_list if row.get("tag_1") == filter_value]
    result_dict = {row["path"]: row["key"] for row in filtered}

    return result_dict
