# storage
from google.cloud import storage
def bucket(bucket_name="bucket-project-2"):
    client = storage.Client()
    return client.bucket(bucket_name)

import re
import json

# NDJSON形式でGCSにアップロード
def upload_ndjson_to_gcs(data_list, file_name, folder_name="jquants"):
    sanitized_data = [sanitize_keys(record) for record in data_list]
    ndjson_str = "\n".join(json.dumps(record, ensure_ascii=False) for record in sanitized_data)

    blob_path = f"{folder_name}/{file_name}"
    blob = bucket().blob(blob_path)
    blob.upload_from_string(ndjson_str, content_type="application/json")


# BigQuery カラム名向けサニタイズ関数
def sanitize_keys(record):
    new_record = {}
    for key, value in record.items():
        # 不正な文字をアンダースコアに変換
        clean_key = re.sub(r'[^a-zA-Z0-9_]', '_', key)
        if re.match(r'^[0-9]', clean_key):
            clean_key = '_' + clean_key
        clean_key = re.sub(r'_+', '_', clean_key)
        new_record[clean_key] = value
    return new_record


# sheet 接続
from utilities.api_sheet_1 import get_gspread_client

# 最初に一度だけ keys_to_keep を取得してグローバルに保持
def load_keys_to_keep_from_sheet():
    sheet_1 = get_gspread_client().open_by_url(
        "https://docs.google.com/spreadsheets/d/1fy-1uq5jKGMy9S7AQwf-RyaDjWQ-K2xN0apVdSzo4Lw/edit#gid=0"
    )
    worksheet = sheet_1.worksheet("docs_fs_keyskeep")
    rows = worksheet.get_all_values()
    keys_to_keep = {}
    for new_key, possible_key in rows[1:]:
        if new_key not in keys_to_keep:
            keys_to_keep[new_key] = []
        if possible_key.strip():
            keys_to_keep[new_key].append(possible_key.strip())
    return keys_to_keep

# 取得した辞書を使って key_filter
def key_filter(record, keys_to_keep):
    filtered = {}
    for new_key, possible_keys in keys_to_keep.items():
        for k in possible_keys:
            if k in record:
                filtered[new_key] = record[k]
                break
    return filtered



def upload_ndjson_to_gcs_fs_details(data_list, file_name, folder_name="jquants"):
    sanitized_data = [sanitize_keys(record) for record in data_list]
    keys_to_keep = load_keys_to_keep_from_sheet()
    filtered_data = [key_filter(record, keys_to_keep) for record in sanitized_data]
    ndjson_str = "\n".join(json.dumps(record, ensure_ascii=False) for record in filtered_data)

    blob_path = f"{folder_name}/{file_name}"
    blob = bucket().blob(blob_path)
    blob.upload_from_string(ndjson_str, content_type="application/json")
    print(f"--done fs_details")


def upload_ndjson_to_gcs_sb(data_list, file_name, folder_name="jquants"):
    sanitized_data = [sanitize_keys(record) for record in data_list]
    ndjson_str = "\n".join(json.dumps(record, ensure_ascii=False) for record in sanitized_data)

    if "." in file_name:
        name_parts = file_name.rsplit(".", 1)
        file_name_sb = f"{name_parts[0]}_sb.{name_parts[1]}"
    else:
        file_name_sb = f"{file_name}_sb"

    blob_path = f"{folder_name}/{file_name_sb}"
    blob = bucket().blob(blob_path)
    blob.upload_from_string(ndjson_str, content_type="application/json")
    print(f"--done fs_details_sb")

