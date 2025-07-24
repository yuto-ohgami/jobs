URL_jquants_v1 = "https://api.jquants.com/v1"

# api_jquants
import requests
import json
from utilities.api_jquants import build_headers_jquants
headers_jquants = build_headers_jquants()

# preprocess_bigquery
from utilities.bq_preprocess import upload_ndjson_to_gcs

# slice
import requests
import json
def update_jquants(URL, file_name):
    response = requests.get(URL, headers=headers_jquants)
    records = list(response.json().values())[0]
    upload_ndjson_to_gcs(records, file_name)

def apply_dict_update_jquants_slice(path_dict):
    for path, key in path_dict.items():
        url = f"{URL_jquants_v1}/{path}"
        file_name = f"jquants_{key}.json"
        update_jquants(url, file_name)
        print(f"done {key}")

# codes
import asyncio
import aiohttp

async def request_jquants(session, url, code):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return code, await response.json()
            else:
                return code, None
    except Exception as e:
        print(f"[{code}] Error: {e}")
        return code, None

from datetime import datetime, timedelta

async def request_jquants_async(codes, URL, headers):
    dividend_dict = {}

    # 日付指定
    today = datetime.today().date()
    three_years_ago = today - timedelta(days=3*365)
    from_str = three_years_ago.isoformat()
    to_str = today.isoformat()

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            request_jquants(session, f"{URL}?code={code}&from={from_str}&to={to_str}", code)
            for code in codes
        ]
        results = await asyncio.gather(*tasks)
        for code, data in results:
            dividend_dict[code] = data
    return dividend_dict


async def update_jquants_codes(codes, URL, file_name):
    flat_list = []
    for i in range(0, len(codes), 1000):
        chunk = codes[i:i + 1000]
        chunk_data = await request_jquants_async(chunk, URL, headers_jquants)

        for sub_dict in chunk_data.values():
            for key, data in sub_dict.items():
                for item in data if isinstance(data, list) else []:
                    if isinstance(item, dict):
                        flat_list.append(item)

    upload_ndjson_to_gcs(flat_list, file_name)


def apply_dict_update_jquants_codes_sync(codes, path_dict):
    for path, key in path_dict.items():
        url = f"{URL_jquants_v1}/{path}"
        file_name = f"jquants_{key}.json"

        # 既存のイベントループで非同期関数を実行
        asyncio.get_event_loop().run_until_complete(
            update_jquants_codes(codes, url, file_name)
        )
        print(f"done {key}")


from utilities.bq_preprocess import upload_ndjson_to_gcs_fs_details, upload_ndjson_to_gcs_sb

# codes_fs_details
async def update_jquants_codes_fs_details(codes, URL, file_name):
    flat_list = []
    for i in range(0, len(codes), 1000):
        chunk = codes[i:i + 1000]
        chunk_data = await request_jquants_async(chunk, URL, headers_jquants)

        for sub_dict in chunk_data.values():
            for key, data in sub_dict.items():
                for item in data if isinstance(data, list) else []:
                    if isinstance(item, dict):
                        # 特別処理: FinancialStatement を展開
                        fs_data = item.pop("FinancialStatement", None)
                        if isinstance(fs_data, dict):
                            # 元の item に上書きマージ
                            item.update(fs_data)
                        flat_list.append(item)
    print(f"--downloaded fs_details")
    upload_ndjson_to_gcs_fs_details(flat_list, file_name)
    upload_ndjson_to_gcs_sb(flat_list, file_name)


def apply_dict_update_jquants_codes_sync_fs_details(codes, path_dict):
    for path, key in path_dict.items():
        url = f"{URL_jquants_v1}/{path}"
        file_name = f"jquants_{key}.json"

        # 既存のイベントループで非同期関数を実行
        asyncio.get_event_loop().run_until_complete(
            update_jquants_codes_fs_details(codes, url, file_name)
        )
        print(f"done {key}")
