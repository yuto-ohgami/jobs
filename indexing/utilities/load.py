# ファイル: process_financial/utilities/load.py
from google.cloud import storage
import pandas as pd
import io
import json

def load_ndjson_from_gcs(bucket_name: str, folder_name: str, file_name: str) -> pd.DataFrame:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_path = f"{folder_name}/{file_name}"
    blob = bucket.blob(blob_path)

    ndjson_str = blob.download_as_text()
    data = [json.loads(line) for line in ndjson_str.strip().split('\n')]
    df = pd.DataFrame(data)
    return df

from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq
import io

def load_gcs_parquet_file(bucket_name: str, folder_name: str, file_name: str) -> pd.DataFrame:
    # GCS クライアント作成
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # フルパス（例: "preprocessed/financial_preprocess.parquet"）
    blob_path = f"{folder_name}/{file_name}"
    blob = bucket.blob(blob_path)

    # Parquetファイルをバイトでダウンロード
    parquet_bytes = blob.download_as_bytes()
    
    # pyarrowで読み込み → pandasに変換
    table = pq.read_table(io.BytesIO(parquet_bytes))
    df = table.to_pandas()
    
    return df
