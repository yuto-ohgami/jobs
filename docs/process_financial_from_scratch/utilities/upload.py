from google.cloud import storage
from io import BytesIO
import pandas as pd

def upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, folder_name: str, filename: str):
    """
    DataFrame を GCS にアップロードする（拡張子は filename 側で明示する形式）。

    Parameters:
        df (pd.DataFrame): アップロードするデータ
        bucket_name (str): GCS バケット名
        folder_name (str): GCS 内のフォルダ名（例: 'processed'）
        filename (str): アップロードするファイル名（例: 'financial_pre.parquet'）
    """
    if not filename.endswith(".parquet"):
        raise ValueError("ファイル名は '.parquet' 拡張子を含めて指定してください。")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_path = f"{folder_name}/{filename}"
    blob = bucket.blob(blob_path)

    # DataFrame をバッファにパーケット形式で書き込む
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"✅ Uploaded to gs://{bucket_name}/{blob_path}")
