from utilities.load import load_ndjson_from_gcs
from utilities.api_sheet_1 import paste_dataframe_to_sheet
from preprocess_folder.preprocess_financial import create_df_financial_pre
from preprocess_folder.preprocess_RevisionEarn import create_df_RevisionEarn_pre
from utilities.upload import upload_df_to_gcs  # ← 新しく追加

def main():
    bucket_name = "bucket-project-2"
    folder_jquants = "jquants"
    folder_preprocessed = "preprocessed"
    # ndjson ロード
    df_statements = load_ndjson_from_gcs(bucket_name, folder_jquants, "jquants_statements.json")
    df_fs_details = load_ndjson_from_gcs(bucket_name, folder_jquants, "jquants_fs_details.json")
    print("✅ Done load ")   
    
    sample_years = 5
    # financial
    df_financial_pre = create_df_financial_pre(df_statements, df_fs_details, years=sample_years)
    upload_df_to_gcs(df_financial_pre, bucket_name, folder_preprocessed, "financial_preprocess.parquet")
    print("✅ Done financial")
    # RevisionEarn
    df_RevisionEarn_pre = create_df_RevisionEarn_pre(df_statements, years=sample_years)
    upload_df_to_gcs(df_RevisionEarn_pre, bucket_name, folder_preprocessed, "RevisionEarn_preprocess.parquet")
    print("✅ Done RevisionEarn")
if __name__ == "__main__":
    main()

"""
gcloud builds submit ./preprocess \
  --tag=gcr.io/project-1-465310/preprocess:test_1 \
  --project=project-1-465310
"""