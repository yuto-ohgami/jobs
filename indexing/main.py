from utilities.load import load_ndjson_from_gcs, load_gcs_parquet_file
from utilities.api_sheet_1 import paste_dataframe_to_sheet
from utilities.upload import upload_df_to_gcs
from functions.columns_select import columns_select
from functions.indexing import index_company

def main():
    bucket_name = "bucket-project-2"
    folder_jquants = "jquants"
    folder_processed = "processed"

    # load
    df_company = load_ndjson_from_gcs(bucket_name, folder_jquants, "jquants_info.json")
    df_financial = load_gcs_parquet_file(bucket_name, folder_processed, "financial.parquet")
    df_daily_quantes = load_ndjson_from_gcs(bucket_name, folder_jquants, "jquants_daily_quotes.json")

    # index
    df_company = index_company(df_company, df_financial, "LocalCode", "DisclosedDate")
    df_company = index_company(df_company, df_daily_quantes, "LocalCode", "DisclosedDate")

    # select, paste
    #df_company_selected = columns_select(df_company)
    paste_dataframe_to_sheet(df_company, "docs_company")
    print("")

    #df_financial = load_gcs_parquet_file(bucket_name, folder_processed, "financial.parquet")


if __name__ == "__main__":
    main()

"""
gcloud builds submit ./process_financial \
  --tag=gcr.io/project-1-465310/process_financial:test_1 \
  --project=project-1-465310
"""