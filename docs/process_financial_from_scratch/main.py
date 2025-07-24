from utilities.load import load_ndjson_from_gcs, load_gcs_parquet_file
from utilities.api_sheet_1 import paste_dataframe_to_sheet
from utilities.upload import upload_df_to_gcs
from process_folder.AC_quarter_TTM import AC_PL_QTTM_multiprocess
from process_folder.AC_margin import AC_margin_NetSales, AC_margin_OperatingProfit
from process_folder.AC_growth_YoY import AC_growth_YoY_multiprocess
from process_folder.AC_growth_forward import AC_growth_forward_multiprocess
from process_folder.AC_RevisionEarn import AC_RevisionEarn_multiprocess
from process_folder.AC_growth_forward_RevisionEarn import AC_growth_forward_RevisionEarn_multiprocess
from process_folder.AC_growth_model import AC_growth_model

def main():
    bucket_name = "bucket-project-2"
    folder_preprocessed = "preprocessed"
    
    # ndjson ロード
    df_financial_pre = load_gcs_parquet_file(bucket_name, folder_preprocessed, "financial_preprocess.parquet")
    df_RevisionEarn = load_gcs_parquet_file(bucket_name, folder_preprocessed, "RevisionEarn_preprocess.parquet")
    
    # _quarter, TTM作成
    columns_PL_cumulated = ["NetSales", "OperatingProfit", "OrdinaryProfit", "Profit","GrossProfit", "Selling_general_and_administrative_expenses"]
    df_financial = AC_PL_QTTM_multiprocess(df_financial, columns_PL_cumulated, processes=8)
    print("✅ done quarter,TTM")

    # _margin
    columns_types_PL = {"", "_quarter", "_TTM"}
    columns_margin_NetSales = {"GrossProfit", "OperatingProfit"}
    columns_margin_OperatingProfit = {"OrdinaryProfit", "Profit"}
    df_financial = AC_margin_NetSales(df_financial, columns_margin_NetSales, columns_types_PL)
    df_financial = AC_margin_OperatingProfit(df_financial, columns_margin_OperatingProfit, columns_types_PL)
    print("✅ done margin")
    
    # AC_growth_YoY
    columns_growth = {"NetSales", "GrossProfit", "Selling_general_and_administrative_expenses"}
    df_financial = AC_growth_YoY_multiprocess(df_financial, columns_growth, columns_types_PL)
    print("✅ done growth_YoY")

    # AC_growth_foward
    columns_growth_forward = ["NetSales", "OperatingProfit", "OrdinaryProfit", "Profit"]
    df_financial = AC_growth_forward_multiprocess(df_financial, columns_growth_forward)
    df_financial = AC_RevisionEarn_multiprocess(df_financial, df_RevisionEarn, columns_growth_forward)
    df_financial = AC_growth_forward_RevisionEarn_multiprocess(df_financial, columns_growth_forward)
    df_financial = AC_growth_model(df_financial)
    print("✅ done growth_foward")
    
    folder_processed = "processed"
    upload_df_to_gcs(df_financial, bucket_name, folder_processed, "financial.parquet")
    print("✅ done upload to GCS")
    
    """
    df_financial_test = df_financial[["LocalCode", "NetSales_Forecast","growth_forward_model_NetSales"]]
    paste_dataframe_to_sheet(df_financial_test, "test")
    print("✅ done upload to sheet")
    """
if __name__ == "__main__":
    main()

"""
gcloud builds submit ./process_financial \
  --tag=gcr.io/project-1-465310/process_financial:test_1 \
  --project=project-1-465310
"""