def index_company(df_company, df, column_code, column_latest):
    # 最新レコード抽出（column_code ごとに最新の1件を残す）
    df_latest = df.sort_values(column_latest).drop_duplicates(subset=column_code, keep="last")

    # マージ（df_companyの"Code"とdfのcolumn_codeで結合）
    df_merged = df_company.merge(
        df_latest,
        left_on="Code",
        right_on=column_code,
        how="left",
        suffixes=("", "_dup")  # 重複カラム名を自動で "_dup" にする
    )

    # 重複しているカラム（"_dup" が付いてる）を削除
    dup_cols = [col for col in df_merged.columns if col.endswith("_dup")]
    df_merged.drop(columns=dup_cols, inplace=True)

    # 結合キーの片方を削除（dfの column_code 側）
    if column_code in df_merged.columns and column_code != "Code":
        df_merged.drop(columns=[column_code], inplace=True)

    return df_merged
