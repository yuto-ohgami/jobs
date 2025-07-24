import pandas as pd

def create_df_financial_pre(df_statements: pd.DataFrame, df_fs_details: pd.DataFrame, years: int = 3) -> pd.DataFrame:
    # ① データ結合（LEFT JOIN）
    df = pd.merge(
        df_statements,
        df_fs_details,
        on="DisclosureNumber",
        how="left",
        suffixes=("", "_fs")
    )

    # ② 数値変換（演算対象のカラム）
    for col in ["NetSales", "Cost_of_sales", "Gross_profit"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ③ GrossProfit を計算（優先: Gross_profit → NetSales - Cost_of_sales）
    df["GrossProfit"] = df["Gross_profit"].combine_first(df["NetSales"] - df["Cost_of_sales"])

    # ④ COALESCE 相当の補助関数（空文字・空白 → pd.NA 扱い）
    def coalesce(df, columns):
        df_tmp = df[columns].replace(r"^\s*$", pd.NA, regex=True)
        return df_tmp.bfill(axis=1).iloc[:, 0]

    # ⑤ 各種予想値カラムの COALESCE 処理
    df["NetSales_Forecast"] = coalesce(df, [
        "ForecastNetSales", "NextYearForecastNetSales", "ForecastNonConsolidatedNetSales"
    ])
    df["OperatingProfit_Forecast"] = coalesce(df, [
        "ForecastOperatingProfit", "NextYearForecastOperatingProfit", "ForecastNonConsolidatedOperatingProfit"
    ])
    df["OrdinaryProfit_Forecast"] = coalesce(df, [
        "ForecastOrdinaryProfit", "NextYearForecastOrdinaryProfit", "ForecastNonConsolidatedOrdinaryProfit"
    ])
    df["Profit_Forecast"] = coalesce(df, [
        "ForecastProfit", "NextYearForecastProfit", "ForecastNonConsolidatedProfit"
    ])
    df["EPS_Forecast"] = coalesce(df, [
        "ForecastEarningsPerShare", "NextYearForecastEarningsPerShare", "ForecastNonConsolidatedEarningsPerShare"
    ])

    # ⑥ ドキュメント種別除外 & 年数フィルター
    df = df[
        ~df["TypeOfDocument"].isin(["EarnForecastRevision", "DividendForecastRevision"]) &
        (pd.to_datetime(df["DisclosedDate"], errors="coerce") >= pd.Timestamp.today() - pd.DateOffset(years=years))
    ]   

    # ⑦ 最新の開示のみ残す（ROW_NUMBER() OVER ... QUALIFY 1 相当）
    df["_rank"] = (
        df.sort_values("DisclosedDate", ascending=False)
          .groupby(["LocalCode", "TypeOfDocument", "CurrentPeriodEndDate"])
          .cumcount()
    )
    df = df[df["_rank"] == 0].drop(columns=["_rank"])

    # ⑧ BigQuery に合わせた型変換
    string_cols = ["DisclosureNumber", "LocalCode", "TypeOfDocument", "TypeOfCurrentPeriod"]
    date_cols = ["DisclosedDate", "CurrentPeriodEndDate"]
    float_cols = [
        "AverageNumberOfShares", "NetSales", "OperatingProfit", "OrdinaryProfit", "Profit",
        "Cost_of_sales", "GrossProfit", "Selling_general_and_administrative_expenses",
        "NetSales_Forecast", "OperatingProfit_Forecast", "OrdinaryProfit_Forecast",
        "Profit_Forecast", "EPS_Forecast"
    ]

    for col in string_cols:
        df[col] = df[col].astype(str)

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ⑨ 出力列の順序を定義
    selected_cols = string_cols + date_cols + float_cols
    df = df[selected_cols].sort_values(["LocalCode", "CurrentPeriodEndDate"])

    return df
