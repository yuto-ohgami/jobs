import pandas as pd

def create_df_RevisionEarn_pre(df_statements: pd.DataFrame, years: int = 3) -> pd.DataFrame:
    # ① データの絞り込み
    df = df_statements.copy()
    df = df[
        (df["TypeOfDocument"] == "EarnForecastRevision") &
        (df["TypeOfCurrentPeriod"] == "FY") &
        (pd.to_datetime(df["DisclosedDate"], errors="coerce") >= pd.Timestamp.today() - pd.DateOffset(years=years))
    ]

    # ② COALESCE相当の処理
    def coalesce(df, columns):
        df_tmp = df[columns].replace(r"^\s*$", pd.NA, regex=True)
        return df_tmp.bfill(axis=1).iloc[:, 0]

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

    # ③ 必要なカラムのみ抽出
    selected_cols = [
        "DisclosureNumber", "DisclosedDate", "LocalCode", "CurrentPeriodEndDate",
        "TypeOfCurrentPeriod", "TypeOfDocument",
        "NetSales_Forecast", "OperatingProfit_Forecast", "OrdinaryProfit_Forecast",
        "Profit_Forecast", "EPS_Forecast"
    ]

    df = df[selected_cols].sort_values(["LocalCode", "CurrentPeriodEndDate"])

    return df
