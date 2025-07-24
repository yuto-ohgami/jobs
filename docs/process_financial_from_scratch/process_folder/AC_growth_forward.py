# process_folder/AC_growth_forward.py

import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

# 🔧 個別 LocalCode ごとの処理
def AC_growth_forward_local(df_local, columns_growth_forward):
    df_local = df_local.copy()
    df_local["CurrentPeriodEndDate"] = pd.to_datetime(df_local["CurrentPeriodEndDate"])

    for col in columns_growth_forward:
        col_forecast = f"{col}_Forecast"
        col_growth = f"growth_forward_{col}"

        df_local[col] = pd.to_numeric(df_local[col], errors="coerce")
        df_local[col_forecast] = pd.to_numeric(df_local[col_forecast], errors="coerce")
        df_local[col_growth] = np.nan

        for idx, row in df_local.iterrows():
            base_date = row["CurrentPeriodEndDate"]
            forecast_val = row[col_forecast]

            df_past = df_local[
                (df_local["TypeOfCurrentPeriod"] == "FY") &
                (df_local["CurrentPeriodEndDate"] <= base_date)
            ].sort_values("CurrentPeriodEndDate", ascending=False)

            if not df_past.empty:
                value_past = df_past.iloc[0][col]
                if pd.notna(value_past) and value_past != 0 and pd.notna(forecast_val):
                    df_local.at[idx, col_growth] = (forecast_val - value_past) / value_past

    return df_local

# 🔧 top-level function (multiprocessing が受け取れるように)
def process_one_code_growth_forward(df, code, columns):
    df_local = df[df["LocalCode"] == code]
    return AC_growth_forward_local(df_local, columns)

# 🔁 マルチプロセス処理本体
def AC_growth_forward_multiprocess(df, columns_growth_forward, processes=None):
    codes = df["LocalCode"].unique().tolist()
    total = len(codes)
    if processes is None:
        processes = max(1, cpu_count() - 1)

    func = partial(process_one_code_growth_forward, df, columns=columns_growth_forward)

    with Pool(processes=processes) as pool:
        results = []
        for i, result in enumerate(pool.imap_unordered(func, codes), 1):
            results.append(result)
            if i % 100 == 0 or i == total:
                print(f"📈 Forward {i}/{total}")

    return pd.concat(results).reset_index(drop=True)
