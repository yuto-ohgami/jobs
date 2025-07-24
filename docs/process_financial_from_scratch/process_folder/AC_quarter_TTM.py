from multiprocessing import Pool, cpu_count
from functools import partial
import pandas as pd

def process_one_code(df, code, cols):
    df_local = df[df["LocalCode"] == code]
    df_local = AC_PL_quarter(df_local, cols)
    df_local = AC_PL_TTM(df_local, cols)
    return df_local

def AC_PL_QTTM_multiprocess(df, cols, processes=None):
    codes = df["LocalCode"].unique().tolist()
    total = len(codes)
    if processes is None:
        processes = max(1, cpu_count() - 1)

    with Pool(processes=processes) as pool:
        func = partial(process_one_code, df, cols=cols)
        results = []

        for i, result in enumerate(pool.imap_unordered(func, codes), 1):
            results.append(result)

            # ✅ 通常の1件ずつの進捗（コメントアウト可）
            # print(f"✅ Completed {i}/{total} ({(i/total)*100:.2f}%)")

            # ✅ 100件ごとに進捗をまとめて表示
            if i % 100 == 0 or i == total:
                print(f"QTTM {i}/{total}")

    return pd.concat(results).reset_index(drop=True)

import pandas as pd
import numpy as np
import asyncio

# LocalCode ごとに処理するよう関数変更
def AC_PL_quarter(df_local, cols):
    df_local = df_local.copy()
    df_local["CurrentPeriodEndDate"] = pd.to_datetime(df_local["CurrentPeriodEndDate"])
    prev_map = {'2Q': '1Q', '3Q': '2Q', 'FY': '3Q'}

    def qval(row, col):
        doc = row["TypeOfCurrentPeriod"]
        if doc == "1Q":
            return row[col]
        pdoc = prev_map.get(doc)
        if not pdoc:
            return ""
        past = df_local[
            (df_local["TypeOfCurrentPeriod"] == pdoc) &
            (df_local["CurrentPeriodEndDate"] < row["CurrentPeriodEndDate"])
        ]
        if past.empty:
            return ""
        prev_val = past.sort_values("CurrentPeriodEndDate", ascending=False).iloc[0][col]
        return "" if pd.isna(row[col]) or pd.isna(prev_val) else row[col] - prev_val

    for col in cols:
        df_local[f"{col}_quarter"] = [qval(row, col) for _, row in df_local.iterrows()]
    return df_local


def AC_PL_TTM(df_local, cols):
    df_local = df_local.copy()
    df_local["CurrentPeriodEndDate"] = pd.to_datetime(df_local["CurrentPeriodEndDate"])
    df_local = df_local.sort_values("CurrentPeriodEndDate").reset_index(drop=True)

    ttm_map = {
        "1Q": ["2Q", "3Q", "FY"],
        "2Q": ["3Q", "FY", "1Q"],
        "3Q": ["FY", "1Q", "2Q"],
        "FY": ["1Q", "2Q", "3Q"]
    }

    for col in cols:
        col_quarter = f"{col}_quarter"
        col_ttm = f"{col}_TTM"
        df_local[col_quarter] = pd.to_numeric(df_local[col_quarter], errors="coerce")
        df_local[col_ttm] = np.nan

        for idx, row in df_local.iterrows():
            period = row["TypeOfCurrentPeriod"]
            base_date = row["CurrentPeriodEndDate"]

            target_periods = ttm_map.get(period, []) + [period]
            values = []

            for p in target_periods:
                candidates = df_local[
                    (df_local["TypeOfCurrentPeriod"] == p) &
                    (df_local["CurrentPeriodEndDate"] < base_date)
                ].sort_values("CurrentPeriodEndDate", ascending=False)
                if not candidates.empty:
                    value = candidates.iloc[0][col_quarter]
                    values.append(float(value) if pd.notna(value) else 0)

            if len(values) == 4:
                df_local.at[idx, col_ttm] = sum(values)

    return df_local
