import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

# 🔧 LocalCode 単位の処理関数
def AC_RevisionEarn_local(df_financial_local, df_revision_local, columns_growth_forward):
    df_financial_local = df_financial_local.copy()
    df_revision_local = df_revision_local.copy()

    # 日付変換 & ソート
    df_financial_local["DisclosedDate"] = pd.to_datetime(df_financial_local["DisclosedDate"])
    df_revision_local["DisclosedDate"] = pd.to_datetime(df_revision_local["DisclosedDate"])
    df_financial_local = df_financial_local.sort_values("DisclosedDate").reset_index(drop=True)

    # 初期化
    for col in columns_growth_forward:
        df_financial_local[f"{col}_Forecast_RevisionEarn"] = ""
    df_financial_local["DisclosedDate_RevisionEarn"] = pd.NaT

    for idx in range(len(df_financial_local) - 1):
        current_date = df_financial_local.at[idx, "DisclosedDate"]

        # 同じ LocalCode の中で次の行を取得
        next_idx = df_financial_local[df_financial_local["DisclosedDate"] > current_date].index
        if next_idx.empty:
            continue
        next_date = df_financial_local.at[next_idx[0], "DisclosedDate"]

        # 日付が範囲内の RevisionEarn を取得
        target = df_revision_local[
            (df_revision_local["DisclosedDate"] >= current_date) &
            (df_revision_local["DisclosedDate"] < next_date)
        ]

        if not target.empty:
            row_target = target.iloc[0]
            df_financial_local.at[idx, "DisclosedDate_RevisionEarn"] = row_target["DisclosedDate"]

            for col in columns_growth_forward:
                forecast_col = f"{col}_Forecast"
                if forecast_col in row_target:
                    df_financial_local.at[idx, f"{col}_Forecast_RevisionEarn"] = row_target[forecast_col]

    return df_financial_local


# 🎯 修正：各 LocalCode ごとに分割して処理対象だけ渡す

def process_one_code_revision(code, df_financial, df_revision, columns_growth_forward):
    df_local_fin = df_financial[df_financial["LocalCode"] == code].copy()
    df_local_rev = df_revision[df_revision["LocalCode"] == code].copy()
    return AC_RevisionEarn_local(df_local_fin, df_local_rev, columns_growth_forward)


def AC_RevisionEarn_multiprocess(df_financial, df_revision, columns_growth_forward, processes=None):
    codes = df_financial["LocalCode"].unique().tolist()
    total = len(codes)
    if processes is None:
        processes = max(1, cpu_count() - 1)

    # ✅ 引数に code だけ渡す形で関数を作成（dfはグローバルに）
    with Pool(processes=processes) as pool:
        func = partial(process_one_code_revision, df_financial=df_financial, df_revision=df_revision, columns_growth_forward=columns_growth_forward)

        results = []
        for i, result in enumerate(pool.imap_unordered(func, codes), 1):
            results.append(result)
            if i % 100 == 0 or i == total:
                print(f"📊 RevisionEarn {i}/{total}")

    return pd.concat(results).reset_index(drop=True)

