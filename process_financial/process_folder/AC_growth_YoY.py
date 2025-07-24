import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from functools import partial

# 🔁 LocalCode 単位で成長率（YoY）を計算
def AC_growth_YoY_local(df_local, columns_growth, columns_types_PL):
    df_local = df_local.copy()
    df_local["CurrentPeriodEndDate"] = pd.to_datetime(df_local["CurrentPeriodEndDate"])
    df_local = df_local.sort_values(["TypeOfCurrentPeriod", "CurrentPeriodEndDate"]).reset_index(drop=True)

    for col in columns_growth:
        for suffix in columns_types_PL:
            target_col = f"{col}{suffix}"
            growth_col = f"growth_YoY_{col}{suffix}"

            df_local[growth_col] = np.nan

            for idx, row in df_local.iterrows():
                period = row["TypeOfCurrentPeriod"]
                base_date = row["CurrentPeriodEndDate"]
                current_value = row[target_col]

                # 同一 TypeOfCurrentPeriod、日付の過去データ
                mask = (
                    (df_local["TypeOfCurrentPeriod"] == period) &
                    (df_local["CurrentPeriodEndDate"] < base_date)
                )
                past_df = df_local[mask].sort_values("CurrentPeriodEndDate", ascending=False)

                if not past_df.empty:
                    past_value = past_df.iloc[0][target_col]
                    if pd.notna(current_value) and pd.notna(past_value) and past_value != 0:
                        df_local.at[idx, growth_col] = (current_value - past_value) / past_value

    return df_local

# 🔁 LocalCode 単位の処理関数
def process_growth_one_code(df, code, columns_growth, columns_types_PL):
    df_local = df[df["LocalCode"] == code]
    return AC_growth_YoY_local(df_local, columns_growth, columns_types_PL)

# 🎯 multiprocess実行関数（100件ごとに進捗表示）
def AC_growth_YoY_multiprocess(df, columns_growth, columns_types_PL, processes=None):
    codes = df["LocalCode"].unique().tolist()
    total = len(codes)
    if processes is None:
        processes = max(1, cpu_count() - 1)

    with Pool(processes=processes) as pool:
        func = partial(process_growth_one_code, df, columns_growth=columns_growth, columns_types_PL=columns_types_PL)
        results = []
        for i, result in enumerate(pool.imap_unordered(func, codes), 1):
            results.append(result)
            if i % 100 == 0 or i == total:
                print(f"QTTM (YoY) {i}/{total}")

    return pd.concat(results).reset_index(drop=True)
