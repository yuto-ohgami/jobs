import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

# 🔧 LocalCode 単位で処理する関数
def AC_growth_forward_RevisionEarn_local(df_local, columns_growth_forward):
    df_local = df_local.copy()

    for col in columns_growth_forward:
        col_base = f"{col}_Forecast"
        col_revision = f"{col}_Forecast_RevisionEarn"
        col_growth = f"growth_forward_RevisionEarn_{col}"

        df_local[col_base] = pd.to_numeric(df_local[col_base], errors="coerce")
        df_local[col_revision] = pd.to_numeric(df_local[col_revision], errors="coerce")
        df_local[col_growth] = np.nan

        mask = df_local[col_base].notna() & (df_local[col_base] != 0) & df_local[col_revision].notna()
        df_local.loc[mask, col_growth] = (
            (df_local.loc[mask, col_revision] - df_local.loc[mask, col_base]) / df_local.loc[mask, col_base]
        )

    return df_local

# 🔧 マルチプロセス用の1コード処理関数
def process_one_code_forward_revision(df, code, columns):
    df_local = df[df["LocalCode"] == code]
    return AC_growth_forward_RevisionEarn_local(df_local, columns)

# 🔁 マルチプロセス本体
def AC_growth_forward_RevisionEarn_multiprocess(df, columns_growth_forward, processes=None):
    codes = df["LocalCode"].unique().tolist()
    total = len(codes)
    if processes is None:
        processes = max(1, cpu_count() - 1)

    func = partial(process_one_code_forward_revision, df, columns=columns_growth_forward)

    with Pool(processes=processes) as pool:
        results = []
        for i, result in enumerate(pool.imap_unordered(func, codes), 1):
            results.append(result)
            if i % 100 == 0 or i == total:
                print(f"📊 RevisionEarn {i}/{total}")

    return pd.concat(results).reset_index(drop=True)

