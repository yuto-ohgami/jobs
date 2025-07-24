import pandas as pd
import numpy as np

def AC_growth_model(df):
    df = df.copy()

    # 重み辞書の定義
    weight_dict = {
        "FY": 1.00,
        "1Q": 0.75,
        "2Q": 0.50,
        "3Q": 0.25
    }

    # 数値変換（NaN 処理含む）
    df["growth_forward_NetSales"] = pd.to_numeric(df["growth_forward_NetSales"], errors="coerce")
    df["growth_forward_RevisionEarn_NetSales"] = pd.to_numeric(df["growth_forward_RevisionEarn_NetSales"], errors="coerce")
    df["growth_YoY_NetSales_TTM"] = pd.to_numeric(df["growth_YoY_NetSales_TTM"], errors="coerce")

    # 初期化
    df["growth_forward_model_NetSales"] = np.nan

    # 各行に適用
    for idx, row in df.iterrows():
        period = row["TypeOfCurrentPeriod"]
        w = weight_dict.get(period)
        if w is not None:
            # 優先順位のある gf 値を決定
            gf = row["growth_forward_RevisionEarn_NetSales"] if pd.notna(row["growth_forward_RevisionEarn_NetSales"]) else row["growth_forward_NetSales"]
            gy = row["growth_YoY_NetSales_TTM"]
            if pd.notna(gf) and pd.notna(gy):
                df.at[idx, "growth_forward_model_NetSales"] = gf * w + gy * (1 - w)

    return df
