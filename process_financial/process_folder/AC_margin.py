import pandas as pd

def AC_margin_NetSales(df, columns_margin_sales, columns_types_PL):
    df = df.copy()

    for col in columns_margin_sales:
        for typ in columns_types_PL:
            col_numerator = f"{col}{typ}"
            col_denominator = f"NetSales{typ}"
            col_margin = f"margin_{col}{typ}"

            # 数値に変換（エラーは NaN に）
            df[col_numerator] = pd.to_numeric(df[col_numerator], errors="coerce")
            df[col_denominator] = pd.to_numeric(df[col_denominator], errors="coerce")

            # マージン計算（ゼロ除算回避）
            df[col_margin] = df[col_numerator] / df[col_denominator]
            df[col_margin] = df[col_margin].where(df[col_denominator] != 0)

    return df

def AC_margin_OperatingProfit(df, columns_margin_OperatingProfit, columns_types_PL):
    df = df.copy()

    for col in columns_margin_OperatingProfit:
        for suffix in columns_types_PL:
            col_numerator = f"{col}{suffix}"
            col_denominator = f"OperatingProfit{suffix}"
            col_margin = f"margin_{col}{suffix}"

            # 数値に変換（エラーは NaN に）
            df[col_numerator] = pd.to_numeric(df[col_numerator], errors="coerce")
            df[col_denominator] = pd.to_numeric(df[col_denominator], errors="coerce")

            # マージン計算（ゼロ除算回避）
            df[col_margin] = df[col_numerator] / df[col_denominator]
            df[col_margin] = df[col_margin].where(df[col_denominator] != 0)

    return df
