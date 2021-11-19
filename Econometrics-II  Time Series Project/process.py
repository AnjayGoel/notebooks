# %%
import datetime
import math
import os
import traceback
from datetime import datetime, date, timedelta
import calendar

import numpy as np
import pandas as pd
from tabulate import tabulate
from py_vollib.black_scholes.implied_volatility import *


# %%
def filter_outliers(arr, z_threshold=1):
    if len(arr) == 1:
        return arr
    return np.array([x for x in arr if abs(x - np.mean(arr)) / np.std(arr) < z_threshold])


def str_to_date(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d").date()


def str_to_date_2(date_string):
    return datetime.strptime(date_string, "%d-%b-%Y").date()


def get_last_expiry_date(df: pd.DataFrame):
    sorted_dates = sorted(df["Expiry"].unique())
    print(sorted_dates)
    last_date = sorted_dates[-1]
    if (last_date.year == 2014 and last_date.month == 2) or (
            last_date.year == 2018 and last_date.month == 3):  # Holidays
        return sorted_dates[-2]
    else:
        return sorted(df["Expiry"].unique())[-1]


def get_days_in_month(dt: date):
    return calendar.monthrange(dt.year, dt.month)[1]


def get_prev_month_monday(dt: date):
    if dt.month == 1:
        days_last_month = get_days_in_month(date(year=dt.year - 1, month=12, day=1))
        last_date = date(year=dt.year - 1, month=12, day=days_last_month)
    else:
        days_last_month = get_days_in_month(date(year=dt.year, month=dt.month - 1, day=1))
        last_date = date(year=dt.year, month=dt.month - 1, day=days_last_month)
    if last_date.weekday() < 3:
        return last_date - timedelta(days=last_date.weekday())
    else:
        return last_date + timedelta(days=(7 - last_date.weekday()))


df_nifty = pd.read_csv("csv/nifty.csv")
df_nifty["Date"] = df_nifty["Date"].apply(str_to_date)
df_nifty["Close_Shift"] = df_nifty["Close"].shift(1)
df_nifty["Close_Shift"] = df_nifty["Close"].shift(1)
df_nifty["return"] = np.log(df_nifty["Close"] / df_nifty["Close_Shift"])
df_mibor = pd.read_csv("csv/mibor.csv")
df_mibor["Date"] = df_mibor["Date"].apply(str_to_date_2)


# print(tabulate(df_nifty.tail(50), headers=df_nifty.columns))


def get_mibor_mean(dt: date):
    df_t = df_mibor[df_mibor["Date"].map(lambda dt_curr: dt_curr.year == dt.year and dt_curr.month == dt.month)]
    return round(df_t["3_Month"].mean(), 2)


def get_nifty_std(dt_start: date, dt_end: date):  # Annualized
    df_t = df_nifty[(df_nifty["Date"] >= dt_start) & (df_nifty["Date"] <= dt_end)]
    return df_t["return"].std() * math.sqrt(252)


def get_nifty(dt: date):
    return df_nifty[df_nifty["Date"] == dt].iloc[0]["Close"]


def get_implied_volatility(row):
    try:
        V = row["Close"]
        F = row["Underlying"]
        K = row["Strike Price"]
        r = row["Interest_Rate_(Mibor)"] / 100
        t = np.busday_count(row["Date"], row["Expiry"]) / 252
        flag = "c"
        return implied_volatility(V, F, K, t, r, flag)
    except Exception as e:
        # print((V, F, K, t, r, flag))
        return -1


# %%
def process_row(year, month):
    df = pd.read_csv(f"csv/nifty/{year}-{month}.csv")
    df["Expiry"] = df["Expiry"].apply(str_to_date)
    df["Date"] = df["Date"].apply(str_to_date)
    # print(tabulate(df.sample(10), headers=df.columns))
    latest_expiry = get_last_expiry_date(df)
    df = df[df["Expiry"] == latest_expiry]
    prev_month_date = get_prev_month_monday(latest_expiry)
    print(prev_month_date)
    last_month_date = sorted(df[(df["Date"] >= prev_month_date)]["Date"].unique())[0]
    print(last_month_date)
    underlying_price = get_nifty(last_month_date)
    df = df[(df["Date"] == last_month_date) & (underlying_price >= df["Strike Price"])]
    df = df.sort_values(by=['Turnover'])
    ret_val = df.iloc[-1]
    ret_val["Underlying"] = underlying_price
    ret_val["Interest_Rate_(Mibor)"] = get_mibor_mean(ret_val["Date"])
    ret_val["Realized_Volatility"] = get_nifty_std(last_month_date, latest_expiry)
    ret_val["Implied_Volatility"] = get_implied_volatility(ret_val)

    return ret_val
    # print(tabulate(df, headers=df.columns))
    # print(underlying_price)
    # print(final_date)


def process_row_2(year, month):
    df = pd.read_csv(f"csv/nifty/{year}-{month}.csv")
    df["Expiry"] = df["Expiry"].apply(str_to_date)
    df["Date"] = df["Date"].apply(str_to_date)
    # print(tabulate(df.sample(10), headers=df.columns))
    latest_expiry = get_last_expiry_date(df)
    df = df[df["Expiry"] == latest_expiry]
    # df = df[df["Close"] > abs(df["Underlying"] - df["Strike Price"])]
    prev_month_date = get_prev_month_monday(latest_expiry)
    last_month_date = sorted(df[(df["Date"] >= prev_month_date)]["Date"].unique())[0]
    underlying_price = get_nifty(last_month_date)
    df["Underlying"] = underlying_price
    df["Interest_Rate_(Mibor)"] = get_mibor_mean(latest_expiry)
    df["Implied_Volatility"] = df.apply(lambda row: get_implied_volatility(row), axis=1)

    df_itm = df[
        (df["Date"] == last_month_date) &
        (df["Strike Price"] <= underlying_price) &
        (df["Strike Price"] >= 0.8 * underlying_price) &
        (df["Implied_Volatility"] > 0)
        ].sort_values(by=['Turnover'], ascending=False).head(5)

    df_otm = df[
        (df["Date"] == last_month_date) &
        (df["Strike Price"] >= 1.03 * underlying_price) &
        (df["Strike Price"] <= 1.2 * underlying_price) &
        (df["Implied_Volatility"] > 0)
        ].sort_values(by=['Turnover'], ascending=False).head(5)
    # df = df.sort_values(by=['Turnover'], ascending=False).head(5)
    df["Realized_Volatility"] = get_nifty_std(last_month_date, latest_expiry)
    return {
        "Date": last_month_date,
        "Expiry": latest_expiry,
        "Underlying": underlying_price,
        "Realized_Volatility": get_nifty_std(last_month_date, latest_expiry),
        "Implied_Volatility_itm": df_itm["Implied_Volatility"].tolist(),
        "Implied_Volatility_itm_mean": np.sqrt(np.mean(filter_outliers(df_itm["Implied_Volatility"].to_numpy()) ** 2)),
        "Implied_Volatility_otm": df_otm["Implied_Volatility"].tolist(),
        "Implied_Volatility_otm_mean": np.sqrt(np.mean(filter_outliers(df_otm["Implied_Volatility"].to_numpy()) ** 2)),
        "Strike Prices_itm": df_itm["Strike Price"].tolist(),
        "Strike Prices_otm": df_otm["Strike Price"].tolist()
    }
    # print(tabulate(df, headers=df.columns))
    # print(underlying_price)
    # print(final_date)


def process_row_3(year, month):
    df = pd.read_csv(f"csv/nifty/{year}-{month}.csv")
    df["Expiry"] = df["Expiry"].apply(str_to_date)
    df["Date"] = df["Date"].apply(str_to_date)
    # print(tabulate(df.sample(10), headers=df.columns))
    latest_expiry = get_last_expiry_date(df)
    df = df[df["Expiry"] == latest_expiry]
    # df = df[df["Close"] > abs(df["Underlying"] - df["Strike Price"])]
    prev_month_date = get_prev_month_monday(latest_expiry)
    last_month_date = sorted(df[(df["Date"] >= prev_month_date)]["Date"].unique())[0]
    underlying_price = get_nifty(last_month_date)
    df["Underlying"] = underlying_price
    df["Interest_Rate_(Mibor)"] = get_mibor_mean(latest_expiry)
    df["Implied_Volatility"] = df.apply(lambda row: get_implied_volatility(row), axis=1)

    df_itm = df[
        (df["Date"] == last_month_date) &
        (df["Strike Price"] < 0.9 * underlying_price) &
        (df["Strike Price"] >= 0.5 * underlying_price) &
        (df["Implied_Volatility"] > 0)
        ].sort_values(by=['Turnover'], ascending=False).head(5)

    df_otm = df[
        (df["Date"] == last_month_date) &
        (df["Strike Price"] > 1.1 * underlying_price) &
        (df["Strike Price"] <= 1.5 * underlying_price) &
        (df["Implied_Volatility"] > 0)
        ].sort_values(by=['Turnover'], ascending=False).head(5)

    df_atm = df[
        (df["Date"] == last_month_date) &
        (df["Strike Price"] >= 0.9 * underlying_price) &
        (df["Strike Price"] <= 1.1 * underlying_price) &
        (df["Implied_Volatility"] > 0)
        ].sort_values(by=['Turnover'], ascending=False).head(5)

    df_liquid = df[
        (df["Date"] == last_month_date) &
        (df["Strike Price"] >= 0.5 * underlying_price) &
        (df["Strike Price"] <= 1.5 * underlying_price) &
        (df["Implied_Volatility"] > 0)
        ].sort_values(by=['Turnover'], ascending=False).head(5)
    # df = df.sort_values(by=['Turnover'], ascending=False).head(5)
    df["Realized_Volatility"] = get_nifty_std(last_month_date, latest_expiry)
    return {
        "Date": last_month_date,
        "Expiry": latest_expiry,
        "Underlying": underlying_price,
        "Realized_Volatility": get_nifty_std(last_month_date, latest_expiry),
        "Implied_Volatility_itm": df_itm["Implied_Volatility"].tolist(),
        "Implied_Volatility_itm_mean": np.sqrt(np.mean(filter_outliers(df_itm["Implied_Volatility"].to_numpy()) ** 2)),
        "Implied_Volatility_otm": df_otm["Implied_Volatility"].tolist(),
        "Implied_Volatility_otm_mean": np.sqrt(np.mean(filter_outliers(df_otm["Implied_Volatility"].to_numpy()) ** 2)),
        "Implied_Volatility_atm": df_atm["Implied_Volatility"].tolist(),
        "Implied_Volatility_atm_mean": np.sqrt(np.mean(filter_outliers(df_atm["Implied_Volatility"].to_numpy()) ** 2)),
        "Implied_Volatility_liquid": df_liquid["Implied_Volatility"].tolist(),
        "Implied_Volatility_liquid_mean": np.sqrt(
            np.mean(filter_outliers(df_liquid["Implied_Volatility"].to_numpy()) ** 2)),
        "Strike Prices_itm": df_itm["Strike Price"].tolist(),
        "Strike Prices_otm": df_otm["Strike Price"].tolist(),
        "Strike Prices_atm": df_atm["Strike Price"].tolist(),
        "Strike Prices_liquid": df_liquid["Strike Price"].tolist()
    }
    # print(tabulate(df, headers=df.columns))
    # print(underlying_price)
    # print(final_date)


# %%
df = pd.read_csv("csv/nifty/2010-1.csv")
df = df[df["Underlying"] == -1]
for year in range(2010, 2020):
    for month in range(1, 13):
        # print(process(year, month))
        df = df.append(process_row(year, month))
print(tabulate(df, headers=df.columns))
df.to_csv("csv/final_dataset.csv", index=False)

# %%
# %%
df = pd.DataFrame()
for year in range(2010, 2020):
    for month in range(1, 13):
        # print(process(year, month))
        df = df.append(process_row_3(year, month), ignore_index=True)
        print("----------")
print(tabulate(df, headers=df.columns))
df.to_csv("csv/processed_33.csv", index=False)
df = df.drop(columns=["Implied_Volatility_itm", "Implied_Volatility_otm", "Strike Prices_itm", "Strike Prices_otm"])
df.to_csv("csv/processed_33_compact.csv", index=False)
# %%
# -----------------------------------------------------
# Scratch
df = pd.read_csv("csv/2018-09.csv")
last_date = get_last_expiry_date(df)
print(last_date)
print(get_prev_month_monday(last_date))
print(get_prev_month_monday(last_date).weekday())
# %%
print(get_mibor_mean(date(year=2020, month=11, day=20)))
