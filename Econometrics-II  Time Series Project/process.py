# %%
import datetime
import math
import os
from datetime import datetime, date, timedelta
import calendar

import numpy as np
import pandas as pd
from tabulate import tabulate
from py_vollib.black_scholes.implied_volatility import *


# %%

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
    V = row["Close"]
    F = row["Underlying"]
    K = row["Strike Price"]
    r = row["Interest_Rate_(Mibor)"] / 100
    t = np.busday_count(row["Date"], row["Expiry"]) / 252
    flag = "c"
    print((V, F, K, t, r, flag))
    return implied_volatility(V, F, K, t, r, flag)


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
# -----------------------------------------------------
# Scratch
df = pd.read_csv("csv/2018-09.csv")
last_date = get_last_expiry_date(df)
print(last_date)
print(get_prev_month_monday(last_date))
print(get_prev_month_monday(last_date).weekday())
# %%
print(get_mibor_mean(date(year=2020, month=11, day=20)))
