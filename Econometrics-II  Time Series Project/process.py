# %%
import os
from datetime import datetime, date, timedelta
import calendar
import pandas as pd
from tabulate import tabulate


# %%

def str_to_date(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d").date()


def get_last_expiry_date(df: pd.DataFrame):
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


def get_nifty(dt: date):
    return df_nifty[df_nifty["Date"] == dt].iloc[0]["Close"]


# %%
def process(year, month):
    df = pd.read_csv(f"csv/nifty/{year}-{month}.csv")
    df["Expiry"] = df["Expiry"].apply(str_to_date)
    df["Date"] = df["Date"].apply(str_to_date)
    # print(tabulate(df.sample(10), headers=df.columns))
    prev_month_date = get_prev_month_monday(get_last_expiry_date(df))
    # print(prev_month_date)
    final_date = sorted(df[(df["Date"] >= prev_month_date)]["Date"].unique())[0]
    # print(final_date)
    underlying_price = get_nifty(final_date)
    df = df[(df["Date"] == final_date) & (underlying_price >= df["Strike Price"])]
    df = df.sort_values(by=['Turnover'])
    df["Underlying"] = underlying_price
    return df.iloc[-1]
    # print(tabulate(df, headers=df.columns))
    # print(underlying_price)
    # print(final_date)


# %%
print(process(2015, 9))

# %%
# Scratch

df = pd.read_csv("csv/2018-09.csv")
last_date = get_last_expiry_date(df)
print(last_date)
print(get_prev_month_monday(last_date))
print(get_prev_month_monday(last_date).weekday())
