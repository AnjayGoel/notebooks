import pandas as pd
from tabulate import tabulate

df = pd.read_csv("csv/nifty_old.csv")
df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y %H:%M:%S")

print(df.info())
print(tabulate(df.sample(10), headers=df.columns))
df.to_csv("csv/nifty.csv", date_format='%Y-%m-%d', index=False)
