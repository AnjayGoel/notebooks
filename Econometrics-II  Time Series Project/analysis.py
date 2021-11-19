# %%
import datetime
import math
import os
import traceback
from datetime import datetime, date, timedelta
import calendar
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from tabulate import tabulate

# %%
df = pd.read_csv("csv/processed_33_compact.csv")
df["Date"] = df["Date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date())
df["Expiry"] = df["Expiry"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date())
plt.plot(df["Realized_Volatility"], label="Realized")
# plt.plot(df["Implied_Volatility_itm_mean"], label="Implied ITM")
plt.plot(df["Implied_Volatility_atm_mean"], label="Implied ATM")
plt.plot(df["Implied_Volatility_liquid_mean"], label="Implied Liquid")
plt.plot(df["Implied_Volatility_otm_mean"], label="Implied OTM")
plt.legend(loc="upper left")
plt.savefig("graphs/one.png", dpi=300)
plt.show()
