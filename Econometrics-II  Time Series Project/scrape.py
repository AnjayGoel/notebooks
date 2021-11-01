import logging
import time
from datetime import timedelta
from itertools import product
from multiprocessing import *

import pandas as pd
from nsepy import *
from nsepy.derivatives import get_expiry_date

logging.basicConfig(
    filename="logs.log",
    filemode='a',
    datefmt='%H:%M:%S',
    level=logging.INFO
)
logging.getLogger().addHandler(logging.StreamHandler())


def fetch(args):
    year = args[0]
    month = args[1]
    logging.info(f"Start {year}-{month}")

    """
    if "Date" in pd.read_csv(f"csv/{year}-{month}.csv").columns:
        logging.info(f"Done {year}-{month}")
        return
    """

    try:
        df = pd.DataFrame()
        exp_dates = get_expiry_date(year=year, month=month)
        logging.info(f"Exp dates for {year}-{month}: {str(exp_dates)}")
        for exp_date in exp_dates:
            start_date = exp_date - timedelta(days=30 * 4)
            for i in range(0, 34000, 100):
                df_t = get_history(symbol="BANKNIFTY",
                                   start=start_date,
                                   end=exp_date,
                                   index=True,
                                   option_type='CE',
                                   strike_price=i,
                                   expiry_date=exp_date)
                df_t["Date"] = df_t.index
                df = pd.concat([df, df_t])
        df.to_csv(f"csv/banknifty/{year}-{month}.csv", index=False)
        logging.info(f"Done {year}-{month}")
    except Exception as e:
        logging.error(f"Error: {year}-{month}")
        logging.error(e, exc_info=True)
        time.sleep(1)
        fetch(args)


if __name__ == "__main__":
    pool = Pool(processes=8)
    pool.map(fetch, product([i for i in range(2010, 2020)], [i for i in range(1, 13)]))
