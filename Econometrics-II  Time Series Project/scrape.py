import logging
import time
from datetime import timedelta
from itertools import product
from multiprocessing import *
from lxml import html
import pandas as pd
import requests
from nsepy import *
from nsepy.derivatives import get_expiry_date
from requests.structures import CaseInsensitiveDict
from tabulate import tabulate

logging.basicConfig(
    filename="logs.log",
    filemode='a',
    datefmt='%H:%M:%S',
    level=logging.INFO
)
logging.getLogger().addHandler(logging.StreamHandler())

request_cookies = {
    "cookieToken": "\"DWPRODAPP2:6400@78488016JhYoC5ZkKdOVn3op6vwgbL378488015Jeu0NNTziNe3n6GM5oYG5LN\"",
    "InfoViewPLATFORMSVC_COOKIE_TOKEN": "\"DWPRODAPP2:6400@78488016JhYoC5ZkKdOVn3op6vwgbL378488015Jeu0NNTziNe3n6GM5oYG5LN\"",
    "JSESSIONID": "Urz44QVTClZz5kEqZqwuPHx_LA2OXBg_p0jKT-JUadYWVafrFQ6f!-838047046",
    "OpenDocumentPLATFORMSVC_COOKIE_TOKEN": "",
    "TS01f9b35c": "01d966ad3eb132758c46c5ff246595a61cebf666aef91de58f761a7cef8a1d5458124549056b6996c2362ca7e0b457cd65aeb89cd0fbf5ed75f46772282df867b59d177d94c4bc9100b3df56f1a68d7f8abcd8d353acdefa9d3475664742af21c26f652fb375bccb16544e8292ef6547746a4988dd28c4672a7b63f4202e6f44df14dccf6dd9cf601a71de8e17bc3cb5a1e1cd5500",
    "TS1776a086027": "082160391eab2000193d3663b08c68147ac7d1c459fd70c5908c466995949a44ec01e427ddf4178708e73f382211300011f7db824e212d2c4bf711a4d774ebdba30e6933d0698fc10f344979572d1ded819dc61f62380e12d649f1b15b5ce05f",
    "VINTELASSO": "\"true\""
}


def save_resp(resp, fn):
    f = open(f"{fn}.html", "w+")
    f.write(resp.text)
    f.close()


def fetch_mibor(page_no: int):
    url = f"https://dbie.rbi.org.in/BOE/OpenDocument/1608101729/AnalyticalReporting/webiDHTML/viewer/report.jsp?iViewerID=3&sEntry=we000500009e9df6a65fe8&iReport=0&iReportID=1&sPageMode=QuickDisplay&sReportMode=Viewing&iPage={page_no}&zoom=100&isInteractive=false&isStructure=false&sBid=&iVSlot={page_no}&iHSlot=0&sUndoEnabled=false&nbPage=NaN"

    headers = CaseInsensitiveDict()
    headers["User-Agent"] = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0"
    headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    headers["Accept-Language"] = "en-US,en;q=0.5"
    headers["DNT"] = "1"
    headers["Connection"] = "keep-alive"
    headers[
        "Referer"] = "https://dbie.rbi.org.in/BOE/OpenDocument/1608101729/AnalyticalReporting/webiDHTML/viewer/viewDocument.jsp"
    headers[
        "Cookie"] = """VINTELASSO="true";cookieToken="DWPRODAPP2:6400@78488016JhYoC5ZkKdOVn3op6vwgbL378488015Jeu0NNTziNe3n6GM5oYG5LN";InfoViewPLATFORMSVC_COOKIE_TOKEN="DWPRODAPP2:6400@78488016JhYoC5ZkKdOVn3op6vwgbL378488015Jeu0NNTziNe3n6GM5oYG5LN"; JSESSIONID=Urz44QVTClZz5kEqZqwuPHx_LA2OXBg_p0jKT-JUadYWVafrFQ6f!-838047046; TS01f9b35c=01d966ad3e84209f048b8657e5230ae8b0fac6afd23c1e1891b743f3628460b2240db7eaed44b6c1bd8586b8f2f28e178aee761259e5662ce981caa91344d6f9c650636d681d8594e28015dc74f3bf4d7f201e8e980b43cdf60b3b52ef789c0ef83dbba90ae9e8343c097c5ea64d0815c4712359ceec8062038f272d6af337b8e35e4ab4045dd3ede82c1e44b668be9c24d6862405; TS1776a086027=082160391eab2000f00ae558c923bb141853a360034ffb496650a9fcc4ba397b2628a3d79b5d0cca085c4d8635113000baf1f7a8471f070683ec70c2abbd44358493b05a2b9e1aa6efd5dace8aee1cff7944fcd0a09e7047979f93628ea130ca; OpenDocumentPLATFORMSVC_COOKIE_TOKEN="""
    headers["Upgrade-Insecure-Requests"] = "1"
    headers["Sec-Fetch-Dest"] = "iframe"
    headers["Sec-Fetch-Mode"] = "navigate"
    headers["Sec-Fetch-Site"] = "same-origin"
    headers["Sec-Fetch-User"] = "?1"
    headers["Sec-GPC"] = "1"

    resp = requests.get(url, headers=headers)
    df = pd.read_html(resp.text)[0]
    df = df[[0, 3, 7, 11, 15]]
    df.columns = ["Date", "Overnight", "14_Days", "1_Month", "3_Month"]
    return df


def fetch_nifty_options_data(args):
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
        fetch_nifty_options_data(args)


def process_nifty_options():
    pool = Pool(processes=8)
    pool.map(fetch_nifty_options_data, product([i for i in range(2010, 2020)], [i for i in range(1, 13)]))


def process_mibor():
    pool = Pool(processes=8)
    results = pool.map(fetch_mibor, range(1, 100))
    df_mibor = pd.concat(results).drop_duplicates()
    pool.close()
    pool.join()
    df_mibor.to_csv("csv/mibor.csv", index=False)


if __name__ == "__main__":
    pass
    # df = fetch_mibor(1)
    # print(df)
    # process_mibor()
