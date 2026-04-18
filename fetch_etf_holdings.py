#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股主動式ETF 每日持股 + 收盤價抓取器 (GitHub Actions 版)
"""

import re
import sys
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup


# ============================================================
# 追蹤 ETF 清單 (14 檔，未來新增只要加在這)
# ============================================================
ETFS = [
    "00980A", "00981A", "00982A", "00984A", "00985A",
    "00987A", "00991A", "00992A", "00993A", "00994A",
    "00995A", "00996A", "00400A", "00401A",
]

MONEYDJ_URL = "https://www.moneydj.com/ETF/X/Basic/Basic0007B.xdjhtm?etfid={code}.TW"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


def fetch_etf_holdings(etf_code, session, retries=3):
    url = MONEYDJ_URL.format(code=etf_code)
    for attempt in range(retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            if r.apparent_encoding:
                r.encoding = r.apparent_encoding
            break
        except Exception as e:
            if attempt < retries:
                time.sleep(3 + attempt * 2)
            else:
                raise RuntimeError(f"HTTP 失敗: {e}")

    soup = BeautifulSoup(r.text, "html.parser")
    text_all = soup.get_text(" ", strip=True)

    title = soup.title.get_text() if soup.title else ""
    m = re.match(r"(.+?)\s*[-‧]\s*" + re.escape(etf_code), title)
    etf_name = m.group(1).strip() if m else etf_code

    m = re.search(r"資料日期[:：]\s*(\d{4}/\d{1,2}/\d{1,2})", text_all)
    holdings_date = m.group(1) if m else None

    target_table = None
    for table in soup.find_all("table"):
        headers_text = " ".join(th.get_text(strip=True) for th in table.find_all("th"))
        if "個股名稱" in headers_text and "持有股數" in headers_text:
            target_table = table
            break

    if target_table is None:
        raise ValueError("找不到持股表格")

    holdings = []
    for tr in target_table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        stock_cell = tds[0].get_text(strip=True)
        m2 = re.match(r"(.+?)\(([0-9A-Z]+)\.TW\)", stock_cell)
        if not m2:
            continue
        stock_name = m2.group(1).strip().rstrip("*").strip()
        stock_code = m2.group(2)
        try:
            weight = float(tds[1].get_text(strip=True))
            shares = int(tds[2].get_text(strip=True).replace(",", "").replace(" ", ""))
        except (ValueError, AttributeError):
            continue
        lots = shares // 1000
        if lots < 1:
            continue
        holdings.append({
            "code": stock_code,
            "name": stock_name,
            "lots": lots,
            "weight": round(weight, 2),
        })

    return {"name": etf_name, "holdings_date": holdings_date, "holdings": holdings}


def fetch_prices_bulk_yfinance(codes):
    prices = {}
    try:
        import yfinance as yf
    except ImportError:
        return prices

    tickers = [f"{c}.TW" for c in codes]
    try:
        df = yf.download(
            " ".join(tickers),
            period="5d", interval="1d",
            progress=False, threads=True, auto_adjust=False,
        )
    except Exception:
        return prices

    if df is None or df.empty:
        return prices

    try:
        if len(codes) == 1:
            if "Close" in df.columns:
                v = df["Close"].dropna()
                if not v.empty:
                    prices[codes[0]] = round(float(v.iloc[-1]), 2)
        else:
            close = df["Close"]
            for code in codes:
                t = f"{code}.TW"
                if t in close.columns:
                    v = close[t].dropna()
                    if not v.empty:
                        prices[code] = round(float(v.iloc[-1]), 2)
    except Exception:
        pass
    return prices


def fetch_price_yahoo_html(code, session):
    url = f"https://tw.stock.yahoo.com/quote/{code}.TW"
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        m = re.search(r'"regularMarketPrice"\s*:\s*\{[^}]*?"raw"\s*:\s*([\d.]+)', r.text)
        if m:
            return round(float(m.group(1)), 2)
        soup = BeautifulSoup(r.text, "html.parser")
        span = soup.find("span", class_=re.compile(r"Fz\(32px\)"))
        if span:
            txt = span.get_text(strip=True).replace(",", "")
            return round(float(txt), 2)
    except Exception:
        pass
    return None


def find_prev_snapshot(out_dir, today_date):
    candidates = []
    for f in out_dir.glob("snapshot-*.json"):
        m = re.match(r"snapshot-(\d{4}-\d{2}-\d{2})\.json$", f.name)
        if m and m.group(1) < today_date:
            candidates.append((m.group(1), f))
    if not candidates:
        return None, None
    candidates.sort(reverse=True)
    date_str, path = candidates[0]
    try:
        return date_str, json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, None


def main():
    out_dir = Path("snapshots")
    out_dir.mkdir(parents=True, exist_ok=True)

    today_date = datetime.now().strftime("%Y-%m-%d")
    print(f"\n=== 台股主動式ETF 追蹤器 {today_date} ===\n")

    prev_date, prev_snapshot = find_prev_snapshot(out_dir, today_date)
    if prev_snapshot:
        print(f"前一日快照: {prev_date}")
    else:
        print("前一日快照: 無 (首次執行)")

    print(f"\n[1/3] 抓取 MoneyDJ 持股 ({len(ETFS)} 檔)")
    session = requests.Session()
    all_etf_data = {}
    all_stock_codes = set()
    failed = []

    for i, code in enumerate(ETFS, 1):
        print(f"  [{i:2d}/{len(ETFS)}] {code}  ", end="", flush=True)
        try:
            data = fetch_etf_holdings(code, session)
            all_etf_data[code] = data
            for h in data["holdings"]:
                all_stock_codes.add(h["code"])
            print(f"OK  {data['name'][:18]:18s}  ({data['holdings_date']})  {len(data['holdings']):3d} 檔")
        except Exception as e:
            print(f"FAIL  {e}")
            failed.append(code)
        if i < len(ETFS):
            time.sleep(2)

    prices = {}
    if all_stock_codes:
        print(f"\n[2/3] 抓取 Yahoo 收盤價 ({len(all_stock_codes)} 檔)")
        codes = sorted(all_stock_codes)
        prices = fetch_prices_bulk_yfinance(codes)
        print(f"  yfinance 取得: {len(prices)}/{len(codes)}")

        missing = [c for c in codes if c not in prices]
        if missing:
            print(f"  Yahoo HTML 補抓: {len(missing)} 檔")
            for c in missing:
                p = fetch_price_yahoo_html(c, session)
                if p:
                    prices[c] = p
                time.sleep(0.3)
            print(f"  最終取得: {len(prices)}/{len(codes)}")

    print(f"\n[3/3] 組合快照並儲存")
    snapshot = {
        "today_date": today_date,
        "prev_date": prev_date,
        "prices": prices,
        "today": {
            code: {
                "name": data["name"],
                "holdings_date": data["holdings_date"],
                "holdings": data["holdings"],
            }
            for code, data in all_etf_data.items()
        },
        "prev": (prev_snapshot or {}).get("today", {}),
    }

    out_file = out_dir / f"snapshot-{today_date}.json"
    latest_file = Path("latest.json")

    out_file.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_file.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n完成:")
    print(f"  ETF 成功: {len(snapshot['today'])}/{len(ETFS)}")
    if failed:
        print(f"  失敗 ETF: {', '.join(failed)}")
    print(f"  股票數: {len(all_stock_codes)}")
    print(f"  價格數: {len(prices)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[!] 執行失敗: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
