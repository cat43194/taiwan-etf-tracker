#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股主動式ETF 每日持股 + 收盤價抓取器 (GitHub Actions 版)
v3: 加入解析異常警告 (print 到 log, 不寫 JSON)
"""

import re
import sys
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup


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
    """
    回傳 dict: name, holdings_date, holdings, skipped_rows
    skipped_rows 是本 ETF 跳過的異常列清單
    """
    url = MONEYDJ_URL.format(code=etf_code)
    for attempt in range(retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=60)
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

    # ETF 名稱解析 (v2 修正版)
    etf_name = etf_code
    if soup.title:
        title_text = soup.title.get_text()
        m = re.match(r"^(.+?)-" + re.escape(etf_code) + r"\.TW", title_text)
        if m:
            etf_name = m.group(1).strip()
    if etf_name == etf_code:
        m = re.search(r"([\u4e00-\u9fa5A-Za-z0-9]+?)[〈<\(]" + re.escape(etf_code) + r"\.TW[〉>\)]", text_all)
        if m:
            etf_name = m.group(1).strip()
    if etf_name == etf_code:
        m = re.search(r"([\u4e00-\u9fa5A-Za-z0-9\-]+?)\(" + re.escape(etf_code) + r"\.TW\)\s*-\s*全部持股", text_all)
        if m:
            etf_name = m.group(1).strip()

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
    skipped_rows = []  # 收集本 ETF 異常列

    for tr in target_table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        first_cell = tds[0]
        stock_cell_text = first_cell.get_text(strip=True)

        # 跳過表頭列 (包含「個股名稱」字樣)
        if "個股名稱" in stock_cell_text or not stock_cell_text:
            continue

        # 嘗試 1: 文字含 "XXX(YYYY.TW)" 格式
        m2 = re.match(r"(.+?)\(([0-9A-Z]+)\.TW\)", stock_cell_text)

        # 嘗試 2: 從超連結的 href 取代號 (e.g. etfid=7751.TW)
        if not m2:
            link = first_cell.find("a")
            if link and link.get("href"):
                href = link["href"]
                href_match = re.search(r"etfid=([0-9A-Z]+)\.TW", href)
                if href_match:
                    stock_code = href_match.group(1)
                    stock_name = stock_cell_text.rstrip("*").strip() or "未知"
                    m2 = True
                    # 直接組結果,跳過下面的 m2.group 邏輯
                    try:
                        weight = float(tds[1].get_text(strip=True))
                        shares = int(tds[2].get_text(strip=True).replace(",", "").replace(" ", ""))
                    except (ValueError, AttributeError):
                        # 權重或股數解析失敗,記錄後跳過
                        skipped_rows.append({
                            "raw_text": stock_cell_text,
                            "weight_raw": tds[1].get_text(strip=True) if len(tds) > 1 else "",
                            "shares_raw": tds[2].get_text(strip=True) if len(tds) > 2 else "",
                            "reason": "權重或股數無法解析"
                        })
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
                    continue

        # 嘗試 3: 都失敗 -> 記錄為異常列
        if not m2:
            try:
                weight_raw = tds[1].get_text(strip=True)
                shares_raw = tds[2].get_text(strip=True)
            except Exception:
                weight_raw = ""
                shares_raw = ""
            skipped_rows.append({
                "raw_text": stock_cell_text or "(空白)",
                "weight_raw": weight_raw,
                "shares_raw": shares_raw,
                "reason": "無代號格式"
            })
            continue

        # 原本的 m2 匹配成功邏輯
        if m2 is not True:
            stock_name = m2.group(1).strip().rstrip("*").strip()
            stock_code = m2.group(2)
            try:
                weight = float(tds[1].get_text(strip=True))
                shares = int(tds[2].get_text(strip=True).replace(",", "").replace(" ", ""))
            except (ValueError, AttributeError):
                skipped_rows.append({
                    "raw_text": stock_cell_text,
                    "weight_raw": tds[1].get_text(strip=True),
                    "shares_raw": tds[2].get_text(strip=True),
                    "reason": "權重或股數無法解析"
                })
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

    return {
        "name": etf_name,
        "holdings_date": holdings_date,
        "holdings": holdings,
        "skipped_rows": skipped_rows,
    }


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
    # 全部異常列的彙總 (for 最後一次報告)
    all_skipped = []

    for i, code in enumerate(ETFS, 1):
        print(f"  [{i:2d}/{len(ETFS)}] {code}  ", end="", flush=True)
        try:
            data = fetch_etf_holdings(code, session)
            all_etf_data[code] = data
            for h in data["holdings"]:
                all_stock_codes.add(h["code"])

            skipped_count = len(data.get("skipped_rows", []))
            warn_mark = f"  ⚠️ 跳過 {skipped_count} 列" if skipped_count > 0 else ""
            print(f"OK  {data['name'][:20]:20s}  ({data['holdings_date']})  {len(data['holdings']):3d} 檔{warn_mark}")

            # 即時印出本 ETF 的異常列明細
            if skipped_count > 0:
                for row in data["skipped_rows"]:
                    print(f"      └─ 異常列: '{row['raw_text']}' | 權重={row['weight_raw']} | 股數={row['shares_raw']} | 原因={row['reason']}")
                all_skipped.append({
                    "etf": code,
                    "etf_name": data["name"],
                    "rows": data["skipped_rows"],
                })
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

    # ===== 最終報告 =====
    print(f"\n{'='*60}")
    print(f"執行結果")
    print(f"{'='*60}")
    print(f"  ETF 成功:   {len(snapshot['today'])}/{len(ETFS)}")
    if failed:
        print(f"  ETF 失敗:   {', '.join(failed)}")
    print(f"  股票數:     {len(all_stock_codes)}")
    print(f"  價格數:     {len(prices)}")
    print(f"  異常列總數: {sum(len(x['rows']) for x in all_skipped)}")

    # 異常列總結 (再印一次,方便在 log 底部快速看到)
    if all_skipped:
        print(f"\n{'='*60}")
        print(f"⚠️  資料完整性警告 ({len(all_skipped)} 檔 ETF 有異常列)")
        print(f"{'='*60}")
        for item in all_skipped:
            print(f"\n📌 {item['etf']} ({item['etf_name']}):")
            for row in item["rows"]:
                print(f"   - '{row['raw_text']}' | 權重 {row['weight_raw']}% | 股數 {row['shares_raw']} | {row['reason']}")
        print(f"\n建議: 到 MoneyDJ 網頁核對這些 ETF 後,手動補資料或通知我修正爬蟲\n")
    else:
        print(f"\n✅ 資料完整性:  無異常列\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[!] 執行失敗: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
