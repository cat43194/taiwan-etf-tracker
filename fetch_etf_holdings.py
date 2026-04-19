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

    # ========================================================
    # [1/3] 抓取 MoneyDJ 持股
    # ========================================================
    print(f"\n[1/3] 抓取 MoneyDJ 持股 ({len(ETFS)} 檔)")
    session = requests.Session()
    all_etf_data = {}
    all_stock_codes = set()
    failed = []
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

    # ========================================================
    # 未開盤日檢查: 今天的 holdings_date 跟前一次快照相同 -> 直接結束
    # ========================================================
    today_holdings_dates = [d["holdings_date"] for d in all_etf_data.values() if d.get("holdings_date")]
    # 取眾數 (理論上所有 ETF 的 holdings_date 應該一致)
    if today_holdings_dates:
        most_common_today_hd = max(set(today_holdings_dates), key=today_holdings_dates.count)
    else:
        most_common_today_hd = None

    prev_holdings_dates = []
    if prev_snapshot:
        for etf_data in (prev_snapshot.get("today") or {}).values():
            hd = etf_data.get("holdings_date")
            if hd:
                prev_holdings_dates.append(hd)
    most_common_prev_hd = max(set(prev_holdings_dates), key=prev_holdings_dates.count) if prev_holdings_dates else None

    print(f"\n  本次 holdings_date: {most_common_today_hd}")
    print(f"  上次 holdings_date: {most_common_prev_hd}")

    if most_common_today_hd and most_common_prev_hd and most_common_today_hd == most_common_prev_hd:
        print(f"\n{'='*60}")
        print(f"🛑 台股未開盤日偵測")
        print(f"{'='*60}")
        print(f"  本次與上次的 holdings_date 相同 ({most_common_today_hd})")
        print(f"  -> 不寫新快照, latest.json 保持不變")
        print(f"  -> 下次開市會直接跟 {most_common_prev_hd} 比對")
        print(f"{'='*60}\n")
        return  # 直接結束 main(), 什麼都不動

    # ========================================================
    # [2/3] 抓取收盤價 (只在有新交易日資料時才抓)
    # ========================================================
    print(f"\n[2/3] 抓取收盤價")
    prices = {}
    if all_stock_codes:
        prices = fetch_all_prices(all_stock_codes, session, HEADERS)

    # ========================================================
    # [3/3] 組合快照並儲存
    # ========================================================
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

    print(f"\n{'='*60}")
    print(f"執行結果")
    print(f"{'='*60}")
    print(f"  ETF 成功:   {len(snapshot['today'])}/{len(ETFS)}")
    if failed:
        print(f"  ETF 失敗:   {', '.join(failed)}")
    print(f"  股票數:     {len(all_stock_codes)}")
    print(f"  價格數:     {len(prices)}/{len(all_stock_codes)}")
    print(f"  異常列總數: {sum(len(x['rows']) for x in all_skipped)}")

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
