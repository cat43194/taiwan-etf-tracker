def main():
    out_dir = Path("snapshots")
    out_dir.mkdir(parents=True, exist_ok=True)

    # 初始 today_date 用系統時間 (後面會被 MoneyDJ 的 holdings_date 覆蓋)
    today_date = datetime.now().strftime("%Y-%m-%d")
    print(f"\n=== 台股主動式ETF 追蹤器 (系統時間: {today_date}) ===\n")

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
    # 失敗 ETF 單獨重試 (最多 2 輪,每輪間隔 15 秒)
    # 用途:處理 MoneyDJ 偶發失敗,例如 00984A 某次抓不到
    # ========================================================
    if failed:
        print(f"\n  ⚠️ 主迴圈完成,有 {len(failed)} 檔失敗: {', '.join(failed)}")
        for retry_round in range(1, 3):  # 第 1 輪、第 2 輪
            still_failed = list(failed)
            if not still_failed:
                break
            print(f"\n  ⟳ 重試第 {retry_round}/2 輪 ({len(still_failed)} 檔): {', '.join(still_failed)}")
            print(f"     等候 15 秒讓 MoneyDJ 喘息...")
            time.sleep(15)
            newly_succeeded = []
            for code in still_failed:
                print(f"     重試 {code}  ", end="", flush=True)
                try:
                    data = fetch_etf_holdings(code, session)
                    all_etf_data[code] = data
                    for h in data["holdings"]:
                        all_stock_codes.add(h["code"])
                    skipped_count = len(data.get("skipped_rows", []))
                    warn_mark = f"  ⚠️ 跳過 {skipped_count} 列" if skipped_count > 0 else ""
                    print(f"OK  {data['name'][:20]:20s}  ({data['holdings_date']})  {len(data['holdings']):3d} 檔{warn_mark}")
                    if skipped_count > 0:
                        all_skipped.append({
                            "etf": code,
                            "etf_name": data["name"],
                            "rows": data["skipped_rows"],
                        })
                    newly_succeeded.append(code)
                except Exception as e:
                    print(f"FAIL  {e}")
                time.sleep(3)
            # 更新 failed 清單
            for code in newly_succeeded:
                if code in failed:
                    failed.remove(code)
            if not failed:
                print(f"\n  ✅ 重試成功,所有 ETF 都抓到了")
                break
        if failed:
            print(f"\n  ❌ 重試後仍失敗: {', '.join(failed)}")

    # ========================================================
    # holdings_date 眾數計算
    # ========================================================
    today_holdings_dates = [d["holdings_date"] for d in all_etf_data.values() if d.get("holdings_date")]
    most_common_today_hd = max(set(today_holdings_dates), key=today_holdings_dates.count) if today_holdings_dates else None

    prev_holdings_dates = []
    if prev_snapshot:
        for etf_data in (prev_snapshot.get("today") or {}).values():
            hd = etf_data.get("holdings_date")
            if hd:
                prev_holdings_dates.append(hd)
    most_common_prev_hd = max(set(prev_holdings_dates), key=prev_holdings_dates.count) if prev_holdings_dates else None

    print(f"\n  本次 holdings_date 眾數: {most_common_today_hd}")
    print(f"  上次 holdings_date 眾數: {most_common_prev_hd}")

    # ========================================================
    # 防呆 1: holdings_date 全部抓不到 -> 擋下
    # ========================================================
    if most_common_today_hd is None:
        print(f"\n{'='*60}")
        print(f"🛑 holdings_date 全部抓不到")
        print(f"{'='*60}")
        print(f"  所有 ETF 的資料日期都解析失敗,可能 MoneyDJ 頁面異常")
        print(f"  -> 為避免寫出無日期的快照, 本次不寫檔")
        print(f"{'='*60}\n")
        return

    # ========================================================
    # 關鍵: 用 holdings_date 當檔名日期,避免 cron 延遲跨日
    # ========================================================
    original_today_date = today_date
    today_date = most_common_today_hd.replace("/", "-")
    if original_today_date != today_date:
        print(f"\n  ℹ️ 系統時間是 {original_today_date},但 MoneyDJ 資料日期是 {today_date}")
        print(f"  ℹ️ 為避免 cron 延遲跨日造成資料錯位,改用 MoneyDJ 日期作為檔名")
        # 重新找前一日快照(因為 today_date 可能變小了)
        prev_date, prev_snapshot = find_prev_snapshot(out_dir, today_date)
        if prev_snapshot:
            print(f"  ℹ️ 重新載入前一日快照: {prev_date}")
        # 重新計算上次 holdings_date 眾數
        prev_holdings_dates = []
        if prev_snapshot:
            for etf_data in (prev_snapshot.get("today") or {}).values():
                hd = etf_data.get("holdings_date")
                if hd:
                    prev_holdings_dates.append(hd)
        most_common_prev_hd = max(set(prev_holdings_dates), key=prev_holdings_dates.count) if prev_holdings_dates else None

    # ========================================================
    # 防呆 2: 未開盤日偵測 (任何一檔 ETF 有新資料就通過)
    # ========================================================
    has_any_update = False
    update_detail = []

    if prev_snapshot:
        prev_today = prev_snapshot.get("today", {}) or {}
        for etf_code, today_data in all_etf_data.items():
            today_hd = today_data.get("holdings_date")
            prev_hd = (prev_today.get(etf_code) or {}).get("holdings_date")
            if today_hd and (not prev_hd or today_hd > prev_hd):
                has_any_update = True
                update_detail.append(f"{etf_code}: {prev_hd or '(無)'} -> {today_hd}")
    else:
        has_any_update = True
        update_detail.append("(首次執行,無上次快照)")

    if not has_any_update:
        print(f"\n{'='*60}")
        print(f"🛑 台股未開盤日偵測 (無任何 ETF 有新資料)")
        print(f"{'='*60}")
        print(f"  所有 ETF 的 holdings_date 都跟上次快照相同或更舊")
        print(f"  -> 本次不寫檔, latest.json 保持不變")
        print(f"{'='*60}\n")
        return

    print(f"\n  有新資料的 ETF ({len(update_detail)} 檔):")
    for d in update_detail[:5]:
        print(f"    {d}")
    if len(update_detail) > 5:
        print(f"    ...(還有 {len(update_detail)-5} 檔)")

    # ========================================================
    # 防呆 3: ETF 數量異常
    # ========================================================
    today_etf_count = len(all_etf_data)
    prev_etf_count = len((prev_snapshot or {}).get("today") or {})

    if prev_etf_count > 0 and today_etf_count < prev_etf_count:
        print(f"\n{'='*60}")
        print(f"🛑 ETF 數量異常偵測")
        print(f"{'='*60}")
        print(f"  本次抓到: {today_etf_count} 檔")
        print(f"  上次快照: {prev_etf_count} 檔")
        today_keys = set(all_etf_data.keys())
        prev_keys = set((prev_snapshot or {}).get("today", {}).keys())
        missing = prev_keys - today_keys
        print(f"  失聯 ETF: {', '.join(sorted(missing))}")
        print(f"  -> 為避免污染 latest.json, 本次不寫檔")
        print(f"{'='*60}\n")
        return

    # ========================================================
    # 抓收盤價
    # ========================================================
    print(f"\n[2/3] 抓取收盤價")
    prices = {}
    if all_stock_codes:
        prices = fetch_all_prices(all_stock_codes, session, HEADERS)

    # ========================================================
    # 組合快照並儲存
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
    print(f"  檔名日期:   snapshot-{today_date}.json")
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
