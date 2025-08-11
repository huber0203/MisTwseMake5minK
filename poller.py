import time
import requests
import json
from datetime import datetime
from utils import to_float, first_px, get_today_date_str

class Poller:
    def __init__(self, config, db):
        self.config = config
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.MIS_URL_BASE = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

    def run(self):
        while True:
            symbols_str = self.config.get('symbols', '').strip()
            if not self.config.get('enabled') or not symbols_str:
                time.sleep(60)
                continue
            
            try:
                self.poll_and_save(symbols_str)
            except Exception as e:
                print(f"輪詢迴圈發生錯誤 (An error occurred in the polling loop): {e}")
            
            time.sleep(self.config.get('poll_seconds', 5))

    def poll_and_save(self, symbols_str):
        timestamp = int(time.time() * 1000)
        full_url = f"{self.MIS_URL_BASE}?ex_ch={symbols_str}&json=1&delay=0&_={timestamp}"

        try:
            res = self.session.get(full_url, timeout=4)
            res.raise_for_status()
            data = res.json()
        except (requests.RequestException, ValueError) as e:
            print(f"無法獲取 MIS 資料 (Failed to fetch or parse MIS data): {e}")
            return

        if 'msgArray' not in data or not data['msgArray']: return

        ticks_to_insert, meta_to_upsert = [], []
        today_date = get_today_date_str()

        for msg in data['msgArray']:
            code = (msg.get("c") or "").strip()
            if not code: continue

            meta_to_upsert.append({
                "symbol": code, "trade_date": today_date,
                "day_open": to_float(msg.get("o")), "day_high": to_float(msg.get("h")),
                "day_low": to_float(msg.get("l")), "prev_close": to_float(msg.get("y")),
                "limit_up": to_float(msg.get("u")), "limit_down": to_float(msg.get("w")),
                "short_name": (msg.get("n") or "").strip(), "full_name": (msg.get("nf") or "").strip(),
                "exchange": (msg.get("ex") or "").strip(),
            })

            price, tlong = to_float(msg.get("z")), msg.get("tlong")
            if price is not None and tlong:
                ticks_to_insert.append({
                    "symbol": code, "ts_sec": int(int(tlong) / 1000), "price": price,
                    "vol": int(to_float(msg.get("tv")) or 0),
                    "best_bid": first_px(msg.get("b")), "best_ask": first_px(msg.get("a")),
                })
        
        if meta_to_upsert: self.db.bulk_upsert_daily_meta(meta_to_upsert)
        if ticks_to_insert: self.db.bulk_upsert_ticks(ticks_to_insert)
        
        # --- 收斂日誌：為每支股票分別顯示 ---
        ts_str = datetime.now().strftime('%H:%M:%S')
        for msg in data['msgArray']:
            # 檢查此股票是否有產生 tick 資料
            price, tlong = to_float(msg.get("z")), msg.get("tlong")
            tick_count = 1 if price is not None and tlong else 0
            
            name = msg.get('n', 'N/A')
            o = msg.get('o', '-')
            h = msg.get('h', '-')
            l = msg.get('l', '-')
            z = msg.get('z', '-')
            y = msg.get('y', '-')
            
            summary_log = f"[{name}] 開:{o} 高:{h} 低:{l} 收:{z} (昨收:{y})"
            db_write_log = f"DB Write: 1 meta, {tick_count} ticks."
            
            print(f"[{ts_str}] {summary_log} | {db_write_log}")
