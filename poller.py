import time
    import requests
    from datetime import datetime
    from utils import to_float, first_px, get_today_date_str

    class Poller:
        def __init__(self, app, db):
            self.app = app
            self.db = db
            self.session = requests.Session()
            self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
            self.MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

        def run(self):
            print("Poller thread started.")
            while True:
                config = self.app.config['POLLER_CONFIG']
                if not config.get('enabled') or not config.get('symbols'):
                    # If disabled or no symbols, sleep for 60s and check again
                    time.sleep(60)
                    continue
                
                try:
                    self.poll_and_save(config['symbols'])
                except Exception as e:
                    print(f"An error occurred in the polling loop: {e}")
                
                time.sleep(config.get('poll_seconds', 5))

        def poll_and_save(self, symbols_str):
            params = {
                'ex_ch': symbols_str,
                'json': '1',
                'delay': '0',
                '_': int(time.time() * 1000),
            }
            try:
                res = self.session.get(self.MIS_URL, params=params, timeout=4)
                res.raise_for_status()
                data = res.json()
            except (requests.RequestException, ValueError) as e:
                print(f"Failed to fetch or parse MIS data: {e}")
                return

            if 'msgArray' not in data or not data['msgArray']:
                return

            ticks_to_insert = []
            meta_to_upsert = []
            today_date = get_today_date_str()

            for msg in data['msgArray']:
                # --- Parse Meta Data ---
                code = (msg.get("c") or "").strip()
                if not code:
                    continue

                meta_data = {
                    "symbol": code,
                    "trade_date": today_date,
                    "day_open": to_float(msg.get("o")),
                    "day_high": to_float(msg.get("h")),
                    "day_low": to_float(msg.get("l")),
                    "prev_close": to_float(msg.get("y")),
                    "limit_up": to_float(msg.get("u")),
                    "limit_down": to_float(msg.get("w")),
                    "short_name": (msg.get("n") or "").strip(),
                    "full_name": (msg.get("nf") or "").strip(),
                    "exchange": (msg.get("ex") or "").strip(),
                }
                meta_to_upsert.append(meta_data)

                # --- Parse Tick Data (only if price 'z' is valid) ---
                price = to_float(msg.get("z"))
                tlong = msg.get("tlong")

                if price is not None and tlong:
                    tick_data = {
                        "symbol": code,
                        "ts_sec": int(int(tlong) / 1000),
                        "price": price,
                        "vol": int(to_float(msg.get("tv")) or 0),
                        "best_bid": first_px(msg.get("b")),
                        "best_ask": first_px(msg.get("a")),
                    }
                    ticks_to_insert.append(tick_data)
            
            # --- Bulk DB Operations ---
            if meta_to_upsert:
                self.db.bulk_upsert_daily_meta(meta_to_upsert)
            if ticks_to_insert:
                self.db.bulk_upsert_ticks(ticks_to_insert)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Polled {len(data['msgArray'])} symbols. Upserted {len(meta_to_upsert)} meta, {len(ticks_to_insert)} ticks.")
