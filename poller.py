import time
import requests
import json
import pandas as pd
from datetime import datetime, time as dt_time
from pytz import timezone
from collections import deque
from sqlalchemy import text  # 添加缺失的 text 導入
from utils import to_float, first_px, get_today_date_str

# --- 設定與常數 ---
TAIPEI_TZ = timezone('Asia/Taipei')
N8N_WEBHOOK_URL = "https://ooschool2.zeabur.app/webhook/80260f05-240c-4091-9f0a-772ad18993fd"

# --- 輔助函式與類別 ---

def is_trading_hours():
    """判斷目前是否為台股交易時間 (週一到週五, 09:00 ~ 13:30)"""
    now_tw = datetime.now(TAIPEI_TZ)
    if now_tw.weekday() > 4: return False # 排除週末
    
    current_time = now_tw.time()
    start_time = dt_time(9, 0)
    end_time = dt_time(13, 30)
    
    return start_time <= current_time <= end_time

class VshapeDetector:
    """V型反轉偵測器"""
    def __init__(self, summary_service):
        self.summary_service = summary_service
        # 儲存格式: { 'symbol': deque([{'ts': '11:05', 'low': 58.4}, ...], maxlen=3) }
        self.recent_lows = {}
        # 儲存格式: { 'symbol': '11:15' } # 記錄上次發送V轉通知的時間點
        self.last_notification_time = {}

    def check_and_notify(self, symbol, name, ohlc_df):
        if ohlc_df.shape[0] < 3: return # 需要至少三根K棒才能判斷

        # 初始化該股票的deque
        if symbol not in self.recent_lows:
            self.recent_lows[symbol] = deque(maxlen=3)

        # 更新最近的低點數據
        for idx, row in ohlc_df.iterrows():
            ts_str = idx.strftime('%H:%M')
            # 避免重複加入同一個時間點的資料
            if not any(d['ts'] == ts_str for d in self.recent_lows[symbol]):
                 self.recent_lows[symbol].append({'ts': ts_str, 'low': row['low']})

        # 檢查是否有V轉模式
        if len(self.recent_lows[symbol]) == 3:
            p1, p2, p3 = list(self.recent_lows[symbol])
            
            # V轉條件: l1 > l2 and l3 > l2
            if p1['low'] > p2['low'] and p3['low'] > p2['low']:
                # 檢查是否已為此時間點發送過通知
                if self.last_notification_time.get(symbol) != p3['ts']:
                    print(f"*** 偵測到V型反轉: {name} at {p3['ts']} ***")
                    # *** 核心修改：傳入 symbol 以便獲取完整 summary ***
                    self._send_notification(symbol, name, p1, p2, p3)
                    self.last_notification_time[symbol] = p3['ts']

    def _send_notification(self, symbol, name, p1, p2, p3):
        # *** 核心修改：在發送前，先呼叫 SummaryService 獲取最完整的即時資料 ***
        full_summary = self.summary_service.get_summary(symbol)
        
        def make_json_serializable(obj):
            if isinstance(obj, (pd.Timestamp, datetime)):
                return obj.strftime('%Y-%m-%d') if hasattr(obj, 'strftime') else str(obj)
            elif hasattr(obj, 'item'):  # numpy types
                return obj.item()
            elif isinstance(obj, dict):
                return {k: make_json_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [make_json_serializable(item) for item in obj]
            return obj

        payload = {
            "v_shape_signal": {
                "股票代號": symbol,
                "股票名稱": name,
                "時間": f"{p1['ts']}-{p3['ts']}",
                "價格": f"{p1['low']} > {p2['low']} > {p3['low']}",
            },
            "full_summary": make_json_serializable(full_summary)
        }
        try:
            res = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=5)
            res.raise_for_status()
            print(f"成功發送V轉通知到n8n for {name}.")
        except requests.RequestException as e:
            print(f"發送V轉通知失敗 for {name}: {e}")


class Poller:
    def __init__(self, config, db, summary_service):
        self.config = config
        self.db = db
        self.summary_service = summary_service
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.MIS_URL_BASE = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
        # *** 核心修改：將 summary_service 傳遞給偵測器 ***
        self.v_shape_detector = VshapeDetector(self.summary_service)

    def run(self):
        while True:
            symbols_str = self.config.get('symbols', '').strip()
            if not self.config.get('enabled') or not symbols_str:
                print(f"[{datetime.now(TAIPEI_TZ).strftime('%H:%M:%S')}] 輪詢器已啟用但無追蹤標的，暫停 60 秒。")
                time.sleep(60)
                continue

            if is_trading_hours():
                try:
                    self.poll_and_save(symbols_str)
                except Exception as e:
                    print(f"輪詢迴圈發生錯誤: {e}")
                time.sleep(self.config.get('poll_seconds', 5))
            else:
                # 非交易時間，每小時抓一次
                print(f"[{datetime.now(TAIPEI_TZ).strftime('%H:%M:%S')}] 非交易時間，輪詢器節能模式，每小時執行一次。")
                try:
                    self.poll_and_save(symbols_str)
                except Exception as e:
                    print(f"輪詢迴圈發生錯誤: {e}")
                time.sleep(3600)

    def _get_ticks_for_today(self, symbol):
        """從DB獲取指定股票當日的全部ticks"""
        with self.db.get_session() as session:
            today_start_ts = int(datetime.combine(datetime.now(TAIPEI_TZ).date(), dt_time.min).timestamp())
            stmt = text("SELECT ts_sec, price FROM ticks WHERE symbol = :symbol AND ts_sec >= :start_ts ORDER BY ts_sec ASC")
            df = pd.read_sql(stmt, session.connection(), params={"symbol": symbol, "start_ts": today_start_ts})
            if df.empty: return pd.DataFrame()
            
            df['datetime'] = pd.to_datetime(df['ts_sec'], unit='s', utc=True).dt.tz_convert(TAIPEI_TZ)
            df.set_index('datetime', inplace=True)
            return df

    def poll_and_save(self, symbols_str):
        timestamp = int(time.time() * 1000)
        full_url = f"{self.MIS_URL_BASE}?ex_ch={symbols_str}&json=1&delay=0&_={timestamp}"

        try:
            res = self.session.get(full_url, timeout=4)
            res.raise_for_status()
            data = res.json()
        except (requests.RequestException, ValueError) as e:
            print(f"無法獲取 MIS 資料: {e}")
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
        
        ts_str = datetime.now(TAIPEI_TZ).strftime('%H:%M:%S')
        for msg in data['msgArray']:
            # 日誌輸出
            name = msg.get('n', 'N/A')
            code = msg.get('c', 'N/A')
            summary_log = f"[{name} {code}] 開:{msg.get('o','-')} 高:{msg.get('h','-')} 低:{msg.get('l','-')} 收:{msg.get('z','-')} (昨收:{msg.get('y','-')})"
            print(f"[{ts_str}] {summary_log}")

            # V轉偵測
            symbol = msg.get("c")
            if not symbol: continue
            
            ticks_df = self._get_ticks_for_today(symbol)
            if not ticks_df.empty:
                ohlc_df = ticks_df['price'].resample('5min').ohlc().dropna()
                self.v_shape_detector.check_and_notify(symbol, name, ohlc_df)
