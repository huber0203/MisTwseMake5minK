import pandas as pd
from sqlalchemy import text
from datetime import datetime, time

class SummaryService:
    def __init__(self, db):
        self.db = db

    def get_summary(self, symbol):
        """獲取指定股票當日的即時總結"""
        with self.db.get_session() as session:
            today_start_ts = int(datetime.combine(datetime.today(), time.min).timestamp())
            
            ticks_stmt = text("SELECT ts_sec, price, vol, best_bid, best_ask FROM ticks WHERE symbol = :symbol AND ts_sec >= :start_ts ORDER BY ts_sec ASC")
            ticks_df = pd.read_sql(ticks_stmt, session.connection(), params={"symbol": symbol, "start_ts": today_start_ts})
            
            meta_stmt = text("SELECT * FROM daily_meta WHERE symbol = :symbol ORDER BY trade_date DESC LIMIT 1")
            meta_res = session.execute(meta_stmt, {"symbol": symbol}).fetchone()
            
            return self._process_summary_data(ticks_df, meta_res)

    def get_historical_summary(self, symbol, date_str):
        """獲取指定股票在特定歷史日期的總結"""
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        start_ts = int(datetime.combine(target_date, time.min).timestamp())
        end_ts = int(datetime.combine(target_date, time.max).timestamp())

        with self.db.get_session() as session:
            ticks_stmt = text("""
                SELECT ts_sec, price, vol, best_bid, best_ask 
                FROM ticks 
                WHERE symbol = :symbol AND ts_sec BETWEEN :start_ts AND :end_ts 
                ORDER BY ts_sec ASC
            """)
            ticks_df = pd.read_sql(ticks_stmt, session.connection(), params={"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts})

            meta_stmt = text("SELECT * FROM daily_meta WHERE symbol = :symbol AND trade_date = :trade_date")
            meta_res = session.execute(meta_stmt, {"symbol": symbol, "trade_date": date_str}).fetchone()

            return self._process_summary_data(ticks_df, meta_res)

    def _process_summary_data(self, ticks_df, meta_res):
        """共用的資料處理邏輯"""
        meta_data = dict(meta_res._mapping) if meta_res else {}

        response = {
            "查詢日期": meta_data.get("trade_date", "N/A"),
            "股票代號": meta_data.get("symbol"), "公司簡稱": meta_data.get("short_name"),
            "最新成交價": None, "當日開盤價": meta_data.get("day_open"),
            "當日最高價": meta_data.get("day_high"), "當日最低價": meta_data.get("day_low"),
            "昨日收盤價": meta_data.get("prev_close"), "當日成交量": None,
            "資料來源": "DB", "即時5分": [], "推估五分買賣量": [], "均價": []
        }

        if ticks_df.empty: return response

        ticks_df['datetime'] = pd.to_datetime(ticks_df['ts_sec'], unit='s', utc=True).dt.tz_convert('Asia/Taipei')
        ticks_df.set_index('datetime', inplace=True)

        response["最新成交價"] = ticks_df['price'].iloc[-1]
        response["當日成交量"] = int(ticks_df['vol'].sum())

        ohlc_5m = ticks_df['price'].resample('5T').ohlc()
        vol_5m = ticks_df['vol'].resample('5T').sum()
        
        ticks_df['value'] = ticks_df['price'] * ticks_df['vol']
        vwap_5m = (ticks_df['value'].resample('5T').sum() / vol_5m).dropna()

        def estimate_direction(row):
            if row['best_ask'] and row['price'] >= row['best_ask']: return 'B'
            if row['best_bid'] and row['price'] <= row['best_bid']: return 'S'
            return 'N'
        
        ticks_df['direction'] = ticks_df.apply(estimate_direction, axis=1)
        buy_vol = ticks_df[ticks_df['direction'] == 'B']['vol'].resample('5T').sum().fillna(0)
        sell_vol = ticks_df[ticks_df['direction'] == 'S']['vol'].resample('5T').sum().fillna(0)
        
        for idx, row in ohlc_5m.dropna().iterrows():
            ts_str = idx.strftime('%H:%M')
            response["即時5分"].append(f"{ts_str},O:{row['open']},H:{row['high']},L:{row['low']},C:{row['close']}")
            response["推估五分買賣量"].append(f"{ts_str},B:{int(buy_vol.get(idx, 0))},S:{int(sell_vol.get(idx, 0))}")
        
        for idx, val in vwap_5m.items():
            response["均價"].append(f"{idx.strftime('%H:%M')},{val:.2f}")

        return response
