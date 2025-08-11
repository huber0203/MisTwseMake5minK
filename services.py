import pandas as pd
    from sqlalchemy import text
    from datetime import datetime, time

    class SummaryService:
        def __init__(self, db):
            self.db = db

        def get_summary(self, symbol):
            session = self.db.get_session()
            try:
                # 1. Get today's ticks
                today_start_ts = int(datetime.combine(datetime.today(), time.min).timestamp())
                ticks_stmt = text("""
                    SELECT ts_sec, price, vol, best_bid, best_ask 
                    FROM ticks 
                    WHERE symbol = :symbol AND ts_sec >= :start_ts 
                    ORDER BY ts_sec ASC
                """)
                ticks_df = pd.read_sql(ticks_stmt, session.connection(), params={"symbol": symbol, "start_ts": today_start_ts})

                # 2. Get latest daily meta
                meta_stmt = text("SELECT * FROM daily_meta WHERE symbol = :symbol ORDER BY trade_date DESC LIMIT 1")
                meta_res = session.execute(meta_stmt, {"symbol": symbol}).fetchone()
                meta_data = dict(meta_res._mapping) if meta_res else {}

                # 3. Prepare base response
                response = {
                    "股票代號": meta_data.get("symbol"),
                    "公司簡稱": meta_data.get("short_name"),
                    "最新成交價": None,
                    "當日開盤價": meta_data.get("day_open"),
                    "當日最高價": meta_data.get("day_high"),
                    "當日最低價": meta_data.get("day_low"),
                    "昨日收盤價": meta_data.get("prev_close"),
                    "當日成交量": None,
                    "資料來源": "DB",
                    "即時5分": [],
                    "推估五分買賣量": [],
                    "均價": []
                }

                if ticks_df.empty:
                    return response

                # 4. Process ticks with pandas
                ticks_df['datetime'] = pd.to_datetime(ticks_df['ts_sec'], unit='s', utc=True).dt.tz_convert('Asia/Taipei')
                ticks_df.set_index('datetime', inplace=True)

                # Update summary fields from ticks
                response["最新成交價"] = ticks_df['price'].iloc[-1] if not ticks_df.empty else None
                response["當日成交量"] = int(ticks_df['vol'].sum())

                # 5. Calculate 5-min aggregates
                ohlc_5m = ticks_df['price'].resample('5T').ohlc()
                vol_5m = ticks_df['vol'].resample('5T').sum()
                
                # VWAP
                ticks_df['value'] = ticks_df['price'] * ticks_df['vol']
                vwap_5m = ticks_df['value'].resample('5T').sum() / vol_5m
                vwap_5m.dropna(inplace=True)

                # Estimate Buy/Sell Volume
                def estimate_direction(row):
                    if row['best_ask'] and row['price'] >= row['best_ask']: return 'B'
                    if row['best_bid'] and row['price'] <= row['best_bid']: return 'S'
                    return 'N' # Neutral/Unknown
                
                ticks_df['direction'] = ticks_df.apply(estimate_direction, axis=1)
                buy_vol = ticks_df[ticks_df['direction'] == 'B']['vol'].resample('5T').sum().fillna(0)
                sell_vol = ticks_df[ticks_df['direction'] == 'S']['vol'].resample('5T').sum().fillna(0)
                
                # 6. Format output strings
                for idx, row in ohlc_5m.iterrows():
                    if pd.notna(row['open']):
                        ts_str = idx.strftime('%H:%M')
                        response["即時5分"].append(f"{ts_str},O:{row['open']},H:{row['high']},L:{row['low']},C:{row['close']}")
                        response["推估五分買賣量"].append(f"{ts_str},B:{int(buy_vol.get(idx, 0))},S:{int(sell_vol.get(idx, 0))}")
                
                for idx, val in vwap_5m.items():
                    ts_str = idx.strftime('%H:%M')
                    response["均價"].append(f"{ts_str},{val:.2f}")

                return response

            finally:
                session.close()
