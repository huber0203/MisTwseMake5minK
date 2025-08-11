from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta

class Database:
    def __init__(self, db_url):
        try:
            self.engine = create_engine(db_url, pool_recycle=3600, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            print("資料庫連線成功 (Database connection successful).")
        except Exception as e:
            print(f"資料庫連線失敗 (Error connecting to database): {e}")
            raise

    def get_session(self):
        return self.Session()

    def bulk_upsert_daily_meta(self, records):
        if not records: return
        stmt = text("""
            INSERT INTO daily_meta(symbol, trade_date, day_open, day_high, day_low, prev_close, limit_up, limit_down, short_name, full_name, exchange)
            VALUES (:symbol, :trade_date, :day_open, :day_high, :day_low, :prev_close, :limit_up, :limit_down, :short_name, :full_name, :exchange)
            ON DUPLICATE KEY UPDATE
                day_open = COALESCE(VALUES(day_open), day_open),
                day_high = COALESCE(VALUES(day_high), day_high),
                day_low = COALESCE(VALUES(day_low), day_low),
                prev_close = COALESCE(VALUES(prev_close), prev_close),
                limit_up = COALESCE(VALUES(limit_up), limit_up),
                limit_down = COALESCE(VALUES(limit_down), limit_down),
                short_name = VALUES(short_name),
                full_name = VALUES(full_name),
                exchange = VALUES(exchange);
        """)
        with self.get_session() as session:
            try:
                session.execute(stmt, records)
                session.commit()
            except SQLAlchemyError as e:
                print(f"Error in bulk_upsert_daily_meta: {e}")
                session.rollback()

    def bulk_upsert_ticks(self, records):
        if not records: return
        stmt = text("""
            INSERT INTO ticks(symbol, ts_sec, price, vol, best_bid, best_ask)
            VALUES (:symbol, :ts_sec, :price, :vol, :best_bid, :best_ask)
            ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                vol = VALUES(vol),
                best_bid = VALUES(best_bid),
                best_ask = VALUES(best_ask);
        """)
        with self.get_session() as session:
            try:
                session.execute(stmt, records)
                session.commit()
            except SQLAlchemyError as e:
                print(f"Error in bulk_upsert_ticks: {e}")
                session.rollback()

    def prune_old_data(self, days_to_keep=60):
        """刪除超過指定天數的舊資料"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
        cutoff_ts = int(cutoff_date.timestamp())

        print(f"開始清理 {days_to_keep} 天前的舊資料 (cutoff: {cutoff_date_str})...")
        
        with self.get_session() as session:
            try:
                # 刪除舊的 ticks
                ticks_stmt = text("DELETE FROM ticks WHERE ts_sec < :cutoff_ts")
                ticks_res = session.execute(ticks_stmt, {"cutoff_ts": cutoff_ts})
                
                # 刪除舊的 daily_meta
                meta_stmt = text("DELETE FROM daily_meta WHERE trade_date < :cutoff_date_str")
                meta_res = session.execute(meta_stmt, {"cutoff_date_str": cutoff_date_str})
                
                session.commit()
                print(f"清理完成。刪除了 {ticks_res.rowcount} 筆 tick 資料和 {meta_res.rowcount} 筆 meta 資料。")
            except SQLAlchemyError as e:
                print(f"清理舊資料時發生錯誤: {e}")
                session.rollback()
