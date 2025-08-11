from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

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
