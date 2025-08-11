import os
import sys
import threading
import time
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel, Field
from typing import Optional
from dotenv import load_dotenv
from datetime import datetime

# --- GPS 導航：確保 Python 能找到同資料夾的檔案 ---
current_path = os.path.dirname(os.path.abspath(__file__))
if current_path not in sys.path:
    sys.path.append(current_path)
# --- GPS 導航結束 ---

# 載入我們原有的商業邏輯
from database import Database
from poller import Poller
from services import SummaryService

# 讀取 .env 檔案中的環境變數 (主要用於本機)
load_dotenv()

# --- 打印環境變數以供除錯 ---
print("\n================== DEBUG: CHECKING ENVIRONMENT VARIABLES ==================")
db_url_set = 'YES' if os.environ.get('DATABASE_URL') else 'NO'
admin_token_set = 'YES' if os.environ.get('ADMIN_TOKEN') else 'NO'
poller_symbols_value = os.environ.get('POLLER_SYMBOLS', '!!! POLLER_SYMBOLS NOT FOUND !!!')
print(f"DATABASE_URL is set: {db_url_set}")
print(f"ADMIN_TOKEN is set: {admin_token_set}")
print(f"POLLER_SYMBOLS value: '{poller_symbols_value}'")
print("=========================================================================\n")
# --- 除錯日誌結束 ---


# --- FastAPI App 初始化 ---
app = FastAPI(
    title="Zeabur MIS Poller (v6)",
    description="""
台灣股市 MIS 輪詢服務，具備以下功能：
- `/`: 健康檢查
- `/config`: 動態更新輪詢設定
- `/summary`: 查詢指定股票**當日**的即時行情
- `/summary/historical`: 查詢指定股票在**特定歷史日期**的行情
    """,
    version="6.0.0"
)

# --- 關鍵修正：將逗號分隔的 symbols 轉換為用 | 分隔 ---
def process_symbols(symbols_str: str) -> str:
    # 將使用者習慣的逗號分隔，轉換為 API 需要的 | 分隔
    return symbols_str.replace(',', '|')

# --- 全域狀態與設定 ---
initial_symbols = os.environ.get('POLLER_SYMBOLS', '')
poller_config = {
    'enabled': os.environ.get('POLLER_ENABLED', 'true').lower() == 'true',
    'symbols': process_symbols(initial_symbols), # 在啟動時就進行轉換
    'poll_seconds': int(os.environ.get('POLLER_SECONDS', 5)),
}
ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN', 'your-secret-token')

# --- 依賴注入 ---
db_url = os.environ.get('DATABASE_URL')
if not db_url:
    print("錯誤：DATABASE_URL 環境變數未設定。")
    sys.exit(1)

db = Database(db_url)
summary_service = SummaryService(db)

# --- 背景任務 ---
def run_pruner():
    """定期清理舊資料的背景任務"""
    while True:
        db.prune_old_data(days_to_keep=60)
        # 睡 24 小時
        time.sleep(86400)

@app.on_event("startup")
def startup_event():
    poller = Poller(poller_config, db)
    poller_thread = threading.Thread(target=poller.run, daemon=True)
    poller_thread.start()
    print("背景輪詢器已啟動。")

    pruner_thread = threading.Thread(target=run_pruner, daemon=True)
    pruner_thread.start()
    print("背景資料清理器已啟動 (每 24 小時執行一次)。")

# --- API 端點 (Endpoints) ---
class ConfigModel(BaseModel):
    enabled: Optional[bool] = None
    symbols: Optional[str] = None # API 接收的仍然是逗號分隔的字串
    poll_seconds: Optional[int] = Field(None, gt=0)

async def verify_token(x_admin_token: str = Header(...)):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing admin token")

@app.get("/")
def health_check():
    return {"status": "ok", "poller_config": poller_config}

@app.put("/config", dependencies=[Depends(verify_token)])
def update_config(config: ConfigModel):
    if config.enabled is not None:
        poller_config['enabled'] = config.enabled
    if config.symbols is not None:
        # 當透過 API 更新時，也進行轉換
        poller_config['symbols'] = process_symbols(config.symbols)
    if config.poll_seconds is not None:
        poller_config['poll_seconds'] = config.poll_seconds
    print(f"設定已更新: {poller_config}")
    return {"status": "success", "new_config": poller_config}

@app.get("/summary")
def get_summary(symbol: str):
    if not symbol:
        raise HTTPException(status_code=400, detail="Query parameter 'symbol' is required.")
    clean_symbol = symbol.split('.')[0]
    summary_data = summary_service.get_summary(clean_symbol)
    return summary_data

@app.get("/summary/historical")
def get_historical_summary(symbol: str, date: str):
    """查詢歷史日期的行情摘要"""
    if not symbol or not date:
        raise HTTPException(status_code=400, detail="Query parameters 'symbol' and 'date' are required.")
    
    try:
        # 驗證日期格式
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

    clean_symbol = symbol.split('.')[0]
    summary_data = summary_service.get_historical_summary(clean_symbol, date)
    return summary_data

# --- 本機測試啟動點 ---
if __name__ == "__main__":
    print("正在以 uvicorn 啟動伺服器...")
    print("請訪問 http://127.0.0.1:8000/docs 查看 API 文件")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
