import os
import sys
import threading
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel, Field
from typing import Optional
from dotenv import load_dotenv

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

# --- 新增：啟動時打印環境變數以供除錯 ---
print("\n================== DEBUG: CHECKING ENVIRONMENT VARIABLES ==================")
# 為了安全，我們不直接打印密碼，只檢查是否存在
db_url_set = 'YES' if os.environ.get('DATABASE_URL') else 'NO'
admin_token_set = 'YES' if os.environ.get('ADMIN_TOKEN') else 'NO'
# 我們最關心的 POLLER_SYMBOLS，直接打印出它的值
poller_symbols_value = os.environ.get('POLLER_SYMBOLS', '!!! POLLER_SYMBOLS NOT FOUND !!!')

print(f"DATABASE_URL is set: {db_url_set}")
print(f"ADMIN_TOKEN is set: {admin_token_set}")
print(f"POLLER_SYMBOLS value: '{poller_symbols_value}'") # 用引號包起來，方便看清前後是否有空格
print("=========================================================================\n")
# --- 除錯日誌結束 ---


# --- FastAPI App 初始化 ---
app = FastAPI(
    title="Zeabur MIS Poller (Clean)",
    description="使用 FastAPI 運行的台灣股市 MIS 輪詢服務",
    version="5.0.0"
)

# --- 全域狀態與設定 ---
poller_config = {
    'enabled': os.environ.get('POLLER_ENABLED', 'true').lower() == 'true',
    'symbols': os.environ.get('POLLER_SYMBOLS', ''), # 如果沒找到，預設為空字串
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
@app.on_event("startup")
def startup_event():
    poller = Poller(poller_config, db)
    poller_thread = threading.Thread(target=poller.run, daemon=True)
    poller_thread.start()
    print("背景輪詢器已啟動。")

# --- API 端點 (Endpoints) ---
class ConfigModel(BaseModel):
    enabled: Optional[bool] = None
    symbols: Optional[str] = None
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
        poller_config['symbols'] = config.symbols
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

# --- 本機測試啟動點 ---
if __name__ == "__main__":
    print("正在以 uvicorn 啟動伺服器...")
    print("請訪問 http://127.0.0.1:8000/docs 查看 API 文件")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
