import os
import threading
from flask import Flask, request, jsonify
from werkzeug.exceptions import Unauthorized, BadRequest

from database import Database
from poller import Poller
from services import SummaryService

# --- App Initialization ---
app = Flask(__name__)

# --- Configuration ---
app.config['ADMIN_TOKEN'] = os.environ.get('ADMIN_TOKEN', 'your-secret-token')
app.config['DATABASE_URL'] = os.environ.get('DATABASE_URL', 'mysql+pymysql://user:password@host/dbname')

# In-memory state for poller configuration (thread-safe for simple types in CPython)
app.config['POLLER_CONFIG'] = {
    'enabled': os.environ.get('POLLER_ENABLED', 'true').lower() == 'true',
    'symbols': os.environ.get('POLLER_SYMBOLS', 'tse_2330.tw,otc_6488.tw'),
    'poll_seconds': int(os.environ.get('POLLER_SECONDS', 5)),
}

# --- Database & Services ---
db = Database(app.config['DATABASE_URL'])
summary_service = SummaryService(db)

# --- API Endpoints ---
@app.route('/')
def health_check():
    return jsonify({"status": "ok", "poller_config": app.config['POLLER_CONFIG']})

@app.route('/config', methods=['PUT'])
def update_config():
    # Authentication
    token = request.headers.get('X-Admin-Token')
    if not token or token != app.config['ADMIN_TOKEN']:
        raise Unauthorized("Invalid or missing admin token")

    # Validation
    data = request.get_json()
    if not data:
        raise BadRequest("JSON body required")

    # Update config
    if 'enabled' in data:
        app.config['POLLER_CONFIG']['enabled'] = bool(data['enabled'])
    if 'symbols' in data:
        app.config['POLLER_CONFIG']['symbols'] = str(data['symbols'])
    if 'poll_seconds' in data:
        app.config['POLLER_CONFIG']['poll_seconds'] = int(data['poll_seconds'])

    print(f"Configuration updated: {app.config['POLLER_CONFIG']}")
    return jsonify({"status": "success", "new_config": app.config['POLLER_CONFIG']})

@app.route('/summary', methods=['GET'])
def get_summary():
    symbol = request.args.get('symbol')
    if not symbol:
        raise BadRequest("Query parameter 'symbol' is required.")

    # The user's spec uses pure numbers for symbols, so we strip any .TW suffix
    clean_symbol = symbol.split('.')[0]

    summary_data = summary_service.get_summary(clean_symbol)
    return jsonify(summary_data)

# --- Main Execution ---
if __name__ == '__main__':
    # Start the background poller thread
    poller = Poller(app, db)
    poller_thread = threading.Thread(target=poller.run, daemon=True)
    poller_thread.start()

    # Start the Flask web server
    # For production, use a WSGI server like Gunicorn
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
