from datetime import datetime

def to_float(x):
    """Safely convert input to float, returning None on failure or for invalid strings."""
    try:
        if x is None: return None
        s = str(x).strip()
        if s == "-" or s == "": return None
        return float(s)
    except (ValueError, TypeError):
        return None

def first_px(levels):
    """Extract the first price from an underscore-delimited string like '1190_1195_...'."""
    try:
        if not levels or not isinstance(levels, str): return None
        return to_float(levels.split("_")[0])
    except Exception:
        return None

def get_today_date_str():
    """Returns today's date as a 'YYYY-MM-DD' string."""
    return datetime.today().strftime('%Y-%m-%d')
