from datetime import datetime

def to_float(x):
    try:
        if x is None: return None
        s = str(x).strip()
        if s == "-" or s == "": return None
        return float(s)
    except (ValueError, TypeError):
        return None

def first_px(levels):
    try:
        if not levels or not isinstance(levels, str): return None
        return to_float(levels.split("_")[0])
    except Exception:
        return None

def get_today_date_str():
    return datetime.today().strftime('%Y-%m-%d')
