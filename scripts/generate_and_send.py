"""
Weekly Market Brief — Script d'automatisation v3
Finnhub + FRED → real data → Claude analysis → JSON → PDF → Email
"""

import os, json, base64, re, requests
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO
from pathlib import Path

import anthropic
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, PageBreak, HRFlowable
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER

# ─── Secrets ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
ANTHROPIC_MODEL     = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-6")
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_REFRESH_TOKEN"]
GMAIL_CLIENT_ID     = os.environ["GMAIL_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
EMAIL_FROM          = os.environ["EMAIL_FROM"]
EMAIL_TO            = os.environ["EMAIL_TO"]
FINNHUB_API_KEY     = os.environ["FINNHUB_API_KEY"]
FRED_API_KEY        = os.environ["FRED_API_KEY"]

NOW      = datetime.utcnow()
DATE_FR  = NOW.strftime("%A %d %B %Y")
WEEK_N   = NOW.strftime("S%W")
TODAY    = NOW.strftime("%Y-%m-%d")
NEXT_FRI = (NOW + timedelta(days=(4 - NOW.weekday()) % 7 + 7)).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════════════
#  GMAIL TOKEN
# ══════════════════════════════════════════════════════════════════════════════
def get_fresh_access_token() -> str:
    print("[0/6] Refreshing Gmail token...")
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": GMAIL_CLIENT_ID, "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": GMAIL_REFRESH_TOKEN, "grant_type": "refresh_token",
    })
    if resp.status_code != 200:
        raise RuntimeError(f"Gmail token error: {resp.text}")
    print("[0/6] ✅ Gmail token refreshed")
    return resp.json()["access_token"]


# ══════════════════════════════════════════════════════════════════════════════
#  REAL DATA: FINNHUB + FRED
# ══════════════════════════════════════════════════════════════════════════════
def fetch_finnhub(endpoint: str, params: dict) -> dict:
    params["token"] = FINNHUB_API_KEY
    try:
        r = requests.get(f"https://finnhub.io/api/v1/{endpoint}", params=params, timeout=10)
        return r.json() if r.status_code == 200 else {}
    except Exception as e:
        print(f"  ⚠️ Finnhub {endpoint}: {e}")
        return {}

def fetch_fred(series_id: str) -> dict:
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations", params={
            "series_id": series_id, "api_key": FRED_API_KEY,
            "file_type": "json", "limit": 5, "sort_order": "desc"
        }, timeout=10)
        if r.status_code == 200:
            obs = r.json().get("observations", [])
            return {"series": series_id, "latest": obs[0] if obs else {}, "recent": obs[:5]}
    except Exception as e:
        print(f"  ⚠️ FRED {series_id}: {e}")
    return {}

def fetch_yfinance_data() -> dict:
    """Fetch real market data via yfinance — no API key required."""
    import yfinance as yf
    from datetime import date
    results = {}

    def safe_fetch(ticker_sym, period="5d", interval="1d"):
        try:
            t = yf.Ticker(ticker_sym)
            hist = t.history(period=period, interval=interval, auto_adjust=True)
            if hist.empty:
                return None
            return hist
        except Exception as e:
            print(f"  ⚠️ yfinance {ticker_sym}: {e}")
            return None

    def weekly_stats(ticker_sym, label):
        """Get last close, weekly change, YTD change."""
        try:
            t = yf.Ticker(ticker_sym)
            # Get 1 year of data to compute YTD
            hist = t.history(period="1y", interval="1d", auto_adjust=True)
            if hist.empty:
                return None
            last_close = hist['Close'].iloc[-1]
            # Weekly change: vs 5 trading days ago
            prev_close = hist['Close'].iloc[-6] if len(hist) >= 6 else hist['Close'].iloc[0]
            weekly_chg = (last_close - prev_close) / prev_close * 100
            # YTD change: vs first trading day of the year
            year_start = date(NOW.year, 1, 1)
            ytd_hist = hist[hist.index.date >= year_start]
            if len(ytd_hist) > 1:
                ytd_start = ytd_hist['Close'].iloc[0]
                ytd_chg = (last_close - ytd_start) / ytd_start * 100
            else:
                ytd_chg = 0.0
            # Weekly high/low
            week_data = hist.iloc[-5:] if len(hist) >= 5 else hist
            return {
                "label":      label,
                "ticker":     ticker_sym,
                "close":      round(last_close, 2),
                "weekly_pct": round(weekly_chg, 2),
                "ytd_pct":    round(ytd_chg, 2),
                "week_high":  round(week_data['High'].max(), 2),
                "week_low":   round(week_data['Low'].min(), 2),
                "date":       str(hist.index[-1].date()),
            }
        except Exception as e:
            print(f"  ⚠️ yfinance weekly_stats {ticker_sym}: {e}")
            return None

    # ── Equity indices
    print("  [yf] Fetching equity indices...")
    indices_map = {
        "^GSPC":   "S&P 500",
        "^IXIC":   "NASDAQ",
        "^DJI":    "Dow Jones",
        "^RUT":    "Russell 2000",
        "^FCHI":   "CAC 40",
        "^GDAXI":  "DAX",
        "^FTSE":   "FTSE 100",
        "^STOXX50E":"EuroStoxx 50",
        "^N225":   "Nikkei 225",
        "^HSI":    "Hang Seng",
        "000001.SS":"Shanghai Comp.",
        "^KS11":   "KOSPI",
    }
    indices_data = {}
    for ticker, name in indices_map.items():
        d = weekly_stats(ticker, name)
        if d:
            indices_data[name] = d
            print(f"    ✓ {name}: {d['close']} ({d['weekly_pct']:+.2f}%w / {d['ytd_pct']:+.2f}%ytd)")
    results["indices"] = indices_data

    # ── US Sector ETFs
    print("  [yf] Fetching US sector ETFs...")
    sector_map = {
        "XLK":  "Technology",
        "XLF":  "Financials",
        "XLE":  "Energy",
        "XLV":  "Healthcare",
        "XLI":  "Industrials",
        "XLY":  "Consumer Discr.",
        "XLB":  "Materials",
        "XLU":  "Utilities",
        "XLRE": "Real Estate",
        "XLC":  "Comm. Services",
        "XLP":  "Cons. Staples",
    }
    sectors_data = {}
    for ticker, name in sector_map.items():
        d = weekly_stats(ticker, name)
        if d:
            sectors_data[name] = d
            print(f"    ✓ {name}: {d['weekly_pct']:+.2f}%")
    results["sectors"] = sectors_data

    # ── Forex pairs
    print("  [yf] Fetching forex pairs...")
    forex_map = {
        "EURUSD=X":  "EUR/USD",
        "GBPUSD=X":  "GBP/USD",
        "JPY=X":     "USD/JPY",
        "CHF=X":     "USD/CHF",
        "AUDUSD=X":  "AUD/USD",
        "CNY=X":     "USD/CNY",
        "BRL=X":     "USD/BRL",
        "MXN=X":     "USD/MXN",
        "DX-Y.NYB":  "DXY",
    }
    forex_data = {}
    for ticker, name in forex_map.items():
        d = weekly_stats(ticker, name)
        if d:
            forex_data[name] = d
            print(f"    ✓ {name}: {d['close']} ({d['weekly_pct']:+.2f}%)")
    results["forex"] = forex_data

    # ── Commodities
    print("  [yf] Fetching commodities...")
    commo_map = {
        # Energy
        "CL=F":   ("WTI Crude",    "$/bbl",  "energy"),
        "BZ=F":   ("Brent Crude",  "$/bbl",  "energy"),
        "NG=F":   ("Natural Gas",  "$/MMBtu","energy"),
        "HO=F":   ("Heating Oil",  "$/gal",  "energy"),
        "RB=F":   ("Gasoline RBOB","$/gal",  "energy"),
        # Metals
        "GC=F":   ("Gold",         "$/oz",   "metals"),
        "SI=F":   ("Silver",       "$/oz",   "metals"),
        "HG=F":   ("Copper",       "$/lb",   "metals"),
        "PL=F":   ("Platinum",     "$/oz",   "metals"),
        "PA=F":   ("Palladium",    "$/oz",   "metals"),
        "ALI=F":  ("Aluminum",     "$/t",    "metals"),
        # Agricultural
        "ZW=F":   ("Wheat",        "¢/bu",   "agricultural"),
        "ZC=F":   ("Corn",         "¢/bu",   "agricultural"),
        "ZS=F":   ("Soybeans",     "¢/bu",   "agricultural"),
        "KC=F":   ("Coffee",       "¢/lb",   "agricultural"),
        "SB=F":   ("Sugar",        "¢/lb",   "agricultural"),
        "CT=F":   ("Cotton",       "¢/lb",   "agricultural"),
    }
    commo_data = {"energy": {}, "metals": {}, "agricultural": {}}
    for ticker, (name, unit, cat) in commo_map.items():
        d = weekly_stats(ticker, name)
        if d:
            d["unit"] = unit
            commo_data[cat][name] = d
            print(f"    ✓ {name}: {d['close']} {unit} ({d['weekly_pct']:+.2f}%)")
    results["commodities"] = commo_data

    # ── VIX real value
    print("  [yf] Fetching VIX...")
    vix_d = weekly_stats("^VIX", "VIX")
    if vix_d:
        results["vix"] = vix_d
        print(f"    ✓ VIX: {vix_d['close']} ({vix_d['weekly_pct']:+.2f}%)")

    # ── Yield curve via yfinance (Treasury ETFs for shape context)
    print("  [yf] Fetching yield data...")
    yield_map = {
        "^IRX": "3M",   # 13-week T-bill
        "^FVX": "5Y",
        "^TNX": "10Y",
        "^TYX": "30Y",
    }
    yield_data = {}
    for ticker, mat in yield_map.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d", interval="1d", auto_adjust=True)
            if not hist.empty:
                val = round(hist['Close'].iloc[-1] / 10, 3) if ticker == "^IRX" else round(hist['Close'].iloc[-1], 3)
                yield_data[mat] = val
                print(f"    ✓ US {mat}: {val}%")
        except Exception as e:
            print(f"  ⚠️ yield {ticker}: {e}")
    results["us_yields"] = yield_data

    return results


def collect_real_data() -> dict:
    print("[1/6] Collecting real market data (yfinance + Finnhub + FRED)...")
    data = {}

    # ── yfinance: equity indices, sectors, forex, commodities, VIX, yields
    try:
        data["yf"] = fetch_yfinance_data()
    except Exception as e:
        print(f"  ⚠️ yfinance global error: {e}")
        data["yf"] = {}

    # ── Finnhub: economic calendar
    next_week_end = (NOW + timedelta(days=14)).strftime("%Y-%m-%d")
    eco_cal = fetch_finnhub("calendar/economic", {"from": TODAY, "to": next_week_end})
    events = eco_cal.get("economicCalendar", [])
    high_impact = [e for e in events if e.get("impact") in ("high", "3", 3)][:20]
    all_events  = events[:40]
    data["economic_calendar"] = {
        "high_impact": high_impact,
        "all": all_events,
        "count": len(events)
    }

    # ── Finnhub: earnings calendar
    earnings = fetch_finnhub("calendar/earnings", {"from": TODAY, "to": next_week_end})
    earning_list = earnings.get("earningsCalendar", [])
    majors = ["AAPL","MSFT","AMZN","GOOGL","META","NVDA","TSLA","JPM","GS","MS",
              "BAC","WMT","HD","V","MA","UNH","JNJ","PFE","XOM","CVX",
              "LVMH.PA","MC.PA","SAP","ASML","TTE.PA","SAN.MC","BNP.PA"]
    major_earnings = [e for e in earning_list if e.get("symbol") in majors]
    data["earnings_calendar"] = {
        "major": major_earnings[:15],
        "all": earning_list[:30],
        "count": len(earning_list)
    }

    # ── FRED: macro US
    print("  [FRED] Fetching macro data...")
    fred_series = {
        "fed_funds_rate":    "FEDFUNDS",
        "unemployment":      "UNRATE",
        "cpi_yoy":           "CPIAUCSL",
        "10y_treasury":      "DGS10",
        "2y_treasury":       "DGS2",
        "3m_treasury":       "DTB3",
        "30y_mortgage":      "MORTGAGE30US",
        "gdp_growth":        "A191RL1Q225SBEA",
        "m2_money_supply":   "M2SL",
        "credit_spreads_hy": "BAMLH0A0HYM2",
        "vix_fred":          "VIXCLS",
        "10y_breakeven":     "T10YIE",
        "yield_spread_2s10s":"T10Y2Y",
    }
    fred_data = {}
    for name, series_id in fred_series.items():
        result = fetch_fred(series_id)
        if result.get("latest"):
            fred_data[name] = {
                "value": result["latest"].get("value"),
                "date":  result["latest"].get("date"),
                "series": series_id
            }
    data["fred"] = fred_data

    # ── Finnhub: 6 US ETF quotes (for sentiment check)
    symbols_quotes = {}
    for sym in ["SPY", "QQQ", "IWM", "GLD", "TLT", "USO"]:
        q = fetch_finnhub("quote", {"symbol": sym})
        if q.get("c"):
            symbols_quotes[sym] = {
                "price":  q.get("c"),
                "change": q.get("d"),
                "pct":    q.get("dp"),
                "prev":   q.get("pc")
            }
    data["etf_quotes"] = symbols_quotes

    # ── Finnhub: news sentiment
    data["market_sentiment"] = fetch_finnhub("news-sentiment", {"symbol": "SPY"})

    yf_keys = list(data.get("yf", {}).keys())
    print(f"[1/6] ✅ Data collected — indices:{len(data['yf'].get('indices',{}))}, "
          f"sectors:{len(data['yf'].get('sectors',{}))}, "
          f"forex:{len(data['yf'].get('forex',{}))}, "
          f"commo:{sum(len(v) for v in data['yf'].get('commodities',{}).values())}, "
          f"FRED:{len(data['fred'])}, "
          f"eco_cal:{data['economic_calendar']['count']}, "
          f"earnings:{data['earnings_calendar']['count']}")
    return data



# ══════════════════════════════════════════════════════════════════════════════
#  CLAUDE: REPORT GENERATION
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  GMAIL DAILY BRIEFS READER
# ══════════════════════════════════════════════════════════════════════════════
def extract_email_text(payload: dict) -> str:
    """Recursively extract plain text or HTML from a Gmail message payload."""
    import base64 as b64
    mime = payload.get("mimeType","")
    body_data = payload.get("body",{}).get("data","")

    if mime == "text/plain" and body_data:
        try:
            return b64.urlsafe_b64decode(body_data + "==").decode("utf-8", errors="replace")
        except Exception:
            return ""

    # Fallback to HTML stripped
    if mime == "text/html" and body_data:
        try:
            html = b64.urlsafe_b64decode(body_data + "==").decode("utf-8", errors="replace")
            # Very basic HTML strip
            text = re.sub(r'<[^>]+>', ' ', html)
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        except Exception:
            return ""

    # Recurse into multipart
    for part in payload.get("parts", []):
        result = extract_email_text(part)
        if result:
            return result
    return ""


def fetch_daily_briefs_from_gmail(access_token: str) -> str:
    """Read last 7 days of Daily Market Watch emails from Gmail."""
    print("[1b/6] Reading Daily Market Watch emails from Gmail...")
    headers = {"Authorization": f"Bearer {access_token}"}
    week_ago = (NOW - timedelta(days=8)).strftime("%Y/%m/%d")

    # Try both label formats Gmail might use
    queries = [
        f"label:Trading/Daily-Market-Watch after:{week_ago}",
        f"label:Daily-Market-Watch after:{week_ago}",
        f'subject:"Daily Market Watch" after:{week_ago}',
    ]

    messages = []
    for query in queries:
        resp = requests.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers=headers,
            params={"q": query, "maxResults": 10},
            timeout=15,
        )
        if resp.status_code == 200:
            found = resp.json().get("messages", [])
            if found:
                print(f"  Found {len(found)} emails with query: {query[:60]}")
                messages = found
                break

    if not messages:
        print("  ⚠️ No Daily Market Watch emails found — continuing without them")
        return ""

    briefs = []
    for msg_ref in messages[:7]:  # max 7 emails (one per day)
        try:
            msg_resp = requests.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_ref['id']}",
                headers=headers,
                params={"format": "full"},
                timeout=15,
            )
            if msg_resp.status_code != 200:
                continue
            msg = msg_resp.json()

            # Extract headers
            subject = date_str = ""
            for h in msg.get("payload", {}).get("headers", []):
                if h["name"] == "Subject": subject  = h["value"]
                if h["name"] == "Date":    date_str = h["value"]

            body = extract_email_text(msg.get("payload", {}))
            if body:
                # Truncate to ~3000 chars per email to avoid token overload
                body_trunc = body[:3000] + ("..." if len(body) > 3000 else "")
                briefs.append(f"--- {subject} ({date_str}) ---\n{body_trunc}")
                print(f"  ✓ {subject[:60]} ({len(body)} chars)")
        except Exception as e:
            print(f"  ⚠️ Error reading email: {e}")
            continue

    if not briefs:
        return ""

    result = "\n\n".join(briefs)
    print(f"[1b/6] ✅ {len(briefs)} daily briefs loaded ({len(result)} chars total)")
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  BUILD REAL NUMBERS SKELETON — Python builds all numerical fields
#  Claude will NEVER touch prices, percentages or financial values
# ══════════════════════════════════════════════════════════════════════════════
def build_real_data_skeleton(real_data: dict) -> dict:
    """Build the complete numerical skeleton from fetched data.
    Claude will only fill text/narrative fields — never numbers."""
    yf   = real_data.get("yf", {})
    fred = real_data.get("fred", {})

    def fmt(v, decimals=2, suffix=""):
        if v is None: return "N/A"
        if isinstance(v, float): return f"{v:.{decimals}f}{suffix}"
        return f"{v}{suffix}"

    def pct(v):
        if v is None: return "N/A"
        sign = "+" if float(v) >= 0 else ""
        return f"{sign}{float(v):.2f}%"

    def direction(v):
        if v is None: return "neutral"
        return "up" if float(v) >= 0 else "down"

    # ── Index name map: yfinance ticker → display name
    idx_map = {
        "S&P 500":       "^GSPC",
        "NASDAQ":        "^IXIC",
        "Dow Jones":     "^DJI",
        "Russell 2000":  "^RUT",
        "CAC 40":        "^FCHI",
        "DAX":           "^GDAXI",
        "FTSE 100":      "^FTSE",
        "EuroStoxx 50":  "^STOXX50E",
        "Nikkei 225":    "^N225",
        "Hang Seng":     "^HSI",
        "Shanghai Comp.":"000001.SS",
        "KOSPI":         "^KS11",
    }
    indices_yf = yf.get("indices", {})

    indices = []
    for display_name, ticker in idx_map.items():
        d = indices_yf.get(display_name)
        if d:
            indices.append({
                "name":   display_name,
                "value":  fmt(d["close"], 2),
                "change": pct(d["weekly_pct"]),
                "ytd":    pct(d["ytd_pct"]),
                "change_num": round(float(d["weekly_pct"]), 2),
            })
        else:
            indices.append({"name": display_name, "value": "N/A", "change": "N/A", "ytd": "N/A", "change_num": 0})

    # ── Sector performance
    sectors_yf = yf.get("sectors", {})
    sector_order = ["Technology","Financials","Energy","Healthcare","Industrials",
                    "Consumer Discr.","Materials","Utilities","Real Estate","Comm. Services","Cons. Staples"]
    sectors = []
    for name in sector_order:
        d = sectors_yf.get(name)
        if d:
            sectors.append({
                "sector":     name,
                "change":     pct(d["weekly_pct"]),
                "direction":  direction(d["weekly_pct"]),
                "change_num": round(float(d["weekly_pct"]), 2),
            })

    # ── Forex pairs
    forex_yf = yf.get("forex", {})
    forex_order = [
        ("EUR/USD","EUR/USD"), ("GBP/USD","GBP/USD"), ("USD/JPY","USD/JPY"),
        ("USD/CHF","USD/CHF"), ("AUD/USD","AUD/USD"), ("USD/CNY","USD/CNY"),
        ("USD/BRL","USD/BRL"), ("USD/MXN","USD/MXN"),
    ]
    dxy = forex_yf.get("DXY", {})
    forex_pairs = []
    for display, key in forex_order:
        d = forex_yf.get(display)
        if d:
            forex_pairs.append({
                "pair":        display,
                "value":       fmt(d["close"], 4),
                "change":      pct(d["weekly_pct"]),
                "change_num":  round(float(d["weekly_pct"]), 2),
                "direction":   direction(d["weekly_pct"]),
                "weekly_high": fmt(d["week_high"], 4),
                "weekly_low":  fmt(d["week_low"], 4),
                "support":     "FILL_TEXT",
                "resistance":  "FILL_TEXT",
                "analysis":    "FILL_TEXT",
            })

    # ── Commodities
    commo_yf = yf.get("commodities", {})
    def build_commo_items(cat_key):
        items = []
        for name, d in commo_yf.get(cat_key, {}).items():
            items.append({
                "name":       name,
                "value":      fmt(d["close"], 2),
                "unit":       d.get("unit", ""),
                "change":     pct(d["weekly_pct"]),
                "change_num": round(float(d["weekly_pct"]), 2),
                "direction":  direction(d["weekly_pct"]),
                "analysis":   "FILL_TEXT",
                "drivers":    ["FILL_TEXT", "FILL_TEXT"],
            })
        return items

    # ── US Yields from FRED + yfinance
    us_yields_yf = yf.get("us_yields", {})
    f_10y = fred.get("10y_treasury", {}).get("value")
    f_2y  = fred.get("2y_treasury",  {}).get("value")
    f_3m  = fred.get("3m_treasury",  {})
    yield_curve_data = [
        {"maturity":"3M",  "us": float(us_yields_yf.get("3M",  f_3m.get("value", 5.3) or 5.3)),  "de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":True},
        {"maturity":"2Y",  "us": float(f_2y  or us_yields_yf.get("2Y",  4.7) or 4.7),  "de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":True},
        {"maturity":"5Y",  "us": float(us_yields_yf.get("5Y",  4.4) or 4.4),  "de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":False},
        {"maturity":"10Y", "us": float(f_10y or us_yields_yf.get("10Y", 4.3) or 4.3), "de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":False},
        {"maturity":"30Y", "us": float(us_yields_yf.get("30Y", 4.5) or 4.5), "de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":False},
    ]

    # ── VIX real value
    vix_d = yf.get("vix", {})
    vix_real = fmt(vix_d.get("close"), 2) if vix_d else "N/A"

    # ── HY spread from FRED
    hy_spread = fred.get("credit_spreads_hy", {}).get("value", "N/A")

    skeleton = {
        "section4_equities": {
            "indices":            indices,
            "sector_performance": sectors,
        },
        "section5_bonds": {
            "yield_curves": {
                "data": yield_curve_data,
            },
            "derivatives_and_options": {
                "vix": vix_real,
            },
            "credit_defaults": {
                "data": [
                    {"category":"US Investment Grade", "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                    {"category":"US High Yield",       "rate":"N/A","trend":"N/A","spread": str(hy_spread),"spread_trend":"N/A"},
                    {"category":"EU Investment Grade", "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                    {"category":"EU High Yield",       "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                    {"category":"EM Sovereigns",       "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                    {"category":"US Mortgages",        "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                    {"category":"US Auto Loans",       "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                    {"category":"US Credit Cards",     "rate":"N/A","trend":"N/A","spread":"N/A","spread_trend":"N/A"},
                ]
            }
        },
        "section6_forex": {
            "dollar_index": {
                "value":  fmt(dxy.get("close"), 2) if dxy else "N/A",
                "change": pct(dxy.get("weekly_pct")) if dxy else "N/A",
            },
            "pairs": forex_pairs,
        },
        "section7_commodities": {
            "energy":       {"items": build_commo_items("energy")},
            "metals":       {"items": build_commo_items("metals")},
            "agricultural": {"items": build_commo_items("agricultural")},
        },
    }
    return skeleton


def inject_real_data(report: dict, skeleton: dict) -> dict:
    """Overwrite Claude's hallucinated numbers with Python-fetched real data."""
    import copy
    result = copy.deepcopy(report)

    # Section 4 — indices and sectors (full overwrite of numerical fields)
    s4_skel = skeleton.get("section4_equities", {})
    s4_rep  = result.setdefault("section4_equities", {})
    # Overwrite index values
    real_idx = {i["name"]: i for i in s4_skel.get("indices", [])}
    for i, idx in enumerate(s4_rep.get("indices", [])):
        name = idx.get("name","")
        if name in real_idx:
            idx.update({k: real_idx[name][k] for k in ["value","change","ytd","change_num"] if k in real_idx[name]})
    # Overwrite sector values
    real_sec = {s["sector"]: s for s in s4_skel.get("sector_performance", [])}
    for sec in s4_rep.get("sector_performance", []):
        name = sec.get("sector","")
        if name in real_sec:
            sec.update({k: real_sec[name][k] for k in ["change","direction","change_num"] if k in real_sec[name]})

    # Section 5 — yield curve data and VIX
    s5_skel = skeleton.get("section5_bonds", {})
    s5_rep  = result.setdefault("section5_bonds", {})
    yc_skel = s5_skel.get("yield_curves", {})
    yc_rep  = s5_rep.setdefault("yield_curves", {})
    if yc_skel.get("data"):
        real_yields = {d["maturity"]: d for d in yc_skel["data"]}
        for d in yc_rep.get("data", []):
            if d.get("maturity") in real_yields:
                d["us"] = real_yields[d["maturity"]]["us"]
    # VIX
    der_skel = s5_skel.get("derivatives_and_options", {})
    der_rep  = s5_rep.setdefault("derivatives_and_options", {})
    if der_skel.get("vix") and der_skel["vix"] != "N/A":
        der_rep["vix"] = der_skel["vix"]
    # Credit spreads
    cd_skel = s5_skel.get("credit_defaults", {}).get("data", [])
    cd_rep  = s5_rep.get("credit_defaults", {}).get("data", [])
    real_cd = {d["category"]: d for d in cd_skel}
    for d in cd_rep:
        if d.get("category") in real_cd:
            real_val = real_cd[d["category"]]
            for k in ["rate","trend","spread","spread_trend"]:
                if real_val.get(k) and real_val[k] != "N/A":
                    d[k] = real_val[k]

    # Section 6 — forex
    s6_skel = skeleton.get("section6_forex", {})
    s6_rep  = result.setdefault("section6_forex", {})
    dxy_skel = s6_skel.get("dollar_index", {})
    dxy_rep  = s6_rep.setdefault("dollar_index", {})
    for k in ["value","change"]:
        if dxy_skel.get(k) and dxy_skel[k] != "N/A":
            dxy_rep[k] = dxy_skel[k]
    real_fx = {p["pair"]: p for p in s6_skel.get("pairs", [])}
    for p in s6_rep.get("pairs", s6_rep.get("forex_pairs", [])):
        name = p.get("pair","")
        if name in real_fx:
            for k in ["value","change","change_num","direction","weekly_high","weekly_low"]:
                if real_fx[name].get(k) is not None:
                    p[k] = real_fx[name][k]

    # Section 7 — commodities
    s7_skel = skeleton.get("section7_commodities", {})
    s7_rep  = result.setdefault("section7_commodities", {})
    for cat in ["energy","metals","agricultural"]:
        real_items = {i["name"]: i for i in s7_skel.get(cat, {}).get("items", [])}
        for item in s7_rep.get(cat, {}).get("items", []):
            name = item.get("name","")
            if name in real_items:
                for k in ["value","unit","change","change_num","direction"]:
                    if real_items[name].get(k) is not None:
                        item[k] = real_items[name][k]

    print("[2b] ✅ Real data injected — all numerical fields overwritten with yfinance/FRED values")
    return result


def generate_report_json(real_data: dict, daily_briefs: str = "") -> dict:
    print(f"[2/6] Calling Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    yf       = real_data.get("yf", {})
    fred     = real_data.get("fred", {})
    eco_cal  = real_data.get("economic_calendar", {})
    earnings = real_data.get("earnings_calendar", {})

    def fmt_pct(v):
        if v is None: return "N/A"
        sign = "+" if float(v) >= 0 else ""
        return f"{sign}{float(v):.2f}%"

    # ── Build rich real-data context for Claude to write ACCURATE prose
    # Indices
    idx_lines = []
    for name, d in yf.get("indices", {}).items():
        idx_lines.append(
            f"  {name}: {d['close']:.2f}  weekly={fmt_pct(d['weekly_pct'])}  "
            f"ytd={fmt_pct(d['ytd_pct'])}  week_hi={d['week_high']}  week_lo={d['week_low']}"
        )

    # Sectors
    sec_lines = []
    for name, d in yf.get("sectors", {}).items():
        sec_lines.append(f"  {name}: weekly={fmt_pct(d['weekly_pct'])}  ytd={fmt_pct(d['ytd_pct'])}")

    # Forex
    fx_lines = []
    for name, d in yf.get("forex", {}).items():
        fx_lines.append(
            f"  {name}: {d['close']:.4f}  weekly={fmt_pct(d['weekly_pct'])}  "
            f"hi={d['week_high']}  lo={d['week_low']}"
        )

    # Commodities
    commo_lines = {"energy": [], "metals": [], "agricultural": []}
    for cat, items in yf.get("commodities", {}).items():
        for name, d in items.items():
            commo_lines[cat].append(
                f"  {name} ({d['unit']}): {d['close']:.2f}  weekly={fmt_pct(d['weekly_pct'])}"
            )

    # VIX
    vix_d   = yf.get("vix", {})
    vix_str = f"{vix_d['close']:.2f} (weekly {fmt_pct(vix_d['weekly_pct'])})" if vix_d else "N/A"

    # Yields
    us_yields = yf.get("us_yields", {})
    yield_lines = [f"  US {m}: {v}%" for m, v in us_yields.items()]
    fred_2y  = fred.get("2y_treasury",  {}).get("value","N/A")
    fred_10y = fred.get("10y_treasury", {}).get("value","N/A")
    fred_3m  = fred.get("3m_treasury",  {}).get("value","N/A")
    if fred_2y  != "N/A": yield_lines.append(f"  US 2Y (FRED): {fred_2y}%")
    if fred_10y != "N/A": yield_lines.append(f"  US 10Y (FRED): {fred_10y}%")
    if fred_3m  != "N/A": yield_lines.append(f"  US 3M (FRED): {fred_3m}%")

    # FRED macro
    fred_lines = [f"  {k}: {v['value']} ({v['date']})" for k, v in fred.items()]

    # Calendars
    eco_json      = json.dumps(eco_cal.get("high_impact", [])[:15], ensure_ascii=False)
    earnings_json = json.dumps(earnings.get("all", [])[:20], ensure_ascii=False)

    # ── Infer market direction from real data for Claude context
    sp500_d   = yf.get("indices", {}).get("S&P 500", {})
    sp500_wk  = sp500_d.get("weekly_pct", 0) or 0
    sp500_ytd = sp500_d.get("ytd_pct", 0) or 0

    idx_block    = "\n".join(idx_lines)    or "  (no data fetched)"
    sec_block    = "\n".join(sec_lines)    or "  (no data fetched)"
    fx_block     = "\n".join(fx_lines)     or "  (no data fetched)"
    en_block     = "\n".join(commo_lines["energy"])      or "  (no data)"
    mt_block     = "\n".join(commo_lines["metals"])      or "  (no data)"
    ag_block     = "\n".join(commo_lines["agricultural"])or "  (no data)"
    yield_block  = "\n".join(yield_lines)  or "  (no data)"
    fred_block   = "\n".join(fred_lines)   or "  (no data)"

    system = (
        f"You are a senior financial analyst (ex-Goldman Sachs, global macro hedge fund). "
        f"Date: {DATE_FR} (Week {WEEK_N}, {NOW.year}). "
        f"You are given REAL market data fetched live this week. "
        f"Your job: write ONLY the analytical TEXT fields (narratives, analysis, body text, headlines, strategies). "
        f"The numerical values (prices, percentages) are already handled — focus on explaining WHY markets moved, "
        f"WHAT the implications are, and WHAT investors should do. "
        f"Your text must directly reference the real numbers provided (e.g. 'the S&P 500 gained X% this week...'). "
        f"ALL output must be in ENGLISH. "
        f"Reply ONLY with valid JSON, no markdown fences."
    )

    briefs_section = f"""
══════════════════════════════════════════════════════
DAILY MARKET WATCH EMAILS — Past 7 days
(Use these as primary source for market events and news)
══════════════════════════════════════════════════════
{daily_briefs}
""" if has_briefs else """
══════════════════════════════════════════════════════
NOTE: No daily briefs available — use web_search tool
to look up this week's key market events before writing.
══════════════════════════════════════════════════════
"""

    user = f"""Write an in-depth weekly market report based on the sources below.

IMPORTANT: Do NOT invent events or news. Base your analysis on:
1. The Daily Market Watch emails below (primary source for events)
2. The real market data (primary source for prices/levels)  
3. Web searches you perform for any gaps or additional context
{briefs_section}

══════════════════════════════════════════════════════
REAL MARKET DATA — THIS WEEK (fetched live via yfinance)
══════════════════════════════════════════════════════

EQUITY INDICES (real closes, weekly & YTD changes):
{idx_block}

US SECTOR ETFs (real weekly performance):
{sec_block}

FOREX (real closes, weekly changes):
{fx_block}

COMMODITIES — ENERGY:
{en_block}

COMMODITIES — METALS:
{mt_block}

COMMODITIES — AGRICULTURAL:
{ag_block}

US TREASURY YIELDS:
{yield_block}

FRED MACRO DATA (US):
{fred_block}

VIX: {vix_str}

ECONOMIC CALENDAR (next week, real Finnhub data):
{eco_json}

EARNINGS CALENDAR (next week, real Finnhub data):
{earnings_json}

══════════════════════════════════════════════════════
YOUR TASK: Write ONLY text/narrative fields in this JSON.
For numerical fields (value, change, ytd, etc.) put placeholder "REAL_DATA" — they will be replaced.
Your text must be grounded in the real numbers above.
══════════════════════════════════════════════════════

JSON to complete (ALL text fields mandatory, minimum 20,000 characters total):
{{
  "week": "Week of {NOW.strftime('%B %d')}–{(NOW + timedelta(days=6)).strftime('%B %d, %Y')}",
  "generated_at": "{DATE_FR}",
  "market_temperature": "risk_on|risk_off|neutral",
  "market_temperature_label": "one sentence grounded in real data above",

  "section1_summary": {{
    "title": "Market Overview",
    "paragraphs": [
      "§1 — 5-6 lines referencing actual index levels and weekly moves above",
      "§2 — 5-6 lines on macro context, sector rotation, key drivers",
      "§3 — 5-6 lines on the tone/theme of the week and what it means for investors"
    ]
  }},

  "section2_major_events": {{
    "title": "Major Events",
    "events": [
      {{
        "title": "event title",
        "body": "8-10 lines: causes, mechanisms, market impact — reference real price moves above",
        "sentiment": "positive|negative|neutral",
        "impact_score": 8,
        "affected_assets": ["S&P 500", "US Treasuries"]
      }}
    ]
  }},

  "section3_blind_spots": {{
    "title": "Blind Spots — What the Market Underestimates",
    "events": [
      {{
        "title": "risk or theme title",
        "body": "6-8 lines — underappreciated risk or opportunity",
        "sentiment": "neutral",
        "why_ignored": "reason this is not in consensus"
      }}
    ]
  }},

  "section4_equities": {{
    "title": "Equity Markets",
    "indices": [
      {{"name":"S&P 500","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"NASDAQ","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"Dow Jones","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"Russell 2000","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"CAC 40","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"DAX","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"FTSE 100","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"EuroStoxx 50","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"Nikkei 225","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"Hang Seng","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"Shanghai Comp.","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}},
      {{"name":"KOSPI","value":"REAL_DATA","change":"REAL_DATA","ytd":"REAL_DATA","change_num":0}}
    ],
    "sector_performance": [
      {{"sector":"Technology","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Financials","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Energy","change":"REAL_DATA","direction":"down","change_num":0}},
      {{"sector":"Healthcare","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Industrials","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Consumer Discr.","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Materials","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Utilities","change":"REAL_DATA","direction":"down","change_num":0}},
      {{"sector":"Real Estate","change":"REAL_DATA","direction":"down","change_num":0}},
      {{"sector":"Comm. Services","change":"REAL_DATA","direction":"up","change_num":0}},
      {{"sector":"Cons. Staples","change":"REAL_DATA","direction":"down","change_num":0}}
    ],
    "us":   {{"headline":"headline referencing real S&P/NASDAQ moves","body":"10-12 lines referencing actual index levels and % moves","direction":"bullish|bearish|neutral","key_driver":"main driver this week","risk":"main risk"}},
    "eu":   {{"headline":"headline referencing real CAC/DAX moves","body":"10-12 lines","direction":"bullish|bearish|neutral","key_driver":"...","risk":"..."}},
    "asia": {{"headline":"headline referencing real Nikkei/Hang Seng moves","body":"10-12 lines","direction":"bullish|bearish|neutral","key_driver":"...","risk":"..."}}
  }},

  "section5_bonds": {{
    "title": "Bonds & Rates",
    "macro_context": "5-6 lines using real yield data above — reference specific yield levels",
    "bond_market": {{
      "title": "Global Bond Market",
      "why": "5-6 lines on causes — reference real FRED rates",
      "implies": "5-6 lines implications for investors",
      "strategy": "3-4 lines actionable strategy"
    }},
    "yield_curves": {{
      "title": "Yield Curves",
      "shape": "normal|inverted|flat|humped",
      "interpretation": "4-5 lines — reference real 2Y/10Y levels above",
      "why": "causes of current curve shape", "implies": "investor implications", "strategy": "actionable strategy",
      "data": [
        {{"maturity":"3M","us":0.0,"de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":true}},
        {{"maturity":"2Y","us":0.0,"de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":true}},
        {{"maturity":"5Y","us":0.0,"de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":false}},
        {{"maturity":"10Y","us":0.0,"de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":false}},
        {{"maturity":"30Y","us":0.0,"de":0.0,"fr":0.0,"jp":0.0,"uk":0.0,"short_term":false}}
      ]
    }},
    "derivatives_and_options": {{
      "title": "Derivatives, Options & Futures",
      "vix": "REAL_DATA",
      "vix_interpretation": "4-5 lines interpreting real VIX level {vix_str}",
      "put_call_ratio": "estimate based on real VIX and market context",
      "put_call_interpretation": "4-5 lines",
      "skew": "estimate based on real market conditions",
      "skew_interpretation": "4-5 lines",
      "term_structure": "contango|backwardation|flat",
      "term_structure_interpretation": "4-5 lines",
      "futures_positioning": "5-6 lines COT analysis",
      "market_signal": "bullish|bearish|neutral",
      "market_signal_explanation": "5-6 lines",
      "options_strategy": "4-5 lines",
      "key_levels": [
        {{"asset":"S&P 500","support":"technical level","resistance":"technical level","key_strike":"options level"}},
        {{"asset":"Gold","support":"...","resistance":"...","key_strike":"..."}},
        {{"asset":"EUR/USD","support":"...","resistance":"...","key_strike":"..."}}
      ]
    }},
    "credit_defaults": {{
      "title": "Credit & Default Rates",
      "macro_view": "4-5 lines using real FRED HY spread data",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"category":"US Investment Grade","rate":"estimate","trend":"stable|rising|falling","spread":"estimate","spread_trend":"compression|widening|stable"}},
        {{"category":"US High Yield","rate":"estimate","trend":"...","spread":"REAL_DATA","spread_trend":"..."}},
        {{"category":"EU Investment Grade","rate":"estimate","trend":"...","spread":"estimate","spread_trend":"..."}},
        {{"category":"EU High Yield","rate":"estimate","trend":"...","spread":"estimate","spread_trend":"..."}},
        {{"category":"EM Sovereigns","rate":"estimate","trend":"...","spread":"estimate","spread_trend":"..."}},
        {{"category":"US Mortgages","rate":"estimate","trend":"...","spread":"estimate","spread_trend":"..."}},
        {{"category":"US Auto Loans","rate":"estimate","trend":"...","spread":"estimate","spread_trend":"..."}},
        {{"category":"US Credit Cards","rate":"estimate","trend":"...","spread":"estimate","spread_trend":"..."}}
      ]
    }}
  }},

  "section6_forex": {{
    "title": "Foreign Exchange Markets",
    "dollar_index": {{"value":"REAL_DATA","change":"REAL_DATA","interpretation":"4-5 lines on DXY using real value above"}},
    "narrative": "8-10 lines using real FX data above — reference specific pair moves",
    "pairs": [
      {{"pair":"EUR/USD","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"3-4 lines on drivers referencing real levels","support":"technical","resistance":"technical","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"GBP/USD","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"USD/JPY","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"USD/CHF","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"AUD/USD","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"USD/CNY","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"USD/BRL","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}},
      {{"pair":"USD/MXN","value":"REAL_DATA","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","support":"...","resistance":"...","weekly_high":"REAL_DATA","weekly_low":"REAL_DATA"}}
    ]
  }},

  "section7_commodities": {{
    "title": "Commodities",
    "energy": {{
      "narrative": "8-10 lines referencing real energy prices above",
      "items": [
        {{"name":"WTI Crude","value":"REAL_DATA","unit":"$/bbl","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"3-4 lines on real move","drivers":["driver1","driver2"]}},
        {{"name":"Brent Crude","value":"REAL_DATA","unit":"$/bbl","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Natural Gas","value":"REAL_DATA","unit":"$/MMBtu","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Heating Oil","value":"REAL_DATA","unit":"$/gal","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Gasoline RBOB","value":"REAL_DATA","unit":"$/gal","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}}
      ]
    }},
    "metals": {{
      "narrative": "8-10 lines referencing real metal prices above",
      "items": [
        {{"name":"Gold","value":"REAL_DATA","unit":"$/oz","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"3-4 lines","drivers":["driver1","driver2"]}},
        {{"name":"Silver","value":"REAL_DATA","unit":"$/oz","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Copper","value":"REAL_DATA","unit":"$/lb","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Platinum","value":"REAL_DATA","unit":"$/oz","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Palladium","value":"REAL_DATA","unit":"$/oz","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Aluminum","value":"REAL_DATA","unit":"$/t","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}}
      ]
    }},
    "agricultural": {{
      "narrative": "8-10 lines referencing real agri prices above",
      "items": [
        {{"name":"Wheat","value":"REAL_DATA","unit":"¢/bu","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"3-4 lines","drivers":["driver1","driver2"]}},
        {{"name":"Corn","value":"REAL_DATA","unit":"¢/bu","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Soybeans","value":"REAL_DATA","unit":"¢/bu","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Coffee","value":"REAL_DATA","unit":"¢/lb","change":"REAL_DATA","change_num":0,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Sugar","value":"REAL_DATA","unit":"¢/lb","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Cotton","value":"REAL_DATA","unit":"¢/lb","change":"REAL_DATA","change_num":0,"direction":"down","analysis":"...","drivers":["...","..."]}}
      ]
    }}
  }},

  "section8_synthesis": {{
    "title": "Strategic Synthesis",
    "regime": "risk_on|risk_off|transition|stagflation|goldilocks",
    "regime_description": "5-6 lines grounded in real data above",
    "global_view": "MINIMUM 15 sentences synthesizing all the real data above — indices, rates, forex, commodities, macro",
    "macro_thesis": "6-8 lines forward-looking view based on real data",
    "upcoming_events": "5-6 lines on next week's calendar events (use real calendar data above)",
    "strategy": [
      {{"type":"Tactical Allocation","recommendation":"specific trade idea","rationale":"5-6 lines referencing real data","timeframe":"1-4 weeks","conviction":"high|medium|low"}},
      {{"type":"Hedging","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Long Opportunity","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Short Opportunity","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Risk Management","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}}
    ]
  }},

  "section9_economic_calendar": {{
    "title": "Economic Calendar — Next Week",
    "note": "Source: Finnhub",
    "events": [
      {{"date":"day month","time":"HH:MM","country":"US","flag":"🇺🇸","event":"event name from calendar data above","impact":"high|medium|low","previous":"from data","forecast":"consensus estimate","context":"2-3 lines why this matters"}}
    ]
  }},

  "section10_earnings_calendar": {{
    "title": "Earnings Calendar — Next Week",
    "note": "Source: Finnhub",
    "earnings": [
      {{"date":"day month","symbol":"TICK","company":"Company Name","sector":"sector","timing":"before open|after close","eps_estimate":"$X.XX","revenue_estimate":"$XXB","context":"2-3 lines on key issues","impact_potential":"high|medium|low"}}
    ]
  }}
}}

Write detailed, professional analyses. Total must exceed 20,000 characters.
"""

    for attempt in range(3):
        try:
            raw = ""
            with client.messages.stream(
                model=ANTHROPIC_MODEL, max_tokens=32000,
                system=system,
                messages=[{"role":"user","content":user}],
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
            ) as stream:
                for text in stream.text_stream:
                    raw += text
            print(f"[2/6] Streaming complete — {len(raw)} characters")
            match = re.search(r'\{[\s\S]*\}', raw)
            if not match:
                print(f"[2/6] ⚠️ No JSON found (attempt {attempt+1}/3)")
                continue
            json_str = match.group()
            try:
                report = json.loads(json_str)
                print("[2/6] ✅ JSON report generated")
                return report
            except json.JSONDecodeError as e:
                print(f"[2/6] ⚠️ Invalid JSON (attempt {attempt+1}/3): {e}")
                for end in range(len(json_str)-1, 0, -1):
                    if json_str[end] == '}':
                        try:
                            report = json.loads(json_str[:end+1])
                            print("[2/6] ✅ JSON repaired")
                            return report
                        except:
                            continue
        except Exception as e:
            print(f"[2/6] ⚠️ API error (attempt {attempt+1}/3): {e}")

    raise ValueError("Failed to generate report after 3 attempts")



# ══════════════════════════════════════════════════════════════════════════════
#  SAVE JSON
# ══════════════════════════════════════════════════════════════════════════════
def save_json(report: dict, real_data: dict):
    print("[3/6] Saving JSON...")
    Path("data").mkdir(exist_ok=True)
    # Enrich report with raw data for the web viewer
    output = {**report, "_raw_data": {
        "fred": real_data.get("fred", {}),
        "etf_quotes": real_data.get("etf_quotes", {}),
    }}
    with open("data/latest-report.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("[3/6] ✅ JSON saved")


# ══════════════════════════════════════════════════════════════════════════════
#  PDF GENERATION
# ══════════════════════════════════════════════════════════════════════════════
def generate_pdf(report: dict) -> bytes:
    print("[4/6] Generating PDF...")
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
        rightMargin=15*mm, leftMargin=15*mm,
        topMargin=18*mm, bottomMargin=18*mm)

    # ── Color palette (white background PDF — high contrast)
    DARK   = colors.HexColor('#0D0D1A')   # near-black for body text
    MID    = colors.HexColor('#3A3A52')   # secondary text
    MUTED  = colors.HexColor('#6B6B85')   # muted / footnotes
    GOLD   = colors.HexColor('#9A7A3E')   # section headers / accent
    GOLD2  = colors.HexColor('#B89050')   # lighter gold
    GREEN  = colors.HexColor('#1A7A4A')   # positive
    RED    = colors.HexColor('#C03030')   # negative
    BLUE   = colors.HexColor('#2A5FAA')   # yields / bonds
    AMBER  = colors.HexColor('#B07020')   # neutral / warning
    WHITE  = colors.white
    LBKG   = colors.HexColor('#F5F5FA')   # light background for tables
    LBKG2  = colors.HexColor('#EEEEF6')   # alternating row
    LBDR   = colors.HexColor('#D0D0E0')   # light border
    HBKG   = colors.HexColor('#1A1A2E')   # dark header bg

    PW = 210*mm - 30*mm  # usable page width

    def ps(name, **kw):
        defaults = dict(fontName='Helvetica', fontSize=9, textColor=DARK, leading=13)
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    # ── Shared styles
    S_COVER_TITLE = ps('ct', fontSize=36, fontName='Helvetica-Bold', textColor=DARK,
                       alignment=TA_CENTER, spaceAfter=6, leading=40)
    S_COVER_SUB   = ps('cs', fontSize=12, textColor=GOLD, alignment=TA_CENTER, spaceAfter=4)
    S_COVER_DATE  = ps('cd', fontSize=9,  textColor=MUTED, alignment=TA_CENTER, spaceAfter=16)
    S_COVER_DISC  = ps('cdi',fontSize=7,  textColor=MUTED, alignment=TA_CENTER)
    S_SEC_NUM     = ps('sn', fontSize=8,  fontName='Helvetica-Bold', textColor=GOLD, spaceAfter=1)
    S_SEC_TITLE   = ps('st', fontSize=16, fontName='Helvetica-Bold', textColor=DARK,
                       spaceBefore=0, spaceAfter=4, leading=20)
    S_BODY        = ps('bd', fontSize=8.5,textColor=MID,  leading=13, spaceAfter=5)
    S_BOLD        = ps('bl', fontSize=8.5,fontName='Helvetica-Bold', textColor=DARK, spaceAfter=3)
    S_SMALL       = ps('sm', fontSize=7.5,textColor=MUTED, spaceAfter=2)
    S_ING_LBL     = ps('il', fontSize=7,  fontName='Helvetica-Bold', textColor=GOLD,
                       spaceBefore=6, spaceAfter=2, textTransform='uppercase')

    story = []

    def hr(c=LBDR, t=0.5, sb=4, sa=4):
        return HRFlowable(width='100%', thickness=t, color=c, spaceAfter=sa, spaceBefore=sb)

    def sec(num, title, color=DARK):
        story.append(Spacer(1, 6*mm))
        story.append(Paragraph(f'{str(num).zfill(2)}', S_SEC_NUM))
        story.append(Paragraph(title, S_SEC_TITLE))
        story.append(hr(GOLD, 1.5, 0, 6))

    def tbl(data, cols, zebra=True):
        t = Table(data, colWidths=cols, repeatRows=1)
        style_cmds = [
            ('BACKGROUND',  (0,0), (-1,0),  HBKG),
            ('TEXTCOLOR',   (0,0), (-1,0),  GOLD2),
            ('FONTNAME',    (0,0), (-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',    (0,0), (-1,-1), 7.5),
            ('FONTNAME',    (0,1), (-1,-1), 'Helvetica'),
            ('TEXTCOLOR',   (0,1), (-1,-1), DARK),
            ('GRID',        (0,0), (-1,-1), 0.3, LBDR),
            ('TOPPADDING',  (0,0), (-1,-1), 4),
            ('BOTTOMPADDING',(0,0),(-1,-1), 4),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('RIGHTPADDING',(0,0), (-1,-1), 6),
            ('VALIGN',      (0,0), (-1,-1), 'MIDDLE'),
        ]
        if zebra:
            for i in range(1, len(data), 2):
                style_cmds.append(('BACKGROUND', (0,i), (-1,i), LBKG))
            for i in range(2, len(data), 2):
                style_cmds.append(('BACKGROUND', (0,i), (-1,i), LBKG2))
        t.setStyle(TableStyle(style_cmds))
        return t

    def ingredient(obj, accent=GOLD):
        if not obj: return
        story.append(Spacer(1, 3*mm))
        t_data = [[
            Paragraph('WHY WE ARE HERE',   ps('il1', fontSize=7, fontName='Helvetica-Bold', textColor=accent)),
            Paragraph('WHAT IT IMPLIES',   ps('il2', fontSize=7, fontName='Helvetica-Bold', textColor=accent)),
            Paragraph('STRATEGY',          ps('il3', fontSize=7, fontName='Helvetica-Bold', textColor=accent)),
        ],[
            Paragraph(str(obj.get('why','—')),      S_BODY),
            Paragraph(str(obj.get('implies','—')),  S_BODY),
            Paragraph(str(obj.get('strategy','—')), S_BODY),
        ]]
        t = Table(t_data, colWidths=[PW/3]*3)
        t.setStyle(TableStyle([
            ('BOX',         (0,0), (-1,-1), 0.5, LBDR),
            ('INNERGRID',   (0,0), (-1,-1), 0.3, LBDR),
            ('BACKGROUND',  (0,0), (-1,0),  LBKG),
            ('TOPPADDING',  (0,0), (-1,-1), 5),
            ('BOTTOMPADDING',(0,0),(-1,-1), 5),
            ('LEFTPADDING', (0,0), (-1,-1), 7),
            ('RIGHTPADDING',(0,0), (-1,-1), 7),
            ('VALIGN',      (0,0), (-1,-1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 3*mm))

    def chg_color(s):
        s = str(s or '')
        return GREEN if s.startswith('+') else (RED if s.startswith('-') else AMBER)

    def chg_para(s, style_name):
        return Paragraph(str(s), ps(style_name, fontSize=8, fontName='Helvetica-Bold', textColor=chg_color(s)))

    # ════════════════════════════════
    #  COVER PAGE
    # ════════════════════════════════
    temp = report.get('market_temperature','neutral')
    temp_c = GREEN if temp=='risk_on' else RED if temp=='risk_off' else AMBER
    temp_label = {'risk_on':'RISK ON  ▲','risk_off':'RISK OFF  ▼','neutral':'NEUTRAL  ◆'}.get(temp, temp.upper())

    story.append(Spacer(1, 30*mm))
    # Gold rule top
    story.append(HRFlowable(width='100%', thickness=3, color=GOLD, spaceAfter=14))
    story.append(Paragraph('WEEKLY MARKET BRIEF', S_COVER_TITLE))
    story.append(Paragraph(report.get('week',''), S_COVER_SUB))
    story.append(Paragraph(f"Generated {report.get('generated_at', DATE_FR)}", S_COVER_DATE))
    story.append(HRFlowable(width='100%', thickness=1, color=LBDR, spaceAfter=12))
    # Temp badge as a 1-cell table
    temp_tbl = Table([[Paragraph(temp_label, ps('tb', fontSize=13, fontName='Helvetica-Bold',
        textColor=temp_c, alignment=TA_CENTER))]],
        colWidths=[PW])
    temp_tbl.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,-1), colors.HexColor('#F8F8FF')),
        ('BOX',(0,0),(-1,-1), 1, temp_c),
        ('TOPPADDING',(0,0),(-1,-1), 10),
        ('BOTTOMPADDING',(0,0),(-1,-1), 10),
    ]))
    story.append(temp_tbl)
    story.append(Paragraph(report.get('market_temperature_label',''),
        ps('tll', fontSize=10, textColor=MID, alignment=TA_CENTER, spaceAfter=0, spaceBefore=6)))
    story.append(Spacer(1, 60*mm))
    story.append(HRFlowable(width='100%', thickness=1, color=GOLD, spaceAfter=6))
    story.append(Paragraph(
        "AI-generated report (Claude — Anthropic) · Data sources: yfinance, Finnhub, FRED. Not investment advice.",
        S_COVER_DISC))
    story.append(PageBreak())

    # ════════════════════════════════
    #  S1 — Market Overview
    # ════════════════════════════════
    s1 = report.get('section1_summary',{})
    sec(1, s1.get('title','Market Overview'))
    for p in s1.get('paragraphs',[]): story.append(Paragraph(str(p), S_BODY))

    # ════════════════════════════════
    #  S2 — Major Events
    # ════════════════════════════════
    s2 = report.get('section2_major_events',{})
    sec(2, s2.get('title','Major Events'))
    for ev in s2.get('events',[]):
        sc_color = GREEN if ev.get('sentiment')=='positive' else RED if ev.get('sentiment')=='negative' else GOLD
        score = ev.get('impact_score','')
        stars = '★'*int(score) if score else ''
        story.append(Paragraph(f"{ev.get('title','')}  {stars}", S_BOLD))
        story.append(Paragraph(str(ev.get('body','')), S_BODY))
        assets = ev.get('affected_assets',[])
        if assets:
            story.append(Paragraph(f"Affected assets: {', '.join(assets)}",
                ps('aa', fontSize=7.5, textColor=GOLD, spaceAfter=4)))
        story.append(hr(sc_color, 0.4))

    # ════════════════════════════════
    #  S3 — Blind Spots
    # ════════════════════════════════
    s3 = report.get('section3_blind_spots',{})
    sec(3, s3.get('title','Blind Spots'))
    for ev in s3.get('events',[]):
        story.append(Paragraph(str(ev.get('title','')), S_BOLD))
        story.append(Paragraph(str(ev.get('body','')), S_BODY))
        if ev.get('why_ignored'):
            story.append(Paragraph(f"Why overlooked: {ev['why_ignored']}",
                ps('wi', fontSize=7.5, fontName='Helvetica-Bold', textColor=BLUE, spaceAfter=4)))
        story.append(hr(BLUE, 0.4))

    story.append(PageBreak())

    # ════════════════════════════════
    #  S4 — Equity Markets
    # ════════════════════════════════
    s4 = report.get('section4_equities',{})
    sec(4, s4.get('title','Equity Markets'))
    idx = s4.get('indices',[])
    if idx:
        data = [['Index','Close','Weekly','YTD']]
        for i in idx:
            data.append([
                i.get('name',''), i.get('value',''),
                chg_para(i.get('change',''), 'ic'),
                chg_para(i.get('ytd',''), 'iy'),
            ])
        story.append(tbl(data, [75*mm, 35*mm, 35*mm, 35*mm]))
        story.append(Spacer(1,4*mm))

    sectors = s4.get('sector_performance',[])
    if sectors:
        story.append(Paragraph('US Sector Performance',
            ps('sp', fontSize=8, fontName='Helvetica-Bold', textColor=GOLD, spaceAfter=4)))
        data = [['Sector','Weekly Change']]
        for s in sectors:
            data.append([s.get('sector',''), chg_para(s.get('change',''), 'sc2')])
        story.append(tbl(data, [PW*0.65, PW*0.35]))
        story.append(Spacer(1,4*mm))

    for key, label, flag in [('us','United States','🇺🇸'),('eu','Europe','🇪🇺'),('asia','Asia','🌏')]:
        reg = s4.get(key,{})
        if not reg: continue
        dir_c = {'bullish':GREEN,'bearish':RED,'neutral':AMBER}.get(reg.get('direction',''), GOLD)
        story.append(Paragraph(f"{flag} {label}  ·  {reg.get('direction','').upper()}",
            ps('rl', fontSize=8, fontName='Helvetica-Bold', textColor=dir_c, spaceAfter=2)))
        story.append(Paragraph(str(reg.get('headline','')), S_BOLD))
        story.append(Paragraph(str(reg.get('body','')), S_BODY))
        if reg.get('key_driver'):
            story.append(Paragraph(f"Key driver: {reg['key_driver']}",
                ps('kd', fontSize=7.5, textColor=GREEN, spaceAfter=2)))
        if reg.get('risk'):
            story.append(Paragraph(f"Main risk: {reg['risk']}",
                ps('rk', fontSize=7.5, textColor=RED, spaceAfter=6)))

    story.append(PageBreak())

    # ════════════════════════════════
    #  S5 — Bonds & Rates
    # ════════════════════════════════
    s5 = report.get('section5_bonds',{})
    sec(5, s5.get('title','Bonds & Rates'))
    if s5.get('macro_context'):
        story.append(Paragraph(str(s5['macro_context']), S_BODY))

    ingredient(s5.get('bond_market'), GOLD)

    yc = s5.get('yield_curves',{})
    ingredient(yc, BLUE)
    yc_data = yc.get('data',[])
    if yc_data:
        data = [['Maturity','US','DE','FR','JP','UK']]
        for d in yc_data:
            data.append([d.get('maturity',''),
                f"{d.get('us','')}%", f"{d.get('de','')}%",
                f"{d.get('fr','')}%", f"{d.get('jp','')}%",
                f"{d.get('uk','')}%"])
        story.append(tbl(data, [22*mm, 29*mm, 29*mm, 29*mm, 29*mm, 29*mm]))
        story.append(Spacer(1,3*mm))
        shape_labels = {'normal':'Normal','inverted':'Inverted ⚠','flat':'Flat','humped':'Humped',
                        'normale':'Normal','inverted':'Inverted ⚠','inversée':'Inverted ⚠','plate':'Flat','en_bosse':'Humped'}
        if yc.get('shape'):
            story.append(Paragraph(f"Curve shape: {shape_labels.get(yc['shape'], yc['shape'])}",
                ps('ys', fontSize=8, fontName='Helvetica-Bold', textColor=AMBER, spaceAfter=3)))
        if yc.get('interpretation'):
            story.append(Paragraph(str(yc['interpretation']), S_BODY))

    # Derivatives
    der = s5.get('derivatives_and_options',{})
    if der:
        story.append(Spacer(1,5*mm))
        story.append(Paragraph('Derivatives, Options & Futures',
            ps('dh', fontSize=12, fontName='Helvetica-Bold', textColor=DARK, spaceBefore=4, spaceAfter=4)))
        story.append(hr(AMBER, 1, 0, 5))

        vix_val = float(str(der.get('vix',0) or 0).replace(',','.').split()[0] if der.get('vix') else 0)
        vc  = GREEN if vix_val < 15 else (AMBER if vix_val < 25 else RED)
        sig = der.get('market_signal','neutral')
        sc  = GREEN if sig=='bullish' else RED if sig=='bearish' else AMBER

        kpi_data = [[
            Paragraph('VIX',           ps('kl1', fontSize=7, fontName='Helvetica-Bold', textColor=GOLD2, alignment=TA_CENTER)),
            Paragraph('PUT/CALL RATIO', ps('kl2', fontSize=7, fontName='Helvetica-Bold', textColor=GOLD2, alignment=TA_CENTER)),
            Paragraph('SKEW',           ps('kl3', fontSize=7, fontName='Helvetica-Bold', textColor=GOLD2, alignment=TA_CENTER)),
            Paragraph('TERM STRUCTURE', ps('kl4', fontSize=7, fontName='Helvetica-Bold', textColor=GOLD2, alignment=TA_CENTER)),
            Paragraph('SIGNAL',         ps('kl5', fontSize=7, fontName='Helvetica-Bold', textColor=GOLD2, alignment=TA_CENTER)),
        ],[
            Paragraph(str(der.get('vix','—')),            ps('kv1', fontSize=14, fontName='Helvetica-Bold', textColor=vc,   alignment=TA_CENTER)),
            Paragraph(str(der.get('put_call_ratio','—')), ps('kv2', fontSize=14, fontName='Helvetica-Bold', textColor=DARK, alignment=TA_CENTER)),
            Paragraph(str(der.get('skew','—')),            ps('kv3', fontSize=14, fontName='Helvetica-Bold', textColor=DARK, alignment=TA_CENTER)),
            Paragraph(str(der.get('term_structure','—')), ps('kv4', fontSize=11, fontName='Helvetica-Bold', textColor=BLUE, alignment=TA_CENTER)),
            Paragraph(sig.upper(),                         ps('kv5', fontSize=11, fontName='Helvetica-Bold', textColor=sc,   alignment=TA_CENTER)),
        ]]
        kt = Table(kpi_data, colWidths=[PW/5]*5)
        kt.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), HBKG),
            ('BACKGROUND', (0,1), (-1,1), LBKG),
            ('BOX',        (0,0), (-1,-1), 0.5, LBDR),
            ('INNERGRID',  (0,0), (-1,-1), 0.3, LBDR),
            ('TOPPADDING', (0,0), (-1,-1), 7),
            ('BOTTOMPADDING',(0,0),(-1,-1),7),
            ('VALIGN',     (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(kt)
        story.append(Spacer(1,4*mm))

        for label, key in [
            ('VIX Interpretation',        'vix_interpretation'),
            ('Put/Call Analysis',          'put_call_interpretation'),
            ('Volatility Skew',            'skew_interpretation'),
            ('Term Structure',             'term_structure_interpretation'),
            ('Futures Positioning (COT)',  'futures_positioning'),
            ('Synthetic Signal',           'market_signal_explanation'),
            ('Options Strategy',           'options_strategy'),
        ]:
            if der.get(key):
                story.append(Paragraph(label, S_BOLD))
                story.append(Paragraph(str(der[key]), S_BODY))

        kl = der.get('key_levels',[])
        if kl:
            story.append(Paragraph('Key Levels',
                ps('klh', fontSize=8, fontName='Helvetica-Bold', textColor=GOLD, spaceAfter=3)))
            data = [['Asset','Support','Resistance','Key Strike']]
            for k in kl:
                data.append([k.get('asset',''), k.get('support',''),
                    k.get('resistance',''), k.get('key_strike','')])
            story.append(tbl(data, [PW*0.28, PW*0.24, PW*0.24, PW*0.24]))

    ingredient(s5.get('credit_defaults'), RED)
    dd = s5.get('credit_defaults',{}).get('data',[])
    if dd:
        data = [['Category','Default Rate','Trend','Spread (bps)','Spread Δ']]
        for d in dd:
            trend = d.get('trend','')
            tc = RED if trend in ('rising','hausse') else (GREEN if trend in ('falling','baisse') else AMBER)
            st = d.get('spread_trend','')
            sc2 = RED if st in ('widening','widening_fr','élargissement') else (GREEN if st in ('compression',) else AMBER)
            data.append([
                d.get('category',''), f"{d.get('rate','')}%",
                Paragraph(str(trend), ps('tr', fontSize=7.5, fontName='Helvetica-Bold', textColor=tc)),
                str(d.get('spread','')),
                Paragraph(str(st), ps('st2', fontSize=7.5, fontName='Helvetica-Bold', textColor=sc2)),
            ])
        story.append(tbl(data, [60*mm, 25*mm, 28*mm, 30*mm, 37*mm]))

    story.append(PageBreak())

    # ════════════════════════════════
    #  S6 — Forex
    # ════════════════════════════════
    s6 = report.get('section6_forex',{})
    sec(6, s6.get('title','Foreign Exchange Markets'))
    dxy = s6.get('dollar_index',{})
    if dxy.get('value'):
        col = chg_color(dxy.get('change',''))
        story.append(Paragraph(
            f"Dollar Index (DXY): {dxy.get('value','—')}  {dxy.get('change','')}",
            ps('dxy', fontSize=11, fontName='Helvetica-Bold', textColor=col, spaceAfter=4)))
        if dxy.get('interpretation'):
            story.append(Paragraph(str(dxy['interpretation']), S_BODY))

    story.append(Paragraph('FX Market Dynamics', S_BOLD))
    story.append(Paragraph(str(s6.get('narrative', s6.get('forex_narrative','—'))), S_BODY))
    fx = s6.get('pairs', s6.get('forex_pairs',[]))
    if fx:
        data = [['Pair','Rate','Weekly','Support','Resistance']]
        for p in fx:
            data.append([p.get('pair',''), p.get('value',''),
                chg_para(p.get('change',''), 'fc'),
                p.get('support','—'), p.get('resistance','—')])
        story.append(tbl(data, [30*mm, 32*mm, 28*mm, 30*mm, 30*mm + (PW-150*mm)]))
        for p in fx[:5]:
            if p.get('analysis'):
                story.append(Paragraph(f"{p['pair']}: {p['analysis']}",
                    ps('fa', fontSize=8, textColor=MID, spaceAfter=3, leftIndent=6)))
        story.append(Spacer(1,4*mm))

    story.append(PageBreak())

    # ════════════════════════════════
    #  S7 — Commodities
    # ════════════════════════════════
    s7c = report.get('section7_commodities',{})
    sec(7, s7c.get('title','Commodities'))
    for cat_key, cat_icon, cat_name in [
        ('energy',      '⚡', 'Energy'),
        ('metals',      '🪨', 'Metals'),
        ('agricultural','🌾', 'Agricultural'),
    ]:
        cat = s7c.get(cat_key,{})
        if not cat: continue
        story.append(Paragraph(f"{cat_icon}  {cat_name}",
            ps('cl', fontSize=11, fontName='Helvetica-Bold', textColor=DARK, spaceBefore=8, spaceAfter=3)))
        story.append(hr(GOLD, 0.5, 0, 4))
        if cat.get('narrative'):
            story.append(Paragraph(str(cat['narrative']), S_BODY))
        items = cat.get('items',[])
        if items:
            data = [['Asset','Price','Weekly','Key Drivers']]
            for item in items:
                drivers = ' · '.join(item.get('drivers',[])[:3]) if item.get('drivers') else '—'
                data.append([
                    item.get('name',''),
                    f"{item.get('value','')} {item.get('unit','')}",
                    chg_para(item.get('change',''), 'ic2'),
                    Paragraph(drivers, ps('dr2', fontSize=7.5, textColor=MID)),
                ])
            story.append(tbl(data, [35*mm, 32*mm, 24*mm, PW-91*mm]))
            for item in items[:4]:
                if item.get('analysis'):
                    story.append(Paragraph(f"  {item['name']}: {item['analysis']}",
                        ps('ca', fontSize=7.5, textColor=MID, spaceAfter=2, leftIndent=6)))
        story.append(Spacer(1,4*mm))

    story.append(PageBreak())

    # ════════════════════════════════
    #  S8 — Strategic Synthesis
    # ════════════════════════════════
    s8 = report.get('section8_synthesis',{})
    sec(8, s8.get('title','Strategic Synthesis'))
    regime = s8.get('regime','neutral')
    regime_labels = {
        'risk_on':'RISK ON','risk_off':'RISK OFF','transition':'TRANSITION',
        'stagflation':'STAGFLATION','goldilocks':'GOLDILOCKS'
    }
    rc = {'risk_on':GREEN,'risk_off':RED,'stagflation':RED,'goldilocks':GREEN,'transition':AMBER}.get(regime, AMBER)
    story.append(Paragraph(regime_labels.get(regime, regime.upper()),
        ps('rg', fontSize=11, fontName='Helvetica-Bold', textColor=rc, spaceAfter=4)))
    if s8.get('regime_description'):
        story.append(Paragraph(str(s8['regime_description']), S_BODY))
    story.append(Paragraph('Global View', S_BOLD))
    story.append(Paragraph(str(s8.get('global_view','—')), S_BODY))
    story.append(Paragraph('Macro Thesis', S_BOLD))
    story.append(Paragraph(str(s8.get('macro_thesis','—')), S_BODY))
    story.append(Paragraph('Key Events to Watch', S_BOLD))
    story.append(Paragraph(str(s8.get('upcoming_events','—')), S_BODY))
    story.append(Paragraph('Recommended Strategies', S_BOLD))
    for st in s8.get('strategy',[]):
        conv = st.get('conviction','').lower()
        cc = GREEN if conv in ('high','haute') else (AMBER if conv in ('medium','moyenne') else MUTED)
        story.append(Paragraph(
            f"{st.get('type','').upper()}  ·  Conviction: {conv.upper()}  ·  {st.get('timeframe','')}",
            ps('sty', fontSize=8, fontName='Helvetica-Bold', textColor=cc, spaceAfter=1)))
        story.append(Paragraph(str(st.get('recommendation','')), S_BOLD))
        story.append(Paragraph(str(st.get('rationale','')), S_BODY))
        story.append(hr(GREEN, 0.4))

    story.append(PageBreak())

    # ════════════════════════════════
    #  S9 — Economic Calendar
    # ════════════════════════════════
    s9 = report.get('section9_economic_calendar',{})
    sec(9, s9.get('title','Economic Calendar — Next Week'))
    if s9.get('note'):
        story.append(Paragraph(str(s9['note']), S_SMALL))
    cal = s9.get('events',[])
    if cal:
        data = [['Date','Time','Country','Event','Impact','Previous','Forecast']]
        for ev in cal:
            ic = RED if ev.get('impact')=='high' else (AMBER if ev.get('impact')=='medium' else MUTED)
            imp_label = 'HIGH' if ev.get('impact')=='high' else ('MED.' if ev.get('impact')=='medium' else 'LOW')
            data.append([
                ev.get('date',''), ev.get('time',''),
                f"{ev.get('flag','')} {ev.get('country','')}",
                Paragraph(str(ev.get('event','')), ps('ev', fontSize=7.5, textColor=DARK)),
                Paragraph(imp_label, ps('imp', fontSize=7, fontName='Helvetica-Bold', textColor=ic)),
                str(ev.get('previous','—')), str(ev.get('forecast','—')),
            ])
        story.append(tbl(data, [24*mm, 12*mm, 18*mm, 58*mm, 14*mm, 16*mm, 16*mm + (PW-158*mm)]))
        for ev in cal:
            if ev.get('context'):
                story.append(Paragraph(f"  → {ev.get('event','')} : {ev['context']}",
                    ps('ctx', fontSize=7.5, textColor=MUTED, spaceAfter=2, leftIndent=6)))

    # ════════════════════════════════
    #  S10 — Earnings Calendar
    # ════════════════════════════════
    s10 = report.get('section10_earnings_calendar',{})
    sec(10, s10.get('title','Earnings Calendar — Next Week'))
    if s10.get('note'):
        story.append(Paragraph(str(s10['note']), S_SMALL))
    earnings_list = s10.get('earnings',[])
    if earnings_list:
        data = [['Date','Symbol','Company','Sector','Timing','EPS Est.','Revenue Est.','Impact']]
        for e in earnings_list:
            ic = RED if e.get('impact_potential')=='high' else (AMBER if e.get('impact_potential')=='medium' else MUTED)
            data.append([
                e.get('date',''), e.get('symbol',''),
                Paragraph(str(e.get('company','')), ps('co', fontSize=7.5, textColor=DARK)),
                e.get('sector',''), e.get('timing',''),
                e.get('eps_estimate','—'), e.get('revenue_estimate','—'),
                Paragraph(str(e.get('impact_potential','')).upper(),
                    ps('ip', fontSize=7, fontName='Helvetica-Bold', textColor=ic)),
            ])
        story.append(tbl(data, [18*mm, 16*mm, 38*mm, 22*mm, 22*mm, 15*mm, 18*mm, (PW-149*mm)]))
        for e in earnings_list:
            if e.get('context'):
                story.append(Paragraph(f"  {e.get('company','')} ({e.get('symbol','')}): {e['context']}",
                    ps('ec', fontSize=7.5, textColor=MUTED, spaceAfter=2, leftIndent=6)))

    doc.build(story)
    pdf = buffer.getvalue()
    print(f"[4/6] ✅ PDF generated ({len(pdf)//1024} KB)")
    return pdf


def build_email_html(report: dict) -> str:
    s4 = report.get('section4_equities',{})
    s7 = report.get('section8_synthesis',{})
    temp = report.get('market_temperature','neutral')
    temp_c = '#4ade80' if temp=='risk_on' else '#f87171' if temp=='risk_off' else '#fbbf24'
    temp_label = {'risk_on':'RISK ON ▲','risk_off':'RISK OFF ▼','neutral':'NEUTRE ◆'}.get(temp, temp.upper())

    rows = ''.join([
        f'<tr><td style="padding:6px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;font-size:12px;color:#c8c8d8">{i.get("name","")}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;color:#c8c8d8">{i.get("value","")}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;color:{"#4ade80" if (i.get("change","") or "").startswith("+") else "#f87171"}">{i.get("change","")}</td></tr>'
        for i in s4.get('indices',[])[:8]
    ])
    strats = ''.join([
        f'<div style="border-left:2px solid #3db87a;padding:8px 12px;margin:6px 0;background:#0d0d16">'
        f'<div style="color:#3db87a;font-family:monospace;font-size:9px;text-transform:uppercase;margin-bottom:3px">{s.get("type","")} · {s.get("conviction","")}</div>'
        f'<div style="color:#e8e8f0;font-size:13px;font-weight:bold;margin-bottom:3px">{s.get("recommendation","")}</div>'
        f'<div style="color:#6b6b80;font-size:12px;line-height:1.5">{s.get("rationale","")}</div></div>'
        for s in s7.get('strategy',[])
    ])
    paras = ''.join([f'<p style="color:#8888a0;font-size:13px;line-height:1.8;margin:0 0 10px">{p}</p>' for p in report.get('section1_summary',{}).get('paragraphs',[])])

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#07070d;color:#c8c8d8;font-family:Arial,sans-serif;margin:0;padding:20px">
<div style="max-width:640px;margin:0 auto">
  <div style="background:#b8965a;height:3px;margin-bottom:24px"></div>
  <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:16px">
    <div>
      <div style="font-size:9px;color:#b8965a;letter-spacing:3px;text-transform:uppercase;margin-bottom:6px">{report.get('week','')}</div>
      <div style="font-size:28px;font-family:Georgia,serif;color:#e8e8f4">Weekly <span style="color:#b8965a;font-style:italic">Market</span> Brief</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:12px;color:{temp_c};font-family:monospace;font-weight:bold">{temp_label}</div>
      <div style="font-size:10px;color:#525268">{report.get('market_temperature_label','')}</div>
    </div>
  </div>
  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:6px;padding:16px;margin-bottom:16px">
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Weekly Summary</div>
    {paras}
  </div>
  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:6px;padding:16px;margin-bottom:16px">
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Key Indices</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Index</th>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Value</th>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Weekly</th>
      </tr></thead><tbody>{rows}</tbody>
    </table>
  </div>
  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:6px;padding:16px;margin-bottom:16px">
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Weekly Strategies</div>
    {strats}
  </div>
  <p style="color:#2a2a3a;font-size:10px;text-align:center;margin-top:20px">📎 Full report attached as PDF · Finnhub + FRED + Claude API · Not investment advice</p>
  <div style="background:#b8965a;height:2px;margin-top:16px"></div>
</div></body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
#  SEND EMAIL
# ══════════════════════════════════════════════════════════════════════════════
def send_email(report: dict, pdf: bytes, access_token: str):
    print(f"[5/6] Sending email to {EMAIL_TO}...")
    week = report.get('week','Weekly Report')
    msg = MIMEMultipart('mixed')
    msg['From'] = f"MarketBrief <{EMAIL_FROM}>"; msg['To'] = EMAIL_TO
    msg['Subject'] = f"{week} — MarketBrief"
    msg.attach(MIMEText(build_email_html(report), 'html', 'utf-8'))
    pdf_part = MIMEBase('application','pdf')
    pdf_part.set_payload(pdf); encoders.encode_base64(pdf_part)
    pdf_part.add_header('Content-Disposition','attachment',filename=f"marketbrief-{week.replace(' ','-').lower()}.pdf")
    msg.attach(pdf_part)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')
    resp = requests.post('https://gmail.googleapis.com/gmail/v1/users/me/messages/send',
        headers={'Authorization':f'Bearer {access_token}','Content-Type':'application/json'},
        json={'raw': raw})
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Gmail API {resp.status_code}: {resp.text}")
    print(f"[5/6] ✅ Email sent to {EMAIL_TO}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print(f"\n{'='*60}\n  WEEKLY MARKET BRIEF — {DATE_FR}\n{'='*60}\n")
    token        = get_fresh_access_token()
    real_data    = collect_real_data()
    # Read last 7 days of Daily Market Watch emails
    daily_briefs = fetch_daily_briefs_from_gmail(token)
    # Build numerical skeleton BEFORE calling Claude
    skeleton     = build_real_data_skeleton(real_data)
    # Claude generates text/narratives — grounded in real emails + web search
    report       = generate_report_json(real_data, daily_briefs)
    # Overwrite ALL Claude numbers with real fetched values (no hallucinated prices)
    report       = inject_real_data(report, skeleton)
    save_json(report, real_data)
    pdf          = generate_pdf(report)
    send_email(report, pdf, token)
    print(f"\n[6/6] ✅ Brief generated and sent!\n{'='*60}\n")
