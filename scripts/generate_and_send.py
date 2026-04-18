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
def generate_report_json(real_data: dict) -> dict:
    print(f"[2/6] Calling Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    fred    = real_data.get("fred", {})
    eco_cal = real_data.get("economic_calendar", {})
    earnings= real_data.get("earnings_calendar", {})
    etfs    = real_data.get("etf_quotes", {})

    fred_context = "\n".join([f"  - {k}: {v['value']} ({v['date']})" for k, v in fred.items()])
    etf_context  = "\n".join([f"  - {sym}: ${v['price']} ({v['pct']:+.2f}%)" if isinstance(v.get('pct'), float) else f"  - {sym}: ${v['price']}" for sym, v in etfs.items()])
    eco_events_context = json.dumps(eco_cal.get("high_impact", [])[:15], ensure_ascii=False)
    earnings_context   = json.dumps(earnings.get("all", [])[:20], ensure_ascii=False)

    system = (
        f"You are a senior financial analyst (ex-Goldman Sachs, global macro hedge fund). "
        f"You produce an ultra-professional, dense, educational, and actionable weekly market report. "
        f"Date: {DATE_FR} (Week {WEEK_N}, {NOW.year}). "
        f"You have access to real market data below. Use it as the factual base. "
        f"Every section MUST include: root causes, investor implications, concrete strategy. "
        f"Analyses must be long, developed, and educational. "
        f"ALL text in the JSON must be in ENGLISH. "
        f"Reply ONLY with valid JSON, no surrounding text, no markdown fences."
    )

    user = f"""Generate a COMPLETE and IN-DEPTH weekly financial markets report in ENGLISH.

REAL DATA AVAILABLE:
=== FRED (US Macro) ===
{fred_context}

=== ETF Quotes ===
{etf_context}

=== Economic Calendar (real events) ===
{eco_events_context}

=== Earnings Calendar (real) ===
{earnings_context}

KEY INSTRUCTIONS:
- Every analysis must have AT LEAST 4-5 developed paragraphs
- ALWAYS explain root causes and transmission mechanisms
- Include precise figures wherever possible
- Derivatives section: provide ACTUAL numerical estimates for VIX, Put/Call ratio, skew
- Economic calendar: AT LEAST 12 events with context and forecasts
- Earnings calendar: list major companies with context and expectations
- Be didactic: explain concepts for a sophisticated but non-professional-trader audience
- Commodities: segregate by Energy / Metals / Agricultural with narrative per category
- Synthesis global_view: MINIMUM 15-18 lines — this is the centerpiece of the report
- ALL text must be in ENGLISH

Expected JSON (ALL keys mandatory):
{{
  "week": "Week of {NOW.strftime('%B %d')}–{(NOW + timedelta(days=6)).strftime('%B %d, %Y')}",
  "generated_at": "{DATE_FR}",
  "market_temperature": "risk_on|risk_off|neutral",
  "market_temperature_label": "one sentence explanation",

  "section1_summary": {{
    "title": "Market Overview",
    "paragraphs": ["§1 dense 5-6 lines min", "§2 long", "§3 week tone and synthesis"]
  }},

  "section2_major_events": {{
    "title": "Major Events",
    "events": [
      {{
        "title": "...",
        "body": "long developed analysis with causes, mechanisms and implications (8-10 lines minimum)",
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
        "title": "...",
        "body": "developed analysis (6-8 lines min)",
        "sentiment": "neutral",
        "why_ignored": "reason this is under-covered"
      }}
    ]
  }},

  "section4_equities": {{
    "title": "Equity Markets",
    "indices": [
      {{"name":"S&P 500","value":"5,650","change":"+1.2%","ytd":"+4.1%"}},
      {{"name":"NASDAQ","value":"17,800","change":"+1.8%","ytd":"+6.2%"}},
      {{"name":"Dow Jones","value":"42,100","change":"+0.9%","ytd":"+2.8%"}},
      {{"name":"Russell 2000","value":"2,050","change":"+0.6%","ytd":"-1.2%"}},
      {{"name":"CAC 40","value":"8,200","change":"+1.1%","ytd":"+9.3%"}},
      {{"name":"DAX","value":"22,800","change":"+2.3%","ytd":"+14.1%"}},
      {{"name":"FTSE 100","value":"8,650","change":"+0.7%","ytd":"+6.5%"}},
      {{"name":"EuroStoxx 50","value":"5,400","change":"+1.5%","ytd":"+10.2%"}},
      {{"name":"Nikkei 225","value":"38,200","change":"-0.4%","ytd":"-5.1%"}},
      {{"name":"Hang Seng","value":"23,400","change":"+2.1%","ytd":"+15.3%"}},
      {{"name":"Shanghai Comp.","value":"3,380","change":"+0.8%","ytd":"+1.2%"}},
      {{"name":"Kospi","value":"2,550","change":"+0.3%","ytd":"-2.4%"}}
    ],
    "sector_performance": [
      {{"sector":"Technology","change":"+2.1%","direction":"up","change_num":2.1}},
      {{"sector":"Financials","change":"+0.8%","direction":"up","change_num":0.8}},
      {{"sector":"Energy","change":"-1.2%","direction":"down","change_num":-1.2}},
      {{"sector":"Healthcare","change":"+0.3%","direction":"up","change_num":0.3}},
      {{"sector":"Industrials","change":"+0.5%","direction":"up","change_num":0.5}},
      {{"sector":"Consumer Discr.","change":"-0.4%","direction":"down","change_num":-0.4}},
      {{"sector":"Materials","change":"+0.7%","direction":"up","change_num":0.7}},
      {{"sector":"Utilities","change":"-0.2%","direction":"down","change_num":-0.2}},
      {{"sector":"Real Estate","change":"-0.6%","direction":"down","change_num":-0.6}},
      {{"sector":"Comm. Services","change":"+1.4%","direction":"up","change_num":1.4}},
      {{"sector":"Cons. Staples","change":"-0.1%","direction":"down","change_num":-0.1}}
    ],
    "us": {{
      "headline": "...",
      "body": "long US analysis: macro drivers, institutional flows, positioning, earnings (10-12 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "main factor in 1 sentence",
      "risk": "main risk in 1 sentence"
    }},
    "eu": {{
      "headline": "...",
      "body": "long EU analysis (10-12 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }},
    "asia": {{
      "headline": "...",
      "body": "long Asia analysis (10-12 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }}
  }},

  "section5_bonds": {{
    "title": "Bonds & Rates",
    "macro_context": "global macro rate context 5-6 lines",
    "bond_market": {{
      "title": "Global Bond Market",
      "why": "detailed explanation of causes (5-6 lines)",
      "implies": "detailed implications for investors (5-6 lines)",
      "strategy": "concrete actionable strategy (3-4 lines)"
    }},
    "yield_curves": {{
      "title": "Yield Curves",
      "shape": "normal|inverted|flat|humped",
      "interpretation": "what the curve shape signals (4-5 lines)",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"maturity":"3M","us":5.3,"de":3.1,"fr":3.4,"jp":0.1,"uk":5.1,"short_term":true}},
        {{"maturity":"6M","us":5.2,"de":3.0,"fr":3.3,"jp":0.1,"uk":5.0,"short_term":true}},
        {{"maturity":"1Y","us":5.0,"de":2.9,"fr":3.2,"jp":0.2,"uk":4.9,"short_term":true}},
        {{"maturity":"2Y","us":4.7,"de":2.8,"fr":3.0,"jp":0.3,"uk":4.6,"short_term":true}},
        {{"maturity":"5Y","us":4.4,"de":2.6,"fr":2.9,"jp":0.5,"uk":4.3,"short_term":false}},
        {{"maturity":"10Y","us":4.3,"de":2.5,"fr":2.8,"jp":0.7,"uk":4.2,"short_term":false}},
        {{"maturity":"30Y","us":4.5,"de":2.7,"fr":3.0,"jp":1.8,"uk":4.6,"short_term":false}}
      ]
    }},
    "derivatives_and_options": {{
      "title": "Derivatives, Options & Futures",
      "vix": "18.4",
      "vix_interpretation": "VIX level analysis and what it signals about risk perception (4-5 lines)",
      "put_call_ratio": "0.82",
      "put_call_interpretation": "put/call ratio analysis and what options positioning reveals (4-5 lines)",
      "skew": "-1.8%",
      "skew_interpretation": "volatility skew analysis and implications (4-5 lines)",
      "term_structure": "contango|backwardation|flat",
      "term_structure_interpretation": "term structure analysis and what it reveals (4-5 lines)",
      "futures_positioning": "fund positioning on futures analysis (COT report, longs/shorts) (5-6 lines)",
      "market_signal": "bullish|bearish|neutral",
      "market_signal_explanation": "synthetic signal based on all derivatives (5-6 lines)",
      "options_strategy": "recommended options strategy for current environment (4-5 lines)",
      "key_levels": [
        {{"asset": "S&P 500", "support": "5,480", "resistance": "5,720", "key_strike": "5,600"}},
        {{"asset": "Gold", "support": "2,880", "resistance": "3,050", "key_strike": "3,000"}},
        {{"asset": "EUR/USD", "support": "1.0650", "resistance": "1.0950", "key_strike": "1.0800"}}
      ]
    }},
    "credit_defaults": {{
      "title": "Credit & Default Rates",
      "macro_view": "macro credit market view (4-5 lines)",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"category":"US Investment Grade","rate":"0.8","trend":"stable","spread":"120","spread_trend":"compression"}},
        {{"category":"US High Yield","rate":"3.2","trend":"rising","spread":"380","spread_trend":"widening"}},
        {{"category":"EU Investment Grade","rate":"0.6","trend":"stable","spread":"95","spread_trend":"compression"}},
        {{"category":"EU High Yield","rate":"2.8","trend":"stable","spread":"340","spread_trend":"stable"}},
        {{"category":"EM Sovereigns","rate":"4.1","trend":"rising","spread":"450","spread_trend":"widening"}},
        {{"category":"US Mortgages","rate":"1.1","trend":"stable","spread":"180","spread_trend":"stable"}},
        {{"category":"US Auto Loans","rate":"2.4","trend":"rising","spread":"220","spread_trend":"widening"}},
        {{"category":"US Credit Cards","rate":"4.1","trend":"rising","spread":"500","spread_trend":"widening"}}
      ]
    }}
  }},

  "section6_forex": {{
    "title": "Foreign Exchange Markets",
    "dollar_index": {{"value":"104.2","change":"-0.4%","interpretation":"DXY analysis and implications for global markets (4-5 lines)"}},
    "narrative": "in-depth analysis of global FX dynamics: dollar, rate differentials, central bank policy divergence, carry trades, geopolitical flows (8-10 lines)",
    "pairs": [
      {{"pair":"EUR/USD","value":"1.0820","change":"+0.3%","change_num":0.3,"direction":"up","analysis":"detailed context and drivers including ECB/Fed divergence, eurozone growth (3-4 lines)","support":"1.0650","resistance":"1.0950","weekly_high":"1.0890","weekly_low":"1.0750"}},
      {{"pair":"GBP/USD","value":"1.2950","change":"-0.1%","change_num":-0.1,"direction":"down","analysis":"...","support":"1.2800","resistance":"1.3100","weekly_high":"...","weekly_low":"..."}},
      {{"pair":"USD/JPY","value":"148.50","change":"+0.6%","change_num":0.6,"direction":"up","analysis":"...","support":"146.00","resistance":"151.00","weekly_high":"...","weekly_low":"..."}},
      {{"pair":"USD/CHF","value":"0.8920","change":"+0.2%","change_num":0.2,"direction":"up","analysis":"...","support":"0.8800","resistance":"0.9050","weekly_high":"...","weekly_low":"..."}},
      {{"pair":"AUD/USD","value":"0.6280","change":"-0.5%","change_num":-0.5,"direction":"down","analysis":"...","support":"0.6150","resistance":"0.6400","weekly_high":"...","weekly_low":"..."}},
      {{"pair":"USD/CNY","value":"7.2350","change":"+0.1%","change_num":0.1,"direction":"up","analysis":"...","support":"7.1800","resistance":"7.3000","weekly_high":"...","weekly_low":"..."}},
      {{"pair":"USD/BRL","value":"5.8500","change":"+0.8%","change_num":0.8,"direction":"up","analysis":"...","support":"5.7000","resistance":"6.0000","weekly_high":"...","weekly_low":"..."}},
      {{"pair":"USD/MXN","value":"17.20","change":"-0.3%","change_num":-0.3,"direction":"down","analysis":"...","support":"16.80","resistance":"17.60","weekly_high":"...","weekly_low":"..."}}
    ]
  }},

  "section7_commodities": {{
    "title": "Commodities",
    "energy": {{
      "narrative": "in-depth energy market analysis: OPEC+ strategy, geopolitical supply disruptions, demand/consumption trends, refining margins, contango/backwardation in oil futures, natural gas seasonality and storage (8-10 lines)",
      "items": [
        {{"name":"WTI Crude","value":"71.50","unit":"$/bbl","change":"-1.2%","change_num":-1.2,"direction":"down","analysis":"3-4 line analysis","drivers":["OPEC+ compliance","US strategic reserve","demand China"]}},
        {{"name":"Brent Crude","value":"75.20","unit":"$/bbl","change":"-0.9%","change_num":-0.9,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Natural Gas","value":"2.45","unit":"$/MMBtu","change":"+3.1%","change_num":3.1,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Heating Oil","value":"2.58","unit":"$/gal","change":"-0.8%","change_num":-0.8,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Gasoline RBOB","value":"2.12","unit":"$/gal","change":"+0.5%","change_num":0.5,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Uranium","value":"72.00","unit":"$/lb","change":"+2.3%","change_num":2.3,"direction":"up","analysis":"...","drivers":["...","..."]}}
      ]
    }},
    "metals": {{
      "narrative": "in-depth metals analysis: gold as safe haven and rate correlation, silver dual industrial/monetary role, copper as economic barometer (China PMI, infrastructure), platinum/palladium supply from Russia/South Africa, dollar impact on all metals (8-10 lines)",
      "items": [
        {{"name":"Gold","value":"2,950","unit":"$/oz","change":"+0.8%","change_num":0.8,"direction":"up","analysis":"3-4 line analysis","drivers":["real yields","central bank buying","USD"]}},
        {{"name":"Silver","value":"32.50","unit":"$/oz","change":"+1.2%","change_num":1.2,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Copper","value":"4.32","unit":"$/lb","change":"+2.1%","change_num":2.1,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Platinum","value":"965","unit":"$/oz","change":"-0.3%","change_num":-0.3,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Palladium","value":"950","unit":"$/oz","change":"-1.4%","change_num":-1.4,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Aluminum","value":"2,450","unit":"$/t","change":"+0.9%","change_num":0.9,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Nickel","value":"15,200","unit":"$/t","change":"-2.1%","change_num":-2.1,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Lithium","value":"10.80","unit":"$/kg","change":"-3.2%","change_num":-3.2,"direction":"down","analysis":"...","drivers":["...","..."]}}
      ]
    }},
    "agricultural": {{
      "narrative": "in-depth agricultural analysis: weather patterns (La Nina/El Nino impact), Black Sea corridor and geopolitical grain supply, biofuel demand competing with food use, USDA projections, South American harvest outlook, water scarcity and climate risk (8-10 lines)",
      "items": [
        {{"name":"Wheat","value":"540","unit":"¢/bu","change":"-0.8%","change_num":-0.8,"direction":"down","analysis":"3-4 line analysis","drivers":["Black Sea supply","US drought","demand Asia"]}},
        {{"name":"Corn","value":"465","unit":"¢/bu","change":"+0.4%","change_num":0.4,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Soybeans","value":"985","unit":"¢/bu","change":"+1.1%","change_num":1.1,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Coffee","value":"385","unit":"¢/lb","change":"+4.2%","change_num":4.2,"direction":"up","analysis":"...","drivers":["...","..."]}},
        {{"name":"Sugar","value":"19.80","unit":"¢/lb","change":"-1.3%","change_num":-1.3,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Cotton","value":"68.50","unit":"¢/lb","change":"-0.6%","change_num":-0.6,"direction":"down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Lumber","value":"570","unit":"$/MBF","change":"+2.8%","change_num":2.8,"direction":"up","analysis":"...","drivers":["...","..."]}}
      ]
    }}
  }},

  "section8_synthesis": {{
    "title": "Strategic Synthesis",
    "regime": "risk_on|risk_off|transition|stagflation|goldilocks",
    "regime_description": "detailed description of current market regime (5-6 lines)",
    "global_view": "VERY LONG holistic synthesis: interconnection between rates/equities/dollar/commodities, institutional flows, macro backdrop, policy outlook, positioning, historical analogies, tail risks — MINIMUM 15 sentences, this must be the most comprehensive section of the report",
    "macro_thesis": "main macro thesis for the next 4-8 weeks (6-8 lines)",
    "upcoming_events": "key events to watch next week with context (5-6 lines)",
    "strategy": [
      {{"type":"Tactical Allocation","recommendation":"...","rationale":"developed 5-6 lines","timeframe":"1-4 weeks","conviction":"high|medium|low"}},
      {{"type":"Hedging","recommendation":"...","rationale":"developed 5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Long Opportunity","recommendation":"...","rationale":"developed 5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Short Opportunity","recommendation":"...","rationale":"developed 5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Risk Management","recommendation":"...","rationale":"developed 5-6 lines","timeframe":"...","conviction":"..."}}
    ]
  }},

  "section9_economic_calendar": {{
    "title": "Economic Calendar — Next Week",
    "note": "Sources: Finnhub, Bloomberg consensus",
    "events": [
      {{
        "date": "Monday March 17",
        "time": "08:30",
        "country": "US",
        "flag": "🇺🇸",
        "event": "...",
        "impact": "high|medium|low",
        "previous": "...",
        "forecast": "...",
        "context": "why this indicator matters this week (2-3 lines)"
      }}
    ]
  }},

  "section10_earnings_calendar": {{
    "title": "Earnings Calendar — Next Week",
    "note": "Sources: Finnhub",
    "earnings": [
      {{
        "date": "Monday March 17",
        "symbol": "AAPL",
        "company": "Apple Inc.",
        "sector": "Technology",
        "timing": "before open|after close",
        "eps_estimate": "$2.34",
        "revenue_estimate": "$124.3B",
        "context": "what the market is watching and key issues for this report (2-3 lines)",
        "impact_potential": "high|medium|low"
      }}
    ]
  }}
}}

IMPORTANT: Generate LONG and DEVELOPED analyses. Report must be 20,000–30,000 characters.
Use real FRED and Finnhub data provided as factual base.
ALL TEXT IN ENGLISH."""

    for attempt in range(3):
        try:
            raw = ""
            with client.messages.stream(
                model=ANTHROPIC_MODEL, max_tokens=32000,
                system=system, messages=[{"role":"user","content":user}]
            ) as stream:
                for text in stream.text_stream:
                    raw += text
            print(f"[2/6] Streaming complete — {len(raw)} characters received")
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
    token     = get_fresh_access_token()
    real_data = collect_real_data()
    report    = generate_report_json(real_data)
    save_json(report, real_data)
    pdf       = generate_pdf(report)
    send_email(report, pdf, token)
    print(f"\n[6/6] ✅ Brief generated and sent!\n{'='*60}\n")
