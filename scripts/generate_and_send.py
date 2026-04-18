"""
Weekly Market Brief — Script d'automatisation v3
Finnhub + FRED → données réelles → Claude analyse → JSON → PDF → Email
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
#  TOKEN GMAIL
# ══════════════════════════════════════════════════════════════════════════════
def get_fresh_access_token() -> str:
    print("[0/6] Renouvellement du token Gmail...")
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": GMAIL_CLIENT_ID, "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": GMAIL_REFRESH_TOKEN, "grant_type": "refresh_token",
    })
    if resp.status_code != 200:
        raise RuntimeError(f"Token Gmail impossible : {resp.text}")
    print("[0/6] ✅ Token Gmail renouvelé")
    return resp.json()["access_token"]


# ══════════════════════════════════════════════════════════════════════════════
#  DONNÉES RÉELLES : FINNHUB + FRED
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
#  CLAUDE : GÉNÉRATION DU RAPPORT
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
#  SAUVEGARDE JSON
# ══════════════════════════════════════════════════════════════════════════════
def save_json(report: dict, real_data: dict):
    print("[3/6] Sauvegarde JSON...")
    Path("data").mkdir(exist_ok=True)
    # Enrichir le rapport avec les données brutes pour le site web
    output = {**report, "_raw_data": {
        "fred": real_data.get("fred", {}),
        "etf_quotes": real_data.get("etf_quotes", {}),
    }}
    with open("data/latest-report.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("[3/6] ✅ JSON sauvegardé")


# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION PDF
# ══════════════════════════════════════════════════════════════════════════════
def generate_pdf(report: dict) -> bytes:
    print("[4/6] Génération du PDF...")
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
        rightMargin=14*mm, leftMargin=14*mm,
        topMargin=20*mm, bottomMargin=20*mm)

    GOLD  = colors.HexColor('#C8A96E')
    LIGHT = colors.HexColor('#E8E8F0')
    MUTED = colors.HexColor('#8888A0')
    GREEN = colors.HexColor('#4ADE80')
    RED   = colors.HexColor('#F87171')
    BLUE  = colors.HexColor('#7B9CFF')
    YELLOW= colors.HexColor('#FBBF24')
    BG    = colors.HexColor('#111118')
    BG2   = colors.HexColor('#0D0D16')

    def ps(name, **kw): return ParagraphStyle(name, **kw)
    S_T  = ps('t',  fontSize=24, textColor=LIGHT, fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=6)
    S_W  = ps('w',  fontSize=10, textColor=GOLD,  fontName='Helvetica', alignment=TA_CENTER, spaceAfter=4)
    S_SB = ps('sb', fontSize=8,  textColor=MUTED, fontName='Helvetica', alignment=TA_CENTER, spaceAfter=20)
    S_SC = ps('sc', fontSize=14, textColor=LIGHT, fontName='Helvetica-Bold', spaceBefore=12, spaceAfter=6)
    S_BD = ps('bd', fontSize=9,  textColor=MUTED, fontName='Helvetica', leading=14, spaceAfter=6)
    S_BL = ps('bl', fontSize=9,  textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=4)
    S_GN = ps('gn', fontSize=8,  textColor=GREEN, fontName='Helvetica-Bold', spaceAfter=2)
    S_D  = ps('d',  fontSize=7,  textColor=MUTED, fontName='Helvetica', alignment=TA_CENTER)

    story = []
    def hr(c=GOLD, t=0.5): return HRFlowable(width='100%', thickness=t, color=c, spaceAfter=5, spaceBefore=2)
    def sec(num, title):
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph(f"{str(num).zfill(2)}  {title}", S_SC))
        story.append(hr())
    def ingredient(obj, c=GOLD):
        if not obj: return
        story.append(Paragraph(obj.get('title',''), ps('it', fontSize=8, textColor=c, fontName='Helvetica-Bold', spaceAfter=3)))
        for label, key in [('Pourquoi','why'),('Ce que ça induit','implies'),('Stratégie','strategy')]:
            story.append(Paragraph(label, S_BL))
            story.append(Paragraph(str(obj.get(key,'—')), S_BD))
    def tbl(data, cols, header_gold=True):
        t = Table(data, colWidths=cols)
        style = [
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',(0,0),(-1,0),GOLD if header_gold else LIGHT),
            ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(-1,-1),7.5),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#111118'),colors.HexColor('#15151E')]),
            ('GRID',(0,0),(-1,-1),0.3,colors.HexColor('#2A2A3A')),
            ('PADDING',(0,0),(-1,-1),5),
            ('TEXTCOLOR',(0,1),(-1,-1),MUTED),
        ]
        t.setStyle(TableStyle(style))
        return t

    # Couverture
    temp = report.get('market_temperature','neutral')
    temp_c = GREEN if temp=='risk_on' else RED if temp=='risk_off' else YELLOW
    temp_label = {'risk_on':'RISK ON ▲','risk_off':'RISK OFF ▼','neutral':'NEUTRE ◆'}.get(temp, temp.upper())

    story += [Spacer(1,25*mm), Paragraph("MARKET BRIEF", S_T),
              Paragraph(report.get('week',''), S_W),
              Paragraph(f"Généré le {report.get('generated_at', DATE_FR)}", S_SB),
              hr(GOLD, 2), Spacer(1,6*mm),
              Paragraph(temp_label, ps('tl', fontSize=14, textColor=temp_c, fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=4)),
              Paragraph(report.get('market_temperature_label',''), ps('tll', fontSize=9, textColor=MUTED, fontName='Helvetica', alignment=TA_CENTER, spaceAfter=20)),
              Spacer(1,40*mm),
              Paragraph("Rapport produit par IA (Claude — Anthropic) · Données : Finnhub, FRED. Non contractuel.", S_D),
              PageBreak()]

    # S1 - Résumé
    s1 = report.get('section1_summary',{})
    sec(1, s1.get('title','Résumé des marchés'))
    for p in s1.get('paragraphs',[]): story.append(Paragraph(str(p), S_BD))

    # S2 - Événements
    s2 = report.get('section2_major_events',{})
    sec(2, s2.get('title','Événements majeurs'))
    for ev in s2.get('events',[]):
        c = GREEN if ev.get('sentiment')=='positive' else RED if ev.get('sentiment')=='negative' else GOLD
        score = ev.get('impact_score', '')
        story.append(Paragraph(f"{ev.get('title','')}  {'★'*int(score) if score else ''}", S_BL))
        story.append(Paragraph(str(ev.get('body','')), S_BD))
        assets = ev.get('affected_assets',[])
        if assets:
            story.append(Paragraph(f"Actifs concernés : {', '.join(assets)}", ps('aa', fontSize=7.5, textColor=GOLD, fontName='Helvetica', spaceAfter=4)))
        story.append(hr(c, 0.3))

    # S3 - Angles morts
    s3 = report.get('section3_blind_spots',{})
    sec(3, s3.get('title','Angles morts'))
    for ev in s3.get('events',[]):
        story.append(Paragraph(str(ev.get('title','')), S_BL))
        story.append(Paragraph(str(ev.get('body','')), S_BD))
        if ev.get('why_ignored'):
            story.append(Paragraph(f"Pourquoi sous-estimé : {ev['why_ignored']}", ps('wi', fontSize=7.5, textColor=BLUE, fontName='Helvetica-Bold', spaceAfter=4)))
        story.append(hr(BLUE, 0.3))

    story.append(PageBreak())

    # S4 - Actions
    s4 = report.get('section4_equities',{})
    sec(4, s4.get('title','Marchés Actions'))
    idx = s4.get('indices',[])
    if idx:
        data = [['Indice','Valeur','Hebdo','YTD']]
        for i in idx:
            c = GREEN if (i.get('change','') or '').startswith('+') else RED
            cy= GREEN if (i.get('ytd','') or '').startswith('+') else RED
            data.append([
                i.get('name',''), i.get('value',''),
                Paragraph(str(i.get('change','')), ps('ic', fontSize=8, textColor=c, fontName='Helvetica-Bold')),
                Paragraph(str(i.get('ytd','')), ps('iy', fontSize=8, textColor=cy, fontName='Helvetica-Bold'))
            ])
        story.append(tbl(data,[62*mm,35*mm,35*mm,35*mm]))
        story.append(Spacer(1,4*mm))

    # Secteurs
    sectors = s4.get('sector_performance',[])
    if sectors:
        story.append(Paragraph('Performance sectorielle', ps('sp', fontSize=8, textColor=GOLD, fontName='Helvetica-Bold', spaceAfter=4)))
        data = [['Secteur','Variation']]
        for s in sectors:
            c = GREEN if s.get('direction')=='up' else RED
            data.append([s.get('sector',''),
                Paragraph(str(s.get('change','')), ps('sc2', fontSize=8, textColor=c, fontName='Helvetica-Bold'))])
        story.append(tbl(data,[110*mm,57*mm]))
        story.append(Spacer(1,4*mm))

    for key, label in [('us','États-Unis'),('eu','Europe'),('asia','Asie')]:
        reg = s4.get(key,{})
        dc = {'bullish':GREEN,'bearish':RED,'neutral':YELLOW}.get(reg.get('direction',''), GOLD)
        story.append(Paragraph(f"{label} · {reg.get('direction','').upper()}", ps('rl', fontSize=8, textColor=dc, fontName='Helvetica-Bold', spaceAfter=1)))
        story.append(Paragraph(str(reg.get('headline','')), S_BL))
        story.append(Paragraph(str(reg.get('body','')), S_BD))
        if reg.get('key_driver'):
            story.append(Paragraph(f"Driver principal : {reg['key_driver']}", ps('kd', fontSize=7.5, textColor=GREEN, fontName='Helvetica', spaceAfter=2)))
        if reg.get('risk'):
            story.append(Paragraph(f"Risque : {reg['risk']}", ps('rk', fontSize=7.5, textColor=RED, fontName='Helvetica', spaceAfter=6)))

    story.append(PageBreak())

    # S5 - Obligations
    s5 = report.get('section5_bonds',{})
    sec(5, s5.get('title','Obligations & Taux'))
    if s5.get('macro_context'):
        story.append(Paragraph(str(s5['macro_context']), S_BD))
    ingredient(s5.get('bond_market'), GOLD)

    yc = s5.get('yield_curves',{})
    ingredient(yc, BLUE)
    yc_data = yc.get('data',[])
    if yc_data:
        data = [['Maturité','US','DE','FR','JP','UK']]
        for d in yc_data:
            data.append([d.get('maturity',''),f"{d.get('us','')}%",f"{d.get('de','')}%",
                f"{d.get('fr','')}%",f"{d.get('jp','')}%",f"{d.get('uk','')}%"])
        story.append(tbl(data,[25*mm,28*mm,28*mm,28*mm,28*mm,28*mm]))
        story.append(Spacer(1,4*mm))
        if yc.get('shape'):
            shape_labels = {'normale':'Courbe normale','inversée':'Courbe inversée ⚠️','plate':'Courbe plate','en_bosse':'Courbe en bosse'}
            story.append(Paragraph(f"Forme : {shape_labels.get(yc['shape'], yc['shape'])}", ps('ys', fontSize=8, textColor=YELLOW, fontName='Helvetica-Bold', spaceAfter=3)))
        if yc.get('interpretation'):
            story.append(Paragraph(str(yc['interpretation']), S_BD))

    # Dérivés
    der = s5.get('derivatives_and_options',{})
    if der:
        story.append(Paragraph('Dérivés, Options & Futures', ps('dh', fontSize=11, textColor=LIGHT, fontName='Helvetica-Bold', spaceBefore=8, spaceAfter=5)))
        story.append(hr(YELLOW, 0.4))

        # VIX row
        vix = float(der.get('vix',0) or 0)
        vc = GREEN if vix<15 else (YELLOW if vix<25 else RED)
        signal = der.get('market_signal','neutral')
        sc = GREEN if signal=='bullish' else RED if signal=='bearish' else YELLOW

        data = [['VIX','Put/Call','Skew','Structure','Signal']]
        data.append([
            Paragraph(str(der.get('vix','—')), ps('vi', fontSize=12, textColor=vc, fontName='Helvetica-Bold')),
            Paragraph(str(der.get('put_call_ratio','—')), ps('pc', fontSize=12, textColor=LIGHT, fontName='Helvetica-Bold')),
            Paragraph(str(der.get('skew','—')), ps('sk', fontSize=12, textColor=LIGHT, fontName='Helvetica-Bold')),
            Paragraph(str(der.get('term_structure','—')), ps('ts', fontSize=10, textColor=BLUE, fontName='Helvetica-Bold')),
            Paragraph(signal.upper(), ps('sig', fontSize=10, textColor=sc, fontName='Helvetica-Bold')),
        ])
        t = Table(data, colWidths=[33*mm]*5)
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',(0,0),(-1,0),GOLD),('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(-1,0),7),
            ('BACKGROUND',(0,1),(-1,-1),colors.HexColor('#0F0F18')),
            ('GRID',(0,0),(-1,-1),0.3,colors.HexColor('#2A2A3A')),
            ('PADDING',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'CENTER'),
        ]))
        story.append(t); story.append(Spacer(1,4*mm))

        for label, key in [
            ('Interprétation VIX','vix_interpretation'),
            ('Analyse Put/Call','put_call_interpretation'),
            ('Analyse du Skew','skew_interpretation'),
            ('Structure à Terme','term_structure_interpretation'),
            ('Positionnement Futures (COT)','futures_positioning'),
            ('Signal de Marché','market_signal_explanation'),
            ('Stratégie d\'Options','options_strategy'),
        ]:
            if der.get(key):
                story.append(Paragraph(label, S_BL))
                story.append(Paragraph(str(der[key]), S_BD))

        # Niveaux clés
        kl = der.get('key_levels',[])
        if kl:
            story.append(Paragraph('Niveaux Clés', ps('kl', fontSize=8, textColor=GOLD, fontName='Helvetica-Bold', spaceAfter=3)))
            data = [['Actif','Support','Résistance','Strike Clé']]
            for k in kl:
                data.append([k.get('asset',''),k.get('support',''),k.get('resistance',''),k.get('key_strike','')])
            story.append(tbl(data,[45*mm,40*mm,40*mm,42*mm]))

    ingredient(s5.get('credit_defaults'), RED)
    dd = s5.get('credit_defaults',{}).get('data',[])
    if dd:
        data = [['Catégorie','Défaut','Tendance','Spread (bps)','Spread ∆']]
        for d in dd:
            tc = RED if d.get('trend')=='hausse' else (GREEN if d.get('trend')=='baisse' else YELLOW)
            sc2= RED if d.get('spread_trend')=='élargissement' else (GREEN if d.get('spread_trend')=='compression' else YELLOW)
            data.append([
                d.get('category',''), f"{d.get('rate','')}%",
                Paragraph(str(d.get('trend','')), ps('tr', fontSize=7.5, textColor=tc, fontName='Helvetica-Bold')),
                str(d.get('spread','')),
                Paragraph(str(d.get('spread_trend','')), ps('st', fontSize=7.5, textColor=sc2, fontName='Helvetica-Bold'))
            ])
        story.append(tbl(data,[55*mm,22*mm,28*mm,28*mm,34*mm]))

    story.append(PageBreak())

    # S6 - Forex
    s6 = report.get('section6_forex',{})
    sec(6, s6.get('title','Foreign Exchange Markets'))
    dxy = s6.get('dollar_index',{})
    if dxy:
        col = GREEN if (dxy.get('change','') or '').startswith('+') else RED
        story.append(Paragraph(f"Dollar Index (DXY): {dxy.get('value','—')}  {dxy.get('change','')}", ps('dxy', fontSize=10, textColor=col, fontName='Helvetica-Bold', spaceAfter=4)))
        if dxy.get('interpretation'):
            story.append(Paragraph(str(dxy['interpretation']), S_BD))
    story.append(Paragraph('FX Market Dynamics', S_BL))
    story.append(Paragraph(str(s6.get('narrative', s6.get('forex_narrative','—'))), S_BD))
    fx = s6.get('pairs', s6.get('forex_pairs',[]))
    if fx:
        data = [['Pair','Value','Change','Support','Resistance']]
        for p in fx:
            col = GREEN if p.get('direction')=='up' else RED
            data.append([p.get('pair',''), p.get('value',''),
                Paragraph(str(p.get('change','')), ps('fc', fontSize=8, textColor=col, fontName='Helvetica-Bold')),
                p.get('support','—'), p.get('resistance','—')])
        story.append(tbl(data,[28*mm,28*mm,28*mm,28*mm,28*mm]))
        for p in fx[:6]:
            if p.get('analysis'):
                story.append(Paragraph(f"{p['pair']}: {p['analysis']}", ps('fa', fontSize=8, textColor=MUTED, fontName='Helvetica', spaceAfter=3, leftIndent=6)))
        story.append(Spacer(1,4*mm))

    story.append(PageBreak())

    # S7 - Commodities
    s7c = report.get('section7_commodities',{})
    sec(7, s7c.get('title','Commodities'))
    for cat_key, cat_label in [('energy','⚡ Energy'),('metals','🪨 Metals'),('agricultural','🌾 Agricultural')]:
        cat = s7c.get(cat_key,{})
        if not cat: continue
        story.append(Paragraph(cat_label, ps('cl', fontSize=11, textColor=GOLD, fontName='Helvetica-Bold', spaceBefore=8, spaceAfter=4)))
        story.append(hr(GOLD, 0.4))
        if cat.get('narrative'):
            story.append(Paragraph(str(cat['narrative']), S_BD))
        items = cat.get('items',[])
        if items:
            data = [['Asset','Value','Change','Key Drivers']]
            for item in items:
                col = GREEN if item.get('direction')=='up' else RED
                drivers = ' · '.join(item.get('drivers',[])) if item.get('drivers') else '—'
                data.append([item.get('name',''), f"{item.get('value','')} {item.get('unit','')}",
                    Paragraph(str(item.get('change','')), ps('ic2', fontSize=8, textColor=col, fontName='Helvetica-Bold')),
                    Paragraph(drivers, ps('dr2', fontSize=7, textColor=MUTED, fontName='Helvetica'))])
            story.append(tbl(data,[32*mm,30*mm,24*mm,81*mm]))
            for item in items[:3]:
                if item.get('analysis'):
                    story.append(Paragraph(f"  {item['name']}: {item['analysis']}", ps('ca', fontSize=8, textColor=MUTED, fontName='Helvetica', spaceAfter=3, leftIndent=6)))
        story.append(Spacer(1,4*mm))

    story.append(PageBreak())

    # S7 - Synthèse
    s7 = report.get('section8_synthesis',{})
    sec(7, s7.get('title','Synthèse Stratégique'))
    regime = s7.get('regime','neutral')
    regime_labels = {
        'risk_on':'RISK ON','risk_off':'RISK OFF','transition':'TRANSITION',
        'stagflation':'STAGFLATION','goldilocks':'GOLDILOCKS'
    }
    rc = {'risk_on':GREEN,'risk_off':RED,'stagflation':RED,'goldilocks':GREEN,'transition':YELLOW}.get(regime, YELLOW)
    story.append(Paragraph(f"Régime : {regime_labels.get(regime, regime.upper())}", ps('rg', fontSize=10, textColor=rc, fontName='Helvetica-Bold', spaceAfter=4)))
    if s7.get('regime_description'):
        story.append(Paragraph(str(s7['regime_description']), S_BD))
    story.append(Paragraph("Vue d'ensemble globale", S_BL))
    story.append(Paragraph(str(s7.get('global_view','—')), S_BD))
    story.append(Paragraph("Thèse macro", S_BL))
    story.append(Paragraph(str(s7.get('macro_thesis','—')), S_BD))
    story.append(Paragraph("À surveiller la semaine prochaine", S_BL))
    story.append(Paragraph(str(s7.get('upcoming_events','—')), S_BD))
    story.append(Paragraph("Stratégies recommandées", S_BL))
    for st in s7.get('strategy',[]):
        conv = st.get('conviction','')
        cc = GREEN if conv=='haute' else (YELLOW if conv=='moyenne' else MUTED)
        story.append(Paragraph(f"{st.get('type','').upper()}  ·  conviction : {conv.upper()}", ps('sty', fontSize=8, textColor=cc, fontName='Helvetica-Bold', spaceAfter=1)))
        story.append(Paragraph(str(st.get('recommendation','')), S_BL))
        story.append(Paragraph(str(st.get('rationale','')), S_BD))
        if st.get('timeframe'):
            story.append(Paragraph(f"Horizon : {st['timeframe']}", ps('tf', fontSize=7.5, textColor=MUTED, fontName='Helvetica', spaceAfter=5)))
        story.append(hr(GREEN, 0.3))

    story.append(PageBreak())

    # S8 - Calendrier économique
    s8 = report.get('section9_economic_calendar',{})
    sec(8, s8.get('title','Economic Calendar'))
    if s8.get('note'):
        story.append(Paragraph(str(s8['note']), ps('n', fontSize=7, textColor=MUTED, fontName='Helvetica', spaceAfter=6)))
    cal = s8.get('events',[])
    if cal:
        data = [['Date','Time','Country','Event','Impact','Previous','Forecast']]
        for ev in cal:
            ic = RED if ev.get('impact')=='high' else (YELLOW if ev.get('impact')=='medium' else MUTED)
            flag = ev.get('flag','')
            data.append([
                ev.get('date',''), ev.get('time',''),
                f"{flag} {ev.get('country','')}",
                Paragraph(str(ev.get('event','')), ps('ev', fontSize=7.5, textColor=LIGHT, fontName='Helvetica')),
                Paragraph('HIGH' if ev.get('impact')=='high' else ('MED.' if ev.get('impact')=='medium' else 'LOW'),
                    ps('imp', fontSize=7, textColor=ic, fontName='Helvetica-Bold')),
                str(ev.get('previous','—')), str(ev.get('forecast','—'))
            ])
        story.append(tbl(data,[22*mm,12*mm,16*mm,60*mm,14*mm,14*mm,14*mm]))
        # Contextes
        for ev in cal:
            if ev.get('context'):
                story.append(Paragraph(f"→ {ev.get('event','')} : {ev['context']}", ps('ctx', fontSize=7.5, textColor=MUTED, fontName='Helvetica', spaceAfter=3, leftIndent=6)))

    # S9 - Calendrier résultats
    s9 = report.get('section10_earnings_calendar',{})
    sec(9, s9.get('title','Earnings Calendar'))
    if s9.get('note'):
        story.append(Paragraph(str(s9['note']), ps('n2', fontSize=7, textColor=MUTED, fontName='Helvetica', spaceAfter=6)))
    earnings_list = s9.get('earnings',[])
    if earnings_list:
        data = [['Date','Symbole','Société','Secteur','Timing','EPS est.','CA est.','Impact']]
        for e in earnings_list:
            ic = RED if e.get('impact_potential')=='high' else (YELLOW if e.get('impact_potential')=='medium' else MUTED)
            data.append([
                e.get('date',''), e.get('symbol',''),
                Paragraph(str(e.get('company','')), ps('co', fontSize=7.5, textColor=LIGHT, fontName='Helvetica')),
                e.get('sector',''), e.get('timing',''),
                e.get('eps_estimate','—'), e.get('revenue_estimate','—'),
                Paragraph(str(e.get('impact_potential','').upper()), ps('ip', fontSize=7, textColor=ic, fontName='Helvetica-Bold'))
            ])
        story.append(tbl(data,[18*mm,16*mm,38*mm,24*mm,22*mm,14*mm,16*mm,14*mm]))
        for e in earnings_list:
            if e.get('context'):
                story.append(Paragraph(f"→ {e.get('company','')} ({e.get('symbol','')}) : {e['context']}", ps('ec', fontSize=7.5, textColor=MUTED, fontName='Helvetica', spaceAfter=3, leftIndent=6)))

    doc.build(story)
    pdf = buffer.getvalue()
    print(f"[4/6] ✅ PDF généré ({len(pdf)//1024} KB)")
    return pdf


# ══════════════════════════════════════════════════════════════════════════════
#  EMAIL HTML
# ══════════════════════════════════════════════════════════════════════════════
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
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Indice</th>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Valeur</th>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Hebdo</th>
      </tr></thead><tbody>{rows}</tbody>
    </table>
  </div>
  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:6px;padding:16px;margin-bottom:16px">
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Weekly Strategies</div>
    {strats}
  </div>
  <p style="color:#2a2a3a;font-size:10px;text-align:center;margin-top:20px">📎 Rapport complet en Full report attached as PDF · Finnhub + FRED + Claude API · Not investment advice</p>
  <div style="background:#b8965a;height:2px;margin-top:16px"></div>
</div></body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
#  ENVOI EMAIL
# ══════════════════════════════════════════════════════════════════════════════
def send_email(report: dict, pdf: bytes, access_token: str):
    print(f"[5/6] Envoi email à {EMAIL_TO}...")
    week = report.get('week','Rapport hebdomadaire')
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
    print(f"[5/6] ✅ Email envoyé à {EMAIL_TO}")


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
    print(f"\n[6/6] ✅ Brief complet envoyé !\n{'='*60}\n")    fred     = real_data.get("fred", {})
    eco_cal  = real_data.get("economic_calendar", {})
    earnings = real_data.get("earnings_calendar", {})
    yf       = real_data.get("yf", {})

    # ── Format real indices for prompt
    def fmt_pct(v): return f"{v:+.2f}%" if isinstance(v, (int,float)) else str(v)

    indices_lines = []
    for name, d in yf.get("indices", {}).items():
        indices_lines.append(f"  {name}: close={d['close']}, weekly={fmt_pct(d['weekly_pct'])}, ytd={fmt_pct(d['ytd_pct'])}, date={d['date']}")

    sectors_lines = []
    for name, d in yf.get("sectors", {}).items():
        sectors_lines.append(f"  {name}: weekly={fmt_pct(d['weekly_pct'])}, ytd={fmt_pct(d['ytd_pct'])}")

    forex_lines = []
    for name, d in yf.get("forex", {}).items():
        forex_lines.append(f"  {name}: close={d['close']}, weekly={fmt_pct(d['weekly_pct'])}, week_high={d['week_high']}, week_low={d['week_low']}")

    commo_lines = {"energy": [], "metals": [], "agricultural": []}
    for cat, items in yf.get("commodities", {}).items():
        for name, d in items.items():
            commo_lines[cat].append(f"  {name} ({d['unit']}): close={d['close']}, weekly={fmt_pct(d['weekly_pct'])}")

    vix_real = yf.get("vix", {})
    vix_line = f"VIX: {vix_real.get('close','n/a')} (weekly {fmt_pct(vix_real.get('weekly_pct',0))})" if vix_real else "VIX: n/a"

    yields = yf.get("us_yields", {})
    yield_lines = [f"  US {m}: {v}%" for m, v in yields.items()]

    fred_lines = [f"  {k}: {v['value']} ({v['date']})" for k, v in fred.items()]
    fred_context = "\n".join(fred_lines)

    eco_events_context = json.dumps(eco_cal.get("high_impact", [])[:15], ensure_ascii=False)
    earnings_context   = json.dumps(earnings.get("all", [])[:20], ensure_ascii=False)

    indices_block    = "\n".join(indices_lines)    or "  (no data)"
    sectors_block    = "\n".join(sectors_lines)    or "  (no data)"
    forex_block      = "\n".join(forex_lines)      or "  (no data)"
    energy_block     = "\n".join(commo_lines["energy"])      or "  (no data)"
    metals_block     = "\n".join(commo_lines["metals"])      or "  (no data)"
    agri_block       = "\n".join(commo_lines["agricultural"])or "  (no data)"
    yields_block     = "\n".join(yield_lines)      or "  (no data)"

    system = (
        f"You are a senior financial analyst (ex-Goldman Sachs, global macro hedge fund). "
        f"You produce an ultra-professional, dense, educational, and actionable weekly market report. "
        f"Date: {DATE_FR} (Week {WEEK_N}, {NOW.year}). "
        f"CRITICAL: You have REAL market data below fetched live from yfinance and FRED. "
        f"You MUST use these EXACT numbers in the JSON output — do NOT change, round differently, or invent any price, percentage, or value. "
        f"Your job is to write analysis and narrative around these real numbers, NOT to generate new numbers. "
        f"If a data point is missing (no data), write 'N/A' for that value. "
        f"ALL text in the JSON must be in ENGLISH. "
        f"Reply ONLY with valid JSON, no surrounding text, no markdown fences."
    )

    user = f"""Generate a COMPLETE and IN-DEPTH weekly financial markets report in ENGLISH.

═══════════════════════════════════════════════════════
REAL MARKET DATA — USE THESE EXACT NUMBERS, DO NOT INVENT
═══════════════════════════════════════════════════════

=== EQUITY INDICES (real closes + weekly/YTD changes) ===
{indices_block}

=== US SECTOR ETFs (real weekly performance) ===
{sectors_block}

=== FOREX (real closes + weekly changes) ===
{forex_block}

=== COMMODITIES — ENERGY ===
{energy_block}

=== COMMODITIES — METALS ===
{metals_block}

=== COMMODITIES — AGRICULTURAL ===
{agri_block}

=== US TREASURY YIELDS (real) ===
{yields_block}

=== FRED MACRO DATA ===
{fred_context}

=== {vix_line} ===

=== ECONOMIC CALENDAR (next week, real Finnhub data) ===
{eco_events_context}

=== EARNINGS CALENDAR (next week, real Finnhub data) ===
{earnings_context}

═══════════════════════════════════════════════════════
STRICT RULES:
1. For every price/value/percentage shown above: copy it EXACTLY into the JSON
2. Do NOT round differently, do NOT use memory values, do NOT invent numbers
3. For missing data (no data above): use "N/A"
4. Write rich, educational ANALYSIS around these real numbers
5. Minimum 20,000 characters total
6. ALL TEXT IN ENGLISH
═══════════════════════════════════════════════════════

Expected JSON structure (ALL keys mandatory):
{{
  "week": "Week of {NOW.strftime('%B %d')}–{(NOW + timedelta(days=6)).strftime('%B %d, %Y')}",
  "generated_at": "{DATE_FR}",
  "market_temperature": "risk_on|risk_off|neutral",
  "market_temperature_label": "one sentence explanation based on the real data above",

  "section1_summary": {{
    "title": "Market Overview",
    "paragraphs": ["§1 dense 5-6 lines", "§2 long", "§3 tone and synthesis — all referencing real data above"]
  }},

  "section2_major_events": {{
    "title": "Major Events",
    "events": [
      {{
        "title": "...",
        "body": "8-10 lines: causes, mechanisms, implications",
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
        "body": "6-8 lines",
        "sentiment": "neutral",
        "why_ignored": "reason this is under-covered"
      }}
    ]
  }},

  "section4_equities": {{
    "title": "Equity Markets",
    "indices": [
      {{"name":"S&P 500",      "value":"USE REAL VALUE FROM DATA ABOVE","change":"USE REAL WEEKLY PCT","ytd":"USE REAL YTD PCT"}},
      {{"name":"NASDAQ",       "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"Dow Jones",    "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"Russell 2000", "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"CAC 40",       "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"DAX",          "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"FTSE 100",     "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"EuroStoxx 50", "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"Nikkei 225",   "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"Hang Seng",    "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"Shanghai Comp.","value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}},
      {{"name":"KOSPI",        "value":"USE REAL","change":"USE REAL","ytd":"USE REAL"}}
    ],
    "sector_performance": [
      {{"sector":"Technology",     "change":"USE REAL WEEKLY PCT FROM XLK","direction":"up|down","change_num": 0.0}},
      {{"sector":"Financials",     "change":"USE REAL FROM XLF","direction":"up|down","change_num": 0.0}},
      {{"sector":"Energy",         "change":"USE REAL FROM XLE","direction":"up|down","change_num": 0.0}},
      {{"sector":"Healthcare",     "change":"USE REAL FROM XLV","direction":"up|down","change_num": 0.0}},
      {{"sector":"Industrials",    "change":"USE REAL FROM XLI","direction":"up|down","change_num": 0.0}},
      {{"sector":"Consumer Discr.","change":"USE REAL FROM XLY","direction":"up|down","change_num": 0.0}},
      {{"sector":"Materials",      "change":"USE REAL FROM XLB","direction":"up|down","change_num": 0.0}},
      {{"sector":"Utilities",      "change":"USE REAL FROM XLU","direction":"up|down","change_num": 0.0}},
      {{"sector":"Real Estate",    "change":"USE REAL FROM XLRE","direction":"up|down","change_num": 0.0}},
      {{"sector":"Comm. Services", "change":"USE REAL FROM XLC","direction":"up|down","change_num": 0.0}},
      {{"sector":"Cons. Staples",  "change":"USE REAL FROM XLP","direction":"up|down","change_num": 0.0}}
    ],
    "us":   {{"headline":"...","body":"10-12 lines using real index values","direction":"bullish|bearish|neutral","key_driver":"...","risk":"..."}},
    "eu":   {{"headline":"...","body":"10-12 lines using real CAC/DAX values","direction":"bullish|bearish|neutral","key_driver":"...","risk":"..."}},
    "asia": {{"headline":"...","body":"10-12 lines using real Nikkei/Hang Seng values","direction":"bullish|bearish|neutral","key_driver":"...","risk":"..."}}
  }},

  "section5_bonds": {{
    "title": "Bonds & Rates",
    "macro_context": "5-6 lines using real FRED yield data",
    "bond_market": {{"title":"Global Bond Market","why":"5-6 lines","implies":"5-6 lines","strategy":"3-4 lines"}},
    "yield_curves": {{
      "title": "Yield Curves",
      "shape": "normal|inverted|flat|humped",
      "interpretation": "4-5 lines interpreting the real yield data above",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"maturity":"3M","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":true}},
        {{"maturity":"6M","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":true}},
        {{"maturity":"1Y","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":true}},
        {{"maturity":"2Y","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":true}},
        {{"maturity":"5Y","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":false}},
        {{"maturity":"10Y","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":false}},
        {{"maturity":"30Y","us": 0.0,"de": 0.0,"fr": 0.0,"jp": 0.0,"uk": 0.0,"short_term":false}}
      ]
    }},
    "derivatives_and_options": {{
      "title": "Derivatives, Options & Futures",
      "vix": "USE REAL VIX VALUE FROM DATA ABOVE",
      "vix_interpretation": "4-5 lines",
      "put_call_ratio": "estimate based on real VIX level and market context",
      "put_call_interpretation": "4-5 lines",
      "skew": "estimate based on real VIX and market direction",
      "skew_interpretation": "4-5 lines",
      "term_structure": "contango|backwardation|flat",
      "term_structure_interpretation": "4-5 lines",
      "futures_positioning": "5-6 lines (COT context)",
      "market_signal": "bullish|bearish|neutral",
      "market_signal_explanation": "5-6 lines",
      "options_strategy": "4-5 lines",
      "key_levels": [
        {{"asset":"S&P 500","support":"...","resistance":"...","key_strike":"..."}},
        {{"asset":"Gold",   "support":"...","resistance":"...","key_strike":"..."}},
        {{"asset":"EUR/USD","support":"...","resistance":"...","key_strike":"..."}}
      ]
    }},
    "credit_defaults": {{
      "title": "Credit & Default Rates",
      "macro_view": "4-5 lines using real FRED HY spread data",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"category":"US Investment Grade","rate":"...","trend":"stable|rising|falling","spread":"...","spread_trend":"compression|widening|stable"}},
        {{"category":"US High Yield","rate":"...","trend":"...","spread":"USE REAL FRED BAMLH0A0HYM2 VALUE","spread_trend":"..."}},
        {{"category":"EU Investment Grade","rate":"...","trend":"...","spread":"...","spread_trend":"..."}},
        {{"category":"EU High Yield","rate":"...","trend":"...","spread":"...","spread_trend":"..."}},
        {{"category":"EM Sovereigns","rate":"...","trend":"...","spread":"...","spread_trend":"..."}},
        {{"category":"US Mortgages","rate":"...","trend":"...","spread":"...","spread_trend":"..."}},
        {{"category":"US Auto Loans","rate":"...","trend":"...","spread":"...","spread_trend":"..."}},
        {{"category":"US Credit Cards","rate":"...","trend":"...","spread":"...","spread_trend":"..."}}
      ]
    }}
  }},

  "section6_forex": {{
    "title": "Foreign Exchange Markets",
    "dollar_index": {{"value":"USE REAL DXY VALUE","change":"USE REAL DXY WEEKLY PCT","interpretation":"4-5 lines"}},
    "narrative": "8-10 lines using the real FX data above",
    "pairs": [
      {{"pair":"EUR/USD","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"3-4 lines","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"GBP/USD","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"USD/JPY","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"USD/CHF","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"AUD/USD","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"USD/CNY","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"USD/BRL","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}},
      {{"pair":"USD/MXN","value":"USE REAL","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","support":"...","resistance":"...","weekly_high":"USE REAL","weekly_low":"USE REAL"}}
    ]
  }},

  "section7_commodities": {{
    "title": "Commodities",
    "energy": {{
      "narrative": "8-10 lines using real energy prices above",
      "items": [
        {{"name":"WTI Crude",    "value":"USE REAL","unit":"$/bbl",  "change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"3-4 lines","drivers":["...","..."]}},
        {{"name":"Brent Crude",  "value":"USE REAL","unit":"$/bbl",  "change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Natural Gas",  "value":"USE REAL","unit":"$/MMBtu","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Heating Oil",  "value":"USE REAL","unit":"$/gal",  "change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Gasoline RBOB","value":"USE REAL","unit":"$/gal",  "change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}}
      ]
    }},
    "metals": {{
      "narrative": "8-10 lines using real metal prices above",
      "items": [
        {{"name":"Gold",     "value":"USE REAL","unit":"$/oz","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"3-4 lines","drivers":["...","..."]}},
        {{"name":"Silver",   "value":"USE REAL","unit":"$/oz","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Copper",   "value":"USE REAL","unit":"$/lb","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Platinum", "value":"USE REAL","unit":"$/oz","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Palladium","value":"USE REAL","unit":"$/oz","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Aluminum", "value":"USE REAL","unit":"$/t", "change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}}
      ]
    }},
    "agricultural": {{
      "narrative": "8-10 lines using real agricultural prices above",
      "items": [
        {{"name":"Wheat",   "value":"USE REAL","unit":"¢/bu","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"3-4 lines","drivers":["...","..."]}},
        {{"name":"Corn",    "value":"USE REAL","unit":"¢/bu","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Soybeans","value":"USE REAL","unit":"¢/bu","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Coffee",  "value":"USE REAL","unit":"¢/lb","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Sugar",   "value":"USE REAL","unit":"¢/lb","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}},
        {{"name":"Cotton",  "value":"USE REAL","unit":"¢/lb","change":"USE REAL","change_num": 0.0,"direction":"up|down","analysis":"...","drivers":["...","..."]}}
      ]
    }}
  }},

  "section8_synthesis": {{
    "title": "Strategic Synthesis",
    "regime": "risk_on|risk_off|transition|stagflation|goldilocks",
    "regime_description": "5-6 lines based on real data above",
    "global_view": "MINIMUM 15 sentences: interconnection between rates/equities/dollar/commodities using all the real numbers above",
    "macro_thesis": "6-8 lines forward-looking thesis",
    "upcoming_events": "5-6 lines on next week's key events",
    "strategy": [
      {{"type":"Tactical Allocation","recommendation":"...","rationale":"5-6 lines","timeframe":"1-4 weeks","conviction":"high|medium|low"}},
      {{"type":"Hedging","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Long Opportunity","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Short Opportunity","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}},
      {{"type":"Risk Management","recommendation":"...","rationale":"5-6 lines","timeframe":"...","conviction":"..."}}
    ]
  }},

  "section9_economic_calendar": {{
    "title": "Economic Calendar — Next Week",
    "note": "Sources: Finnhub real data",
    "events": [
      {{"date":"...","time":"...","country":"...","flag":"...","event":"...","impact":"high|medium|low","previous":"...","forecast":"...","context":"2-3 lines"}}
    ]
  }},

  "section10_earnings_calendar": {{
    "title": "Earnings Calendar — Next Week",
    "note": "Sources: Finnhub real data",
    "earnings": [
      {{"date":"...","symbol":"...","company":"...","sector":"...","timing":"before open|after close","eps_estimate":"...","revenue_estimate":"...","context":"2-3 lines","impact_potential":"high|medium|low"}}
    ]
  }}
}}

REMEMBER: Copy EXACT values from the real data section above. Your value for S&P 500 must match exactly what is shown above. Same for every single price and percentage."""
