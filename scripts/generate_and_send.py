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

def collect_real_data() -> dict:
    print("[1/6] Collecte des données réelles (Finnhub + FRED)...")
    data = {}

    # ── Calendrier économique (semaine prochaine)
    next_week_end = (NOW + timedelta(days=14)).strftime("%Y-%m-%d")
    eco_cal = fetch_finnhub("calendar/economic", {"from": TODAY, "to": next_week_end})
    events = eco_cal.get("economicCalendar", [])
    # Filtrer les événements à fort impact
    high_impact = [e for e in events if e.get("impact") in ("high", "3", 3)][:20]
    all_events  = events[:40]
    data["economic_calendar"] = {
        "high_impact": high_impact,
        "all": all_events,
        "count": len(events)
    }

    # ── Calendrier des résultats d'entreprises
    earnings = fetch_finnhub("calendar/earnings", {"from": TODAY, "to": next_week_end})
    earning_list = earnings.get("earningsCalendar", [])
    # Trier par symbol connu (grandes caps)
    majors = ["AAPL","MSFT","AMZN","GOOGL","META","NVDA","TSLA","JPM","GS","MS",
              "BAC","WMT","HD","V","MA","UNH","JNJ","PFE","XOM","CVX",
              "LVMH.PA","MC.PA","SAP","ASML","TTE.PA","SAN.MC","BNP.PA"]
    major_earnings = [e for e in earning_list if e.get("symbol") in majors]
    data["earnings_calendar"] = {
        "major": major_earnings[:15],
        "all": earning_list[:30],
        "count": len(earning_list)
    }

    # ── Données FRED (macro US)
    print("  Fetching FRED macro data...")
    fred_series = {
        "fed_funds_rate":   "FEDFUNDS",
        "unemployment":     "UNRATE",
        "cpi_yoy":          "CPIAUCSL",
        "10y_treasury":     "DGS10",
        "2y_treasury":      "DGS2",
        "30y_mortgage":     "MORTGAGE30US",
        "gdp_growth":       "A191RL1Q225SBEA",
        "m2_money_supply":  "M2SL",
        "credit_spreads_hy":"BAMLH0A0HYM2",
        "vix":              "VIXCLS",
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

    # ── Options market data (via Finnhub quotes pour indices)
    print("  Fetching options/market sentiment data...")
    symbols_quotes = {}
    for sym in ["SPY", "QQQ", "IWM", "GLD", "TLT", "USO"]:
        q = fetch_finnhub("quote", {"symbol": sym})
        if q.get("c"):
            symbols_quotes[sym] = {
                "price":   q.get("c"),
                "change":  q.get("d"),
                "pct":     q.get("dp"),
                "high":    q.get("h"),
                "low":     q.get("l"),
                "prev":    q.get("pc")
            }
    data["etf_quotes"] = symbols_quotes

    # ── Sentiment du marché
    sentiment = fetch_finnhub("news-sentiment", {"symbol": "SPY"})
    data["market_sentiment"] = sentiment

    print(f"[1/6] ✅ Données collectées — {len(all_events)} événements éco, {len(earning_list)} résultats, {len(fred_data)} séries FRED")
    return data


# ══════════════════════════════════════════════════════════════════════════════
#  CLAUDE : GÉNÉRATION DU RAPPORT
# ══════════════════════════════════════════════════════════════════════════════
def generate_report_json(real_data: dict) -> dict:
    print(f"[2/6] Appel Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    fred = real_data.get("fred", {})
    eco_cal = real_data.get("economic_calendar", {})
    earnings = real_data.get("earnings_calendar", {})
    etfs = real_data.get("etf_quotes", {})

    # Préparer le contexte de données réelles
    fred_context = "\n".join([f"  - {k}: {v['value']} ({v['date']})" for k, v in fred.items()])
    etf_context  = "\n".join([f"  - {sym}: ${v['price']} ({v['pct']:+.2f}%)" if isinstance(v.get('pct'), float) else f"  - {sym}: ${v['price']}" for sym, v in etfs.items()])
    
    eco_events_context = json.dumps(eco_cal.get("high_impact", [])[:15], ensure_ascii=False)
    earnings_context   = json.dumps(earnings.get("major", [])[:10], ensure_ascii=False)

    system = (
        f"Tu es un analyste financier senior de premier rang (ex-Goldman Sachs, hedge fund macro global). "
        f"Tu produis un rapport hebdomadaire ultra-professionnel, dense, didactique et actionnable. "
        f"Date : {DATE_FR} ({WEEK_N} {NOW.year}). "
        f"Tu as accès aux données de marché réelles ci-dessous. Utilise-les comme base factuelle. "
        f"Chaque section DOIT inclure : les causes profondes, les implications pour les investisseurs, "
        f"et une stratégie concrète. Les analyses doivent être longues, développées, éducatives. "
        f"Réponds UNIQUEMENT en JSON valide, sans texte autour, sans balises markdown."
    )

    user = f"""Génère un rapport hebdomadaire COMPLET et APPROFONDI des marchés financiers en français.

DONNÉES RÉELLES DISPONIBLES:
=== FRED (Macro US) ===
{fred_context}

=== ETF Quotes ===
{etf_context}

=== Calendrier Économique (vrais événements) ===
{eco_events_context}

=== Résultats d'Entreprises (vrais) ===
{earnings_context}

INSTRUCTIONS IMPORTANTES:
- Chaque analyse doit faire AU MOINS 3-4 paragraphes développés
- Explique TOUJOURS les causes profondes et les mécanismes de transmission
- Inclus des chiffres précis partout où c'est possible
- La section dérivés doit analyser Put/Call ratio, contango/backwardation, skew, term structure
- Le calendrier économique doit avoir AU MOINS 10 événements avec contexte et prévisions
- Le calendrier résultats doit lister les entreprises majeures avec contexte et attentes
- Sois didactique : explique les concepts pour un lecteur sophistiqué mais pas trader professionnel

JSON attendu (TOUTES les clés obligatoires) :
{{
  "week": "Semaine du X au Y {NOW.strftime('%B %Y')}",
  "generated_at": "{DATE_FR}",
  "market_temperature": "risk_on|risk_off|neutral",
  "market_temperature_label": "explication en 1 phrase",

  "section1_summary": {{
    "title": "Vue d'ensemble des marchés",
    "paragraphs": ["§1 long et dense (5-6 lignes min)", "§2 long", "§3 synthèse et ton de la semaine"]
  }},

  "section2_major_events": {{
    "title": "Événements majeurs",
    "events": [
      {{
        "title": "...",
        "body": "analyse longue et développée avec causes, mécanismes et implications (6-8 lignes minimum)",
        "sentiment": "positive|negative|neutral",
        "impact_score": 8,
        "affected_assets": ["S&P 500", "Obligations US"]
      }}
    ]
  }},

  "section3_blind_spots": {{
    "title": "Angles morts — Ce que le marché sous-estime",
    "events": [
      {{
        "title": "...",
        "body": "analyse développée (5-6 lignes min)",
        "sentiment": "neutral",
        "why_ignored": "raison pour laquelle c'est sous-médiatisé"
      }}
    ]
  }},

  "section4_equities": {{
    "title": "Marchés Actions",
    "indices": [
      {{"name":"S&P 500","value":"...","change":"...","ytd":"..."}},
      {{"name":"NASDAQ","value":"...","change":"...","ytd":"..."}},
      {{"name":"Dow Jones","value":"...","change":"...","ytd":"..."}},
      {{"name":"Russell 2000","value":"...","change":"...","ytd":"..."}},
      {{"name":"CAC 40","value":"...","change":"...","ytd":"..."}},
      {{"name":"DAX","value":"...","change":"...","ytd":"..."}},
      {{"name":"FTSE 100","value":"...","change":"...","ytd":"..."}},
      {{"name":"EuroStoxx 50","value":"...","change":"...","ytd":"..."}},
      {{"name":"Nikkei 225","value":"...","change":"...","ytd":"..."}},
      {{"name":"Hang Seng","value":"...","change":"...","ytd":"..."}},
      {{"name":"Shanghai Comp.","value":"...","change":"...","ytd":"..."}},
      {{"name":"Kospi","value":"...","change":"...","ytd":"..."}}
    ],
    "sector_performance": [
      {{"sector":"Technologie","change":"+2.1%","direction":"up"}},
      {{"sector":"Finance","change":"+0.8%","direction":"up"}},
      {{"sector":"Énergie","change":"-1.2%","direction":"down"}},
      {{"sector":"Santé","change":"+0.3%","direction":"up"}},
      {{"sector":"Industrie","change":"+0.5%","direction":"up"}},
      {{"sector":"Consommation discr.","change":"-0.4%","direction":"down"}},
      {{"sector":"Matériaux","change":"+0.7%","direction":"up"}},
      {{"sector":"Utilities","change":"-0.2%","direction":"down"}}
    ],
    "us": {{
      "headline": "...",
      "body": "analyse longue US avec causes macros, flux institutionnels, positionnement (8-10 lignes)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "facteur principal en 1 phrase",
      "risk": "risque principal en 1 phrase"
    }},
    "eu": {{
      "headline": "...",
      "body": "analyse longue UE (8-10 lignes)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }},
    "asia": {{
      "headline": "...",
      "body": "analyse longue Asie (8-10 lignes)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }}
  }},

  "section5_bonds": {{
    "title": "Obligations & Taux",
    "macro_context": "contexte macro général des taux en 4-5 lignes",
    "bond_market": {{
      "title": "Marché Obligataire Global",
      "why": "explication longue des causes (4-5 lignes)",
      "implies": "implications détaillées pour investisseurs (4-5 lignes)",
      "strategy": "stratégie concrète et actionnable (3-4 lignes)"
    }},
    "yield_curves": {{
      "title": "Courbes de Taux",
      "shape": "normale|inversée|plate|en_bosse",
      "interpretation": "ce que la forme de la courbe signale (3-4 lignes)",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"maturity":"3M","us":5.3,"de":3.1,"fr":3.4,"jp":0.1,"uk":5.1}},
        {{"maturity":"6M","us":5.2,"de":3.0,"fr":3.3,"jp":0.1,"uk":5.0}},
        {{"maturity":"1Y","us":5.0,"de":2.9,"fr":3.2,"jp":0.2,"uk":4.9}},
        {{"maturity":"2Y","us":4.7,"de":2.8,"fr":3.0,"jp":0.3,"uk":4.6}},
        {{"maturity":"5Y","us":4.4,"de":2.6,"fr":2.9,"jp":0.5,"uk":4.3}},
        {{"maturity":"10Y","us":4.3,"de":2.5,"fr":2.8,"jp":0.7,"uk":4.2}},
        {{"maturity":"30Y","us":4.5,"de":2.7,"fr":3.0,"jp":1.8,"uk":4.6}}
      ]
    }},
    "derivatives_and_options": {{
      "title": "Dérivés, Options & Futures",
      "vix": "...",
      "vix_interpretation": "ce que le niveau du VIX signale sur la perception du risque (3-4 lignes)",
      "put_call_ratio": "...",
      "put_call_interpretation": "analyse du ratio put/call et ce que le positionnement des options révèle (3-4 lignes)",
      "skew": "...",
      "skew_interpretation": "analyse du skew de volatilité et implications (3-4 lignes)",
      "term_structure": "contango|backwardation|flat",
      "term_structure_interpretation": "analyse de la structure à terme et ce qu'elle révèle (3-4 lignes)",
      "futures_positioning": "analyse du positionnement des fonds sur les futures (COT report, longs/shorts) (4-5 lignes)",
      "market_signal": "bullish|bearish|neutral",
      "market_signal_explanation": "signal synthétique basé sur l'ensemble des dérivés (4-5 lignes)",
      "options_strategy": "stratégie d'options recommandée selon l'environnement (3-4 lignes)",
      "key_levels": [
        {{"asset": "S&P 500", "support": "...", "resistance": "...", "key_strike": "..."}},
        {{"asset": "Or", "support": "...", "resistance": "...", "key_strike": "..."}},
        {{"asset": "EUR/USD", "support": "...", "resistance": "...", "key_strike": "..."}}
      ]
    }},
    "credit_defaults": {{
      "title": "Crédit & Taux de Défaut",
      "macro_view": "vue macro du marché crédit (3-4 lignes)",
      "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"category":"Investment Grade US","rate":"0.8","trend":"stable","spread":"120","spread_trend":"compression"}},
        {{"category":"High Yield US","rate":"3.2","trend":"hausse","spread":"380","spread_trend":"élargissement"}},
        {{"category":"Investment Grade EU","rate":"0.6","trend":"stable","spread":"95","spread_trend":"compression"}},
        {{"category":"High Yield EU","rate":"2.8","trend":"stable","spread":"340","spread_trend":"stable"}},
        {{"category":"EM Sovereigns","rate":"4.1","trend":"hausse","spread":"450","spread_trend":"élargissement"}},
        {{"category":"Mortgage US","rate":"1.1","trend":"stable","spread":"180","spread_trend":"stable"}},
        {{"category":"Auto Loans US","rate":"2.4","trend":"hausse","spread":"220","spread_trend":"élargissement"}},
        {{"category":"Credit Cards US","rate":"4.1","trend":"hausse","spread":"500","spread_trend":"élargissement"}}
      ]
    }}
  }},

  "section6_forex_commodities": {{
    "title": "Forex & Matières Premières",
    "dollar_index": {{"value":"...","change":"...","interpretation":"analyse DXY et implications (3-4 lignes)"}},
    "forex_narrative": "analyse longue des dynamiques forex globales (6-8 lignes)",
    "commodities_narrative": "analyse longue des commodités (6-8 lignes)",
    "forex_pairs": [
      {{"pair":"EUR/USD","value":"...","change":"...","direction":"up|down","analysis":"contexte et drivers (2-3 lignes)","support":"...","resistance":"..."}},
      {{"pair":"GBP/USD","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}},
      {{"pair":"USD/JPY","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}},
      {{"pair":"USD/CHF","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}},
      {{"pair":"AUD/USD","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}},
      {{"pair":"USD/CNY","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}},
      {{"pair":"USD/BRL","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}},
      {{"pair":"USD/MXN","value":"...","change":"...","direction":"up|down","analysis":"...","support":"...","resistance":"..."}}
    ],
    "commodities": [
      {{"name":"WTI Oil","value":"...","unit":"$/bbl","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Brent","value":"...","unit":"$/bbl","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Natural Gas","value":"...","unit":"$/MMBtu","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Gold","value":"...","unit":"$/oz","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Silver","value":"...","unit":"$/oz","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Copper","value":"...","unit":"$/lb","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Platinum","value":"...","unit":"$/oz","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Wheat","value":"...","unit":"$/bu","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Corn","value":"...","unit":"$/bu","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Soybeans","value":"...","unit":"$/bu","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Lumber","value":"...","unit":"$/MBF","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}},
      {{"name":"Bitcoin","value":"...","unit":"USD","change":"...","direction":"up|down","analysis":"...","drivers":["...", "..."]}}
    ]
  }},

  "section7_synthesis": {{
    "title": "Synthèse Stratégique",
    "regime": "risk_on|risk_off|transition|stagflation|goldilocks",
    "regime_description": "description du régime de marché actuel (4-5 lignes)",
    "global_view": "synthèse holistique très développée (8-10 lignes)",
    "macro_thesis": "thèse macro principale pour les 4-8 prochaines semaines (5-6 lignes)",
    "upcoming_events": "événements clés à surveiller la semaine prochaine (4-5 lignes)",
    "strategy": [
      {{"type":"Allocation Tactique","recommendation":"...","rationale":"développé (4-5 lignes)","timeframe":"1-4 semaines","conviction":"haute|moyenne|basse"}},
      {{"type":"Couverture","recommendation":"...","rationale":"développé (4-5 lignes)","timeframe":"...","conviction":"..."}},
      {{"type":"Opportunité Long","recommendation":"...","rationale":"développé (4-5 lignes)","timeframe":"...","conviction":"..."}},
      {{"type":"Opportunité Short","recommendation":"...","rationale":"développé (4-5 lignes)","timeframe":"...","conviction":"..."}},
      {{"type":"Gestion du Risque","recommendation":"...","rationale":"développé (4-5 lignes)","timeframe":"...","conviction":"..."}}
    ]
  }},

  "section8_economic_calendar": {{
    "title": "Calendrier Économique — Semaine prochaine",
    "note": "Sources : Finnhub, Bloomberg consensus",
    "events": [
      {{
        "date": "Lundi 17 mars",
        "time": "14:30",
        "country": "US",
        "flag": "🇺🇸",
        "event": "...",
        "impact": "high|medium|low",
        "previous": "...",
        "forecast": "...",
        "context": "pourquoi cet indicateur est important cette semaine (2-3 lignes)"
      }}
    ]
  }},

  "section9_earnings_calendar": {{
    "title": "Résultats d'Entreprises — Semaine prochaine",
    "note": "Sources : Finnhub",
    "earnings": [
      {{
        "date": "Lundi 17 mars",
        "symbol": "AAPL",
        "company": "Apple Inc.",
        "sector": "Technologie",
        "timing": "avant ouverture|après clôture",
        "eps_estimate": "...",
        "revenue_estimate": "...",
        "context": "enjeux de ce rapport et ce que le marché surveille (2-3 lignes)",
        "impact_potential": "high|medium|low"
      }}
    ]
  }}
}}

IMPORTANT: Génère des analyses LONGUES et DÉVELOPPÉES. Le rapport doit faire entre 15 000 et 25 000 caractères.
Utilise les vraies données FRED et Finnhub fournies comme base factuelle."""

    for attempt in range(3):
        try:
            raw = ""
            with client.messages.stream(
                model=ANTHROPIC_MODEL, max_tokens=32000,
                system=system, messages=[{"role":"user","content":user}]
            ) as stream:
                for text in stream.text_stream:
                    raw += text
            print(f"[2/6] Streaming terminé — {len(raw)} caractères reçus")
            match = re.search(r'\{[\s\S]*\}', raw)
            if not match:
                print(f"[2/6] ⚠️ Pas de JSON (essai {attempt+1}/3)")
                continue
            json_str = match.group()
            try:
                report = json.loads(json_str)
                print("[2/6] ✅ Rapport JSON généré")
                return report
            except json.JSONDecodeError as e:
                print(f"[2/6] ⚠️ JSON invalide (essai {attempt+1}/3) : {e}")
                # Réparation
                for end in range(len(json_str)-1, 0, -1):
                    if json_str[end] == '}':
                        try:
                            report = json.loads(json_str[:end+1])
                            print("[2/6] ✅ JSON réparé")
                            return report
                        except:
                            continue
        except Exception as e:
            print(f"[2/6] ⚠️ Erreur API (essai {attempt+1}/3) : {e}")

    raise ValueError("Impossible de générer le rapport après 3 essais")


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

    # S6 - Forex & Commodités
    s6 = report.get('section6_forex_commodities',{})
    sec(6, s6.get('title','Forex & Matières Premières'))
    dxy = s6.get('dollar_index',{})
    if dxy:
        c = GREEN if (dxy.get('change','') or '').startswith('+') else RED
        story.append(Paragraph(f"Dollar Index (DXY) : {dxy.get('value','—')}  {dxy.get('change','')}", ps('dxy', fontSize=10, textColor=c, fontName='Helvetica-Bold', spaceAfter=4)))
        if dxy.get('interpretation'):
            story.append(Paragraph(str(dxy['interpretation']), S_BD))
    story.append(Paragraph('Dynamiques Forex', S_BL))
    story.append(Paragraph(str(s6.get('forex_narrative','—')), S_BD))
    fx = s6.get('forex_pairs',[])
    if fx:
        data = [['Paire','Valeur','Variation','Support','Résistance']]
        for p in fx:
            c = GREEN if p.get('direction')=='up' else RED
            data.append([p.get('pair',''), p.get('value',''),
                Paragraph(str(p.get('change','')), ps('fc', fontSize=8, textColor=c, fontName='Helvetica-Bold')),
                p.get('support','—'), p.get('resistance','—')])
        story.append(tbl(data,[28*mm,28*mm,28*mm,28*mm,28*mm]))
        # Analyses individuelles
        for p in fx[:4]:
            if p.get('analysis'):
                story.append(Paragraph(f"{p['pair']} : {p['analysis']}", ps('fa', fontSize=8, textColor=MUTED, fontName='Helvetica', spaceAfter=3, leftIndent=6)))
        story.append(Spacer(1,4*mm))

    story.append(Paragraph('Dynamiques Commodités', S_BL))
    story.append(Paragraph(str(s6.get('commodities_narrative','—')), S_BD))
    cm = s6.get('commodities',[])
    if cm:
        data = [['Actif','Valeur','Variation','Drivers principaux']]
        for c in cm:
            col = GREEN if c.get('direction')=='up' else RED
            drivers = ' · '.join(c.get('drivers',[])) if c.get('drivers') else '—'
            data.append([c.get('name',''), f"{c.get('value','')} {c.get('unit','')}",
                Paragraph(str(c.get('change','')), ps('cc', fontSize=8, textColor=col, fontName='Helvetica-Bold')),
                Paragraph(drivers, ps('dr', fontSize=7, textColor=MUTED, fontName='Helvetica'))])
        story.append(tbl(data,[28*mm,32*mm,24*mm,83*mm]))

    story.append(PageBreak())

    # S7 - Synthèse
    s7 = report.get('section7_synthesis',{})
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
    s8 = report.get('section8_economic_calendar',{})
    sec(8, s8.get('title','Calendrier Économique'))
    if s8.get('note'):
        story.append(Paragraph(str(s8['note']), ps('n', fontSize=7, textColor=MUTED, fontName='Helvetica', spaceAfter=6)))
    cal = s8.get('events',[])
    if cal:
        data = [['Date','Heure','Pays','Événement','Impact','Précédent','Prévision']]
        for ev in cal:
            ic = RED if ev.get('impact')=='high' else (YELLOW if ev.get('impact')=='medium' else MUTED)
            flag = ev.get('flag','')
            data.append([
                ev.get('date',''), ev.get('time',''),
                f"{flag} {ev.get('country','')}",
                Paragraph(str(ev.get('event','')), ps('ev', fontSize=7.5, textColor=LIGHT, fontName='Helvetica')),
                Paragraph('FORT' if ev.get('impact')=='high' else ('MOY.' if ev.get('impact')=='medium' else 'FAIBLE'),
                    ps('imp', fontSize=7, textColor=ic, fontName='Helvetica-Bold')),
                str(ev.get('previous','—')), str(ev.get('forecast','—'))
            ])
        story.append(tbl(data,[22*mm,12*mm,16*mm,60*mm,14*mm,14*mm,14*mm]))
        # Contextes
        for ev in cal:
            if ev.get('context'):
                story.append(Paragraph(f"→ {ev.get('event','')} : {ev['context']}", ps('ctx', fontSize=7.5, textColor=MUTED, fontName='Helvetica', spaceAfter=3, leftIndent=6)))

    # S9 - Calendrier résultats
    s9 = report.get('section9_earnings_calendar',{})
    sec(9, s9.get('title','Résultats d\'Entreprises'))
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
    s7 = report.get('section7_synthesis',{})
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
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Résumé de la semaine</div>
    {paras}
  </div>
  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:6px;padding:16px;margin-bottom:16px">
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Indices clés</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Indice</th>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Valeur</th>
        <th style="text-align:left;padding:5px 10px;color:#525268;font-size:9px;border-bottom:1px solid #1e1e2e">Hebdo</th>
      </tr></thead><tbody>{rows}</tbody>
    </table>
  </div>
  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:6px;padding:16px;margin-bottom:16px">
    <div style="color:#b8965a;font-size:9px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Stratégies de la semaine</div>
    {strats}
  </div>
  <p style="color:#2a2a3a;font-size:10px;text-align:center;margin-top:20px">📎 Rapport complet en PDF joint · Finnhub + FRED + Claude API · Non contractuel</p>
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
    print(f"\n[6/6] ✅ Brief complet envoyé !\n{'='*60}\n")
