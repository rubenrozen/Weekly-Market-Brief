"""
Weekly Market Brief — Script d'automatisation
Appelé chaque vendredi à 19h (Panama) par GitHub Actions.
Un seul appel Claude → génère JSON → PDF → Email.
"""

import os
import json
import base64
import smtplib
import requests
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO

import anthropic
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

# ─── Configuration ────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
ANTHROPIC_MODEL   = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-6")
GMAIL_TOKEN       = os.environ["GMAIL_TOKEN"]
EMAIL_FROM        = os.environ["EMAIL_FROM"]
EMAIL_TO          = os.environ["EMAIL_TO"]

NOW      = datetime.now()
DATE_FR  = NOW.strftime("%A %d %B %Y")
WEEK_NUM = NOW.strftime("S%W")

# ─── ÉTAPE 1 : Appel Claude (unique) ─────────────────────────────────────────
def generate_report_json() -> dict:
    print(f"[1/4] Appel Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = (
        f"Tu es un analyste financier senior de premier plan. "
        f"Tu produis un rapport hebdomadaire professionnel et actionnable. "
        f"Date d'analyse : {DATE_FR} ({WEEK_NUM} {NOW.year}). "
        f"Réponds UNIQUEMENT en JSON valide, sans texte autour, sans balises markdown."
    )

    user_prompt = f"""Génère un rapport hebdomadaire complet des marchés financiers en français pour la semaine du {DATE_FR}.

JSON attendu (toutes clés obligatoires, valeurs réelles et cohérentes avec les marchés actuels) :
{{
  "week": "Semaine du X au Y mois {NOW.year}",
  "generated_at": "{DATE_FR}",
  "section1_summary": {{
    "title": "Résumé des marchés",
    "paragraphs": ["paragraphe 1 dense et informatif", "paragraphe 2"]
  }},
  "section2_major_events": {{
    "title": "Événements majeurs",
    "events": [
      {{"title": "...", "body": "explication détaillée et impact investisseurs", "sentiment": "positive"}}
    ]
  }},
  "section3_blind_spots": {{
    "title": "Angles morts — événements sous-médiatisés",
    "events": [
      {{"title": "...", "body": "...", "sentiment": "neutral"}}
    ]
  }},
  "section4_equities": {{
    "title": "Marchés Actions",
    "indices": [
      {{"name": "S&P 500", "value": "5800", "change": "+1.2%"}},
      {{"name": "NASDAQ", "value": "18500", "change": "-0.5%"}},
      {{"name": "CAC 40", "value": "8100", "change": "+0.8%"}},
      {{"name": "DAX", "value": "18200", "change": "+1.1%"}},
      {{"name": "Nikkei", "value": "38000", "change": "-0.3%"}},
      {{"name": "Hang Seng", "value": "17500", "change": "+2.1%"}},
      {{"name": "FTSE 100", "value": "8500", "change": "+0.6%"}},
      {{"name": "Shanghai", "value": "3300", "change": "+1.5%"}}
    ],
    "us":   {{"headline": "...", "body": "analyse détaillée US drivers et direction", "direction": "bullish"}},
    "eu":   {{"headline": "...", "body": "analyse détaillée UE", "direction": "neutral"}},
    "asia": {{"headline": "...", "body": "analyse détaillée Asie", "direction": "bearish"}}
  }},
  "section5_bonds": {{
    "title": "Obligations & Dérivés",
    "bond_market":     {{"title": "Marché Obligataire", "why": "...", "implies": "...", "strategy": "..."}},
    "yield_curves":    {{"title": "Courbes de Taux", "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"maturity": "3M",  "us": 5.3, "de": 3.1, "fr": 3.4, "jp": 0.1}},
        {{"maturity": "2Y",  "us": 4.7, "de": 2.8, "fr": 3.0, "jp": 0.3}},
        {{"maturity": "5Y",  "us": 4.4, "de": 2.6, "fr": 2.9, "jp": 0.5}},
        {{"maturity": "10Y", "us": 4.3, "de": 2.5, "fr": 2.8, "jp": 0.7}},
        {{"maturity": "30Y", "us": 4.5, "de": 2.7, "fr": 3.0, "jp": 1.8}}
      ]
    }},
    "derivatives":     {{"title": "Produits Dérivés", "why": "...", "implies": "...", "strategy": "...", "vix": "18.5", "put_call": "0.85", "skew": "modéré"}},
    "credit_defaults": {{"title": "Taux de Défaut", "why": "...", "implies": "...", "strategy": "...",
      "data": [
        {{"category": "Investment Grade", "rate": "0.8", "trend": "stable"}},
        {{"category": "High Yield",       "rate": "3.2", "trend": "hausse"}},
        {{"category": "Mortgage",         "rate": "1.1", "trend": "stable"}},
        {{"category": "Auto",             "rate": "2.4", "trend": "hausse"}},
        {{"category": "Credit Card",      "rate": "4.1", "trend": "hausse"}}
      ]
    }}
  }},
  "section6_forex_commodities": {{
    "title": "Forex & Matières Premières",
    "forex_narrative": "paragraphe dynamiques forex",
    "commodities_narrative": "paragraphe dynamiques commodités",
    "forex_pairs": [
      {{"pair": "EUR/USD", "value": "1.0850", "change": "+0.3%", "direction": "up"}},
      {{"pair": "GBP/USD", "value": "1.2650", "change": "-0.1%", "direction": "down"}},
      {{"pair": "USD/JPY", "value": "148.5",  "change": "+0.5%", "direction": "up"}},
      {{"pair": "USD/CHF", "value": "0.8950", "change": "-0.2%", "direction": "down"}},
      {{"pair": "AUD/USD", "value": "0.6540", "change": "+0.4%", "direction": "up"}},
      {{"pair": "USD/CNY", "value": "7.2400", "change": "+0.1%", "direction": "up"}}
    ],
    "commodities": [
      {{"name": "WTI Oil",  "value": "72.5",  "unit": "$/bbl",    "change": "-1.2%", "direction": "down"}},
      {{"name": "Brent",    "value": "76.8",  "unit": "$/bbl",    "change": "-0.9%", "direction": "down"}},
      {{"name": "Gold",     "value": "2050",  "unit": "$/oz",     "change": "+0.6%", "direction": "up"}},
      {{"name": "Silver",   "value": "23.5",  "unit": "$/oz",     "change": "+1.1%", "direction": "up"}},
      {{"name": "Copper",   "value": "3.85",  "unit": "$/lb",     "change": "+0.8%", "direction": "up"}},
      {{"name": "Wheat",    "value": "580",   "unit": "$/bu",     "change": "-0.4%", "direction": "down"}},
      {{"name": "Corn",     "value": "445",   "unit": "$/bu",     "change": "+0.2%", "direction": "up"}},
      {{"name": "Nat. Gas", "value": "1.85",  "unit": "$/MMBtu",  "change": "-2.1%", "direction": "down"}}
    ]
  }},
  "section7_synthesis": {{
    "title": "Synthèse Holistique",
    "global_view": "paragraphe de synthèse holistique",
    "upcoming_events": "paragraphe événements à venir semaine prochaine",
    "strategy": [
      {{"type": "Allocation",   "recommendation": "...", "rationale": "..."}},
      {{"type": "Couverture",   "recommendation": "...", "rationale": "..."}},
      {{"type": "Opportunité",  "recommendation": "...", "rationale": "..."}},
      {{"type": "Risque clé",   "recommendation": "...", "rationale": "..."}}
    ]
  }},
  "section8_calendar": {{
    "title": "Calendrier Économique — Semaine suivante",
    "events": [
      {{"date": "Lundi",   "country": "US", "event": "...", "impact": "high", "previous": "...", "forecast": "..."}},
      {{"date": "Mardi",   "country": "EU", "event": "...", "impact": "high", "previous": "...", "forecast": "..."}},
      {{"date": "Mercredi","country": "CN", "event": "...", "impact": "high", "previous": "...", "forecast": "..."}},
      {{"date": "Jeudi",   "country": "US", "event": "...", "impact": "high", "previous": "...", "forecast": "..."}},
      {{"date": "Vendredi","country": "EU", "event": "...", "impact": "high", "previous": "...", "forecast": "..."}}
    ]
  }}
}}

Remplis TOUTES les valeurs avec de vraies données actuelles. Le rapport doit être professionnel, dense et actionnable."""

    message = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=8000,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}]
    )

    raw = message.content[0].text
    # Extraire le JSON
    import re
    match = re.search(r'\{[\s\S]*\}', raw)
    if not match:
        raise ValueError("Aucun JSON valide dans la réponse Claude")
    
    report = json.loads(match.group())
    print(f"[1/4] ✅ Rapport JSON généré ({len(raw)} caractères)")
    return report


# ─── ÉTAPE 2 : Génération PDF ─────────────────────────────────────────────────
def generate_pdf(report: dict) -> bytes:
    print("[2/4] Génération du PDF...")
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=14*mm,
        leftMargin=14*mm,
        topMargin=20*mm,
        bottomMargin=20*mm,
        title=f"MarketBrief — {report.get('week', '')}",
    )

    # Couleurs
    GOLD   = colors.HexColor('#C8A96E')
    DARK   = colors.HexColor('#111118')
    LIGHT  = colors.HexColor('#E8E8F0')
    MUTED  = colors.HexColor('#6B6B80')
    GREEN  = colors.HexColor('#4ADE80')
    RED    = colors.HexColor('#F87171')
    BLUE   = colors.HexColor('#7B9CFF')
    YELLOW = colors.HexColor('#FBBF24')
    BG     = colors.HexColor('#0A0A0F')

    # Styles
    styles = getSampleStyleSheet()

    def style(name, **kwargs):
        return ParagraphStyle(name, **kwargs)

    S_TITLE = style('s_title', fontSize=22, textColor=LIGHT, fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=4)
    S_WEEK  = style('s_week',  fontSize=9,  textColor=GOLD,  fontName='Helvetica',      alignment=TA_CENTER, spaceAfter=2)
    S_SUB   = style('s_sub',   fontSize=8,  textColor=MUTED, fontName='Helvetica',      alignment=TA_CENTER, spaceAfter=16)
    S_SEC   = style('s_sec',   fontSize=13, textColor=LIGHT, fontName='Helvetica-Bold', spaceBefore=12, spaceAfter=6)
    S_SECN  = style('s_secn',  fontSize=7,  textColor=GOLD,  fontName='Helvetica',      spaceBefore=12)
    S_BODY  = style('s_body',  fontSize=8.5,textColor=MUTED, fontName='Helvetica',      leading=13, spaceAfter=6)
    S_LABEL = style('s_label', fontSize=7,  textColor=GOLD,  fontName='Helvetica-Bold', spaceAfter=2)
    S_BOLD  = style('s_bold',  fontSize=8.5,textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=3)
    S_GREEN = style('s_green', fontSize=7,  textColor=GREEN, fontName='Helvetica-Bold', spaceAfter=2)
    S_DISC  = style('s_disc',  fontSize=6.5,textColor=MUTED, fontName='Helvetica',      alignment=TA_CENTER, spaceAfter=0)

    story = []

    def hr(color=GOLD, thickness=1):
        return HRFlowable(width='100%', thickness=thickness, color=color, spaceAfter=6, spaceBefore=2)

    def section_header(num, title):
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph(f"{str(num).zfill(2)}  {title}", S_SEC))
        story.append(hr(GOLD, 0.5))

    def ingredient_block(obj, label_color=GOLD):
        if not obj:
            return
        story.append(Paragraph(obj.get('title',''), style('ing_t', fontSize=8, textColor=label_color, fontName='Helvetica-Bold', spaceAfter=4)))
        for label, key in [('Pourquoi on en est là', 'why'), ('Ce que ça induit', 'implies'), ('Stratégie', 'strategy')]:
            story.append(Paragraph(label, S_BOLD))
            story.append(Paragraph(obj.get(key, '—'), S_BODY))

    # ── PAGE DE COUVERTURE ────────────────────────────────
    story.append(Spacer(1, 30*mm))
    story.append(Paragraph("MARKET BRIEF", S_TITLE))
    story.append(Paragraph(report.get('week', ''), S_WEEK))
    story.append(Paragraph(f"Généré le {report.get('generated_at', DATE_FR)}", S_SUB))
    story.append(hr(GOLD, 2))
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Rapport hebdomadaire des marchés financiers — Alimenté par Claude API (Anthropic)", S_DISC))
    story.append(Spacer(1, 60*mm))
    story.append(Paragraph("Ce rapport est produit par intelligence artificielle à des fins d'information uniquement. Il ne constitue pas un conseil en investissement.", S_DISC))
    story.append(PageBreak())

    # ── SECTION 1 ─────────────────────────────────────────
    s1 = report.get('section1_summary', {})
    section_header(1, s1.get('title', 'Résumé des marchés'))
    for p in s1.get('paragraphs', []):
        story.append(Paragraph(p, S_BODY))

    # ── SECTION 2 ─────────────────────────────────────────
    s2 = report.get('section2_major_events', {})
    section_header(2, s2.get('title', 'Événements majeurs'))
    for ev in s2.get('events', []):
        sc = ev.get('sentiment','neutral')
        c  = GREEN if sc=='positive' else RED if sc=='negative' else GOLD
        story.append(Paragraph(ev.get('title',''), style('ev_t', fontSize=9, textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=2)))
        story.append(Paragraph(ev.get('body',''), S_BODY))
        story.append(hr(c, 0.3))

    # ── SECTION 3 ─────────────────────────────────────────
    s3 = report.get('section3_blind_spots', {})
    section_header(3, s3.get('title', 'Angles morts'))
    for ev in s3.get('events', []):
        story.append(Paragraph(ev.get('title',''), style('ev3_t', fontSize=9, textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=2)))
        story.append(Paragraph(ev.get('body',''), S_BODY))
        story.append(hr(BLUE, 0.3))

    story.append(PageBreak())

    # ── SECTION 4 ─────────────────────────────────────────
    s4 = report.get('section4_equities', {})
    section_header(4, s4.get('title', 'Marchés Actions'))

    # Tableau indices
    idx = s4.get('indices', [])
    if idx:
        data = [['Indice', 'Valeur', 'Variation']]
        for i in idx:
            chg = i.get('change','')
            c   = GREEN if chg.startswith('+') else RED
            data.append([
                Paragraph(i.get('name',''), style('idx_n', fontSize=8, textColor=LIGHT, fontName='Helvetica')),
                Paragraph(i.get('value',''), style('idx_v', fontSize=8, textColor=LIGHT, fontName='Helvetica-Bold')),
                Paragraph(chg, style('idx_c', fontSize=8, textColor=c, fontName='Helvetica-Bold'))
            ])
        t = Table(data, colWidths=[70*mm, 50*mm, 50*mm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',  (0,0), (-1,0), GOLD),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,0), 7),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#111118'), colors.HexColor('#15151E')]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#2A2A3A')),
            ('PADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(t)
        story.append(Spacer(1, 4*mm))

    # Régions
    for key, label in [('us','🇺🇸 États-Unis'), ('eu','🇪🇺 Europe'), ('asia','🌏 Asie')]:
        reg = s4.get(key, {})
        dc  = {'bullish': GREEN, 'bearish': RED, 'neutral': YELLOW}.get(reg.get('direction',''), GOLD)
        story.append(Paragraph(f"{label} · {reg.get('direction','—').upper()}", style('reg_l', fontSize=7, textColor=dc, fontName='Helvetica-Bold', spaceAfter=1)))
        story.append(Paragraph(reg.get('headline',''), style('reg_h', fontSize=9, textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=2)))
        story.append(Paragraph(reg.get('body',''), S_BODY))

    story.append(PageBreak())

    # ── SECTION 5 ─────────────────────────────────────────
    s5 = report.get('section5_bonds', {})
    section_header(5, s5.get('title', 'Obligations & Dérivés'))
    ingredient_block(s5.get('bond_market'), GOLD)
    ingredient_block(s5.get('yield_curves'), BLUE)

    # Courbes de taux
    yc = s5.get('yield_curves', {}).get('data', [])
    if yc:
        data = [['Maturité', 'US', 'DE', 'FR', 'JP']]
        for d in yc:
            data.append([d.get('maturity',''), f"{d.get('us','')}%", f"{d.get('de','')}%", f"{d.get('fr','')}%", f"{d.get('jp','')}%"])
        t = Table(data, colWidths=[30*mm,30*mm,30*mm,30*mm,30*mm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',  (0,0), (-1,0), GOLD),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 7.5),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#111118'), colors.HexColor('#15151E')]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#2A2A3A')),
            ('PADDING', (0,0), (-1,-1), 5),
            ('TEXTCOLOR', (1,1), (1,-1), BLUE),
        ]))
        story.append(t)
        story.append(Spacer(1, 4*mm))

    ingredient_block(s5.get('derivatives'), YELLOW)
    der = s5.get('derivatives', {})
    vix = float(der.get('vix', 0) or 0)
    vc  = GREEN if vix < 15 else (YELLOW if vix < 25 else RED)
    story.append(Paragraph(
        f"<b><font color='#{vc.hexval()[1:]}'>VIX : {der.get('vix','—')}</font></b>  |  "
        f"Put/Call : {der.get('put_call','—')}  |  Skew : {der.get('skew','—')}",
        style('der_s', fontSize=9, textColor=MUTED, fontName='Helvetica', spaceAfter=6)
    ))

    ingredient_block(s5.get('credit_defaults'), RED)
    dd = s5.get('credit_defaults', {}).get('data', [])
    if dd:
        data = [['Catégorie', 'Taux', 'Tendance']]
        for d in dd:
            tc = RED if d.get('trend')=='hausse' else (GREEN if d.get('trend')=='baisse' else YELLOW)
            data.append([
                d.get('category',''),
                f"{d.get('rate','')}%",
                Paragraph(d.get('trend',''), style('tr_s', fontSize=7.5, textColor=tc, fontName='Helvetica-Bold'))
            ])
        t = Table(data, colWidths=[80*mm, 35*mm, 55*mm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',  (0,0), (-1,0), GOLD),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 7.5),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#111118'), colors.HexColor('#15151E')]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#2A2A3A')),
            ('PADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(t)

    story.append(PageBreak())

    # ── SECTION 6 ─────────────────────────────────────────
    s6 = report.get('section6_forex_commodities', {})
    section_header(6, s6.get('title', 'Forex & Matières Premières'))
    story.append(Paragraph('Dynamiques Forex', S_BOLD))
    story.append(Paragraph(s6.get('forex_narrative', '—'), S_BODY))

    fx = s6.get('forex_pairs', [])
    if fx:
        data = [['Paire', 'Valeur', 'Variation']]
        for p in fx:
            c = GREEN if p.get('direction')=='up' else RED
            data.append([p.get('pair',''), p.get('value',''),
                Paragraph(p.get('change',''), style('fx_c', fontSize=8, textColor=c, fontName='Helvetica-Bold'))])
        t = Table(data, colWidths=[50*mm, 50*mm, 70*mm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',  (0,0), (-1,0), GOLD),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 7.5),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#111118'), colors.HexColor('#15151E')]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#2A2A3A')),
            ('PADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(t)
        story.append(Spacer(1, 4*mm))

    story.append(Paragraph('Dynamiques Commodités', S_BOLD))
    story.append(Paragraph(s6.get('commodities_narrative', '—'), S_BODY))

    cm = s6.get('commodities', [])
    if cm:
        data = [['Actif', 'Valeur', 'Variation']]
        for c in cm:
            col = GREEN if c.get('direction')=='up' else RED
            data.append([
                c.get('name',''),
                f"{c.get('value','')} {c.get('unit','')}",
                Paragraph(c.get('change',''), style('cm_c', fontSize=8, textColor=col, fontName='Helvetica-Bold'))
            ])
        t = Table(data, colWidths=[50*mm, 60*mm, 60*mm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',  (0,0), (-1,0), GOLD),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 7.5),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#111118'), colors.HexColor('#15151E')]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#2A2A3A')),
            ('PADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(t)

    story.append(PageBreak())

    # ── SECTION 7 ─────────────────────────────────────────
    s7 = report.get('section7_synthesis', {})
    section_header(7, s7.get('title', 'Synthèse Holistique'))
    story.append(Paragraph('Vue d\'ensemble', S_BOLD))
    story.append(Paragraph(s7.get('global_view', '—'), S_BODY))
    story.append(Paragraph('Événements à venir', S_BOLD))
    story.append(Paragraph(s7.get('upcoming_events', '—'), S_BODY))
    story.append(Paragraph('Stratégies recommandées', S_BOLD))
    for st in s7.get('strategy', []):
        story.append(Paragraph(st.get('type','').upper(), S_GREEN))
        story.append(Paragraph(st.get('recommendation',''), style('st_r', fontSize=9, textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=2)))
        story.append(Paragraph(st.get('rationale',''), S_BODY))
        story.append(hr(GREEN, 0.3))

    # ── SECTION 8 ─────────────────────────────────────────
    s8 = report.get('section8_calendar', {})
    section_header(8, s8.get('title', 'Calendrier Économique'))
    cal = s8.get('events', [])
    if cal:
        data = [['Date', 'Pays', 'Événement', 'Impact', 'Précédent', 'Prévision']]
        for ev in cal:
            ic = RED if ev.get('impact')=='high' else YELLOW
            data.append([
                ev.get('date',''),
                ev.get('country',''),
                ev.get('event',''),
                Paragraph('FORT' if ev.get('impact')=='high' else 'MOYEN',
                    style('imp', fontSize=7, textColor=ic, fontName='Helvetica-Bold')),
                ev.get('previous','—'),
                ev.get('forecast','—'),
            ])
        t = Table(data, colWidths=[22*mm, 14*mm, 60*mm, 18*mm, 18*mm, 18*mm])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',  (0,0), (-1,0), GOLD),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 7),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#111118'), colors.HexColor('#15151E')]),
            ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#2A2A3A')),
            ('PADDING', (0,0), (-1,-1), 4),
            ('TEXTCOLOR', (0,1), (-1,-1), MUTED),
        ]))
        story.append(t)

    # Build
    doc.build(story)
    pdf_bytes = buffer.getvalue()
    print(f"[2/4] ✅ PDF généré ({len(pdf_bytes)//1024} KB)")
    return pdf_bytes


# ─── ÉTAPE 3 : Email HTML ─────────────────────────────────────────────────────
def build_email_html(report: dict) -> str:
    s4 = report.get('section4_equities', {})
    s7 = report.get('section7_synthesis', {})
    indices_rows = ''.join([
        f'<tr><td style="padding:6px 10px;border-bottom:1px solid #2a2a3a;font-family:monospace;font-size:13px;color:#e8e8f0">{i["name"]}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #2a2a3a;font-family:monospace;font-size:13px;color:#e8e8f0">{i["value"]}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #2a2a3a;font-family:monospace;font-size:13px;color:{"#4ade80" if i.get("change","").startswith("+") else "#f87171"}">{i["change"]}</td></tr>'
        for i in s4.get('indices', [])
    ])
    strategies = ''.join([
        f'<div style="border-left:3px solid #4ade80;padding:8px 12px;margin:6px 0;background:#1a1a24;border-radius:0 6px 6px 0">'
        f'<div style="color:#4ade80;font-family:monospace;font-size:10px;text-transform:uppercase;margin-bottom:3px">{s["type"]}</div>'
        f'<div style="color:#e8e8f0;font-size:13px;font-weight:bold;margin-bottom:3px">{s["recommendation"]}</div>'
        f'<div style="color:#6b6b80;font-size:12px">{s["rationale"]}</div></div>'
        for s in s7.get('strategy', [])
    ])
    paragraphs = ''.join([f'<p style="color:#9090a8;font-size:13px;line-height:1.7;margin:0 0 8px">{p}</p>' for p in report.get('section1_summary', {}).get('paragraphs', [])])

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#0a0a0f;color:#e8e8f0;font-family:Arial,sans-serif;margin:0;padding:20px">
<div style="max-width:620px;margin:0 auto">
  <div style="background:#c8a96e;height:4px;border-radius:2px;margin-bottom:22px"></div>
  <div style="font-family:Georgia,serif;font-size:10px;color:#c8a96e;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px">{report.get('week','')}</div>
  <h1 style="font-family:Georgia,serif;font-size:26px;margin:0 0 4px;color:#e8e8f0">Weekly <span style="color:#c8a96e">Market</span> Brief</h1>
  <p style="color:#6b6b80;font-size:11px;margin:0 0 22px">Généré automatiquement le {report.get('generated_at', DATE_FR)}</p>
  <div style="background:#111118;border:1px solid #2a2a3a;border-radius:8px;padding:15px;margin-bottom:18px">
    <div style="color:#c8a96e;font-size:10px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px">Résumé des marchés</div>
    {paragraphs}
  </div>
  <div style="background:#111118;border:1px solid #2a2a3a;border-radius:8px;padding:15px;margin-bottom:18px">
    <div style="color:#c8a96e;font-size:10px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px">Indices</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr>
        <th style="text-align:left;padding:5px 10px;color:#6b6b80;font-size:10px;font-family:monospace;border-bottom:1px solid #2a2a3a">Indice</th>
        <th style="text-align:left;padding:5px 10px;color:#6b6b80;font-size:10px;font-family:monospace;border-bottom:1px solid #2a2a3a">Valeur</th>
        <th style="text-align:left;padding:5px 10px;color:#6b6b80;font-size:10px;font-family:monospace;border-bottom:1px solid #2a2a3a">Variation</th>
      </tr></thead>
      <tbody>{indices_rows}</tbody>
    </table>
  </div>
  <div style="background:#111118;border:1px solid #2a2a3a;border-radius:8px;padding:15px;margin-bottom:18px">
    <div style="color:#c8a96e;font-size:10px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px">Stratégies de la semaine</div>
    {strategies}
  </div>
  <p style="color:#3a3a4a;font-size:10px;text-align:center;margin-top:20px">📎 Rapport complet joint en PDF · Généré automatiquement par GitHub Actions + Claude API · Non contractuel</p>
  <div style="background:#c8a96e;height:2px;border-radius:1px;margin-top:18px"></div>
</div></body></html>"""


# ─── ÉTAPE 4 : Envoi email via Gmail API ──────────────────────────────────────
def send_email(report: dict, pdf_bytes: bytes):
    print(f"[3/4] Envoi email à {EMAIL_TO}...")
    week = report.get('week', 'Rapport hebdomadaire')

    # Construire le message MIME
    msg = MIMEMultipart('mixed')
    msg['From']    = f"MarketBrief <{EMAIL_FROM}>"
    msg['To']      = EMAIL_TO
    msg['Subject'] = f"{week} — MarketBrief"

    # Corps HTML
    html_part = MIMEText(build_email_html(report), 'html', 'utf-8')
    msg.attach(html_part)

    # PDF en pièce jointe
    pdf_part = MIMEBase('application', 'pdf')
    pdf_part.set_payload(pdf_bytes)
    encoders.encode_base64(pdf_part)
    filename = f"marketbrief-{week.replace(' ', '-').lower()}.pdf"
    pdf_part.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(pdf_part)

    # Encoder en base64 URL-safe pour Gmail API
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')

    # Appel Gmail API
    response = requests.post(
        'https://gmail.googleapis.com/gmail/v1/users/me/messages/send',
        headers={
            'Authorization': f'Bearer {GMAIL_TOKEN}',
            'Content-Type': 'application/json'
        },
        json={'raw': raw}
    )

    if response.status_code not in (200, 201):
        raise RuntimeError(f"Gmail API erreur {response.status_code}: {response.text}")

    print(f"[3/4] ✅ Email envoyé avec succès à {EMAIL_TO}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"\n{'='*60}")
    print(f"  WEEKLY MARKET BRIEF — {DATE_FR}")
    print(f"{'='*60}\n")

    report   = generate_report_json()
    pdf      = generate_pdf(report)
    send_email(report, pdf)

    print(f"\n[4/4] ✅ Tout terminé — Brief de la semaine envoyé !")
    print(f"{'='*60}\n")
