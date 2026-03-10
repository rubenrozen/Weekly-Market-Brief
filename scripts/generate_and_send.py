"""
Weekly Market Brief — Script d'automatisation
Appelé chaque vendredi à 19h (Panama) par GitHub Actions.
Refresh token → access token → Claude → JSON → PDF → Email + sauvegarde JSON
"""

import os, json, base64, re, requests
from datetime import datetime
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

ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
ANTHROPIC_MODEL     = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-6")
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_REFRESH_TOKEN"]
GMAIL_CLIENT_ID     = os.environ["GMAIL_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
EMAIL_FROM          = os.environ["EMAIL_FROM"]
EMAIL_TO            = os.environ["EMAIL_TO"]

NOW     = datetime.now()
DATE_FR = NOW.strftime("%A %d %B %Y")
WEEK_N  = NOW.strftime("S%W")


def get_fresh_access_token() -> str:
    print("[0/5] Renouvellement du token Gmail...")
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": GMAIL_CLIENT_ID, "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": GMAIL_REFRESH_TOKEN, "grant_type": "refresh_token",
    })
    if resp.status_code != 200:
        raise RuntimeError(f"Token Gmail impossible : {resp.text}")
    print("[0/5] ✅ Token renouvelé")
    return resp.json()["access_token"]


def generate_report_json() -> dict:
    print(f"[1/5] Appel Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system = (f"Tu es un analyste financier senior. Rapport hebdomadaire professionnel. "
              f"Date : {DATE_FR} ({WEEK_N} {NOW.year}). "
              f"Réponds UNIQUEMENT en JSON valide, sans texte autour, sans balises markdown.")
    user = f"""Génère un rapport hebdomadaire complet des marchés financiers en français pour la semaine du {DATE_FR}.

JSON (toutes clés obligatoires, valeurs réelles et cohérentes) :
{{
  "week": "Semaine du X au Y mois {NOW.year}",
  "generated_at": "{DATE_FR}",
  "section1_summary": {{"title":"Résumé des marchés","paragraphs":["§1","§2"]}},
  "section2_major_events": {{"title":"Événements majeurs","events":[{{"title":"...","body":"...","sentiment":"positive|negative|neutral"}}]}},
  "section3_blind_spots": {{"title":"Angles morts","events":[{{"title":"...","body":"...","sentiment":"neutral"}}]}},
  "section4_equities": {{
    "title":"Marchés Actions",
    "indices":[
      {{"name":"S&P 500","value":"5800","change":"+1.2%"}},{{"name":"NASDAQ","value":"18500","change":"-0.5%"}},
      {{"name":"CAC 40","value":"8100","change":"+0.8%"}},{{"name":"DAX","value":"18200","change":"+1.1%"}},
      {{"name":"Nikkei","value":"38000","change":"-0.3%"}},{{"name":"Hang Seng","value":"17500","change":"+2.1%"}},
      {{"name":"FTSE 100","value":"8500","change":"+0.6%"}},{{"name":"Shanghai","value":"3300","change":"+1.5%"}}
    ],
    "us":{{"headline":"...","body":"...","direction":"bullish|bearish|neutral"}},
    "eu":{{"headline":"...","body":"...","direction":"bullish|bearish|neutral"}},
    "asia":{{"headline":"...","body":"...","direction":"bullish|bearish|neutral"}}
  }},
  "section5_bonds": {{
    "title":"Obligations & Dérivés",
    "bond_market":{{"title":"Marché Obligataire","why":"...","implies":"...","strategy":"..."}},
    "yield_curves":{{"title":"Courbes de Taux","why":"...","implies":"...","strategy":"...","data":[
      {{"maturity":"3M","us":5.3,"de":3.1,"fr":3.4,"jp":0.1}},
      {{"maturity":"2Y","us":4.7,"de":2.8,"fr":3.0,"jp":0.3}},
      {{"maturity":"5Y","us":4.4,"de":2.6,"fr":2.9,"jp":0.5}},
      {{"maturity":"10Y","us":4.3,"de":2.5,"fr":2.8,"jp":0.7}},
      {{"maturity":"30Y","us":4.5,"de":2.7,"fr":3.0,"jp":1.8}}
    ]}},
    "derivatives":{{"title":"Produits Dérivés","why":"...","implies":"...","strategy":"...","vix":"18.5","put_call":"0.85","skew":"modéré"}},
    "credit_defaults":{{"title":"Taux de Défaut","why":"...","implies":"...","strategy":"...","data":[
      {{"category":"Investment Grade","rate":"0.8","trend":"stable"}},
      {{"category":"High Yield","rate":"3.2","trend":"hausse"}},
      {{"category":"Mortgage","rate":"1.1","trend":"stable"}},
      {{"category":"Auto","rate":"2.4","trend":"hausse"}},
      {{"category":"Credit Card","rate":"4.1","trend":"hausse"}}
    ]}}
  }},
  "section6_forex_commodities": {{
    "title":"Forex & Matières Premières",
    "forex_narrative":"...","commodities_narrative":"...",
    "forex_pairs":[
      {{"pair":"EUR/USD","value":"1.0850","change":"+0.3%","direction":"up"}},
      {{"pair":"GBP/USD","value":"1.2650","change":"-0.1%","direction":"down"}},
      {{"pair":"USD/JPY","value":"148.5","change":"+0.5%","direction":"up"}},
      {{"pair":"USD/CHF","value":"0.8950","change":"-0.2%","direction":"down"}},
      {{"pair":"AUD/USD","value":"0.6540","change":"+0.4%","direction":"up"}},
      {{"pair":"USD/CNY","value":"7.2400","change":"+0.1%","direction":"up"}}
    ],
    "commodities":[
      {{"name":"WTI Oil","value":"72.5","unit":"$/bbl","change":"-1.2%","direction":"down"}},
      {{"name":"Brent","value":"76.8","unit":"$/bbl","change":"-0.9%","direction":"down"}},
      {{"name":"Gold","value":"2050","unit":"$/oz","change":"+0.6%","direction":"up"}},
      {{"name":"Silver","value":"23.5","unit":"$/oz","change":"+1.1%","direction":"up"}},
      {{"name":"Copper","value":"3.85","unit":"$/lb","change":"+0.8%","direction":"up"}},
      {{"name":"Wheat","value":"580","unit":"$/bu","change":"-0.4%","direction":"down"}},
      {{"name":"Corn","value":"445","unit":"$/bu","change":"+0.2%","direction":"up"}},
      {{"name":"Nat. Gas","value":"1.85","unit":"$/MMBtu","change":"-2.1%","direction":"down"}}
    ]
  }},
  "section7_synthesis": {{
    "title":"Synthèse Holistique",
    "global_view":"...","upcoming_events":"...",
    "strategy":[
      {{"type":"Allocation","recommendation":"...","rationale":"..."}},
      {{"type":"Couverture","recommendation":"...","rationale":"..."}},
      {{"type":"Opportunité","recommendation":"...","rationale":"..."}},
      {{"type":"Risque clé","recommendation":"...","rationale":"..."}}
    ]
  }},
  "section8_calendar": {{
    "title":"Calendrier Économique — Semaine suivante",
    "events":[
      {{"date":"Lundi","country":"US","event":"...","impact":"high","previous":"...","forecast":"..."}},
      {{"date":"Mardi","country":"EU","event":"...","impact":"high","previous":"...","forecast":"..."}},
      {{"date":"Mercredi","country":"CN","event":"...","impact":"high","previous":"...","forecast":"..."}},
      {{"date":"Jeudi","country":"US","event":"...","impact":"high","previous":"...","forecast":"..."}},
      {{"date":"Vendredi","country":"EU","event":"...","impact":"high","previous":"...","forecast":"..."}}
    ]
  }}
}}
Remplis TOUTES les valeurs avec de vraies données actuelles. Professionnel, dense, actionnable."""

    for attempt in range(3):
        msg = client.messages.create(
            model=ANTHROPIC_MODEL, max_tokens=8000,
            system=system, messages=[{"role":"user","content":user}]
        )
        raw = msg.content[0].text
        match = re.search(r'\{[\s\S]*\}', raw)
        if not match:
            print(f"[1/5] ⚠️ Pas de JSON détecté (essai {attempt+1}/3), relance...")
            continue
        json_str = match.group()
        try:
            report = json.loads(json_str)
            print("[1/5] ✅ Rapport JSON généré")
            return report
        except json.JSONDecodeError as e:
            print(f"[1/5] ⚠️ JSON invalide (essai {attempt+1}/3) : {e}")
            # Tentative de réparation : tronquer au dernier objet complet
            try:
                # Trouver la dernière accolade fermante valide
                for end in range(len(json_str)-1, 0, -1):
                    if json_str[end] == '}':
                        try:
                            report = json.loads(json_str[:end+1])
                            print("[1/5] ✅ JSON réparé et parsé avec succès")
                            return report
                        except:
                            continue
            except:
                pass
            print(f"[1/5] Relance complète (essai {attempt+2}/3)...")
    raise ValueError("Impossible de parser le JSON après 3 essais")


def save_json(report: dict):
    """Sauvegarde le JSON dans data/latest-report.json pour affichage sur le site."""
    print("[2/5] Sauvegarde du JSON...")
    Path("data").mkdir(exist_ok=True)
    with open("data/latest-report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("[2/5] ✅ JSON sauvegardé dans data/latest-report.json")


def generate_pdf(report: dict) -> bytes:
    print("[3/5] Génération du PDF...")
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
        rightMargin=14*mm, leftMargin=14*mm,
        topMargin=20*mm, bottomMargin=20*mm)

    GOLD  = colors.HexColor('#C8A96E')
    LIGHT = colors.HexColor('#E8E8F0')
    MUTED = colors.HexColor('#6B6B80')
    GREEN = colors.HexColor('#4ADE80')
    RED   = colors.HexColor('#F87171')
    BLUE  = colors.HexColor('#7B9CFF')
    YELLOW= colors.HexColor('#FBBF24')

    def ps(name, **kw): return ParagraphStyle(name, **kw)
    S_T  = ps('t',  fontSize=22, textColor=LIGHT, fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=4)
    S_W  = ps('w',  fontSize=9,  textColor=GOLD,  fontName='Helvetica', alignment=TA_CENTER, spaceAfter=2)
    S_SB = ps('sb', fontSize=8,  textColor=MUTED, fontName='Helvetica', alignment=TA_CENTER, spaceAfter=16)
    S_SC = ps('sc', fontSize=13, textColor=LIGHT, fontName='Helvetica-Bold', spaceBefore=10, spaceAfter=5)
    S_BD = ps('bd', fontSize=8.5,textColor=MUTED, fontName='Helvetica', leading=13, spaceAfter=5)
    S_BL = ps('bl', fontSize=8.5,textColor=LIGHT, fontName='Helvetica-Bold', spaceAfter=3)
    S_GN = ps('gn', fontSize=7,  textColor=GREEN, fontName='Helvetica-Bold', spaceAfter=2)
    S_D  = ps('d',  fontSize=6.5,textColor=MUTED, fontName='Helvetica', alignment=TA_CENTER)

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
            story.append(Paragraph(obj.get(key,'—'), S_BD))
    def tbl(data, cols):
        t = Table(data, colWidths=cols)
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#1A1A24')),
            ('TEXTCOLOR',(0,0),(-1,0),GOLD),('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(-1,-1),7.5),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#111118'),colors.HexColor('#15151E')]),
            ('GRID',(0,0),(-1,-1),0.3,colors.HexColor('#2A2A3A')),
            ('PADDING',(0,0),(-1,-1),5),('TEXTCOLOR',(0,1),(-1,-1),MUTED),
        ]))
        return t

    story += [Spacer(1,30*mm), Paragraph("MARKET BRIEF", S_T),
              Paragraph(report.get('week',''), S_W),
              Paragraph(f"Généré le {report.get('generated_at', DATE_FR)}", S_SB),
              hr(GOLD, 2), Spacer(1,60*mm),
              Paragraph("Rapport produit par IA (Claude — Anthropic). Non contractuel.", S_D), PageBreak()]

    s1 = report.get('section1_summary',{})
    sec(1, s1.get('title','Résumé'))
    for p in s1.get('paragraphs',[]): story.append(Paragraph(p, S_BD))

    s2 = report.get('section2_major_events',{})
    sec(2, s2.get('title','Événements majeurs'))
    for ev in s2.get('events',[]):
        c = GREEN if ev.get('sentiment')=='positive' else RED if ev.get('sentiment')=='negative' else GOLD
        story.append(Paragraph(ev.get('title',''), S_BL))
        story.append(Paragraph(ev.get('body',''), S_BD))
        story.append(hr(c, 0.3))

    s3 = report.get('section3_blind_spots',{})
    sec(3, s3.get('title','Angles morts'))
    for ev in s3.get('events',[]):
        story.append(Paragraph(ev.get('title',''), S_BL))
        story.append(Paragraph(ev.get('body',''), S_BD))
        story.append(hr(BLUE, 0.3))

    story.append(PageBreak())
    s4 = report.get('section4_equities',{})
    sec(4, s4.get('title','Marchés Actions'))
    idx = s4.get('indices',[])
    if idx:
        data = [['Indice','Valeur','Variation']]
        for i in idx:
            c = GREEN if (i.get('change','') or '').startswith('+') else RED
            data.append([i.get('name',''), i.get('value',''),
                Paragraph(i.get('change',''), ps('ic', fontSize=8, textColor=c, fontName='Helvetica-Bold'))])
        story.append(tbl(data,[70*mm,50*mm,50*mm])); story.append(Spacer(1,4*mm))
    for key, label in [('us','US'),('eu','Europe'),('asia','Asie')]:
        reg = s4.get(key,{})
        dc  = {'bullish':GREEN,'bearish':RED,'neutral':YELLOW}.get(reg.get('direction',''), GOLD)
        story.append(Paragraph(f"{label} · {reg.get('direction','').upper()}", ps('rl', fontSize=7, textColor=dc, fontName='Helvetica-Bold', spaceAfter=1)))
        story.append(Paragraph(reg.get('headline',''), S_BL))
        story.append(Paragraph(reg.get('body',''), S_BD))

    story.append(PageBreak())
    s5 = report.get('section5_bonds',{})
    sec(5, s5.get('title','Obligations & Dérivés'))
    ingredient(s5.get('bond_market'), GOLD)
    ingredient(s5.get('yield_curves'), BLUE)
    yc = s5.get('yield_curves',{}).get('data',[])
    if yc:
        data = [['Maturité','US','DE','FR','JP']]
        for d in yc:
            data.append([d.get('maturity',''),f"{d.get('us','')}%",f"{d.get('de','')}%",f"{d.get('fr','')}%",f"{d.get('jp','')}%"])
        story.append(tbl(data,[30*mm]*5)); story.append(Spacer(1,4*mm))
    ingredient(s5.get('derivatives'), YELLOW)
    der = s5.get('derivatives',{})
    story.append(Paragraph(f"VIX : {der.get('vix','—')}  |  Put/Call : {der.get('put_call','—')}  |  Skew : {der.get('skew','—')}", S_BD))
    ingredient(s5.get('credit_defaults'), RED)
    dd = s5.get('credit_defaults',{}).get('data',[])
    if dd:
        data = [['Catégorie','Taux','Tendance']]
        for d in dd:
            tc = RED if d.get('trend')=='hausse' else (GREEN if d.get('trend')=='baisse' else YELLOW)
            data.append([d.get('category',''),f"{d.get('rate','')}%",
                Paragraph(d.get('trend',''), ps('tr', fontSize=7.5, textColor=tc, fontName='Helvetica-Bold'))])
        story.append(tbl(data,[80*mm,35*mm,55*mm]))

    story.append(PageBreak())
    s6 = report.get('section6_forex_commodities',{})
    sec(6, s6.get('title','Forex & Commodités'))
    story.append(Paragraph('Dynamiques Forex', S_BL))
    story.append(Paragraph(s6.get('forex_narrative','—'), S_BD))
    fx = s6.get('forex_pairs',[])
    if fx:
        data = [['Paire','Valeur','Variation']]
        for p in fx:
            c = GREEN if p.get('direction')=='up' else RED
            data.append([p.get('pair',''),p.get('value',''),
                Paragraph(p.get('change',''), ps('fc', fontSize=8, textColor=c, fontName='Helvetica-Bold'))])
        story.append(tbl(data,[50*mm,55*mm,65*mm])); story.append(Spacer(1,4*mm))
    story.append(Paragraph('Dynamiques Commodités', S_BL))
    story.append(Paragraph(s6.get('commodities_narrative','—'), S_BD))
    cm = s6.get('commodities',[])
    if cm:
        data = [['Actif','Valeur','Variation']]
        for c in cm:
            col = GREEN if c.get('direction')=='up' else RED
            data.append([c.get('name',''),f"{c.get('value','')} {c.get('unit','')}",
                Paragraph(c.get('change',''), ps('cc', fontSize=8, textColor=col, fontName='Helvetica-Bold'))])
        story.append(tbl(data,[50*mm,60*mm,60*mm]))

    story.append(PageBreak())
    s7 = report.get('section7_synthesis',{})
    sec(7, s7.get('title','Synthèse'))
    story.append(Paragraph("Vue d'ensemble", S_BL))
    story.append(Paragraph(s7.get('global_view','—'), S_BD))
    story.append(Paragraph("Événements à venir", S_BL))
    story.append(Paragraph(s7.get('upcoming_events','—'), S_BD))
    story.append(Paragraph("Stratégies recommandées", S_BL))
    for st in s7.get('strategy',[]):
        story.append(Paragraph(st.get('type','').upper(), S_GN))
        story.append(Paragraph(st.get('recommendation',''), S_BL))
        story.append(Paragraph(st.get('rationale',''), S_BD))
        story.append(hr(GREEN, 0.3))

    s8 = report.get('section8_calendar',{})
    sec(8, s8.get('title','Calendrier'))
    cal = s8.get('events',[])
    if cal:
        data = [['Date','Pays','Événement','Impact','Précédent','Prévision']]
        for ev in cal:
            ic = RED if ev.get('impact')=='high' else YELLOW
            data.append([ev.get('date',''),ev.get('country',''),ev.get('event',''),
                Paragraph('FORT' if ev.get('impact')=='high' else 'MOYEN',
                    ps('imp', fontSize=7, textColor=ic, fontName='Helvetica-Bold')),
                ev.get('previous','—'),ev.get('forecast','—')])
        story.append(tbl(data,[22*mm,14*mm,60*mm,18*mm,18*mm,18*mm]))

    doc.build(story)
    pdf = buffer.getvalue()
    print(f"[3/5] ✅ PDF généré ({len(pdf)//1024} KB)")
    return pdf


def build_email_html(report: dict) -> str:
    s4 = report.get('section4_equities',{})
    s7 = report.get('section7_synthesis',{})
    rows = ''.join([
        f'<tr><td style="padding:6px 10px;border-bottom:1px solid #2a2a3a;font-family:monospace;font-size:13px;color:#e8e8f0">{i["name"]}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #2a2a3a;font-family:monospace;color:#e8e8f0">{i["value"]}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #2a2a3a;font-family:monospace;color:{"#4ade80" if (i.get("change","") or "").startswith("+") else "#f87171"}">{i["change"]}</td></tr>'
        for i in s4.get('indices',[])
    ])
    strats = ''.join([
        f'<div style="border-left:3px solid #4ade80;padding:8px 12px;margin:6px 0;background:#1a1a24;border-radius:0 6px 6px 0">'
        f'<div style="color:#4ade80;font-family:monospace;font-size:10px;text-transform:uppercase;margin-bottom:3px">{s["type"]}</div>'
        f'<div style="color:#e8e8f0;font-size:13px;font-weight:bold;margin-bottom:3px">{s["recommendation"]}</div>'
        f'<div style="color:#6b6b80;font-size:12px">{s["rationale"]}</div></div>'
        for s in s7.get('strategy',[])
    ])
    paras = ''.join([f'<p style="color:#9090a8;font-size:13px;line-height:1.7;margin:0 0 8px">{p}</p>' for p in report.get('section1_summary',{}).get('paragraphs',[])])
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#0a0a0f;color:#e8e8f0;font-family:Arial,sans-serif;margin:0;padding:20px">
<div style="max-width:620px;margin:0 auto">
  <div style="background:#c8a96e;height:4px;border-radius:2px;margin-bottom:22px"></div>
  <div style="font-size:10px;color:#c8a96e;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px">{report.get('week','')}</div>
  <h1 style="font-family:Georgia,serif;font-size:26px;margin:0 0 4px">Weekly <span style="color:#c8a96e">Market</span> Brief</h1>
  <p style="color:#6b6b80;font-size:11px;margin:0 0 22px">Généré automatiquement le {report.get('generated_at', DATE_FR)}</p>
  <div style="background:#111118;border:1px solid #2a2a3a;border-radius:8px;padding:15px;margin-bottom:18px">
    <div style="color:#c8a96e;font-size:10px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px">Résumé</div>{paras}
  </div>
  <div style="background:#111118;border:1px solid #2a2a3a;border-radius:8px;padding:15px;margin-bottom:18px">
    <div style="color:#c8a96e;font-size:10px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px">Indices</div>
    <table style="width:100%;border-collapse:collapse"><thead><tr>
      <th style="text-align:left;padding:5px 10px;color:#6b6b80;font-size:10px;border-bottom:1px solid #2a2a3a">Indice</th>
      <th style="text-align:left;padding:5px 10px;color:#6b6b80;font-size:10px;border-bottom:1px solid #2a2a3a">Valeur</th>
      <th style="text-align:left;padding:5px 10px;color:#6b6b80;font-size:10px;border-bottom:1px solid #2a2a3a">Variation</th>
    </tr></thead><tbody>{rows}</tbody></table>
  </div>
  <div style="background:#111118;border:1px solid #2a2a3a;border-radius:8px;padding:15px;margin-bottom:18px">
    <div style="color:#c8a96e;font-size:10px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px">Stratégies</div>{strats}
  </div>
  <p style="color:#3a3a4a;font-size:10px;text-align:center">📎 Rapport complet en PDF joint · GitHub Actions + Claude API · Non contractuel</p>
  <div style="background:#c8a96e;height:2px;margin-top:18px"></div>
</div></body></html>"""


def send_email(report: dict, pdf: bytes, access_token: str):
    print(f"[4/5] Envoi email à {EMAIL_TO}...")
    week = report.get('week','Rapport hebdomadaire')
    msg = MIMEMultipart('mixed')
    msg['From'] = f"MarketBrief <{EMAIL_FROM}>"; msg['To'] = EMAIL_TO
    msg['Subject'] = f"{week} — MarketBrief"
    msg.attach(MIMEText(build_email_html(report), 'html', 'utf-8'))
    pdf_part = MIMEBase('application','pdf')
    pdf_part.set_payload(pdf); encoders.encode_base64(pdf_part)
    pdf_part.add_header('Content-Disposition','attachment',filename=f"marketbrief-{week.replace(' ','-').lower()}.pdf")
    msg.attach(pdf_part)
    raw  = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')
    resp = requests.post('https://gmail.googleapis.com/gmail/v1/users/me/messages/send',
        headers={'Authorization':f'Bearer {access_token}','Content-Type':'application/json'},
        json={'raw': raw})
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Gmail API {resp.status_code}: {resp.text}")
    print(f"[4/5] ✅ Email envoyé à {EMAIL_TO}")


if __name__ == '__main__':
    print(f"\n{'='*60}\n  WEEKLY MARKET BRIEF — {DATE_FR}\n{'='*60}\n")
    token  = get_fresh_access_token()
    report = generate_report_json()
    save_json(report)          # ← nouveau : sauvegarde pour le site web
    pdf    = generate_pdf(report)
    send_email(report, pdf, token)
    print(f"\n[5/5] ✅ Brief envoyé et publié !\n{'='*60}\n")
