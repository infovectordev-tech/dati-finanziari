import os
import re
import feedparser
import math
import json
import time
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
import unicodedata

# Boto3 per Cloudflare R2
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import yfinance as yf
import ta
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from financial_lexicon import LEXICON

# Indicatori tecnici e statistica
from ta.momentum import RSIIndicator, StochasticOscillator, WilliamsRIndicator
from ta.trend import MACD, EMAIndicator, CCIIndicator
from ta.volatility import BollingerBands
from urllib.parse import quote_plus, urlparse, urlunparse
from collections import defaultdict
from scipy.stats import spearmanr, pearsonr
from statsmodels.stats.multitest import multipletests
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm

# --- SETUP AI: TURBO-VADER (VADER + Expanded Financial Lexicon) ---
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
except:
    print("Spacy model not found, proceeding without lemmatization for compatibility.")

# AGGIORNA IL LESSICO UNA VOLTA SOLA
sia = SentimentIntensityAnalyzer()
sia.lexicon.update(LEXICON)

# --- SECRETS E VARIABILI D'AMBIENTE ---
FMP_API_KEY = os.getenv("FMP_API_KEY")

# ==============================================================================
# PREZZI MOLTO PICCOLI (es. SHIB ~0.000004)
# ==============================================================================
SOGLIA_MICRO_PREZZO = 0.001

def is_micro_price(price):
    try: return 0 < abs(float(price)) < SOGLIA_MICRO_PREZZO
    except (TypeError, ValueError): return False

def fmt_micro(val):
    # Decimali fissi (mai notazione scientifica: l'app mostra alcune stringhe cosi' come sono)
    return f"{float(val):.12f}".rstrip("0").rstrip(".")

# Ticker Yahoo "riciclati": prima di questa data lo storico appartiene a un altro asset.
# Toncoin e' diventato Gram (06/2026): Yahoo ha abbandonato TON11419-USD (1 sola candela) e su
# GRAM-USD, prima del 12/06/2026, ci sono i prezzi di un vecchio token omonimo (~0.002).
STORICO_VALIDO_DAL = {"GRAM-USD": "2026-06-12"}

def controlla_salute_storico(ticker, data):
    # Un ticker rinominato/delistato non genera errori: senza questo avviso passa inosservato
    if data.empty:
        print(f"⚠️ TICKER SOSPETTO {ticker}: nessun dato da Yahoo")
        return
    giorni_fermo = (pd.Timestamp.now().normalize() - data.index[-1].tz_localize(None)).days
    if len(data) < 30:
        print(f"⚠️ TICKER SOSPETTO {ticker}: solo {len(data)} candele (rinominato su Yahoo o appena quotato?)")
    if giorni_fermo > 7:
        print(f"⚠️ TICKER SOSPETTO {ticker}: storico fermo da {giorni_fermo} giorni (delistato?)")

def scarica_storico(ticker, period="5y"):
    data = yf.download(ticker, period=period, auto_adjust=True, progress=False)
    if ticker in STORICO_VALIDO_DAL and not data.empty:
        data = data[data.index >= STORICO_VALIDO_DAL[ticker]]
    if isinstance(data.columns, pd.MultiIndex):
        try: data = data.xs(ticker, axis=1, level=1)
        except: pass
    if ticker != "SHIB-USD" or data.empty:
        return data

    # Yahoo arrotonda lo storico di SHIB-USD a 6 decimali (prezzi a gradini):
    # ricostruiamo OHLC in USD da SHIB-KRW / KRW=X. Il volume resta quello di SHIB-USD.
    krw = yf.download("SHIB-KRW", period=period, auto_adjust=True, progress=False)
    fx = yf.download("KRW=X", period=period, auto_adjust=True, progress=False)
    for df in (krw, fx):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
    if krw.empty or fx.empty or fx["Close"].dropna().empty:
        print("SHIB: SHIB-KRW o KRW=X non disponibili, asset saltato.")
        return data.iloc[0:0]  # meglio nessun dato che dati falsati

    krw = krw.dropna(subset=["Close"])
    # Il forex non quota nel weekend: si usa l'ultimo cambio disponibile
    cambio = fx["Close"].dropna().reindex(krw.index, method="ffill").bfill()
    data = data.reindex(krw.index)
    for col in ["Open", "High", "Low", "Close"]:
        data[col] = krw[col] / cambio
    return data

# --- CLASSE GESTORE CLOUDFLARE R2 ---
class CloudflareR2Manager:
    def __init__(self, bucket_name="trading-data", base_folder=""):
        self.bucket_name = bucket_name
        self.base_folder = base_folder.strip("/")
        
        self.account_id = os.environ.get("R2_ACCOUNT_ID")
        self.access_key = os.environ.get("R2_ACCESS_KEY_ID")
        self.secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
        
        if not all([self.account_id, self.access_key, self.secret_key]):
            raise ValueError("ERRORE: Chiavi Cloudflare R2 mancanti nei Secrets!")

        self.s3 = boto3.client(
            's3',
            endpoint_url=f"https://{self.account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version='s3v4'),
            region_name='auto'
        )

    def _get_full_path(self, file_path):
        """Unisce la cartella base al percorso del file (se presente)."""
        if self.base_folder and not file_path.startswith(self.base_folder):
            return f"{self.base_folder}/{file_path}"
        return file_path

    def read_json(self, file_path):
        """Legge un file JSON da R2. Ritorna {} se non esiste."""
        full_path = self._get_full_path(file_path)
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=full_path)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return {} 
            print(f"Errore R2 in lettura di {full_path}: {e}")
            return {}
        except Exception as e:
            print(f"Errore generico lettura {full_path}: {e}")
            return {}

    def write_file(self, file_path, content, is_json=False):
        """Scrive un file su R2 (HTML o JSON)."""
        full_path = self._get_full_path(file_path)
        content_type = 'application/json' if is_json else 'text/html'
        
        if is_json and not isinstance(content, str):
            content = json.dumps(content, indent=4)
            
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=full_path,
                Body=content,
                ContentType=content_type,
                CacheControl='max-age=14400' # 4 Ore
            )
            return True
        except Exception as e:
            print(f"Errore R2 in scrittura di {full_path}: {e}")
            return False

# --- INIZIALIZZAZIONE CLOUDFLARE ---
r2_manager = CloudflareR2Manager(bucket_name="trading-data", base_folder="")

# --- CONFIGURAZIONE CARTELLA OUTPUT SU R2 ---
TARGET_FOLDER = "hybrid_results"
TEST_FOLDER = f"{TARGET_FOLDER}/forward_testing" 
ARCHIVE_FOLDER = f"{TARGET_FOLDER}/news_archive"

# --- INSIDER FLOW (file additivo insider/<SYMBOL>.json, ignorato dal client Java) ---
INSIDER_FOLDER = f"{TARGET_FOLDER}/insider"

MAX_TRADES = 25
WINDOW_DAYS = 90        # finestra preferita per la barra buy/sell
FETCH_DAYS = 365        # quanto indietro si scaricano le operazioni
# Gli insider operano a intermittenza: se negli ultimi 90 giorni non c'e' nulla, la finestra si allarga
# (180, poi 365) e il JSON dichiara i giorni effettivi in d90.days, cosi' la barra non resta mai vuota.
WINDOW_STEPS = (90, 180, 365)

_fx_cache = {}


def _fx_rate(frm, to):
    """Cambio frm->to da Yahoo (es. 'USDEUR=X'). None se non disponibile (le operazioni verranno escluse)."""
    frm, to = str(frm).upper(), str(to).upper()
    if frm == to or not to:
        return 1.0
    key = (frm, to)
    if key not in _fx_cache:
        rate = None
        try:
            h = yf.Ticker(f"{frm}{to}=X").history(period="5d")
            if len(h):
                rate = float(h["Close"].iloc[-1])
        except Exception as e:
            print(f"FX {frm}->{to} non disponibile: {e}")
        _fx_cache[key] = rate if rate and rate > 0 else None
    return _fx_cache[key]


def _norm_cols(df):
    df = df.copy()
    df.columns = [str(c).replace("\xa0", " ").strip() for c in df.columns]
    return df


def _money(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r"[\$,+]", "", regex=True), errors="coerce"
    )


def build_openinsider_payload(raw_df, currency="USD"):
    """Trasforma la tabella OpenInsider (Form 4) nel JSON per l'app.

    Ritorna None se non c'e' nulla di utile o in caso di qualsiasi errore, cosi'
    non puo' mai compromettere il resto della pipeline.
    """
    try:
        df = _norm_cols(raw_df)
        type_col = "Trade Type"
        if type_col not in df.columns or "Trade Date" not in df.columns:
            return None

        df["_date"] = pd.to_datetime(df["Trade Date"], errors="coerce")
        df["_value"] = _money(df["Value"]).abs() if "Value" in df.columns else 0.0
        df["_qty"] = _money(df["Qty"]).abs() if "Qty" in df.columns else 0.0
        df["_price"] = _money(df["Price"]) if "Price" in df.columns else 0.0
        df["_owned"] = _money(df["Owned"]) if "Owned" in df.columns else None
        t = df[type_col].astype(str)
        df["_buy"] = t.str.contains("Purchase", na=False)
        df["_sell"] = t.str.contains("Sale", na=False)
        df["_oe"] = t.str.contains(r"\+OE", na=False)
        df = df[(df["_buy"] | df["_sell"]) & df["_date"].notna()]
        df = df.sort_values("_date", ascending=False)
        if df.empty:
            return None

        name_col = "Insider Name" if "Insider Name" in df.columns else None
        cutoff = datetime.now() - timedelta(days=FETCH_DAYS)
        trades = []
        for _, r in df[df["_date"] >= cutoff].iterrows():
            owned = r["_owned"]
            trades.append({
                "date": r["_date"].to_pydatetime(),
                "name": str(r[name_col]).strip() if name_col else "",
                "role": str(r.get("Title", "") or "").strip(),
                "type": "buy" if r["_buy"] else "sell",
                "shares": float(r["_qty"]) if pd.notna(r["_qty"]) else 0.0,
                "price": float(r["_price"]) if pd.notna(r["_price"]) else 0.0,
                "value": float(r["_value"]) if pd.notna(r["_value"]) else 0.0,
                "owned": float(owned) if owned is not None and pd.notna(owned) else None,
                "oe": bool(r["_oe"]),
            })
        trades = [t for t in trades if t["value"] > 0 and t["name"]]
        return _assemble_insider_payload(trades, currency, "openinsider")
    except Exception as e:  # mai rompere la pipeline principale
        print(f"build_openinsider_payload errore: {e}")
        return None


# --- INSIDER EUROPA (fonti ufficiali gratuite, senza chiavi ne' limiti) ---
# Germania: BaFin Directors' Dealings (export CSV di tutto il mercato, 1 sola richiesta)
# Francia : AMF/BDIF (elenco JSON + PDF della dichiarazione, parsing con pypdf)
# Italia/Spagna/UK: nessuna fonte aperta affidabile (Consob/CNMV non scaricabili da script).
import csv
import html as _html
import io

INSIDER_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"}

BAFIN_CSV_URL = ("https://portal.mvp.bafin.de/database/DealingsInfo/sucheForm.do?meldepflichtigerName="
                 "&zeitraum=0&d-4000784-e=1&emittentButton=Suche+Emittent&emittentName=&emittentIsin="
                 "&emittentLei=&transaktionsdatumBis=&transaktionsdatumVon=&6578706f7274=1")
DE_ISIN = {
    "SAP.DE": "DE0007164600", "SIE.DE": "DE0007236101", "P911.DE": "DE000PAG9113",
    "ALV.DE": "DE0008404005", "VOW3.DE": "DE0007664039", "MBG.DE": "DE0007100000",
    "DTE.DE": "DE0005557508",
}
# Francia: parola chiave nella ragione sociale AMF (maiuscolo, senza accenti/apostrofi)
FR_ISSUER_KEYS = {
    "MC.PA": ["LVMH"], "TTE.PA": ["TOTALENERGIES"], "OR.PA": ["OREAL"],
    "SAN.PA": ["SANOFI"], "BNP.PA": ["BNP PARIBAS"], "AIR.PA": ["AIRBUS"],
}
AMF_API = "https://bdif.amf-france.org/back/api/v1"
AMF_MAX_PDFS = 40

_DE_ROLES = {"Vorstand": "Management Board", "Aufsichtsrat": "Supervisory Board",
             "in enger Beziehung": "Closely associated", "Sonstige Führungsperson": "Senior executive"}
_bafin_rows_cache = None
_amf_decl_cache = None
_amf_parsed_cache = {}


def _clean_person(s):
    """Decodifica entita' HTML (&amp;) e porta a Title Case i nomi TUTTI MAIUSCOLI."""
    s = _html.unescape(str(s or "")).strip()
    return s.title() if s.isupper() and len(s) > 3 else s


def _assemble_insider_payload(trades, currency, source):
    """trades: lista di dict {date(datetime), name, role, type, shares, price, value, oe, [cur], [owned]}.
    Le operazioni in valuta diversa da `currency` vengono convertite (o escluse se manca il cambio),
    cosi' gli importi non mescolano mai valute. Ritorna None se non ci sono operazioni negli ultimi FETCH_DAYS."""
    if not trades:
        return None
    norm = []
    for t in trades:
        t = dict(t)
        cur = str(t.get("cur") or currency or "").upper()
        if currency and cur and cur != currency.upper():
            rate = _fx_rate(cur, currency)
            if rate is None:
                continue
            t["price"], t["value"] = t["price"] * rate, t["value"] * rate
        norm.append(t)
    cutoff_all = datetime.now() - timedelta(days=FETCH_DAYS)
    trades = sorted((t for t in norm if t["date"] >= cutoff_all), key=lambda t: t["date"], reverse=True)
    if not trades:
        return None
    # Si sceglie la finestra piu' piccola che contiene SIA acquisti SIA vendite: fermarsi alla prima con
    # "qualcosa" dava barre 100% vendite mentre nell'elenco comparivano acquisti piu' vecchi.
    # Se non esiste una finestra con entrambi, si usa la piu' ampia (quella da cui arriva l'elenco).
    days = WINDOW_STEPS[-1]
    buys = sells = []
    for d_ in WINDOW_STEPS:
        cut = datetime.now() - timedelta(days=d_)
        w = [t for t in trades if t["date"] >= cut]
        buys = [t for t in w if t["type"] == "buy"]
        sells = [t for t in w if t["type"] == "sell"]
        days = d_
        if buys and sells:
            break
    return {
        "v": 1,
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "currency": currency,
        "source": source,
        "d90": {                      # chiave storica: la finestra reale e' in "days"
            "days": days,
            "buy": round(sum(t["value"] for t in buys), 2),
            "sell": round(sum(t["value"] for t in sells), 2),
            "buyers": len({t["name"] for t in buys}),
            "sellers": len({t["name"] for t in sells}),
            "buy_n": len(buys),
            "sell_n": len(sells),
        },
        "trades": [
            {
                "date": t["date"].strftime("%Y-%m-%d"), "name": _clean_person(t["name"]), "role": _clean_person(t["role"]),
                "type": t["type"], "shares": round(t["shares"], 2), "price": round(t["price"], 4),
                "value": round(t["value"], 2), "owned": t.get("owned"), "oe": bool(t.get("oe", False)),
            }
            for t in trades[:MAX_TRADES]
        ],
    }


def _eu_number(s):
    """'9.911,77 EUR' -> (9911.77, 'EUR'); '0.0000 Euro' -> (0.0, 'EUR')."""
    s = str(s).strip()
    cur = "EUR"
    m = re.search(r"([A-Za-z]{3,})\s*$", s)
    if m:
        cur = "EUR" if m.group(1).lower() == "euro" else m.group(1).upper()
    num = re.sub(r"[^0-9,.\-]", "", s)
    if "," in num:                       # formato europeo 1.234,56
        num = num.replace(".", "").replace(",", ".")
    try:
        return float(num), cur
    except ValueError:
        return 0.0, cur


def _fetch_bafin_rows():
    global _bafin_rows_cache
    if _bafin_rows_cache is not None:
        return _bafin_rows_cache
    rows = []
    try:
        r = requests.get(BAFIN_CSV_URL, headers=INSIDER_HEADERS, timeout=90)
        r.raise_for_status()
        text = r.content.decode("utf-8-sig")
        rows = list(csv.DictReader(io.StringIO(text), delimiter=";"))
    except Exception as e:
        print(f"BaFin errore: {e}")
    _bafin_rows_cache = rows
    return rows


def build_bafin_payload(adjusted_symbol):
    isin = DE_ISIN.get(adjusted_symbol)
    if not isin:
        return None
    try:
        trades, currency = [], "EUR"
        for r in _fetch_bafin_rows():
            if (r.get("ISIN") or "").strip() != isin:
                continue
            if (r.get("Art des Instruments") or "").strip() != "Aktie":
                continue
            kind = (r.get("Art des Geschäfts") or "").strip()
            if kind not in ("Kauf", "Verkauf"):
                continue                 # "Sonstiges" = assegnazioni/altro, non e' mercato
            try:
                d = datetime.strptime((r.get("Datum des Geschäfts") or "").strip(), "%d.%m.%Y")
            except ValueError:
                continue
            price, currency = _eu_number(r.get("Durchschnittspreis", ""))
            value, _ = _eu_number(r.get("Aggregiertes Volumen", ""))
            if value <= 0:
                continue
            role = _DE_ROLES.get((r.get("Position / Status") or "").strip(), (r.get("Position / Status") or "").strip())
            trades.append({
                "date": d, "name": (r.get("Meldepflichtiger") or "").strip(), "role": role,
                "type": "buy" if kind == "Kauf" else "sell",
                "shares": (value / price) if price > 0 else 0.0, "price": price, "value": value, "cur": currency,
            })
        return _assemble_insider_payload(trades, "EUR", "bafin")
    except Exception as e:
        print(f"build_bafin_payload errore: {e}")
        return None


def _norm_issuer(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9 ]", "", s.upper().replace("'", ""))


def _fetch_amf_declarations():
    """Dichiarazioni dei dirigenti degli ultimi FETCH_DAYS giorni (paginate da 500)."""
    global _amf_decl_cache
    if _amf_decl_cache is not None:
        return _amf_decl_cache
    out = []
    cutoff = datetime.now() - timedelta(days=FETCH_DAYS)
    try:
        for page in range(14):
            r = requests.get(f"{AMF_API}/informations?TypesInformation=DD&Size=500&From={page * 500}",
                             headers=INSIDER_HEADERS, timeout=60)
            r.raise_for_status()
            items = r.json().get("result", [])
            if not items:
                break
            stop = False
            for it in items:
                d = datetime.strptime(it["dateInformation"][:10], "%Y-%m-%d")
                if d < cutoff:
                    stop = True
                    continue
                docs = it.get("documents") or []
                soc = it.get("societes") or []
                if docs and soc and docs[0].get("path"):
                    out.append({"numero": it.get("numero"), "issuer": _norm_issuer(soc[0].get("raisonSociale", "")),
                                "path": docs[0]["path"]})
            if stop:
                break
    except Exception as e:
        print(f"AMF elenco errore: {e}")
    _amf_decl_cache = out
    return out


_FR_MONTHS = {"janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6, "juillet": 7,
              "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11, "decembre": 12}


def _parse_amf_pdf(path):
    """Estrae una transazione dal PDF AMF. Ritorna dict o None."""
    if path in _amf_parsed_cache:
        return _amf_parsed_cache[path]
    res = None
    try:
        from pypdf import PdfReader          # dipendenza opzionale: pip install pypdf
        r = requests.get(f"{AMF_API}/documents/{path}", headers=INSIDER_HEADERS, timeout=60)
        r.raise_for_status()
        text = "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(r.content)).pages)

        def grab(patterns):
            for p in patterns:
                m = re.search(p, text, re.I | re.S)
                if m:
                    return m.group(1).strip()
            return ""

        person = grab([r"PERSON CLOSELY ASSOCIATED\s*:\s*\n(.+?)\n(?:INITIAL|NOTIFICATION)",
                       r"PERSONNE ETROITEMENT LIEE\s*:\s*\n(.+?)\n(?:NOTIFICATION)"])
        nature = grab([r"NATURE OF THE TRANSACTION\s*:\s*([^\n]+)", r"NATURE DE LA TRANSACTION\s*:\s*([^\n]+)"]).lower()
        inst = grab([r"TYPE OF INSTRUMENT\s*:\s*([^\n]+)", r"DESCRIPTION DE L.INSTRUMENT FINANCIER\s*:\s*([^\n]+)"]).lower()
        date_s = grab([r"DATE OF THE TRANSACTION\s*:\s*([^\n]+)", r"DATE DE LA TRANSACTION\s*:\s*([^\n]+)"])
        vol_s = grab([r"AGGREGATED VOLUME\s*:\s*([\d.,]+)", r"INFORMATIONS AGREGEES.*?VOLUME\s*:\s*([\d.,]+)"])
        price_s = grab([r"AGGREGATED INFORMATION\s*PRICE\s*:\s*([^\n]+)", r"INFORMATIONS AGREGEES\s*PRIX\s*:\s*([^\n]+)"])
        oe = bool(re.search(r"(EXERCISE OF A SHARE OPTION|EXERCICE DE PROGRAMMES D.OPTIONS)[^:]*:\s*(YES|OUI)", text, re.I))

        if any(k in nature for k in ("acquisition", "purchase", "achat")):
            kind = "buy"
        elif any(k in nature for k in ("cession", "disposal", "sale", "vente")):
            kind = "sell"
        else:
            kind = None
        price, cur = _eu_number(price_s)
        volume = float(re.sub(r"[^0-9.]", "", vol_s) or 0)       # il PDF usa il punto decimale
        d = None
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                d = datetime.strptime(date_s, fmt)
                break
            except ValueError:
                pass
        if d is None:                                             # mesi francesi
            m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_s)
            if m:
                mon = _FR_MONTHS.get(_norm_issuer(m.group(2)).lower())
                if mon:
                    d = datetime(int(m.group(3)), mon, int(m.group(1)))
        # price == 0 => attribuzione gratuita/vesting, non e' un'operazione di mercato
        if kind and d and price > 0 and volume > 0 and person and not any(
                x in inst for x in ("obligation", "bond", "option", "deriv", "dériv", "warrant")):
            name, _, role = person.partition(",")
            res = {"date": d, "name": name.strip(), "role": role.strip(), "type": kind,
                   "shares": volume, "price": price, "value": price * volume, "oe": oe, "cur": cur}
    except ImportError:
        print("pypdf non installato: salto gli insider francesi (pip install pypdf)")
    except Exception as e:
        print(f"AMF PDF errore ({path[:20]}): {e}")
    _amf_parsed_cache[path] = res
    return res


def build_amf_payload(adjusted_symbol):
    keys = FR_ISSUER_KEYS.get(adjusted_symbol)
    if not keys:
        return None
    try:
        trades = []
        mine = [d for d in _fetch_amf_declarations() if any(k in d["issuer"] for k in keys)][:AMF_MAX_PDFS]
        for d in mine:
            t = _parse_amf_pdf(d["path"])
            if t:
                trades.append(t)
        return _assemble_insider_payload(trades, "EUR", "amf")
    except Exception as e:
        print(f"build_amf_payload errore: {e}")
        return None



# --- ITALIA: eMarket Storage (OAM) ---------------------------------------------
# Elenco comunicati per emittente (HTML pubblico) + PDF nel modello standard
# ESMA/MAR art.19 (bilingue IT/EN). Copre gli emittenti che usano eMarket Storage.
EMS_BASE = "https://www.emarketstorage.it"
IT_ISSUERS = {   # ticker Yahoo -> (id emittente eMarket Storage, ISIN azione)
    "ISP.MI": (142, "IT0000072618"), "ENEL.MI": (1003, "IT0003128367"),
    "G.MI": (236, "IT0000062072"), "LDO.MI": (97, "IT0003856405"),
    "MONC.MI": (80007, "IT0004965148"), "PST.MI": (2355, "IT0003796171"),
    "UCG.MI": (198, "IT0005239360"), "UNI.MI": (1071, "IT0004810456"),
}
IT_MAX_PAGES = 6
IT_MAX_PDFS = 60
_it_parsed_cache = {}


def _parse_qty(s):
    s = str(s).strip()
    if re.fullmatch(r"\d{1,3}([.,]\d{3})+", s):      # 7.250 / 7,250 -> 7250
        return float(re.sub(r"[.,]", "", s))
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return 0.0


def _parse_price(s):
    s = str(s).strip()
    if "," in s and "." in s:
        dec = "," if s.rfind(",") > s.rfind(".") else "."
        s = s.replace("." if dec == "," else ",", "").replace(dec, ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _fetch_ems_list(company_id):
    """[(data datetime, url pdf)] piu' recenti per primi, ultimi FETCH_DAYS giorni."""
    cutoff = datetime.now() - timedelta(days=FETCH_DAYS)
    out, seen = [], set()
    for page in range(IT_MAX_PAGES):
        r = requests.get(f"{EMS_BASE}/it/comunicati-finanziari",
                         params={"azienda": company_id, "titolo": "internal dealing", "page": page},
                         headers=INSIDER_HEADERS, timeout=60)
        r.raise_for_status()
        rows = re.findall(r'href="(/sites/default/files/comunicati/[^"]+\.pdf)"[^>]*>\s*<time[^>]*>\s*(\d{2}/\d{2}/\d{4})', r.text)
        if not rows:
            break
        old = False
        for url, d in rows:
            dt = datetime.strptime(d, "%d/%m/%Y")
            if dt < cutoff:
                old = True
                continue
            if url not in seen:
                seen.add(url)
                out.append((dt, url))
        if old:
            break
        time.sleep(0.4)
    return out


def _parse_esma_pdf(url, isin):
    """Parsa un PDF nel modello ESMA art.19 MAR. Ritorna dict o None."""
    key = (url, isin)
    if key in _it_parsed_cache:
        return _it_parsed_cache[key]
    res = None
    try:
        from pypdf import PdfReader          # dipendenza opzionale: pip install pypdf
        r = requests.get(EMS_BASE + url, headers=INSIDER_HEADERS, timeout=60)
        r.raise_for_status()
        text = "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(r.content)).pages)
        m_isin = re.search(r"ISIN:?\s*([A-Z]{2}[A-Z0-9]{9}\d)", text)
        if m_isin and m_isin.group(1) != isin:
            raise ValueError("strumento diverso")          # obbligazioni, altre azioni, ecc.

        def grab(pat, flags=re.S):
            m = re.search(pat, text, flags)
            return m.group(1).strip() if m else ""

        first = grab(r"First name:\s*\n\s*(.*?)\s*Cognome:")
        last = grab(r"Last name:\s*\n\s*([^\n]+)")
        name = (first + " " + last).strip().title() if (first or last) else "N/D"
        role = grab(r"Role:\s*\n\s*([^\n]+)")
        nature = grab(r"\n([^\n]{2,60})\nA norma dell.articolo 19, paragrafo 6").lower()
        if not nature:
            nature = grab(r"\n([^\n]{2,60})\nPursuant to Article 19\(6\)").lower()
        option = grab(r"programm[ai] di opzioni su azioni[^\n]*\n[^\n]*\n\s*(Si|Sì|No|Yes)\b", re.S | re.I).lower() in ("si", "sì", "yes")
        price = _parse_price(grab(r"Prezzo:\s*([\d.,]+)") or grab(r"Price:\s*([\d.,]+)"))
        qty = _parse_qty(grab(r"Volume aggregato:\s*([\d.,]+)") or grab(r"Aggregated volume:\s*([\d.,]+)"))
        cur = grab(r"Prezzo:\s*[\d.,]+\s*([A-Z]{3})") or "EUR"
        d_s = grab(r"Date of the transaction.*?(20\d{2}-\d{2}-\d{2})")
        d = datetime.strptime(d_s, "%Y-%m-%d") if d_s else None

        if any(k in nature for k in ("acquisto", "acquisizione", "purchase", "acquisition", "buy")):
            kind = "buy"
        elif any(k in nature for k in ("cessione", "vendita", "sale", "disposal", "sell")):
            kind = "sell"
        else:
            kind = None            # assegnazioni, esercizi, donazioni... non sono operazioni di mercato
        if kind and d and price > 0 and qty > 0:
            res = {"date": d, "name": name, "role": role, "type": kind, "shares": qty,
                   "price": price, "value": price * qty, "oe": option, "cur": cur}
    except ImportError:
        print("pypdf non installato: salto gli insider italiani (pip install pypdf)")
    except Exception as e:
        print(f"eMarket PDF errore ({str(url)[-24:]}): {e}")
    _it_parsed_cache[key] = res
    return res


def build_emarket_payload(adjusted_symbol):
    info = IT_ISSUERS.get(adjusted_symbol)
    if not info:
        return None
    try:
        company_id, isin = info
        trades = []
        for dt, url in _fetch_ems_list(company_id)[:IT_MAX_PDFS]:
            t = _parse_esma_pdf(url, isin)
            if t:
                trades.append(t)
            time.sleep(0.2)
        return _assemble_insider_payload(trades, "EUR", "emarketstorage")
    except Exception as e:
        print(f"build_emarket_payload errore: {e}")
        return None


# --- FALLBACK UNIVERSALE: Yahoo Finance (gia' usato dallo script, nessuna chiave) ---
# Copre ad es. Regno Unito, parte di Spagna/Francia. Valuta non esplicita -> "".
def build_yahoo_insider_payload(adjusted_symbol):
    try:
        df = yf.Ticker(adjusted_symbol).insider_transactions
        if df is None or len(df) == 0:
            return None
        trades = []
        for _, r in df.iterrows():
            text = str(r.get("Text", "") or "")
            low = text.lower()
            if low.startswith("buy back") or "exercise" in low:
                continue
            if low.startswith(("bought", "purchase", "buy")):
                kind = "buy"
            elif low.startswith(("sold", "sale", "sell")):
                kind = "sell"
            else:
                continue
            m = re.search(r"price\s+([\d.,]+)", text)
            price = _parse_price(m.group(1)) if m else 0.0
            shares = float(r.get("Shares", 0) or 0)
            value = r.get("Value")
            value = float(value) if value is not None and pd.notna(value) else shares * price
            d = pd.to_datetime(r.get("Start Date"), errors="coerce")
            if pd.isna(d) or value <= 0:
                continue
            name = str(r.get("Insider", "") or "").strip()
            m2 = re.match(r"^(.+?)\s*\((.+)\)$", name)            # "Smith (Andrew)" -> "Andrew Smith"
            if m2:
                name = f"{m2.group(2)} {m2.group(1)}"
            trades.append({"date": d.to_pydatetime().replace(tzinfo=None), "name": name.title(),
                           "role": str(r.get("Position", "") or "").strip(), "type": kind,
                           "shares": shares, "price": price, "value": value})
        return _assemble_insider_payload(trades, "USD" if "." not in str(adjusted_symbol) else "", "yahoo")
    except Exception as e:
        print(f"build_yahoo_insider_payload errore: {e}")
        return None



# --- OLANDA: AFM (registro ufficiale, export XML + pagina di dettaglio per notifica) ---
# Copre gli emittenti registrati nei Paesi Bassi anche se quotati altrove (Stellantis, Ferrari, STM, Airbus...).
import zipfile
import xml.etree.ElementTree as ET

AFM_XML_URL = "https://www.afm.nl/export.aspx?type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=xml"
AFM_DETAIL_URL = "https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-/details?id={id}"
NL_ISSUERS = {   # ticker Yahoo -> nomi emittente nel registro AFM (confronto esatto, senza maiuscole)
    "STLAM.MI": ["stellantis n.v."], "RACE.MI": ["ferrari n.v."],
    "STMMI.MI": ["stmicroelectronics n.v."], "AIR.PA": ["airbus se"],
}
AFM_MAX_DETAILS = 40
_afm_entries_cache = None
_afm_detail_cache = {}


def _fetch_afm_entries():
    """Notifiche degli ultimi FETCH_DAYS giorni: [{id, date, issuer, person, role}]. Un solo download (~5 MB)."""
    global _afm_entries_cache
    if _afm_entries_cache is not None:
        return _afm_entries_cache
    out = []
    try:
        r = requests.get(AFM_XML_URL, headers=INSIDER_HEADERS, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        cutoff = datetime.now() - timedelta(days=FETCH_DAYS)
        for v in root.findall("vermelding"):
            try:
                d = datetime.strptime((v.findtext("transactiedatum") or "").strip(), "%m/%d/%Y %I:%M:%S %p")
            except ValueError:
                continue
            if d < cutoff:
                continue
            out.append({
                "id": (v.findtext("meldingid") or "").strip(), "date": d,
                "issuer": (v.findtext("uitgevendeinstelling") or "").strip().lower(),
                "person": ((v.findtext("meldingsplichtige") or "").strip() or (v.findtext("nauwgelieerdaan") or "").strip()),
                "role": (v.findtext("functie") or "").strip(),
            })
    except Exception as e:
        print(f"AFM elenco errore: {e}")
    _afm_entries_cache = out
    return out


def _parse_afm_detail(mid):
    """Pagina di dettaglio -> (tipo, prezzo, quantita, valuta) oppure None."""
    if mid in _afm_detail_cache:
        return _afm_detail_cache[mid]
    res = None
    try:
        r = requests.get(AFM_DETAIL_URL.format(id=mid), headers=INSIDER_HEADERS, timeout=60)
        r.raise_for_status()
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r.text))
        m = re.search(r"Instrument type (?!ISIN)(.+?) ISIN [A-Z0-9]{12} Transaction category (.+?) Transaction type (.+?) "
                      r"Stock option program (.+?) Trading place (.+?) Price ([\d.,]+) Quantity ([\d.,]+) Unit ([A-Z]{3})", text)
        if m:
            ttype = m.group(3).strip().lower()
            kind = "buy" if ttype in ("koop", "aankoop", "purchase", "buy") else (
                "sell" if ttype in ("verkoop", "sale", "sell") else None)
            price, _ = _eu_number(m.group(6))
            qty, _ = _eu_number(m.group(7))
            inst = m.group(1).strip().lower()                       # solo azioni: niente obbligazioni/derivati
            is_share = ("aandeel" in inst or "aandelen" in inst or "share" in inst) and not any(
                x in inst for x in ("obligatie", "bond", "optie", "option", "warrant", "derivat"))
            if kind and is_share and price > 0 and qty > 0:
                res = (kind, price, qty, m.group(8), m.group(4).strip().lower() in ("ja", "yes"))
    except Exception as e:
        print(f"AFM dettaglio errore ({mid[:12]}): {e}")
    _afm_detail_cache[mid] = res
    return res


def build_afm_payload(adjusted_symbol):
    names = NL_ISSUERS.get(adjusted_symbol)
    if not names:
        return None
    try:
        trades, currency = [], "EUR"
        mine = [e for e in _fetch_afm_entries() if e["issuer"] in names][:AFM_MAX_DETAILS]
        for e in mine:
            det = _parse_afm_detail(e["id"])
            if det:
                kind, price, qty, currency, oe = det
                trades.append({"date": e["date"], "name": e["person"], "role": e["role"], "type": kind,
                               "shares": qty, "price": price, "value": price * qty, "oe": oe, "cur": currency})
            time.sleep(0.25)
        return _assemble_insider_payload(trades, "EUR", "afm")
    except Exception as e:
        print(f"build_afm_payload errore: {e}")
        return None


# --- ENI: tabella ufficiale "Managers' transactions" su eni.com (prezzo dal PDF 1Info) ---
ENI_URL = "https://www.eni.com/en-IT/governance/market-abuse-regulation/transaction-manager.html"
ENI_ISIN = "IT0003132476"


def _strip_tags(s):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s)).replace("&nbsp;", " ").replace("&amp;", "&").strip()


def _eni_pdf_price(url):
    """Prezzo/volume dal PDF (formato '25,0362 EUR 15433'). Ritorna (prezzo, valuta) o (0, 'EUR')."""
    try:
        from pypdf import PdfReader
        full = url if url.startswith("http") else "https://www.eni.com" + url
        r = requests.get(full, headers=INSIDER_HEADERS, timeout=60)
        r.raise_for_status()
        text = "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(r.content)).pages)
        m = re.search(r"Volume\(s\)\s*([\d.,]+)\s*([A-Z]{3})\s+[\d.,]+", text)
        if m:
            return _parse_price(m.group(1)), m.group(2)
    except ImportError:
        pass
    except Exception as e:
        print(f"ENI PDF errore: {e}")
    return 0.0, "EUR"


def _close_on(adjusted_symbol, day):
    """Chiusura Yahoo del giorno (o del precedente): stima del prezzo quando il PDF non e' leggibile."""
    try:
        h = yf.Ticker(adjusted_symbol).history(start=(day - timedelta(days=6)).strftime("%Y-%m-%d"),
                                               end=(day + timedelta(days=2)).strftime("%Y-%m-%d"))
        h = h[h.index.tz_localize(None) <= pd.Timestamp(day)] if len(h) else h
        return float(h["Close"].iloc[-1]) if len(h) else 0.0
    except Exception:
        return 0.0


def build_eni_payload(adjusted_symbol):
    try:
        r = requests.get(ENI_URL, headers=INSIDER_HEADERS, timeout=60)
        r.raise_for_status()
        cutoff = datetime.now() - timedelta(days=FETCH_DAYS)
        trades = []
        cell = r"<td[^>]*>((?:(?!</td>).)*)</td>\s*"        # una cella: non puo' attraversare </td>
        rows = re.findall(r"<tr[^>]*>\s*" + cell * 3 + r"<td[^>]*>\s*([PS])\s*</td>\s*<td[^>]*>\s*([\d.,]+)\s*</td>\s*" + cell + r"</tr>", r.text, re.S)
        for name_h, role_h, date_h, kind, shares_s, notes_h in rows:
            d_m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", _strip_tags(date_h))
            if not d_m or _strip_tags(notes_h):
                continue                              # righe con note (assegnazioni, sell-to-cover...) non sono mercato
            d = datetime(int(d_m.group(3)), int(d_m.group(2)), int(d_m.group(1)))
            if d < cutoff:
                continue
            if len(trades) >= 30:
                break
            shares = _parse_qty(shares_s)
            pdf = re.search(r'href="([^"]+\.pdf)"', date_h)
            price, cur = _eni_pdf_price(pdf.group(1)) if pdf else (0.0, "EUR")
            if price <= 0:
                price = _close_on(adjusted_symbol, d)           # stima (prezzo non disponibile nel PDF)
            if price <= 0 or shares <= 0:
                continue
            trades.append({"date": d, "name": _strip_tags(name_h), "role": _strip_tags(role_h),
                           "type": "buy" if kind == "P" else "sell", "shares": shares, "price": price,
                           "value": price * shares, "oe": False, "cur": cur})
            time.sleep(0.2)
        return _assemble_insider_payload(trades, "EUR", "eni")
    except Exception as e:
        print(f"build_eni_payload errore: {e}")
        return None


# --- BRASILE: CVM dati aperti (VLMO = valori mobiliari negoziati dagli insider) ---
CVM_ZIP_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/VLMO/DADOS/vlmo_cia_aberta_{year}.zip"
BR_ISSUERS = {   # ticker (ADR) -> inizio ragione sociale CVM normalizzata (maiuscolo, senza accenti)
    "ITUB": "ITAU UNIBANCO HOLDING", "ABEV": "AMBEV S.A.", "VALE": "VALE S.A.", "PBR": "PETROLEO BRASILEIRO",
}
_BR_CARGO = {"DIRETOR": "Executive officers", "CONSELHO DE ADMINISTRACAO": "Board of directors",
             "CONTROLADOR": "Controlling shareholder", "CONSELHO FISCAL": "Fiscal council",
             "ORGAO ESTATUTARIO": "Statutory body"}
_br_rows_cache = {}


def _ascii_up(s):
    return unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().upper().strip()


def _fetch_cvm_rows(year):
    """Righe 'con' (consolidato) dell'anno. Un solo download per anno (~700 KB zip)."""
    if year in _br_rows_cache:
        return _br_rows_cache[year]
    rows = []
    try:
        r = requests.get(CVM_ZIP_URL.format(year=year), headers=INSIDER_HEADERS, timeout=120)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            name = next(n for n in z.namelist() if n.endswith(f"con_{year}.csv"))
            text = z.read(name).decode("latin-1")
        rows = list(csv.DictReader(io.StringIO(text), delimiter=";"))
    except Exception as e:
        print(f"CVM {year} errore: {e}")
    _br_rows_cache[year] = rows
    return rows


def build_cvm_payload(adjusted_symbol):
    key = BR_ISSUERS.get(adjusted_symbol)
    if not key:
        return None
    try:
        cutoff = datetime.now() - timedelta(days=FETCH_DAYS)
        years = sorted({datetime.now().year, (datetime.now() - timedelta(days=FETCH_DAYS)).year})
        mine = []
        for y in years:
            for r in _fetch_cvm_rows(y):
                if _ascii_up(r.get("Nome_Companhia", "")).startswith(key) and r.get("Tipo_Empresa") == "Companhia":
                    mine.append(r)
        # ogni mese puo' essere ripresentato (Versao): tiene solo l'ultima versione per Data_Referencia
        latest = {}
        for r in mine:
            ref, ver = r.get("Data_Referencia", ""), int(r.get("Versao") or 1)
            latest[ref] = max(latest.get(ref, 0), ver)
        trades = []
        for r in mine:
            if int(r.get("Versao") or 1) != latest.get(r.get("Data_Referencia", "")):
                continue
            mov = _ascii_up(r.get("Tipo_Movimentacao", ""))
            if mov not in ("COMPRA A VISTA", "VENDA A VISTA") or _ascii_up(r.get("Tipo_Ativo", "")) != "ACOES":
                continue
            try:
                d = datetime.strptime(r.get("Data_Movimentacao", ""), "%Y-%m-%d")
                qty, price, vol = float(r["Quantidade"]), float(r["Preco_Unitario"]), float(r["Volume"])
            except (ValueError, KeyError):
                continue
            if d < cutoff or qty <= 0 or vol <= 0:
                continue
            cargo = _ascii_up(r.get("Tipo_Cargo", ""))
            label = next((v for k, v in _BR_CARGO.items() if cargo.startswith(k)), "Insiders")
            trades.append({"date": d, "name": label, "role": "", "type": "buy" if mov.startswith("COMPRA") else "sell",
                           "shares": abs(qty), "price": price, "value": abs(vol), "oe": False})
        return _assemble_insider_payload(trades, "BRL", "cvm")
    except Exception as e:
        print(f"build_cvm_payload errore: {e}")
        return None

def build_europe_insider_payload(adjusted_symbol):
    """Fonte ufficiale per mercato/emittente, poi fallback Yahoo. None se nulla/errore.
    Chiamata anche per i ticker senza suffisso che OpenInsider non copre (ADR brasiliani, ecc.)."""
    s = str(adjusted_symbol)
    payload = None
    if s in BR_ISSUERS:
        payload = build_cvm_payload(s)
    elif s == "ENI.MI":
        payload = build_eni_payload(s)
    elif s in NL_ISSUERS and s.endswith(".MI"):
        payload = build_afm_payload(s)
    elif s.endswith(".DE"):
        payload = build_bafin_payload(s)
    elif s.endswith(".PA"):
        payload = build_amf_payload(s)
        if payload is None and s in NL_ISSUERS:
            payload = build_afm_payload(s)
    elif s.endswith(".MI"):
        payload = build_emarket_payload(s)
    if payload is None:
        payload = build_yahoo_insider_payload(s)
    return payload


# Paths
file_path = f"{TARGET_FOLDER}/classifica.html"
news_path = f"{TARGET_FOLDER}/news.html"
history_path = f"{TARGET_FOLDER}/history.json"
fire_path = f"{TARGET_FOLDER}/fire.html"
pro_path = f"{TARGET_FOLDER}/classificaPRO.html"
corr_pro_path = f"{TARGET_FOLDER}/correlations_pro.html"
mom_path = f"{TARGET_FOLDER}/classifica_momentum.html"
sector_path = f"{TARGET_FOLDER}/classifica_settori.html"
cache_insider_path = f"{TARGET_FOLDER}/insider_cache.json"

# ==============================================================================
# 1. MAPPE E LISTE COMPLETE
# ==============================================================================
sector_leaders = {
    "1. Big Tech, Software & Internet": "MSFT", "2. Semiconductors & AI": "NVDA", "3. Financial Services": "JPM",
    "4. Automotive & Mobility": "TSLA", "5. Healthcare & Pharma": "LLY", "6. Consumer Goods & Retail": "WMT",
    "7. Industrials & Defense": "CAT", "8. Energy (Oil & Gas)": "OIL", "9. Utilities & Green": "IBE.MC",
    "10. Precious Metals & Materials": "GOLD", "11. Media & Telecom": "NFLX", "12. Indices (Global)": "SPX500",
    "13. Forex (Currencies)": "EURUSD", "14. Crypto Assets": "BTCUSD", "15. ETFs & Funds": "SPY",                 
    "16. Agricultural Commodities": "CORN"     
}

asset_sector_map = {
    "AAPL": "1. Big Tech, Software & Internet", "MSFT": "1. Big Tech, Software & Internet", 
    "GOOGL": "1. Big Tech, Software & Internet", "AMZN": "1. Big Tech, Software & Internet",
    "META": "1. Big Tech, Software & Internet", "ADBE": "1. Big Tech, Software & Internet",
    "CRM": "1. Big Tech, Software & Internet", "ORCL": "1. Big Tech, Software & Internet",
    "IBM": "1. Big Tech, Software & Internet", "NOW": "1. Big Tech, Software & Internet",
    "INTU": "1. Big Tech, Software & Internet", "ADP": "1. Big Tech, Software & Internet",
    "BABA": "1. Big Tech, Software & Internet", "BIDU": "1. Big Tech, Software & Internet",
    "SHOP": "1. Big Tech, Software & Internet", "SNOW": "1. Big Tech, Software & Internet",
    "PLTR": "1. Big Tech, Software & Internet", "TWLO": "1. Big Tech, Software & Internet",
    "DUOL": "1. Big Tech, Software & Internet", "JD": "1. Big Tech, Software & Internet",
    "NET": "1. Big Tech, Software & Internet", "PDD": "1. Big Tech, Software & Internet",
    "BTDR": "1. Big Tech, Software & Internet", "DDOG": "1. Big Tech, Software & Internet",
    "ZM": "1. Big Tech, Software & Internet", "MELI": "1. Big Tech, Software & Internet",
    "SAP.DE": "1. Big Tech, Software & Internet",
    "NVDA": "2. Semiconductors & AI", "INTC": "2. Semiconductors & AI",
    "QCOM": "2. Semiconductors & AI", "ADI": "2. Semiconductors & AI",
    "ARM": "2. Semiconductors & AI", "CSCO": "2. Semiconductors & AI",
    "ACN": "2. Semiconductors & AI", "FIS": "2. Semiconductors & AI",
    "AVGO": "2. Semiconductors & AI", "STM.MI": "2. Semiconductors & AI",
    "ASML.AS": "2. Semiconductors & AI", "TSM": "2. Semiconductors & AI",
    "JPM": "3. Financial Services", "V": "3. Financial Services", 
    "PYPL": "3. Financial Services", "MS": "3. Financial Services",
    "GS": "3. Financial Services", "AXP": "3. Financial Services",
    "SCHW": "3. Financial Services", "C": "3. Financial Services",
    "PLD": "3. Financial Services", "PNC": "3. Financial Services",
    "ICE": "3. Financial Services", "MMC": "3. Financial Services",
    "CME": "3. Financial Services", "AON": "3. Financial Services",
    "TROW": "3. Financial Services", "USB": "3. Financial Services",
    "PSA": "3. Financial Services", "COIN": "3. Financial Services",
    "UCG.MI": "3. Financial Services", "PST.MI": "3. Financial Services",
    "ISP.MI": "3. Financial Services", "MA": "3. Financial Services",
    "BRK-B": "3. Financial Services", "RY": "3. Financial Services",
    "G.MI": "3. Financial Services", "UNI.MI": "3. Financial Services",
    "HDB": "3. Financial Services", "ALV.DE": "3. Financial Services",
    "HSBA.L": "3. Financial Services", "BNP.PA": "3. Financial Services",
    "SAN.MC": "3. Financial Services", "BBVA.MC": "3. Financial Services",
    "ITUB": "3. Financial Services", "NU": "3. Financial Services",
    "DIB.AE": "3. Financial Services", "EMIRATESNBD.AE": "3. Financial Services",
    "TSLA": "4. Automotive & Mobility", "GM": "4. Automotive & Mobility",
    "NIO": "4. Automotive & Mobility", "STLAM.MI": "4. Automotive & Mobility",
    "HTZ": "4. Automotive & Mobility", "LCID": "4. Automotive & Mobility",
    "RIVN": "4. Automotive & Mobility", "UBER": "4. Automotive & Mobility",
    "LYFT": "4. Automotive & Mobility", "NAAS": "4. Automotive & Mobility",
    "BA": "4. Automotive & Mobility", "AIR.PA": "4. Automotive & Mobility",
    "RACE.MI": "4. Automotive & Mobility", "P911.DE": "4. Automotive & Mobility",
    "TM": "4. Automotive & Mobility", "VOW3.DE": "4. Automotive & Mobility",
    "MBG.DE": "4. Automotive & Mobility",
    "LLY": "5. Healthcare & Pharma", "JNJ": "5. Healthcare & Pharma",
    "PFE": "5. Healthcare & Pharma", "MRK": "5. Healthcare & Pharma",
    "ABT": "5. Healthcare & Pharma", "BMY": "5. Healthcare & Pharma",
    "AMGN": "5. Healthcare & Pharma", "CVS": "5. Healthcare & Pharma",
    "BDX": "5. Healthcare & Pharma", "ZTS": "5. Healthcare & Pharma",
    "EW": "5. Healthcare & Pharma", "LNTH": "5. Healthcare & Pharma",
    "SYK": "5. Healthcare & Pharma", "UNH": "5. Healthcare & Pharma",
    "NVO": "5. Healthcare & Pharma", "AZN.L": "5. Healthcare & Pharma",
    "SAN.PA": "5. Healthcare & Pharma",
    "WMT": "6. Consumer Goods & Retail", "KO": "6. Consumer Goods & Retail",
    "PEP": "6. Consumer Goods & Retail", "MCD": "6. Consumer Goods & Retail",
    "NKE": "6. Consumer Goods & Retail", "HD": "6. Consumer Goods & Retail",
    "COST": "6. Consumer Goods & Retail", "SBUX": "6. Consumer Goods & Retail",
    "LOW": "6. Consumer Goods & Retail", "TGT": "6. Consumer Goods & Retail",
    "TJX": "6. Consumer Goods & Retail", "CL": "6. Consumer Goods & Retail",
    "EL": "6. Consumer Goods & Retail", "SCHL": "6. Consumer Goods & Retail",
    "MONC.MI": "6. Consumer Goods & Retail", "ULVR.L": "6. Consumer Goods & Retail",
    "MC.PA": "6. Consumer Goods & Retail", "OR.PA": "6. Consumer Goods & Retail",
    "ITX.MC": "6. Consumer Goods & Retail", "ABEV": "6. Consumer Goods & Retail",
    "CAT": "7. Industrials & Defense", "LMT": "7. Industrials & Defense",
    "ITW": "7. Industrials & Defense", "FDX": "7. Industrials & Defense",
    "NSC": "7. Industrials & Defense", "GE": "7. Industrials & Defense",
    "HON": "7. Industrials & Defense", "DE": "7. Industrials & Defense",
    "LDO.MI": "7. Industrials & Defense", "BKNG": "7. Industrials & Defense",
    "SIE.DE": "7. Industrials & Defense", "EMAAR.AE": "7. Industrials & Defense",
    "SPCX": "7. Industrials & Defense",
    "ANT": "1. Big Tech, Software & Internet",
    "OIL": "8. Energy (Oil & Gas)", "NATGAS": "8. Energy (Oil & Gas)",
    "XOM": "8. Energy (Oil & Gas)", "CVX": "8. Energy (Oil & Gas)",
    "PBR": "8. Energy (Oil & Gas)", "NRG": "8. Energy (Oil & Gas)",
    "ENI.MI": "8. Energy (Oil & Gas)", "SHEL.L": "8. Energy (Oil & Gas)",
    "BP.L": "8. Energy (Oil & Gas)", "TTE.PA": "8. Energy (Oil & Gas)",
    "SO": "9. Utilities & Green", "ENEL.MI": "9. Utilities & Green",
    "DUK": "9. Utilities & Green", "AEP": "9. Utilities & Green",
    "D": "9. Utilities & Green", "HE": "9. Utilities & Green",
    "APD": "9. Utilities & Green", "IBE.MC": "9. Utilities & Green",
    "GOLD": "10. Precious Metals & Materials", "SILVER": "10. Precious Metals & Materials",
    "COPPER": "10. Precious Metals & Materials", "VALE": "10. Precious Metals & Materials",
    "RIO.L": "10. Precious Metals & Materials",
    "NFLX": "11. Media & Telecom", "DIS": "11. Media & Telecom",
    "T": "11. Media & Telecom", "TMUS": "11. Media & Telecom",
    "AMX": "11. Media & Telecom", "ROKU": "11. Media & Telecom",
    "SONY": "11. Media & Telecom", "DTE.DE": "11. Media & Telecom",
    "TEF.MC": "11. Media & Telecom",
    "EURUSD": "13. Forex (Currencies)", "USDJPY": "13. Forex (Currencies)",
    "GBPUSD": "13. Forex (Currencies)", "AUDUSD": "13. Forex (Currencies)",
    "USDCAD": "13. Forex (Currencies)", "USDCHF": "13. Forex (Currencies)",
    "NZDUSD": "13. Forex (Currencies)", "EURGBP": "13. Forex (Currencies)",
    "EURJPY": "13. Forex (Currencies)", "GBPJPY": "13. Forex (Currencies)",
    "AUDJPY": "13. Forex (Currencies)", "CADJPY": "13. Forex (Currencies)",
    "CHFJPY": "13. Forex (Currencies)", "EURAUD": "13. Forex (Currencies)",
    "EURNZD": "13. Forex (Currencies)", "EURCAD": "13. Forex (Currencies)",
    "EURCHF": "13. Forex (Currencies)", "GBPCHF": "13. Forex (Currencies)",
    "AUDCAD": "13. Forex (Currencies)",
    "USDSEK": "13. Forex (Currencies)", "USDNOK": "13. Forex (Currencies)",
    "USDDKK": "13. Forex (Currencies)", "USDPLN": "13. Forex (Currencies)",
    "USDCZK": "13. Forex (Currencies)", "USDHUF": "13. Forex (Currencies)",
    "USDCNY": "13. Forex (Currencies)", "USDHKD": "13. Forex (Currencies)",
    "USDSGD": "13. Forex (Currencies)", "USDINR": "13. Forex (Currencies)",
    "USDKRW": "13. Forex (Currencies)", "USDBRL": "13. Forex (Currencies)",
    "USDMXN": "13. Forex (Currencies)", "USDZAR": "13. Forex (Currencies)",
    "BTCUSD": "14. Crypto Assets", "ETHUSD": "14. Crypto Assets",
    "LTCUSD": "14. Crypto Assets", "XRPUSD": "14. Crypto Assets",
    "BCHUSD": "14. Crypto Assets", "EOSUSD": "14. Crypto Assets",
    "XLMUSD": "14. Crypto Assets", "ADAUSD": "14. Crypto Assets",
    "TRXUSD": "14. Crypto Assets", "NEOUSD": "14. Crypto Assets",
    "DASHUSD": "14. Crypto Assets", "XMRUSD": "14. Crypto Assets",
    "ETCUSD": "14. Crypto Assets", "ZECUSD": "14. Crypto Assets",
    "BNBUSD": "14. Crypto Assets", "DOGEUSD": "14. Crypto Assets",
    "USDTUSD": "14. Crypto Assets", "LINKUSD": "14. Crypto Assets",
    "ATOMUSD": "14. Crypto Assets", "XTZUSD": "14. Crypto Assets",
    "USDCUSD": "14. Crypto Assets", "SOLUSD": "14. Crypto Assets",
    "TONUSD": "14. Crypto Assets", "AVAXUSD": "14. Crypto Assets",
    "DOTUSD": "14. Crypto Assets", "NEARUSD": "14. Crypto Assets",
    "APTUSD": "14. Crypto Assets", "SUIUSD": "14. Crypto Assets",
    "ICPUSD": "14. Crypto Assets", "KASUSD": "14. Crypto Assets",
    "STXUSD": "14. Crypto Assets", "SEIUSD": "14. Crypto Assets",
    "HYPEUSD": "14. Crypto Assets", "POLUSD": "14. Crypto Assets",
    "OPUSD": "14. Crypto Assets", "ARBUSD": "14. Crypto Assets",
    "RENDERUSD": "14. Crypto Assets", "IMXUSD": "14. Crypto Assets",
    "SKYUSD": "14. Crypto Assets", "UNIUSD": "14. Crypto Assets",
    "AAVEUSD": "14. Crypto Assets", "ORCAUSD": "14. Crypto Assets",
    "DAIUSD": "14. Crypto Assets", "SHIBUSD": "14. Crypto Assets",
    "SPY": "15. ETFs & Funds", "VOO": "15. ETFs & Funds", "QQQ": "15. ETFs & Funds",
    "DIA": "15. ETFs & Funds", "ONEQ": "15. ETFs & Funds", "VT": "15. ETFs & Funds",
    "BNDW": "15. ETFs & Funds", "IWM": "15. ETFs & Funds", "VIXY": "15. ETFs & Funds", 
    "EFA": "15. ETFs & Funds", "EEM": "15. ETFs & Funds", "FEZ": "15. ETFs & Funds",
    "EWI": "15. ETFs & Funds", "EWG": "15. ETFs & Funds", "EWU": "15. ETFs & Funds",
    "EWQ": "15. ETFs & Funds", "EWL": "15. ETFs & Funds", "EWP": "15. ETFs & Funds",
    "EWN": "15. ETFs & Funds", "EWJ": "15. ETFs & Funds", "DXJ": "15. ETFs & Funds",
    "EWH": "15. ETFs & Funds", "ASHR": "15. ETFs & Funds", "INDY": "15. ETFs & Funds",
    "EPI": "15. ETFs & Funds", "EWY": "15. ETFs & Funds", "TLT": "15. ETFs & Funds",
    "GLD": "15. ETFs & Funds", "XLF": "15. ETFs & Funds", "XLK": "15. ETFs & Funds", 
    "XLE": "15. ETFs & Funds",
    "XEON.MI": "15. ETFs & Funds",
    "COCOA": "16. Agricultural Commodities", "WHEAT": "16. Agricultural Commodities",
    "CORN": "16. Agricultural Commodities", "SOYBEANS": "16. Agricultural Commodities",
    "SUGAR": "16. Agricultural Commodities"
}

# --- TICKER ESCLUSI DALLE LISTE COMUNI ---
# I simboli qui elencati vengono analizzati normalmente (pagina {SYM}_RESULT.html, archivio news,
# insider, storico, backtest), ma NON compaiono nelle liste condivise: classifica / classificaPRO,
# momentum, settori (non pesano sul punteggio del settore), fire, feed news.html, daily brief,
# pro tips e come "partner" nelle correlazioni degli altri asset.
# Utile per nuove quotazioni (storico corto) o simboli non ancora registrati nelle vecchie app.
# Lista vuota = nessuna esclusione.
EXCLUDED_FROM_RANKINGS = [
    "ANT",
    # Dati di servizio per l'app: cambi USD/xxx (valute del portafoglio) e ETF monetario XEON.
    # Hanno pagina _RESULT.html e prezzi live, ma non devono comparire in classifiche, settori, news, ecc.
    "XEON.MI",
    "USDSEK",
    "USDNOK",
    "USDDKK",
    "USDPLN",
    "USDCZK",
    "USDHUF",
    "USDCNY",
    "USDHKD",
    "USDSGD",
    "USDINR",
    "USDKRW",
    "USDBRL",
    "USDMXN",
    "USDZAR",
]

_EXCLUDED_FROM_RANKINGS_SET = {str(s).strip().upper() for s in EXCLUDED_FROM_RANKINGS if str(s).strip()}

def is_excluded_from_rankings(symbol):
    return bool(_EXCLUDED_FROM_RANKINGS_SET) and str(symbol).strip().upper() in _EXCLUDED_FROM_RANKINGS_SET

TICKER_MAP = {
    "AAPL": "AAPL", "MSFT": "MSFT", "GOOGL": "GOOGL", "AMZN": "AMZN", "META": "META",
    "TSLA": "TSLA", "V": "V", "JPM": "JPM", "JNJ": "JNJ", "WMT": "WMT",
    "NVDA": "NVDA", "PYPL": "PYPL", "SPCX": "SPCX", "ANT": "ANT", "DIS": "DIS", "NFLX": "NFLX", "NIO": "NIO",
    "NRG": "NRG", "ADBE": "ADBE", "INTC": "INTC", "CSCO": "CSCO", "PFE": "PFE",
    "KO": "KO", "PEP": "PEP", "MRK": "MRK", "ABT": "ABT", "XOM": "XOM",
    "MA": "MA", "BRK-B": "BRK-B", "AVGO": "AVGO", "UNH": "UNH",
    "CVX": "CVX", "T": "T", "MCD": "MCD", "NKE": "NKE", "HD": "HD",
    "IBM": "IBM", "CRM": "CRM", "BMY": "BMY", "ORCL": "ORCL", "ACN": "ACN",
    "LLY": "LLY", "QCOM": "QCOM", "HON": "HON", "COST": "COST", "SBUX": "SBUX",
    "CAT": "CAT", "LOW": "LOW", "MS": "MS", "GS": "GS", "AXP": "AXP",
    "INTU": "INTU", "AMGN": "AMGN", "GE": "GE", "FIS": "FIS", "CVS": "CVS",
    "DE": "DE", "BDX": "BDX", "NOW": "NOW", "SCHW": "SCHW", "LMT": "LMT",
    "ADP": "ADP", "C": "C", "PLD": "PLD", "NSC": "NSC", "TMUS": "TMUS",
    "ITW": "ITW", "FDX": "FDX", "PNC": "PNC", "SO": "SO", "APD": "APD",
    "ADI": "ADI", "ICE": "ICE", "ZTS": "ZTS", "TJX": "TJX", "CL": "CL",
    "MMC": "MRSH", "EL": "EL", "GM": "GM", "CME": "CME", "EW": "EW",
    "AON": "AON", "D": "D", "PSA": "PSA", "AEP": "AEP", "TROW": "TROW",
    "LNTH": "LNTH", "HE": "HE", "BTDR": "BTDR", "NAAS": "NAAS", "SCHL": "SCHL",
    "TGT": "TGT", "SYK": "SYK", "BKNG": "BKNG", "DUK": "DUK", "USB": "USB",
    "ARM": "ARM", "BABA": "BABA", "BIDU": "BIDU", "COIN": "COIN",
    "DDOG": "DDOG", "HTZ": "HTZ", "JD": "JD", "LCID": "LCID", "LYFT": "LYFT", "NET": "NET",
    "PDD": "PDD", "PLTR": "PLTR", "RIVN": "RIVN", "ROKU": "ROKU", "SHOP": "SHOP",
    "SNOW": "SNOW", "TWLO": "TWLO", "UBER": "UBER",
    "ZM": "ZM", "DUOL": "DUOL", "PBR": "PBR", "VALE": "VALE", "AMX": "AMX", "MELI": "MELI", "RY": "RY",
    "ISP.MI": "ISP.MI", "ENEL.MI": "ENEL.MI", "STLAM.MI": "STLAM.MI",
    "LDO.MI": "LDO.MI", "PST.MI": "PST.MI", "UCG.MI": "UCG.MI",
    "ENI.MI": "ENI.MI", "G.MI": "G.MI", "UNI.MI": "UNI.MI", 
    "MONC.MI": "MONC.MI", "STM.MI": "STMMI.MI", "RACE.MI": "RACE.MI",
    "BA": "BA", "AIR.PA": "AIR.PA", "SAP.DE": "SAP.DE", "SIE.DE": "SIE.DE", "P911.DE": "P911.DE",
    "ALV.DE": "ALV.DE", "VOW3.DE": "VOW3.DE", "MBG.DE": "MBG.DE", "DTE.DE": "DTE.DE",
    "ASML.AS": "ASML.AS", "NVO": "NVO",
    "SHEL.L": "SHEL.L", "BP.L": "BP.L", "HSBA.L": "HSBA.L", "AZN.L": "AZN.L",
    "ULVR.L": "ULVR.L", "RIO.L": "RIO.L", "MC.PA": "MC.PA", "TTE.PA": "TTE.PA",
    "OR.PA": "OR.PA", "SAN.PA": "SAN.PA", "BNP.PA": "BNP.PA", "SAN.MC": "SAN.MC",
    "IBE.MC": "IBE.MC", "ITX.MC": "ITX.MC", "BBVA.MC": "BBVA.MC", "TEF.MC": "TEF.MC",
    "ITUB": "ITUB", "NU": "NU", "ABEV": "ABEV", "EMAAR.AE": "EMAAR.AE", "DIB.AE": "DIB.AE", "EMIRATESNBD.AE": "EMIRATESNBD.AE",
    "TSM": "TSM", "TM": "TM", "SONY": "SONY", "HDB": "HDB",
    "SPY": "SPY", "VOO": "VOO", "QQQ": "QQQ", "DIA": "DIA", "ONEQ": "ONEQ", "VT": "VT", "BNDW": "BNDW",
    "IWM": "IWM", "VIXY": "VIXY", "EFA": "EFA", "EEM": "EEM", "FEZ": "FEZ", 
    "EWI": "EWI", "EWG": "EWG", "EWU": "EWU", "EWQ": "EWQ", "EWL": "EWL", 
    "EWP": "EWP", "EWN": "EWN", "EWJ": "EWJ", "DXJ": "DXJ", "EWH": "EWH", 
    "ASHR": "ASHR", "INDY": "INDY", "EPI": "EPI", "EWY": "EWY", "TLT": "TLT", 
    "GLD": "GLD", "XLF": "XLF", "XLK": "XLK", "XLE": "XLE",
    "URTH": "URTH", "BND": "BND", "IEMG": "IEMG",
    "XEON.MI": "XEON.MI",
    "EURUSD": "EURUSD=X", "USDJPY": "USDJPY=X", "GBPUSD": "GBPUSD=X",
    "AUDUSD": "AUDUSD=X", "USDCAD": "USDCAD=X", "USDCHF": "USDCHF=X",
    "NZDUSD": "NZDUSD=X", "EURGBP": "EURGBP=X", "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X", "AUDJPY": "AUDJPY=X", "CADJPY": "CADJPY=X",
    "CHFJPY": "CHFJPY=X", "EURAUD": "EURAUD=X", "EURNZD": "EURNZD=X",
    "EURCAD": "EURCAD=X", "EURCHF": "EURCHF=X", "GBPCHF": "GBPCHF=X",
    "AUDCAD": "AUDCAD=X",
    "USDSEK": "USDSEK=X", "USDNOK": "USDNOK=X",
    "USDDKK": "USDDKK=X", "USDPLN": "USDPLN=X",
    "USDCZK": "USDCZK=X", "USDHUF": "USDHUF=X",
    "USDCNY": "USDCNY=X", "USDHKD": "USDHKD=X",
    "USDSGD": "USDSGD=X", "USDINR": "USDINR=X",
    "USDKRW": "USDKRW=X", "USDBRL": "USDBRL=X",
    "USDMXN": "USDMXN=X", "USDZAR": "USDZAR=X",
    "BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD", "LTCUSD": "LTC-USD",
    "XRPUSD": "XRP-USD", "BCHUSD": "BCH-USD", "EOSUSD": "EOS-USD",
    "XLMUSD": "XLM-USD", "ADAUSD": "ADA-USD", "TRXUSD": "TRX-USD",
    "NEOUSD": "NEO-USD", "DASHUSD": "DASH-USD", "XMRUSD": "XMR-USD",
    "ETCUSD": "ETC-USD", "ZECUSD": "ZEC-USD", "BNBUSD": "BNB-USD",
    "DOGEUSD": "DOGE-USD", "USDTUSD": "USDT-USD", "LINKUSD": "LINK-USD",
    "ATOMUSD": "ATOM-USD", "XTZUSD": "XTZ-USD",
    "USDCUSD": "USDC-USD", "SOLUSD": "SOL-USD", "TONUSD": "GRAM-USD",
    "AVAXUSD": "AVAX-USD", "DOTUSD": "DOT-USD", "NEARUSD": "NEAR-USD",
    "APTUSD": "APT21794-USD", "SUIUSD": "SUI20947-USD", "ICPUSD": "ICP-USD",
    "KASUSD": "KAS-USD", "STXUSD": "STX4847-USD", "SEIUSD": "SEI-USD",
    "HYPEUSD": "HYPE32196-USD", "POLUSD": "POL28321-USD", "OPUSD": "OP-USD",
    "ARBUSD": "ARB11841-USD", "RENDERUSD": "RENDER-USD", "IMXUSD": "IMX10603-USD",
    "SKYUSD": "SKY33038-USD", "UNIUSD": "UNI7083-USD", "AAVEUSD": "AAVE-USD",
    "ORCAUSD": "ORCA-USD", "DAIUSD": "DAI-USD", "SHIBUSD": "SHIB-USD",
    "COCOA": "CC=F", "GOLD": "GC=F", "SILVER": "SI=F", "OIL": "CL=F", "NATGAS": "NG=F",
    "COPPER": "HG=F", "WHEAT": "ZW=F", "CORN": "ZC=F", "SOYBEANS": "ZS=F", "SUGAR": "SB=F"
}

symbol_list = list(asset_sector_map.keys())
symbol_list_for_yfinance = [TICKER_MAP.get(s, s) for s in symbol_list]

symbol_name_map = {
    "AAPL": ["Apple", "Apple Inc."], "MSFT": ["Microsoft", "Microsoft Corporation"],
    "GOOGL": ["Google", "Alphabet", "Alphabet Inc."], "AMZN": ["Amazon", "Amazon.com"],
    "META": ["Meta", "Facebook", "Meta Platforms"], "TSLA": ["Tesla", "Tesla Inc."],
    "V": ["Visa", "Visa Inc."], "JPM": ["JPMorgan", "JPMorgan Chase"],
    "JNJ": ["Johnson & Johnson", "JNJ"], "WMT": ["Walmart"], "NVDA": ["NVIDIA", "Nvidia Corp."],
    "PYPL": ["PayPal"],
    "SPCX": ["SpaceX", "Space X", "Space Exploration Technologies", "Space Exploration Technologies Corp.", "Space Exploration Technologies Corporation", "Crew Dragon", "Starlink"],
    "ANT": ["Anthropic", "Anthropic, PBC", "Anthropic PBC"],
    "DIS": ["Disney", "The Walt Disney Company"], "NFLX": ["Netflix"],
    "NIO": ["NIO Inc."], "NRG": ["NRG Energy"], "ADBE": ["Adobe", "Adobe Inc."],
    "INTC": ["Intel", "Intel Corporation"], "CSCO": ["Cisco", "Cisco Systems"], "PFE": ["Pfizer"],
    "KO": ["Coca-Cola", "The Coca-Cola Company"], "PEP": ["Pepsi", "PepsiCo"],
    "MA": ["Mastercard", "Mastercard Inc.", "Mastercard Incorporated", "Master Card"],
    "BRK-B": ["Berkshire Hathaway", "Berkshire", "Warren Buffett company", "BRK.B", "BRK-B", "Berkshire Hathaway Inc."],
    "AVGO": ["Broadcom", "Broadcom Inc.", "Broadcom Corporation"],
    "UNH": ["UnitedHealth", "UnitedHealth Group", "United Health", "UNH"],
    "MRK": ["Merck"], "ABT": ["Abbott", "Abbott Laboratories"], "XOM": ["ExxonMobil", "Exxon"],
    "CVX": ["Chevron"], "T": ["AT&T"], "MCD": ["McDonald's"], "NKE": ["Nike"], "HD": ["Home Depot"],
    "IBM": ["IBM", "International Business Machines"], "CRM": ["Salesforce"],
    "BMY": ["Bristol-Myers", "Bristol-Myers Squibb"], "ORCL": ["Oracle"], "ACN": ["Accenture"],
    "LLY": ["Eli Lilly"], "QCOM": ["Qualcomm"], "HON": ["Honeywell"], "COST": ["Costco"],
    "SBUX": ["Starbucks"], "CAT": ["Caterpillar"], "LOW": ["Lowe's"],
    "MS": ["Morgan Stanley", "Morgan Stanley Bank", "MS bank", "MS financial"],
    "GS": ["Goldman Sachs"], "AXP": ["American Express"], "INTU": ["Intuit"], "AMGN": ["Amgen"],
    "GE": ["General Electric"], "FIS": ["Fidelity National Information Services"],
    "CVS": ["CVS Health"], "DE": ["Deere", "John Deere"], "BDX": ["Becton Dickinson"],
    "NOW": ["ServiceNow"], "SCHW": ["Charles Schwab"], "LMT": ["Lockheed Martin"],
    "ADP": ["ADP", "Automatic Data Processing"], "C": ["Citigroup"], "PLD": ["Prologis"],
    "NSC": ["Norfolk Southern"], "TMUS": ["T-Mobile"], "ITW": ["Illinois Tool Works"],
    "FDX": ["FedEx"], "PNC": ["PNC Financial"], "SO": ["Southern Company"],
    "APD": ["Air Products & Chemicals"], "ADI": ["Analog Devices"],
    "ICE": ["Intercontinental Exchange"], "ZTS": ["Zoetis"], "TJX": ["TJX Companies"],
    "CL": ["Colgate-Palmolive"], "MMC": ["Marsh & McLennan"], "EL": ["Estée Lauder"],
    "GM": ["General Motors"], "CME": ["CME Group"], "EW": ["Edwards Lifesciences"],
    "AON": ["Aon plc"], "D": ["Dominion Energy"], "PSA": ["Public Storage"],
    "AEP": ["American Electric Power"], "TROW": ["T. Rowe Price"], "LNTH": ["Lantheus"],
    "HE": ["Hawaiian Electric"], "BTDR": ["Bitdeer"], "NAAS": ["NaaS Technology"],
    "SCHL": ["Scholastic"], "TGT": ["Target"], "SYK": ["Stryker"],
    "BKNG": ["Booking Holdings", "Booking.com"], "DUK": ["Duke Energy"],
    "USB": ["U.S. Bancorp"], "BABA": ["Alibaba", "Alibaba Group", "阿里巴巴"],
    "HTZ": ["Hertz", "Hertz Global", "Hertz Global Holdings"],
    "UBER": ["Uber", "Uber Technologies", "Uber Technologies Inc."],
    "LYFT": ["Lyft", "Lyft Inc."], "PLTR": ["Palantir", "Palantir Technologies", "Palantir Technologies Inc."],
    "SNOW": ["Snowflake", "Snowflake Inc."], "ROKU": ["Roku", "Roku Inc."],
    "TWLO": ["Twilio", "Twilio Inc."], "COIN": ["Coinbase", "Coinbase Global", "Coinbase Global Inc."],
    "PST.MI": ["Poste Italiane", "Poste Italiane S.p.A."],
    "UCG.MI": ["Unicredit", "UniCredit", "Unicredit S.p.A.", "UniCredit Bank"],
    "ISP.MI": ["Intesa Sanpaolo", "Intesa Sanpaolo S.p.A.", "Gruppo Intesa Sanpaolo", "Intesa Sanpaolo Bank", "Banca Intesa", "Banca Sanpaolo"],
    "ENEL.MI": ["Enel", "Enel S.p.A.", "Gruppo Enel"],
    "STLAM.MI": ["Stellantis", "Stellantis N.V.", "Gruppo Stellantis", "Fiat Chrysler", "FCA", "PSA Group"],
    "LDO.MI": ["Leonardo", "Leonardo S.p.A.", "Leonardo Finmeccanica", "Gruppo Leonardo"],
    "RACE.MI": ["Ferrari", "Ferrari N.V.", "Cavallino Rampante", "Maranello"],
    "ENI.MI": ["Eni", "Eni S.p.A.", "Cane a sei zampe", "Eni gas e luce", "Plenitude"],
    "G.MI": ["Generali", "Assicurazioni Generali", "Generali Group", "Leone di Trieste"],
    "UNI.MI": ["Unipol", "Gruppo Unipol", "Unipol Gruppo", "UnipolSai"],
    "MONC.MI": ["Moncler", "Moncler S.p.A.", "Piumini Moncler"],
    "STM.MI": ["STM", "STMicroelectronics", "STMicro", "ST Microelectronics"],
    "BA": ["Boeing", "The Boeing Company"], "AIR.PA": ["Airbus", "Airbus SE"],
    "SAP.DE": ["SAP", "SAP SE"], "SIE.DE": ["Siemens", "Siemens AG"],
    "ALV.DE": ["Allianz", "Allianz SE"],
    "P911.DE": ["Porsche", "Porsche AG", "Dr. Ing. h.c. F. Porsche AG", "Porsche Automobile"],
    "VOW3.DE": ["Volkswagen", "Volkswagen AG"], "MBG.DE": ["Mercedes-Benz", "Mercedes-Benz Group"],
    "DTE.DE": ["Deutsche Telekom", "Deutsche Telekom AG"],
    "NVO": ["Novo Nordisk", "Novo", "Novo Nordisk A/S"],
    "ASML.AS": ["ASML", "ASML Holding", "ASML NV", "ASML Holding N.V."],
    "SHEL.L": ["Shell", "Shell plc"], "BP.L": ["BP", "BP p.l.c."],
    "HSBA.L": ["HSBC", "HSBC Holdings"], "AZN.L": ["AstraZeneca", "AstraZeneca PLC"],
    "ULVR.L": ["Unilever", "Unilever PLC"], "RIO.L": ["Rio Tinto", "Rio Tinto Group"],
    "MC.PA": ["LVMH", "Moët Hennessy Louis Vuitton"], "TTE.PA": ["TotalEnergies", "TotalEnergies SE"],
    "OR.PA": ["L'Oréal", "L'Oreal"], "SAN.PA": ["Sanofi", "Sanofi S.A."],
    "BNP.PA": ["BNP Paribas", "BNP Paribas S.A."], "SAN.MC": ["Santander", "Banco Santander"],
    "IBE.MC": ["Iberdrola", "Iberdrola S.A."], "ITX.MC": ["Inditex", "Zara"],
    "BBVA.MC": ["BBVA", "Banco Bilbao Vizcaya Argentaria"], "TEF.MC": ["Telefónica", "Telefonica"],
    "ITUB": ["Itaú", "Itaú Unibanco"], "NU": ["Nubank", "Nu Holdings"],
    "ABEV": ["Ambev", "Ambev S.A."], "EMAAR.AE": ["Emaar", "Emaar Properties"],
    "DIB.AE": ["DIB", "Dubai Islamic Bank P.J.S.C.", "Dubai Bank", "Dubai Islamic Bank"],
    "EMIRATESNBD.AE": ["Emirates NBD", "Emirates NBD Bank"],
    "RIVN": ["Rivian", "Rivian Automotive", "Rivian Automotive Inc."],
    "LCID": ["Lucid", "Lucid Motors", "Lucid Group", "Lucid Group Inc."],
    "DDOG": ["Datadog", "Datadog Inc."], "NET": ["Cloudflare", "Cloudflare Inc."],
    "SHOP": ["Shopify", "Shopify Inc."],
    "ZM": ["Zoom", "Zoom Video", "Zoom Video Communications", "Zoom Video Communications Inc."],
    "BIDU": ["Baidu", "百度"], "PDD": ["Pinduoduo", "PDD Holdings", "Pinduoduo Inc.", "拼多多"],
    "JD": ["JD.com", "京东"], "ARM": ["Arm", "Arm Holdings", "Arm Holdings plc"],
    "DUOL": ["Duolingo", "Duolingo Inc.", "DUOL"],
    "PBR": ["Petrobras", "Petróleo Brasileiro S.A.", "Petrobras S.A."],
    "VALE": ["Vale", "Vale S.A.", "Vale SA"],
    "AMX": ["America Movil", "América Móvil", "América Móvil S.A.B. de C.V."],
    "MELI": ["MercadoLibre", "Mercado Libre", "MercadoPago", "MercadoLibre Inc."],
    "RY": ["Royal Bank of Canada", "RBC", "RBC Bank", "RY stock"],
    "TSM": ["TSMC", "Taiwan Semiconductor", "Taiwan Semiconductor Manufacturing", "Taiwan Semi"],
    "TM": ["Toyota", "Toyota Motor", "Toyota Motor Corporation", "Toyota Motors"],
    "SONY": ["Sony", "Sony Group", "Sony Corporation"],
    "HDB": ["HDFC", "HDFC Bank", "Housing Development Finance Corporation"],
    "SPY": ["SPY", "SPDR S&P 500", "SPY ETF", "SPDR S&P 500 ETF Trust", "S&P 500 ETF", "Standard & Poor's 500 ETF", "US 500 ETF"],
    "VOO": ["VOO", "Vanguard S&P 500", "VOO ETF", "Vanguard S&P 500 ETF", "S&P 500 Vanguard"],
    "QQQ": ["QQQ", "Invesco QQQ", "QQQ ETF", "Triple Q", "Nasdaq 100 ETF", "Invesco QQQ Trust", "Nasdaq 100 Invesco"],
    "DIA": ["DIA", "SPDR Dow Jones Industrial Average", "Dow Jones ETF", "DIA ETF", "Dow 30 ETF", "SPDR DIA"],
    "ONEQ": ["ONEQ", "Fidelity Nasdaq Composite", "Nasdaq Composite ETF", "Fidelity ONEQ", "ONEQ ETF"],
    "VT": ["VT", "Vanguard Total World Stock", "VT ETF", "Total World Stock ETF", "Global Stock ETF Vanguard", "All-World ETF Vanguard", "Vanguard VT"],
    "BNDW": ["BNDW", "Vanguard Total World Bond", "BNDW ETF", "Global Bond ETF Vanguard", "Total World Bond ETF", "World Aggregate Bond ETF", "Vanguard BNDW"],
    "XEON.MI": ["XEON", "Xtrackers EUR Overnight Rate Swap", "Xtrackers II EUR Overnight Rate Swap UCITS ETF", "XEON ETF", "Xtrackers XEON", "Euro overnight ETF", "Euro money market ETF", "ETF monetario Euro", "EONIA ETF", "ESTR ETF"],
    "IWM": ["IWM", "iShares Russell 2000", "Russell 2000 ETF", "IWM ETF", "US Small Cap ETF", "iShares IWM"],
    "VIXY": ["VIXY", "ProShares VIX Short-Term Futures", "VIXY ETF", "VIX Short-Term ETF", "VIX Futures ETF", "ProShares VIXY"],
    "EFA": ["EFA", "MSCI EAFE", "MSCI EAFE ETF", "iShares MSCI EAFE", "Developed Markets ex-US ETF", "EAFE Index ETF", "iShares EFA"],
    "EEM": ["EEM", "MSCI Emerging Markets ETF", "iShares EEM", "iShares MSCI Emerging Markets", "Emerging Markets ETF iShares"],
    "FEZ": ["FEZ", "SPDR EURO STOXX 50", "Euro Stoxx 50 ETF", "FEZ ETF", "Eurozone Large Cap ETF", "Euro Stoxx SPDR"],
    "EWI": ["EWI", "iShares MSCI Italy", "Italy ETF", "EWI ETF", "MSCI Italy Index ETF", "Italian Equities ETF", "iShares EWI"],
    "EWG": ["EWG", "iShares MSCI Germany", "Germany ETF", "EWG ETF", "MSCI Germany Index ETF", "German Equities ETF", "iShares EWG"],
    "EWU": ["EWU", "iShares MSCI United Kingdom", "UK ETF", "EWU ETF", "MSCI UK Index ETF", "British Equities ETF", "United Kingdom ETF"],
    "EWQ": ["EWQ", "iShares MSCI France", "France ETF", "EWQ ETF", "MSCI France Index ETF", "French Equities ETF", "iShares EWQ"],
    "EWL": ["EWL", "iShares MSCI Switzerland", "Switzerland ETF", "EWL ETF", "MSCI Switzerland Index ETF", "Swiss Equities ETF"],
    "EWP": ["EWP", "iShares MSCI Spain", "Spain ETF", "EWP ETF", "MSCI Spain Index ETF", "Spanish Equities ETF"],
    "EWN": ["EWN", "iShares MSCI Netherlands", "Netherlands ETF", "EWN ETF", "MSCI Netherlands Index ETF", "Dutch Equities ETF"],
    "EWJ": ["EWJ", "iShares MSCI Japan", "Japan ETF", "EWJ ETF", "MSCI Japan Index ETF", "Japanese Equities ETF", "iShares EWJ"],
    "DXJ": ["DXJ", "WisdomTree Japan Hedged Equity", "Japan Hedged ETF", "DXJ ETF", "Currency Hedged Japan ETF", "WisdomTree DXJ"],
    "EWH": ["EWH", "iShares MSCI Hong Kong", "Hong Kong ETF", "EWH ETF", "MSCI Hong Kong Index ETF", "Hong Kong Equities ETF"],
    "ASHR": ["ASHR", "Xtrackers Harvest CSI 300", "China A-Shares ETF", "ASHR ETF", "Chinese Mainland ETF", "CSI 300 ETF", "Xtrackers ASHR"],
    "INDY": ["INDY", "iShares India 50", "India 50 ETF", "INDY ETF", "Nifty 50 ETF", "iShares INDY"],
    "EPI": ["EPI", "WisdomTree India Earnings", "India Earnings ETF", "EPI ETF", "WisdomTree EPI", "Broad India ETF"],
    "EWY": ["EWY", "iShares MSCI South Korea", "South Korea ETF", "EWY ETF", "MSCI South Korea Index ETF", "Korean Equities ETF"],
    "TLT": ["TLT", "20+ Year Treasury Bond ETF", "iShares 20+ Year Treasury Bond", "Long-term US Treasury ETF", "iShares TLT", "TLT ETF", "US Long Bond ETF"],
    "GLD": ["GLD", "SPDR Gold Shares", "GLD ETF", "SPDR Gold Trust", "Physical Gold ETF", "Gold Bullion ETF SPDR"],
    "XLF": ["XLF", "Financial Select Sector SPDR", "XLF ETF", "US Financials Sector ETF", "SPDR Financial ETF", "S&P 500 Financials ETF"],
    "XLK": ["XLK", "Technology Select Sector SPDR", "XLK ETF", "US Tech Sector ETF", "SPDR Technology ETF", "S&P 500 Tech ETF"],
    "XLE": ["XLE", "Energy Select Sector SPDR", "XLE ETF", "US Energy Sector ETF", "SPDR Energy ETF", "S&P 500 Energy ETF"],
    "EURUSD": ["EUR/USD", "Euro Dollar", "Euro vs USD"],
    "USDJPY": ["USD/JPY", "Dollar Yen", "USD vs JPY"],
    "GBPUSD": ["GBP/USD", "British Pound", "Sterling", "GBP vs USD"],
    "AUDUSD": ["AUD/USD", "Australian Dollar", "Aussie Dollar"],
    "USDCAD": ["USD/CAD", "US Dollar vs Canadian Dollar", "Loonie"],
    "USDCHF": ["USD/CHF", "US Dollar vs Swiss Franc"],
    "NZDUSD": ["NZD/USD", "New Zealand Dollar"],
    "EURGBP": ["EUR/GBP", "Euro vs Pound"],
    "EURJPY": ["EUR/JPY", "Euro vs Yen"],
    "GBPJPY": ["GBP/JPY", "Pound vs Yen"],
    "AUDJPY": ["AUD/JPY", "Aussie vs Yen"],
    "CADJPY": ["CAD/JPY", "Canadian Dollar vs Yen"],
    "CHFJPY": ["CHF/JPY", "Swiss Franc vs Yen"],
    "EURAUD": ["EUR/AUD", "Euro vs Aussie"],
    "EURNZD": ["EUR/NZD", "Euro vs Kiwi"],
    "EURCAD": ["EUR/CAD", "Euro vs Canadian Dollar"],
    "EURCHF": ["EUR/CHF", "Euro vs Swiss Franc"],
    "GBPCHF": ["GBP/CHF", "Pound vs Swiss Franc"],
    "AUDCAD": ["AUD/CAD", "Aussie vs Canadian Dollar"],
    "USDSEK": ["USD/SEK", "Dollar vs Swedish Krona"],
    "USDNOK": ["USD/NOK", "Dollar vs Norwegian Krone"],
    "USDDKK": ["USD/DKK", "Dollar vs Danish Krone"],
    "USDPLN": ["USD/PLN", "Dollar vs Polish Zloty"],
    "USDCZK": ["USD/CZK", "Dollar vs Czech Koruna"],
    "USDHUF": ["USD/HUF", "Dollar vs Hungarian Forint"],
    "USDCNY": ["USD/CNY", "Dollar vs Chinese Yuan"],
    "USDHKD": ["USD/HKD", "Dollar vs Hong Kong Dollar"],
    "USDSGD": ["USD/SGD", "Dollar vs Singapore Dollar"],
    "USDINR": ["USD/INR", "Dollar vs Indian Rupee"],
    "USDKRW": ["USD/KRW", "Dollar vs South Korean Won"],
    "USDBRL": ["USD/BRL", "Dollar vs Brazilian Real"],
    "USDMXN": ["USD/MXN", "Dollar vs Mexican Peso"],
    "USDZAR": ["USD/ZAR", "Dollar vs South African Rand"],
    "BTCUSD": ["Bitcoin", "BTC"], "ETHUSD": ["Ethereum", "ETH"],
    "LTCUSD": ["Litecoin", "LTC"], "XRPUSD": ["Ripple", "XRP"],
    "BCHUSD": ["Bitcoin Cash", "BCH"], "EOSUSD": ["EOS"],
    "XLMUSD": ["Stellar", "XLM"], "ADAUSD": ["Cardano", "ADA"],
    "TRXUSD": ["Tron", "TRX"], "NEOUSD": ["NEO"],
    "DASHUSD": ["Dash crypto", "Dash cryptocurrency", "DASH coin", "DASH token", "Digital Cash", "Dash blockchain", "Dash digital currency"],
    "XMRUSD": ["Monero", "XMR"], "ETCUSD": ["Ethereum Classic", "ETC"],
    "ZECUSD": ["Zcash", "ZEC"], "BNBUSD": ["Binance Coin", "BNB"],
    "DOGEUSD": ["Dogecoin", "DOGE"], "USDTUSD": ["Tether", "USDT"],
    "LINKUSD": ["Chainlink", "LINK"], "ATOMUSD": ["Cosmos", "ATOM"],
    "XTZUSD": ["Tezos", "XTZ"], "USDCUSD": ["USD Coin", "USDC", "USDC Coin"],
    "SOLUSD": ["Solana", "SOL", "Solana token"], "TONUSD": ["Toncoin", "The Open Network", "TON"],
    "AVAXUSD": ["Avalanche", "AVAX", "Avalanche network"], "DOTUSD": ["Polkadot", "DOT", "Polkadot network"],
    "NEARUSD": ["Near Protocol", "NEAR", "Near"], "APTUSD": ["Aptos", "APT", "Aptos network"],
    "SUIUSD": ["Sui", "Sui Network", "SUI token"], "ICPUSD": ["Internet Computer", "ICP", "Internet Computer Protocol"],
    "KASUSD": ["Kaspa", "KAS", "Kaspa network"], "STXUSD": ["Stacks", "STX", "Stacks network"],
    "SEIUSD": ["Sei", "Sei Network", "SEI token"], "HYPEUSD": ["Hyperliquid", "HYPE"],
    "POLUSD": ["Polygon", "POL", "MATIC", "Polygon Ecosystem Token"], "OPUSD": ["Optimism", "OP", "Optimism network"],
    "ARBUSD": ["Arbitrum", "ARB", "Arbitrum network"], "RENDERUSD": ["Render", "RNDR", "Render token"],
    "IMXUSD": ["Immutable", "IMX", "Immutable X"], "SKYUSD": ["Sky", "Maker", "MKR", "Sky token"],
    "UNIUSD": ["Uniswap", "UNI", "Uniswap token"], "AAVEUSD": ["Aave", "AAVE token"],
    "ORCAUSD": ["Orca", "ORCA token"], "DAIUSD": ["Dai", "DAI stablecoin"],
    "SHIBUSD": ["Shiba Inu", "SHIB", "Shiba token"],
    "COCOA": ["Cocoa", "Cocoa Futures"], "GOLD": ["Gold", "XAU/USD", "Gold price", "Gold spot"],
    "SILVER": ["Silver", "XAG/USD", "Silver price", "Silver spot"],
    "OIL": ["Crude oil", "Oil price", "WTI", "Brent", "Brent oil", "WTI crude"],
    "NATGAS": ["Natural gas", "Gas price", "Natgas", "Henry Hub", "NG=F", "Natural gas futures"],
    "COPPER": ["Copper", "Rame", "Cobre", "Cuivre", "Kupfer", "Dr. Copper", "Copper Futures", "High Grade Copper"],
    "WHEAT": ["Wheat", "Grano", "Frumento", "Trigo", "Blé", "Weizen", "Wheat Futures", "Chicago Wheat"],
    "CORN": ["Corn", "Mais", "Maíz", "Maïs", "Corn Futures", "Granoturco"],
    "SOYBEANS": ["Soybeans", "Soia", "Soya", "Soja", "Soybean Futures", "Fagioli di soia"],
    "SUGAR": ["Sugar", "Zucchero", "Sucre", "Azúcar", "Zucker", "Sugar #11", "Sugar Futures"]
}

indicator_data = {}
fundamental_data = {}

# ==============================================================================
# 2. DIZIONARI AI E MACRO PER V2
# ==============================================================================

INSIGHT_DICT = {
    "vol_breakout_bull": [
        {
            "en": "Massive volume surge driving a bullish breakout. Momentum is accelerating.",
            "it": "Forte esplosione dei volumi a supporto del breakout rialzista. Momentum in accelerazione.",
            "es": "Fuerte aumento de volumen apoyando la ruptura alcista. El impulso se acelera.",
            "fr": "Forte augmentation des volumes soutenant la cassure haussière. Le momentum s'accélère.",
            "de": "Massiver Volumenanstieg treibt den bullischen Ausbruch voran. Das Momentum beschleunigt sich.",
            "pt": "Forte aumento de volume apoiando o rompimento de alta. O momento está acelerando.",
            "nl": "Massieve volumestijging stimuleert de bullish uitbraak. Het momentum versnelt.",
            "ar": "زيادة هائلة في الحجم تدفع الاختراق الصعودي. الزخم يتسارع.",
            "hi": "भारी वॉल्यूम उछाल बुलिश ब्रेकआउट को चला रहा है। गति तेज हो रही है।",
            "id": "Lonjakan volume besar mendorong penembusan bullish. Momentum semakin cepat.",
            "ja": "大規模な取引量の急増が強気なブレイクアウトを推進しています。モメンタムが加速しています。",
            "ko": "대규모 거래량 급증이 강세 돌파를 주도하고 있습니다. 모멘텀이 가속화되고 있습니다.",
            "ru": "Резкий скачок объема поддерживает бычий прорыв. Импульс ускоряется.",
            "zh-rCN": "巨大的交易量激增推动了看涨突破。势头正在加速。"
        },
        {
            "en": "Unusually high buying volume is propelling the asset. A strong continuation signal.",
            "it": "Volumi di acquisto insolitamente alti spingono l'asset. Forte segnale di continuazione.",
            "es": "Un volumen de compra inusualmente alto impulsa el activo. Fuerte señal de continuación.",
            "fr": "Un volume d'achat inhabituellement élevé propulse l'actif. Fort signal de continuation.",
            "de": "Ungewöhnlich hohes Kaufvolumen treibt den Vermögenswert an. Starkes Fortsetzungssignal.",
            "pt": "Volume de compra excepcionalmente alto impulsiona o ativo. Forte sinal de continuação.",
            "nl": "Ongewoon hoog koopvolume stuwt het actief. Sterk voortzettingssignaal.",
            "ar": "حجم شراء مرتفع بشكل غير عادي يدفع الأصل. إشارة استمرار قوية.",
            "hi": "असामान्य रूप से उच्च खरीद मात्रा संपत्ति को आगे बढ़ा रही है। मजबूत निरंतरता संकेत।",
            "id": "Volume pembelian yang sangat tinggi mendorong aset. Sinyal kelanjutan yang kuat.",
            "ja": "異常に高い買い取引量が資産を押し上げています。強い継続シグナル。",
            "ko": "비정상적으로 높은 매수 거래량이 자산을 상승시키고 있습니다. 강력한 지속 신호.",
            "ru": "Необычно высокий объем покупок стимулирует рост актива. Сильный сигнал продолжения.",
            "zh-rCN": "异常高的买入量正在推动该资产。强烈的延续信号。"
        }
    ],
    "vol_breakout_bear": [
        {
            "en": "Heavy selling pressure confirmed by unusually high trading volumes.",
            "it": "Forte pressione in vendita confermata da volumi di scambio insolitamente alti.",
            "es": "Fuerte presión de venta confirmada por volúmenes inusualmente altos.",
            "fr": "Forte pression à la vente confirmée par des volumes d'échanges inhabituellement élevés.",
            "de": "Starker Verkaufsdruck, bestätigt durch ungewöhnlich hohe Handelsvolumina.",
            "pt": "Forte pressão de venda confirmada por volumes de negociação extraordinariamente altos.",
            "nl": "Zware verkoopdruk bevestigd door ongebruikelijk hoge handelsvolumes.",
            "ar": "ضغط بيع كثيف تؤكده أحجام تداول عالية بشكل غير عادي.",
            "hi": "असामान्य रूप से उच्च ट्रेडिंग वॉल्यूम द्वारा भारी बिकवाली के दबाव की पुष्टि की गई।",
            "id": "Tekanan jual yang berat dikonfirmasi oleh volume perdagangan yang sangat tinggi.",
            "ja": "異常に高い取引量によって確認された強い売り圧力。",
            "ko": "비정상적으로 높은 거래량으로 확인된 강한 매도 압력.",
            "ru": "Сильное давление продавцов подтверждается необычно высокими объемами торгов.",
            "zh-rCN": "异常高的交易量证实了沉重的抛售压力。"
        },
        {
            "en": "Sharp downside move backed by significant volume. Bears are firmly in control.",
            "it": "Forte ribasso accompagnato da volumi significativi. I ribassisti hanno il pieno controllo.",
            "es": "Fuerte movimiento a la baja respaldado por un volumen significativo. Los bajistas tienen el control.",
            "fr": "Fort mouvement à la baisse soutenu par un volume significatif. Les baissiers contrôlent.",
            "de": "Scharfe Abwärtsbewegung, gestützt durch signifikantes Volumen. Bären haben die Kontrolle.",
            "pt": "Forte movimento de baixa apoiado por volume significativo. Os ursos estão no controle.",
            "nl": "Scherpe neerwaartse beweging gesteund door aanzienlijk volume. Beren hebben de controle.",
            "ar": "حركة هبوطية حادة مدعومة بحجم كبير. الدببة يسيطرون بقوة.",
            "hi": "महत्वपूर्ण मात्रा द्वारा समर्थित तेज गिरावट। मंदड़िए नियंत्रण में हैं।",
            "id": "Penurunan tajam didukung oleh volume yang signifikan. Bear memegang kendali.",
            "ja": "大規模な取引量を伴う急激な下落。弱気派が完全に主導権を握っています。",
            "ko": "상당한 거래량을 동반한 급락. 약세장이 시장을 장악하고 있습니다.",
            "ru": "Резкое движение вниз, подкрепленное значительным объемом. Медведи уверенно контролируют ситуацию.",
            "zh-rCN": "在巨量支撑下急剧下挫。空头完全控制了局面。"
        }
    ],
    "rsi_overbought": [
        {
            "en": "Strong rally, but RSI is in deep overbought territory. Vulnerable to pullbacks.",
            "it": "Rally solido, ma l'RSI è in forte ipercomprato. Rischio di prese di beneficio.",
            "es": "Fuerte repunte, pero el RSI está en sobrecompra. Vulnerable a retrocesos.",
            "fr": "Fort rallye, mais le RSI est en surachat. Vulnérable aux replis.",
            "de": "Starke Rallye, aber der RSI ist überkauft. Anfällig für Rücksetzer.",
            "pt": "Forte rali, mas o RSI está sobrecomprado. Vulnerável a retrocessos.",
            "nl": "Sterke rally, maar RSI is overgekocht. Kwetsbaar voor terugval.",
            "ar": "ارتفاع قوي، لكن مؤشر القوة النسبية في منطقة ذروة الشراء. عرضة للتراجع.",
            "hi": "मजबूत रैली, लेकिन आरएसआई ओवरबॉट क्षेत्र में है। पुलबैक की संभावना है।",
            "id": "Reli kuat, tetapi RSI berada di wilayah overbought. Rentan terhadap penarikan kembali.",
            "ja": "強い上昇ですが、RSIは買われ過ぎの水準にあります。反落に注意が必要です。",
            "ko": "강력한 랠리지만 RSI가 과매수 상태입니다. 하락 조정에 취약합니다.",
            "ru": "Сильное ралли, но RSI в зоне перекупленности. Возможен откат.",
            "zh-rCN": "强劲反弹，但RSI处于严重超买区域。容易出现回调。"
        },
        {
            "en": "Extreme RSI levels suggest the asset is stretched. High probability of a short-term correction.",
            "it": "I livelli estremi di RSI indicano un asset tirato. Alta probabilità di una correzione a breve termine.",
            "es": "Niveles extremos de RSI sugieren que el activo está sobreextendido. Alta probabilidad de corrección.",
            "fr": "Les niveaux extrêmes du RSI suggèrent que l'actif est tendu. Forte probabilité de correction.",
            "de": "Extreme RSI-Werte deuten auf eine Überdehnung hin. Hohe Wahrscheinlichkeit einer Korrektur.",
            "pt": "Níveis extremos de RSI sugerem que o ativo está esticado. Alta probabilidade de correção.",
            "nl": "Extreme RSI-niveaus suggereren dat het actief overbelast is. Grote kans op een correctie.",
            "ar": "مستويات مؤشر القوة النسبية القصوى تشير إلى تشبع الأصل. احتمال كبير لتصحيح قصير الأجل.",
            "hi": "अत्यधिक आरएसआई स्तर संकेत देते हैं कि संपत्ति खिंची हुई है। अल्पकालिक सुधार की उच्च संभावना।",
            "id": "Level RSI ekstrem menunjukkan aset terlalu tinggi. Probabilitas tinggi untuk koreksi jangka pendek.",
            "ja": "RSIが極端な水準にあり、資産の買われ過ぎを示唆。短期的な調整の可能性が高いです。",
            "ko": "극단적인 RSI 수준은 자산이 과매수되었음을 시사합니다. 단기 조정 가능성이 높습니다.",
            "ru": "Экстремальные уровни RSI указывают на перекупленность. Высокая вероятность краткосрочной коррекции.",
            "zh-rCN": "极端的RSI水平表明资产严重超买。短期回调的可能性很高。"
        }
    ],
    "rsi_oversold": [
        {
            "en": "Asset is heavily oversold. Setup suggests a potential technical bounce.",
            "it": "L'asset è in forte ipervenduto. Il setup suggerisce un potenziale rimbalzo tecnico.",
            "es": "El activo está muy sobrevendido. Posible rebote técnico.",
            "fr": "L'actif est fortement survendu. La configuration suggère un rebond technique potentiel.",
            "de": "Der Vermögenswert ist stark überverkauft. Möglicher technischer Rebound.",
            "pt": "O ativo está muito sobrevendido. Possível salto técnico.",
            "nl": "Activa is zwaar oververkocht. Mogelijke technische opleving.",
            "ar": "الأصل في منطقة ذروة البيع. قد يحدث ارتداد فني.",
            "hi": "एसेट भारी ओवरसोल्ड है। सेटअप संभावित तकनीकी उछाल का सुझाव देता है।",
            "id": "Aset sangat oversold. Setup menunjukkan potensi pantulan teknis.",
            "ja": "資産は売られ過ぎです。テクニカルな反発の可能性があります。",
            "ko": "자산이 심하게 과매도되었습니다. 기술적 반등의 가능성이 있습니다.",
            "ru": "Актив сильно перепродан. Возможен технический отскок.",
            "zh-rCN": "资产严重超卖。设定暗示潜在的技术性反弹。"
        },
        {
            "en": "Deep oversold conditions reached. Sellers might be exhausted, watch for potential reversals.",
            "it": "Raggiunte condizioni di profondo ipervenduto. I venditori potrebbero essere esausti, attenzione alle inversioni.",
            "es": "Condiciones de sobreventa profunda. Los vendedores podrían estar agotados, atentos a reversiones.",
            "fr": "Conditions de survente profonde atteintes. Les vendeurs pourraient être épuisés.",
            "de": "Tiefe überverkaufte Bedingungen erreicht. Verkäufer könnten erschöpft sein.",
            "pt": "Condições de sobrevida profunda alcançadas. Vendedores podem estar exaustos.",
            "nl": "Diep oververkochte condities bereikt. Verkopers zijn mogelijk uitgeput.",
            "ar": "الوصول إلى ظروف ذروة البيع العميقة. قد يكون البائعون منهكين، راقب الانعكاسات.",
            "hi": "गहरी ओवरसोल्ड स्थितियों तक पहुंच गया। विक्रेता थक सकते हैं, उलटफेर पर नजर रखें।",
            "id": "Kondisi oversold dalam tercapai. Penjual mungkin kelelahan, perhatikan pembalikan.",
            "ja": "深刻な売られ過ぎの水準に到達しました。売りが枯渇している可能性があり、反転に注目です。",
            "ko": "심각한 과매도 상태에 도달했습니다. 매도세가 소진되었을 수 있으므로 반전 가능성을 주시하세요.",
            "ru": "Достигнута глубокая перепроданность. Продавцы могут быть истощены, следите за разворотами.",
            "zh-rCN": "达到深度超卖状态。抛售可能已经枯竭，注意潜在的反转。"
        }
    ],
    "support_test": [
        {
            "en": "Testing crucial support levels. Price action remains fragile.",
            "it": "Test in corso su livelli di supporto cruciali. La price action resta fragile.",
            "es": "Probando niveles de soporte cruciales. La acción del precio sigue frágil.",
            "fr": "Test de niveaux de support cruciaux. L'action des prix reste fragile.",
            "de": "Testet wichtige Unterstützungsniveaus. Die Preisaktion bleibt fragil.",
            "pt": "Testando níveis de suporte cruciais. A ação do preço continua frágil.",
            "nl": "Test cruciale steunniveaus. Prijsactie blijft kwetsbaar.",
            "ar": "اختبار مستويات دعم حاسمة. حركة السعر لا تزال هشة.",
            "hi": "महत्वपूर्ण समर्थन स्तरों का परीक्षण। मूल्य कार्रवाई नाजुक बनी हुई है।",
            "id": "Menguji level support krusial. Aksi harga tetap rapuh.",
            "ja": "重要なサポートレベルをテスト中。プライスアクションは依然として不安定です。",
            "ko": "중요한 지지선을 테스트 중입니다. 가격 움직임이 여전히 불안정합니다.",
            "ru": "Тестирование ключевых уровней поддержки. Динамика цен остается хрупкой.",
            "zh-rCN": "正在测试关键支撑位。价格走势依然脆弱。"
        },
        {
            "en": "Hovering right on major support. A breakdown here could accelerate the sell-off.",
            "it": "Il prezzo oscilla su un supporto maggiore. Una rottura qui potrebbe accelerare i ribassi.",
            "es": "Rondando un soporte mayor. Una ruptura aquí podría acelerar la venta.",
            "fr": "Oscille sur un support majeur. Une cassure ici pourrait accélérer la vente.",
            "de": "Schwankt an einer wichtigen Unterstützung. Ein Durchbruch hier könnte den Ausverkauf beschleunigen.",
            "pt": "Pairando sobre um suporte importante. Um rompimento aqui pode acelerar as vendas.",
            "nl": "Zweeft net op grote steun. Een doorbraak hier kan de uitverkoop versnellen.",
            "ar": "يحوم مباشرة على الدعم الرئيسي. قد يؤدي الانهيار هنا إلى تسريع عمليات البيع.",
            "hi": "प्रमुख समर्थन पर मँडरा रहा है। यहां टूटने से बिकवाली में तेजी आ सकती है।",
            "id": "Melayang tepat di support utama. Penembusan di sini bisa mempercepat aksi jual.",
            "ja": "主要なサポートライン上で推移しています。ここを下抜けると売りが加速する可能性があります。",
            "ko": "주요 지지선에 머물고 있습니다. 여기가 무너지면 매도세가 가속화될 수 있습니다.",
            "ru": "Колеблется на уровне основной поддержки. Пробой здесь может ускорить распродажу.",
            "zh-rCN": "徘徊在主要支撑位附近。如果跌破，可能会加速抛售。"
        }
    ],
    "resistance_break": [
        {
            "en": "Testing key resistance with positive technical confluence.",
            "it": "Test di resistenze chiave in corso con confluenza tecnica positiva.",
            "es": "Probando resistencia clave con confluencia técnica positiva.",
            "fr": "Test de résistance clé avec une confluence technique positive.",
            "de": "Testet wichtige Widerstände mit positiver technischer Konfluenz.",
            "pt": "Testando resistência chave com confluência técnica positiva.",
            "nl": "Test belangrijke weerstand met positieve technische samenloop.",
            "ar": "اختبار مقاومة رئيسية مع التقاء فني إيجابي.",
            "hi": "सकारात्मक तकनीकी संगम के साथ प्रमुख प्रतिरोध का परीक्षण।",
            "id": "Menguji resistensi kunci dengan konfluensi teknis positif.",
            "ja": "ポジティブなテクニカルコンフルエンスを伴い、主要なレジスタンスをテスト中。",
            "ko": "긍정적인 기술적 융합과 함께 주요 저항선을 테스트 중입니다.",
            "ru": "Тестирование ключевого сопротивления с положительным техническим слиянием.",
            "zh-rCN": "在积极的技术汇合下测试关键阻力位。"
        },
        {
            "en": "Pushing against major resistance. A confirmed breakout opens the door to new highs.",
            "it": "Spinta contro una resistenza maggiore. Un breakout confermato apre le porte a nuovi massimi.",
            "es": "Empujando contra la resistencia mayor. Una ruptura confirmada abre la puerta a nuevos máximos.",
            "fr": "Poussée contre une résistance majeure. Une cassure confirmée ouvre la voie à de nouveaux sommets.",
            "de": "Drängt gegen wichtigen Widerstand. Ein bestätigter Ausbruch öffnet die Tür zu neuen Höchstständen.",
            "pt": "Pressionando contra grande resistência. Um rompimento confirmado abre caminho para novas máximas.",
            "nl": "Duwt tegen grote weerstand. Een bevestigde uitbraak opent de deur naar nieuwe hoogtepunten.",
            "ar": "الضغط ضد المقاومة الرئيسية. الاختراق المؤكد يفتح الباب أمام مستويات عالية جديدة.",
            "hi": "प्रमुख प्रतिरोध के खिलाफ जोर दे रहा है। एक पुष्ट ब्रेकआउट नई ऊंचाई के लिए द्वार खोलता है。",
            "id": "Mendorong terhadap resistensi utama. Penembusan yang dikonfirmasi membuka jalan ke tertinggi baru.",
            "ja": "主要なレジスタンスに迫っています。明確なブレイクアウトは新高値への道を開きます。",
            "ko": "주요 저항선을 압박하고 있습니다. 확고한 돌파는 새로운 고점을 향한 길을 열어줍니다.",
            "ru": "Давление на основное сопротивление. Подтвержденный прорыв открывает путь к новым максимумам.",
            "zh-rCN": "正在冲击主要阻力位。如果确认突破，将打开通往新高的大门。"
        }
    ],
    "generic_bull": [
        {
            "en": "Solid positive momentum driven by a favorable technical setup.",
            "it": "Solido momentum positivo guidato da un setup tecnico favorevole.",
            "es": "Sólido impulso positivo impulsado por una configuración técnica favorable.",
            "fr": "Solide momentum positif soutenu par une configuration technique favorable.",
            "de": "Solides positives Momentum, angetrieben durch ein günstiges technisches Setup.",
            "pt": "Forte momento positivo impulsionado por uma configuração técnica favorável.",
            "nl": "Solide positief momentum gedreven door een gunstige technische opzet.",
            "ar": "زخم إيجابي قوي مدفوع بإعداد فني مناسب.",
            "hi": "अनुकूल तकनीकी सेटअप द्वारा संचालित ठोस सकारात्मक गति।",
            "id": "Momentum positif yang solid didorong oleh pengaturan teknis yang menguntungkan.",
            "ja": "良好なテクニカルセットアップによる強固なポジティブモメンタム。",
            "ko": "유리한 기술적 설정에 의해 주도되는 견고한 상승 모멘텀.",
            "ru": "Уверенный позитивный импульс, обусловленный благоприятной технической картиной.",
            "zh-rCN": "在有利的技术设定推动下，呈现稳健的积极势头。"
        },
        {
            "en": "Steady uptrend supported by healthy price action. Bias remains to the upside.",
            "it": "Trend rialzista costante supportato da un'ottima price action. La tendenza resta al rialzo.",
            "es": "Tendencia alcista constante. El sesgo sigue siendo al alza.",
            "fr": "Tendance haussière régulière. Le biais reste à la hausse.",
            "de": "Stetiger Aufwärtstrend. Die Tendenz bleibt aufwärts gerichtet.",
            "pt": "Tendência de alta constante. O viés continua sendo de alta.",
            "nl": "Gestage opwaartse trend. De voorkeur blijft opwaarts.",
            "ar": "اتجاه صعودي ثابت. لا يزال التحيز نحو الاتجاه الصعودي.",
            "hi": "स्थिर अपट्रेंड। पूर्वाग्रह ऊपर की ओर बना हुआ है।",
            "id": "Uptrend yang stabil. Bias tetap ke arah atas.",
            "ja": "健全な値動きに支えられた堅調な上昇トレンド。依然として上値目線です。",
            "ko": "건전한 가격 흐름이 뒷받침하는 안정적인 상승 추세. 상승 기조가 유지되고 있습니다.",
            "ru": "Устойчивый восходящий тренд. Склонность остается к росту.",
            "zh-rCN": "稳健的上升趋势受到良好价格走势的支撑。偏向依然看涨。"
        }
    ],
    "generic_bear": [
        {
            "en": "Trend remains bearish. Indicators suggest ongoing weakness.",
            "it": "Il trend rimane ribassista. Gli indicatori suggeriscono debolezza persistente.",
            "es": "La tendencia sigue siendo bajista. Los indicadores sugieren debilidad continua.",
            "fr": "La tendance reste baissière. Les indicateurs suggèrent une faiblesse continue.",
            "de": "Trend bleibt bärisch. Indikatoren deuten auf anhaltende Schwäche hin.",
            "pt": "A tendência continua de baixa. Os indicadores sugerem fraqueza contínua.",
            "nl": "Trend blijft bearish. Indicatoren wijzen op aanhoudende zwakte.",
            "ar": "الاتجاه لا يزال هبوطيًا. المؤشرات تشير إلى ضعف مستمر.",
            "hi": "प्रवृत्ति मंदी की बनी हुई है। संकेतक निरंतर कमजोरी का सुझाव देते हैं।",
            "id": "Tren tetap bearish. Indikator menunjukkan pelemahan yang berkelanjutan.",
            "ja": "トレンドは依然として弱気です。指標は継続的な弱さを示唆しています。",
            "ko": "하락 추세가 지속되고 있습니다. 지표들은 지속적인 약세를 시사합니다.",
            "ru": "Тренд остается медвежьим. Индикаторы указывают на сохраняющуюся слабость.",
            "zh-rCN": "趋势依然看跌。指标表明持续疲软。"
        },
        {
            "en": "Lower highs and lower lows persist. Technicals point to further downside risk.",
            "it": "Persistono massimi e minimi decrescenti. L'analisi tecnica punta a ulteriori rischi al ribasso.",
            "es": "Persisten máximos y mínimos más bajos. La técnica apunta a más caídas.",
            "fr": "Les sommets et creux descendants persistent. La technique indique des risques de baisse.",
            "de": "Niedrigere Hochs und Tiefs bleiben bestehen. Die Technik deutet auf weitere Abwärtsrisiken hin.",
            "pt": "Máximas e mínimas mais baixas persistem. A técnica aponta para mais quedas.",
            "nl": "Lagere toppen en bodems houden aan. De techniek wijst op verdere neerwaartse risico's.",
            "ar": "استمرار القمم والقيعان المنخفضة. تشير التحليلات الفنية إلى مزيد من المخاطر الهبوطية.",
            "hi": "निचले उच्च और निचले निम्न बने रहते हैं। तकनीकी आगे के जोखिम की ओर इशारा करते हैं।",
            "id": "Titik tertinggi dan terendah yang lebih rendah terus berlanjut. Indikator teknis menunjukkan risiko penurunan lebih lanjut.",
            "ja": "高値・安値の切り下がりが続いています。テクニカルはさらなる下落リスクを示唆しています。",
            "ko": "고점과 저점이 낮아지는 추세가 지속되고 있습니다. 기술적 지표는 추가 하락 위험을 나타냅니다.",
            "ru": "Продолжают формироваться более низкие максимумы и минимумы. Техника указывает на дальнейшее падение.",
            "zh-rCN": "较低的高点和较低的低点持续存在。技术面指向进一步的下行风险。"
        }
    ]
}

MACRO_SCENARIOS = {
    "strong_bull": [
        {
            "en": "Broad-based buying across sectors. Risk-on sentiment is dominating the session as equities push higher.",
            "it": "Acquisti diffusi su tutti i settori. Il sentiment 'risk-on' domina la sessione spingendo l'azionario al rialzo.",
            "es": "Compras generalizadas en todos los sectores. El sentimiento 'risk-on' domina la sesión.",
            "fr": "Achats généralisés sur l'ensemble des secteurs. Le sentiment de prise de risque domine la séance.",
            "de": "Breit angelegte Käufe über alle Sektoren hinweg. Die Risikobereitschaft dominiert die Sitzung.",
            "pt": "Compras generalizadas em todos os setores. O sentimento de apetite ao risco domina a sessão.",
            "nl": "Brede aankopen over alle sectoren. Het risk-on sentiment domineert de sessie.",
            "ar": "شراء واسع النطاق عبر القطاعات. تسيطر معنويات الإقبال على المخاطرة على الجلسة.",
            "hi": "सभी क्षेत्रों में व्यापक खरीदारी। जोखिम लेने की भावना सत्र पर हावी है।",
            "id": "Pembelian berbasis luas di seluruh sektor. Sentimen risk-on mendominasi sesi ini.",
            "ja": "全セクターにわたる広範な買い。リスクオンの地合いがセッションを支配しています。",
            "ko": "전 섹터에 걸친 광범위한 매수세. 위험 선호 심리가 시장을 주도하고 있습니다.",
            "ru": "Широкомасштабные покупки во всех секторах. Склонность к риску доминирует на сессии.",
            "zh-rCN": "跨板块的广泛买盘。追逐风险的情绪主导了今天的交易。"
        },
        {
            "en": "Strong market breadth highlights widespread optimism. Equities are pushing decisively higher.",
            "it": "L'ampiezza del mercato evidenzia un diffuso ottimismo. L'azionario spinge con decisione al rialzo.",
            "es": "La amplitud del mercado destaca el optimismo. Las acciones empujan al alza.",
            "fr": "L'ampleur du marché souligne l'optimisme. Les actions poussent résolument à la hausse.",
            "de": "Die Marktbreite unterstreicht den Optimismus. Aktien drängen nach oben.",
            "pt": "A amplitude do mercado destaca o otimismo. As ações estão subindo decisivamente.",
            "nl": "De marktbreedte benadrukt het optimisme. Aandelen stijgen resoluut.",
            "ar": "يسلط اتساع السوق الضوء على التفاؤل الواسع. تدفع الأسهم بقوة نحو الارتفاع.",
            "hi": "बाजार की चौड़ाई व्यापक आशावाद पर प्रकाश डालती है। इक्विटी निर्णायक रूप से ऊपर की ओर धकेल रहे हैं।",
            "id": "Luasnya pasar menyoroti optimisme yang meluas. Ekuitas mendorong lebih tinggi.",
            "ja": "市場の広がりが広範な楽観論を浮き彫りにしています。株式は決定的に上昇しています。",
            "ko": "시장의 광범위한 상승세가 전반적인 낙관론을 보여줍니다. 주가가 결정적으로 상승하고 있습니다.",
            "ru": "Широта рынка подчеркивает повсеместный оптимизм. Акции решительно растут.",
            "zh-rCN": "强劲的市场广度突显了广泛的乐观情绪。股市正果断走高。"
        }
    ],
    "strong_bear": [
        {
            "en": "Widespread selling pressure. Investors are shedding risk across the board amid rising market uncertainty.",
            "it": "Forte pressione in vendita. Gli investitori si liberano degli asset a rischio in un clima di incertezza.",
            "es": "Presión de venta generalizada. Los inversores se deshacen del riesgo en medio de la incertidumbre.",
            "fr": "Pression vendeuse généralisée. Les investisseurs se débarrassent de leurs actifs risqués.",
            "de": "Weit verbreiteter Verkaufsdruck. Anleger bauen angesichts steigender Unsicherheit Risiko ab.",
            "pt": "Pressão de venda generalizada. Os investidores estão se desfazendo do risco.",
            "nl": "Wijdverbreide verkoopdruk. Beleggers bouwen risico af te midden van toenemende onzekerheid.",
            "ar": "ضغط بيع واسع النطاق. يتخلى المستثمرون عن المخاطر وسط تزايد عدم اليقين.",
            "hi": "व्यापक बिकवाली का दबाव। निवेशक अनिश्चितता के बीच जोखिम कम कर रहे हैं।",
            "id": "Tekanan jual yang meluas. Investor melepaskan aset berisiko di tengah ketidakpastian.",
            "ja": "広範な売り圧力。不確実性の高まりの中、投資家はリスクを回避しています。",
            "ko": "광범위한 매도 압력. 불확실성이 커지는 가운데 투자자들이 위험 자산을 처분하고 있습니다.",
            "ru": "Повсеместное давление продавцов. Инвесторы избавляются от рисковых активов.",
            "zh-rCN": "广泛的抛售压力。在不确定性增加的情况下，投资者正在全面规避风险。"
        },
        {
            "en": "Risk aversion takes over as sellers dominate the tape. Defensive positioning is clearly evident.",
            "it": "L'avversione al rischio prende il sopravvento mentre i venditori dominano. Posizionamenti difensivi evidenti.",
            "es": "La aversión al riesgo se impone mientras los vendedores dominan.",
            "fr": "L'aversion au risque prend le dessus alors que les vendeurs dominent.",
            "de": "Risikoaversion übernimmt, da Verkäufer dominieren.",
            "pt": "A aversão ao risco assume o controle enquanto os vendedores dominam.",
            "nl": "Risico-aversie neemt de overhand nu verkopers domineren.",
            "ar": "يتولى النفور من المخاطرة زمام الأمور حيث يسيطر البائعون. التمركز الدفاعي واضح.",
            "hi": "जोखिम से बचने की भावना हावी है क्योंकि विक्रेता हावी हैं। रक्षात्मक स्थिति स्पष्ट है।",
            "id": "Penghindaran risiko mengambil alih karena penjual mendominasi. Posisi defensif sangat jelas.",
            "ja": "売り手が相場を支配し、リスク回避の動きが強まっています。ディフェンシブなポジションが明白です。",
            "ko": "매도세가 시장을 장악하면서 위험 회피 심리가 확산되고 있습니다. 방어적 포지셔닝이 뚜렷합니다.",
            "ru": "Неприятие риска берет верх, продавцы доминируют. Защитное позиционирование очевидно.",
            "zh-rCN": "避险情绪占据主导，卖方控制了盘面。防御性仓位明显。"
        }
    ],
    "safe_haven": [
        {
            "en": "Capital is rotating into defensive assets and precious metals as market anxiety increases.",
            "it": "Rotazione di capitali verso asset difensivi e metalli preziosi per via dei crescenti timori sui mercati.",
            "es": "El capital rota hacia activos defensivos y metales preciosos debido a la ansiedad del mercado.",
            "fr": "Les capitaux se dirigent vers les valeurs refuges et les métaux précieux face à l'anxiété du marché.",
            "de": "Kapital fließt in defensive Anlagen und Edelmetalle, da die Marktangst zunimmt.",
            "pt": "O capital está migrando para ativos defensivos e metais preciosos devido à ansiedade do mercado.",
            "nl": "Kapitaal roteert naar defensieve activa en edelmetalen naarmate de marktonrust toeneemt.",
            "ar": "يتحول رأس المال إلى الأصول الدفاعية والمعادن الثمينة مع تزايد قلق السوق.",
            "hi": "बाजार की चिंता बढ़ने के साथ पूंजी रक्षात्मक संपत्ति और कीमती धातुओं में जा रही है।",
            "id": "Modal beralih ke aset defensif dan logam mulia seiring meningkatnya kecemasan pasar.",
            "ja": "市場の不安が高まる中、資金はディフェンシブ資産や貴金属に逃避しています。",
            "ko": "시장 불안이 커지면서 자본이 방어주와 귀금속 등 안전 자산으로 이동하고 있습니다.",
            "ru": "Капитал перетекает в защитные активы и драгоценные металлы на фоне роста тревожности.",
            "zh-rCN": "随着市场焦虑情绪加剧，资金正流向防御性资产和贵金属。"
        },
        {
            "en": "Flight to safety is underway. Gold and silver see inflows as global market anxiety spikes.",
            "it": "Fuga verso la sicurezza in corso. Oro e argento registrano afflussi per il picco di ansia globale.",
            "es": "Vuelo hacia la seguridad. El oro y la plata ven entradas por la ansiedad global.",
            "fr": "Fuite vers la sécurité. L'or et l'argent voient des afflux face à l'anxiété.",
            "de": "Flucht in die Sicherheit. Gold und Silber verzeichnen Zuflüsse.",
            "pt": "Fuga para a segurança em andamento. Ouro e prata recebem fluxos.",
            "nl": "Vlucht naar veiligheid. Goud en zilver zien instroom.",
            "ar": "رحلة إلى الأمان جارية. يشهد الذهب والفضة تدفقات نقدية مع تصاعد قلق السوق العالمي.",
            "hi": "सुरक्षा की ओर उड़ान जारी है। वैश्विक बाजार की चिंता बढ़ने से सोना और चांदी में प्रवाह देखा जा رہا है।",
            "id": "Penerbangan ke aset aman sedang berlangsung. Emas dan perak melihat aliran masuk karena kecemasan pasar.",
            "ja": "安全資産への逃避が進行中。世界的な市場の不安が高まる中、金と銀に資金が流入しています。",
            "ko": "안전 자산으로의 도피가 진행 중입니다. 글로벌 시장 불안이 치솟으면서 금과 은에 자금이 유입되고 있습니다.",
            "ru": "Наблюдается бегство в качество. Золото и серебро видят приток средств на фоне тревоги.",
            "zh-rCN": "正在向安全资产转移。随着全球市场焦虑情绪飙升，资金流入金银。"
        }
    ],
    "consolidation": [
        {
            "en": "Broader markets are trading sideways. Investors are holding positions awaiting the next major catalyst.",
            "it": "Mercati in fase di consolidamento laterale. Gli investitori attendono il prossimo catalizzatore macroeconomico.",
            "es": "Los mercados cotizan lateralmente. Los inversores esperan el próximo gran catalizador.",
            "fr": "Les marchés évoluent sans tendance claire, dans l'attente du prochain catalyseur majeur.",
            "de": "Die Märkte tendieren seitwärts. Die Anleger warten auf den nächsten großen Impuls.",
            "pt": "Os mercados estão operando lateralmente. Os investidores aguardam o próximo catalisador.",
            "nl": "Markten bewegen zijwaarts. Beleggers wachten op de volgende grote katalysator.",
            "ar": "تتداول الأسواق في اتجاه جانبي. ينتظر المستثمرون المحفز الرئيسي التالي.",
            "hi": "बाजार सीमित दायरे में कारोबार कर रहे हैं। निवेशक अगले प्रमुख उत्प्रेरक की प्रतीक्षा कर रहे हैं।",
            "id": "Pasar bergerak sideways. Investor menunggu katalis besar berikutnya.",
            "ja": "市場は方向感に欠ける展開。投資家は次の主要なカタリストを待っています。",
            "ko": "시장이 횡보세를 보이고 있습니다. 투자자들은 다음 주요 촉매제를 기다리며 관망 중입니다.",
            "ru": "Рынки торгуются в боковике. Инвесторы ожидают следующего крупного катализатора.",
            "zh-rCN": "大盘呈横盘整理态势。投资者正在等待下一个主要催化剂。"
        },
        {
            "en": "Lack of clear directional momentum. The market is pausing to digest recent moves before the next leg.",
            "it": "Assenza di momentum direzionale chiaro. Il mercato è in pausa per digerire i recenti movimenti.",
            "es": "Falta de impulso direccional. El mercado hace una pausa para digerir los movimientos.",
            "fr": "Manque d'élan directionnel. Le marché fait une pause pour digérer les mouvements.",
            "de": "Mangel an gerichtetem Momentum. Der Markt pausiert, um Bewegungen zu verdauen.",
            "pt": "Falta de momento direcional. O mercado faz uma pausa para digerir os movimentos.",
            "nl": "Gebrek aan gericht momentum. De markt pauzeert om bewegingen te verteren.",
            "ar": "عدم وجود زخم اتجاهي واضح. يتوقف السوق لاستيعاب التحركات الأخيرة.",
            "hi": "स्पष्ट दिशात्मक गति का अभाव। बाजार हाल की चालों को पचाने के लिए रुक रहा है।",
            "id": "Kurangnya momentum arah yang jelas. Pasar berhenti sejenak untuk mencerna pergerakan baru-baru ini.",
            "ja": "明確な方向感が欠如しています。市場は次の展開を前に、直近の値動きを消化するために小休止しています。",
            "ko": "뚜렷한 방향성이 부족합니다. 시장은 다음 상승/하락 전 최근 움직임을 소화하며 쉬어가고 있습니다.",
            "ru": "Отсутствие четкого направленного импульса. Рынок взял паузу для консолидации.",
            "zh-rCN": "缺乏明确的方向性动能。市场正在暂停，以消化近期的走势。"
        }
    ]
}


# ==============================================================================
# 3. CLASSI LOGICA NUOVA (History & Hybrid)
# ==============================================================================
class HistoryManager:
    def __init__(self, r2_mgr, filename=history_path):
        self.r2 = r2_mgr
        self.filename = filename
        self.data = self.r2.read_json(self.filename)
        self._clean_old_data() 

    def save_data(self):
        self.r2.write_file(self.filename, self.data, is_json=True)

    def _clean_old_data(self):
        limit_date = datetime.now() - timedelta(days=21)
        changed = False
        for ticker in list(self.data.keys()):
            dates = list(self.data[ticker].keys())
            for d in dates:
                try:
                    entry_date = datetime.strptime(d, "%Y-%m-%d")
                    if entry_date < limit_date:
                        del self.data[ticker][d]
                        changed = True
                except: pass

    def update_history(self, ticker, sentiment, news_count):
        today = datetime.now().strftime("%Y-%m-%d")
        if ticker not in self.data: self.data[ticker] = {}
        self.data[ticker][today] = { "sentiment": float(sentiment), "news_count": int(news_count) }

    def calculate_delta_score(self, ticker, current_sent, current_count):
        if ticker not in self.data: return 50.0 
        
        history = self.data[ticker]
        today = datetime.now().strftime("%Y-%m-%d")
        
        past_sentiments = [v['sentiment'] for k, v in history.items() if k != today]
        past_counts = [v['news_count'] for k, v in history.items() if k != today]
        
        if not past_sentiments: return 50.0 
        
        avg_sent = sum(past_sentiments) / len(past_sentiments)
        sent_diff = current_sent - avg_sent
        raw_delta = (sent_diff * 100)
        
        multiplier = 1.0
        MIN_NEWS_FLOOR = 3  
        
        if len(past_counts) >= 2 and current_count >= MIN_NEWS_FLOOR:
            avg_count = np.mean(past_counts)
            std_dev = np.std(past_counts)
            if std_dev < 0.2: std_dev = 0.2
            z_score = (current_count - avg_count) / std_dev
            
            if z_score >= 2.0:      
                multiplier = 2.0    
            elif z_score >= 1.5:    
                multiplier = 1.75   
            elif z_score >= 1.0:    
                multiplier = 1.25   
                
        else:
            avg_simple = sum(past_counts)/len(past_counts) if past_counts else 0
            if current_count >= 5 and current_count >= (avg_simple * 2):
                multiplier = 1.5

        final_delta = 50 + (raw_delta * multiplier)
        return max(min(final_delta, 100), 0)

class BacktestSystem:
    def __init__(self, r2_mgr, folder_name=TEST_FOLDER):
        self.r2 = r2_mgr
        self.folder = folder_name
        self.json_filename = f"{self.folder}/backtest_log.json"
        self.html_filename = f"{self.folder}/reliability_curve.html"
        
        self.data = self._load_data()
        self.load_success = True if self.data else False 
        
    def _load_data(self):
        raw_data = self.r2.read_json(self.json_filename)
        
        if "log" in raw_data and isinstance(raw_data["log"], list):
            new_db = {}
            for entry in raw_data["log"]:
                sym = entry["symbol"]
                date = entry["date"]
                if sym not in new_db: new_db[sym] = {}
                new_db[sym][date] = {
                    "score": entry["score"],
                    "price": entry["start_price"],
                    "results": entry["daily_results"],
                    "status": entry.get("status", "active")
                }
            return new_db
        return raw_data

    def save_data(self):
        if not self.load_success and not self.data:
             return

        for sym in self.data:
            sorted_dates = sorted(self.data[sym].keys(), reverse=True)
            self.data[sym] = {k: self.data[sym][k] for k in sorted_dates}

        self.r2.write_file(self.json_filename, self.data, is_json=True)

    def log_new_prediction(self, symbol, score, current_price):
        if not self.load_success and len(self.data) == 0: return

        today_str = datetime.now().strftime("%Y-%m-%d")
        if symbol not in self.data:
            self.data[symbol] = {}
            
        if today_str in self.data[symbol]:
            self.data[symbol][today_str]["score"] = score
            self.data[symbol][today_str]["price"] = float(current_price)
        else:
            self.data[symbol][today_str] = {
                "score": score,
                "price": float(current_price),
                "results": {},
                "status": "active"
            }

    def update_daily_tracking(self, current_prices_map):
        if not self.load_success and not self.data: return

        today = datetime.now()
        max_days = 20
        
        for symbol, dates_data in self.data.items():
            if symbol not in current_prices_map: continue
            
            for date_key, entry in dates_data.items():
                if entry.get("status") == "closed": continue
                
                try:
                    entry_date = datetime.strptime(date_key, "%Y-%m-%d")
                    days_passed = (today - entry_date).days
                    
                    if days_passed == 0: continue 
                    if days_passed > max_days:
                        entry["status"] = "closed"
                        continue
                        
                    start_price = entry["price"]
                    curr_price = current_prices_map[symbol]
                    change = ((curr_price - start_price) / start_price) * 100
                    
                    entry["results"][str(days_passed)] = round(change, 2)
                except: continue

        self._analyze_stats()

    def _analyze_stats(self):
        stats_by_day = {}
        
        for symbol, dates_data in self.data.items():
            for date_key, entry in dates_data.items():
                
                score = entry["score"]
                direction = 0
                if score >= 55: direction = 1
                elif score <= 45: direction = -1
                else: continue
                
                for day, val in entry["results"].items():
                    if day not in stats_by_day: stats_by_day[day] = {"wins": 0, "total": 0, "ret": 0.0}
                    
                    is_win = (direction == 1 and val > 0.1) or (direction == -1 and val < -0.1)
                    
                    stats_by_day[day]["total"] += 1
                    if is_win: stats_by_day[day]["wins"] += 1
                    stats_by_day[day]["ret"] += val

        curve = []
        best_day = "N/A"
        best_acc = 0.0
        
        for d in sorted(stats_by_day.keys(), key=lambda x: int(x)):
            data = stats_by_day[d]
            if data["total"] < 5: continue
            
            acc = round((data["wins"]/data["total"])*100, 1)
            avg_ret = round(data["ret"]/data["total"], 2)
            curve.append({"day": int(d), "accuracy": acc, "avg_return": avg_ret})
            
            if acc > best_acc:
                best_acc = acc
                best_day = d
                
        self.stats_cache = {"best_day": best_day, "best_acc": best_acc, "curve": curve}

    def generate_report(self):
        if not hasattr(self, 'stats_cache'): 
            self._analyze_stats()
            
        stats = getattr(self, 'stats_cache', {})
        curve = stats.get("curve", [])
        
        html = [
            "<html><head><title>Forward Testing</title>",
            "<style>body{font-family:Arial;padding:20px;} .bar{height:20px;color:white;text-align:right;padding-right:5px;} .g{background:#28a745;} .r{background:#dc3545;} .y{background:#ffc107;color:black;}</style>",
            "</head><body>",
            "<h1>🧪 Forward Testing (Real-time Validation)</h1>",
            f"<p>Analisi basata su segnali reali salvati in passato. Cartella: <i>{self.folder}</i></p>",
            f"<h3>Picco Affidabilità: Giorno {stats.get('best_day','-')} ({stats.get('best_acc',0)}%)</h3>",
            "<table border='1' width='100%' style='border-collapse:collapse;'><tr><th>Giorno</th><th>Win Rate</th><th>Profitto Medio</th></tr>"
        ]
        
        for p in curve:
            d, acc, ret = p['day'], p['accuracy'], p['avg_return']
            
            if acc >= 55: color = "g"
            elif acc >= 48: color = "y"
            else: color = "r"
            
            width = max(acc, 15)
            
            html.append(f"<tr><td>Day {d}</td><td><div class='bar {color}' style='width:{width}%'>{acc}%</div></td><td>{ret}%</td></tr>")
            
        html.append("</table></body></html>")
        
        self.r2.write_file(self.html_filename, "\n".join(html), is_json=False)

class PatternAnalyzer:
    def __init__(self, df):
        if hasattr(df.columns, 'levels'): 
            self.o = df['Open'].iloc[:, 0].values
            self.h = df['High'].iloc[:, 0].values
            self.l = df['Low'].iloc[:, 0].values
            self.c = df['Close'].iloc[:, 0].values
        else:
            self.o = df['Open'].values
            self.h = df['High'].values
            self.l = df['Low'].values
            self.c = df['Close'].values

    def get_pattern_score(self):
        score, _ = self._analyze_logic()
        return score

    def get_pattern_info(self):
        score, patterns = self._analyze_logic()
        pattern_text = ", ".join(patterns) if patterns else "No significant patterns"
        return score, pattern_text

    def _analyze_logic(self):
        score = 0.0
        patterns_found = []
        limit = len(self.c)
        
        if limit < 20: return 0.0, ["Insufficient Data"]
        
        i = limit - 1
        sma_10 = np.mean(self.c[i-9:i+1])
        trend_up = self.c[i] > sma_10 and self.c[i-1] > self.c[i-5]
        trend_down = self.c[i] < sma_10 and self.c[i-1] < self.c[i-5]

        c1, c2, c3 = self.c[i-2], self.c[i-1], self.c[i]
        o1, o2, o3 = self.o[i-2], self.o[i-1], self.o[i]
        h1, h2, h3 = self.h[i-2], self.h[i-1], self.h[i]
        l1, l2, l3 = self.l[i-2], self.l[i-1], self.l[i]
        
        body1 = abs(c1 - o1)
        body2 = abs(c2 - o2)
        body3 = abs(c3 - o3)
        
        if trend_down and c2 < o2 and c3 > o3 and c3 > o2 and o3 < c2: 
            score += 0.4
            patterns_found.append("Bullish Engulfing")
        if trend_up and c2 > o2 and c3 < o3 and c3 < o2 and o3 > c2: 
            score -= 0.4
            patterns_found.append("Bearish Engulfing")

        lower_shadow3 = min(c3, o3) - l3
        upper_shadow3 = h3 - max(c3, o3)
        if lower_shadow3 > (body3 * 2.0) and upper_shadow3 < (body3 * 0.5):
            if trend_down:
                score += 0.3
                patterns_found.append("Hammer")
            elif trend_up:
                score -= 0.3
                patterns_found.append("Hanging Man")

        if upper_shadow3 > (body3 * 2.0) and lower_shadow3 < (body3 * 0.5):
            if trend_up:
                score -= 0.3
                patterns_found.append("Shooting Star")
            elif trend_down:
                score += 0.3
                patterns_found.append("Inverted Hammer")

        if trend_down and c1 < o1 and body1 > (self.h[i-2]-self.l[i-2])*0.5: 
            if body2 < body1 * 0.3: 
                if c3 > o3 and c3 > (o1 + c1) / 2: 
                    score += 0.5
                    patterns_found.append("Morning Star")

        if trend_up and c1 > o1 and body1 > (self.h[i-2]-self.l[i-2])*0.5: 
            if body2 < body1 * 0.3: 
                if c3 < o3 and c3 < (o1 + c1) / 2: 
                    score -= 0.5
                    patterns_found.append("Evening Star")

        if c1 > o1 and c2 > o2 and c3 > o3 and c3 > c2 > c1 and trend_down:
            score += 0.4
            patterns_found.append("3 White Soldiers")
        elif c1 < o1 and c2 < o2 and c3 < o3 and c3 < c2 < c1 and trend_up:
            score -= 0.4
            patterns_found.append("3 Black Crows")

        if body3 <= (h3 - l3) * 0.1 and (h3 - l3) > 0:
            patterns_found.append("Doji")

        curr_price = c3
        lookback = min(126, limit) 
        recent_h = self.h[-lookback:]
        recent_l = self.l[-lookback:]
        max_h = np.max(recent_h)
        min_l = np.min(recent_l)
        threshold = 0.02
        
        if abs(curr_price - max_h) / curr_price <= threshold:
            if trend_up and self.h[i-1] < max_h * 0.98: 
                score -= 0.5
                patterns_found.append("Double Top Resistance")
            else:
                score -= 0.3
                patterns_found.append("At Resistance Level")

        elif abs(curr_price - min_l) / curr_price <= threshold:
            if trend_down and self.l[i-1] > min_l * 1.02: 
                score += 0.5
                patterns_found.append("Double Bottom Support")
            else:
                score += 0.3
                patterns_found.append("At Support Level")

        final_score = max(min(score, 1.0), -1.0)
        return final_score, patterns_found


class HybridScorer:
    def _calculate_rsi(self, series, period=14):
        delta = series.diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean().replace(0, np.nan)
        rs = gain / loss
        rs = rs.fillna(0)
        return 100 - (100 / (1 + rs))

    def _get_technical_score(self, df):
        if len(df) < 50: return 0.0
        
        close = df['Close']
        if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
        
        try:
            sma = float(close.rolling(window=50).mean().iloc[-1])
            curr = float(close.iloc[-1])
            rsi = float(self._calculate_rsi(close).iloc[-1])
        except: return 0.0
        
        score = 0.0
        if curr > sma: score += 0.5
        else: score -= 0.5
        
        if rsi < 30: score += 0.5 
        elif rsi > 70: score -= 0.5 
        return max(min(score, 1.0), -1.0)

    def calculate_probability(self, df, sent_raw, news_n, lead, is_lead, delta_score):
        tech_score = self._get_technical_score(df)
        analyzer = PatternAnalyzer(df)
        pattern_score = analyzer.get_pattern_score()

        curr_lead = 0.0 if is_lead else lead
        delta_factor = (delta_score - 50) / 50.0 
        
        if is_lead:
            if news_n == 0:     
                w_n, w_l, w_t, w_p, w_d = 0.00, 0.00, 0.40, 0.40, 0.20
            elif news_n <= 3:   
                w_n, w_l, w_t, w_p, w_d = 0.20, 0.00, 0.35, 0.30, 0.15
            else:               
                w_n, w_l, w_t, w_p, w_d = 0.40, 0.00, 0.25, 0.20, 0.15
        else:
            if news_n == 0:     
                w_n, w_l, w_t, w_p, w_d = 0.00, 0.20, 0.35, 0.35, 0.10
            elif news_n <= 3:   
                w_n, w_l, w_t, w_p, w_d = 0.15, 0.20, 0.30, 0.25, 0.10
            else:               
                w_n, w_l, w_t, w_p, w_d = 0.35, 0.15, 0.20, 0.20, 0.10
        
        final = (sent_raw * w_n) + \
                (tech_score * w_t) + \
                (pattern_score * w_p) + \
                (curr_lead * w_l) + \
                (delta_factor * w_d)
        
        final = max(min(final, 1.0), -1.0)
        return round(50 + (final * 50), 2)

    def get_signal(self, score):
        if score >= 60: return "STRONG BUY", "green"
        elif score >= 53: return "BUY", "green"
        elif score <= 40: return "STRONG SELL", "red"
        elif score <= 47: return "SELL", "red"
        else: return "HOLD", "black"

# ==============================================================================
# 4. HELPER FUNCTIONS
# ==============================================================================
def generate_query_variants(symbol):
    base_variants = [f"{symbol} stock", f"{symbol} investing", f"{symbol} earnings", f"{symbol} news", f"{symbol} analysis"]
    names = symbol_name_map.get(symbol.upper(), [])
    for name in names:
        base_variants.append(f"{name} stock")
    return list(set(base_variants))

MAX_ARTICLES_PER_SYMBOL = 500

def get_stock_news(symbol):
    query_variants = generate_query_variants(symbol)
    base_url = "https://news.google.com/rss/search?q={}&hl=en-US&gl=US&ceid=US:en"
    now = datetime.utcnow()
    days_90 = now - timedelta(days=90)
    days_30 = now - timedelta(days=30)
    days_7  = now - timedelta(days=7)

    news_90_days, news_30_days, news_7_days = [], [], []
    seen_titles = set()
    total_articles = 0

    for raw_query in query_variants:
        if total_articles >= MAX_ARTICLES_PER_SYMBOL: break
        query = quote_plus(raw_query)
        feed = feedparser.parse(base_url.format(query))
        for entry in feed.entries:
            if total_articles >= MAX_ARTICLES_PER_SYMBOL: break
            try:
                title = entry.title.strip()
                link = entry.link.strip()
                source = entry.source.title if hasattr(entry, 'source') else "Unknown"
                image = None
                if hasattr(entry, 'media_content'): image = entry.media_content[0]['url']
                elif hasattr(entry, 'media_thumbnail'): image = entry.media_thumbnail[0]['url']

                if title.lower() in seen_titles: continue
                seen_titles.add(title.lower())
                try: news_date = datetime.strptime(entry.published, "%a, %d %b %Y %H:%M:%S %Z")
                except: continue

                news_item = (title, news_date, link, source, image)
                if news_date >= days_90: news_90_days.append(news_item)
                if news_date >= days_30: news_30_days.append(news_item)
                if news_date >= days_7: news_7_days.append(news_item)
                total_articles += 1
            except: continue
    return {"last_90_days": news_90_days, "last_30_days": news_30_days, "last_7_days": news_7_days}

def calculate_sentiment_vader(news_items, return_raw=False):
    if not news_items: 
        return 0.5 if not return_raw else 0.0

    scores = []
    now = datetime.utcnow()
    
    for item in news_items:
        title = item[0]
        date = item[1]
        score = sia.polarity_scores(title)['compound']
        days = (now - date).days
        weight = math.exp(-0.03 * days)
        scores.append(score * weight)
        
    avg = sum(scores) / len(scores) if scores else 0
    if return_raw: 
        return avg 
    return (avg + 1) / 2

def clean_google_news_url(url):
    try:
        parsed = urlparse(url)
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', parsed.fragment))
        return clean_url if len(clean_url) < len(url) else url
    except:
        return url

# ==============================================================================
# 5. MAIN LOGIC (FUSIONE COMPLETA)
# ==============================================================================
def get_sentiment_for_all_symbols(symbol_list):
    history_mgr = HistoryManager(r2_manager, history_path)
    scorer = HybridScorer()

    backtester = BacktestSystem(r2_manager, folder_name=TEST_FOLDER)
    current_prices_map = {} 
    
    sentiment_results = {}
    percentuali_combine = {} 
    all_news_entries = []
    crescita_settimanale = {}
    dati_storici_all = {}
    indicator_data = {}
    fundamental_data = {}
    momentum_results = {}
    market_breadth_data = {}
    calendario_economico_globale = []
    
    # Pre-calcolo Leaders
    leader_trends = {}
    for sec, ticker in sector_leaders.items():
        try:
            yf_tick = TICKER_MAP.get(ticker, ticker)
            df = yf.download(yf_tick, period="6mo", progress=False, auto_adjust=True)
            if not df.empty and len(df) > 50:
                close = df['Close']
                if isinstance(close, pd.DataFrame): close = close.iloc[:,0]
                sma = close.rolling(50).mean().iloc[-1]
                curr = close.iloc[-1]
                leader_trends[ticker] = 0.5 if curr > sma else -0.5
            else: leader_trends[ticker] = 0.0
        except: leader_trends[ticker] = 0.0

    # --- SETUP CACHE INSIDER SU CLOUDFLARE R2 ---
    oggi_str = datetime.now().strftime("%Y-%m-%d")
    insider_cache = r2_manager.read_json(cache_insider_path)
    
    # Loop Principale
    for symbol, adjusted_symbol in zip(symbol_list, symbol_list_for_yfinance):
        news_data = get_stock_news(symbol)
        s7_raw = calculate_sentiment_vader(news_data["last_7_days"], return_raw=True)
        s7_norm = calculate_sentiment_vader(news_data["last_7_days"], return_raw=False) 
        news_count_7 = len(news_data["last_7_days"])
        s90 = calculate_sentiment_vader(news_data["last_90_days"])
        sentiment_results[symbol] = {"90_days": s90}
        
        history_mgr.update_history(symbol, s7_norm, news_count_7)
        delta_val = history_mgr.calculate_delta_score(symbol, s7_norm, news_count_7)
        momentum_results[symbol] = delta_val
        
        hybrid_prob = 50.0
        signal_str = "HOLD"
        sig_col = "black"
        tabella_indicatori = None
        dati_storici_html = None
        tabella_fondamentali = None
        pat_text_names = "No significant patterns"
        pat_sentiment_str = "NEUTRAL"
        pat_color = "black"
        pat_score_val = 0.0
        sells_data = None
        buys_data = None
        flow_payload = None
        
        sector = asset_sector_map.get(symbol, "General")
        leader_sym = sector_leaders.get(sector, "SPX500")
        leader_val = leader_trends.get(leader_sym, 0.0)
        is_leader = (symbol == leader_sym)

        try:
            ticker = str(adjusted_symbol).strip().upper()
            data = scarica_storico(ticker)
            controlla_salute_storico(ticker, data)

            if not data.empty:
                data = data.replace(0, np.nan)
                data = data.ffill()
                data = data.bfill()
                
                close = data['Close']
                high = data['High']
                low = data['Low']
                dati_storici_all[symbol] = data.copy()

                analyzer = PatternAnalyzer(data)
                pat_score_val, pat_text_names = analyzer.get_pattern_info()
                
                pat_sentiment_str = "NEUTRAL"
                pat_color = "black"
                if pat_score_val >= 0.3: 
                    pat_sentiment_str = "BULLISH"
                    pat_color = "green"
                elif pat_score_val <= -0.3: 
                    pat_sentiment_str = "BEARISH"
                    pat_color = "red"
                    
                hybrid_prob = scorer.calculate_probability(data, s7_raw, news_count_7, leader_val, is_leader, delta_val)
                percentuali_combine[symbol] = hybrid_prob 
                signal_str, sig_col = scorer.get_signal(hybrid_prob)

                current_price = float(close.iloc[-1])
                current_prices_map[symbol] = current_price
    
                backtester.log_new_prediction(symbol, hybrid_prob, current_price)
            
                try:
                    last_price = close.iloc[-1]
                    last_date = close.index[-1]
                    target_date = last_date - timedelta(days=7)
                    prev_price = close.asof(target_date)

                    if pd.isna(prev_price):
                        idx = max(0, len(close) - 6)
                        prev_price = close.iloc[idx]

                    growth = ((last_price - prev_price) / prev_price) * 100
                    crescita_settimanale[symbol] = round(growth, 2)
                except: 
                    crescita_settimanale[symbol] = 0.0

                rsi = RSIIndicator(close).rsi().iloc[-1]

                try:
                    sma50_val = close.rolling(window=50).mean().iloc[-1]
                    sma200_val = close.rolling(window=200).mean().iloc[-1]
                    market_breadth_data[symbol] = {
                        "rsi": round(rsi, 2),
                        "sma50": 1 if current_price > sma50_val else 0,
                        "sma200": 1 if current_price > sma200_val else 0
                    }
                except:
                    market_breadth_data[symbol] = {"rsi": 50.0, "sma50": 0, "sma200": 0}
                
                macd = MACD(close)
                macd_line = macd.macd().iloc[-1]
                macd_signal = macd.macd_signal().iloc[-1]
                stoch = StochasticOscillator(high, low, close)
                stoch_k = stoch.stoch().iloc[-1]
                stoch_d = stoch.stoch_signal().iloc[-1]
                ema_10 = EMAIndicator(close, window=10).ema_indicator().iloc[-1]
                cci = CCIIndicator(high, low, close).cci().iloc[-1]
                will_r = WilliamsRIndicator(high, low, close).williams_r().iloc[-1]
                bb = BollingerBands(close)
                
                indicators = {
                    "RSI (14)": round(rsi, 2),
                    "MACD Line": round(macd_line, 2),
                    "MACD Signal": round(macd_signal, 2),
                    "Stochastic %K": round(stoch_k, 2),
                    "Stochastic %D": round(stoch_d, 2),
                    "EMA (10)": round(ema_10, 2),
                    "CCI (14)": round(cci, 2),
                    "Williams %R": round(will_r, 2),
                    "BB Upper": round(bb.bollinger_hband().iloc[-1], 2),
                    "BB Lower": round(bb.bollinger_lband().iloc[-1], 2),
                    "BB Width": round(bb.bollinger_wband().iloc[-1], 4),
                }
                micro = is_micro_price(current_price)
                if micro:
                    # round(..., 2) azzererebbe MACD, EMA e Bollinger
                    indicators.update({
                        "MACD Line": float(macd_line),
                        "MACD Signal": float(macd_signal),
                        "EMA (10)": float(ema_10),
                        "BB Upper": float(bb.bollinger_hband().iloc[-1]),
                        "BB Lower": float(bb.bollinger_lband().iloc[-1]),
                    })
                indicator_data[symbol] = indicators
                righe_indicatori = [(k, fmt_micro(v) if micro else v) for k, v in indicators.items()]
                tabella_indicatori = pd.DataFrame(righe_indicatori, columns=["Indicatore", "Valore"]).to_html(index=False, border=0)
                
                tk_obj = yf.Ticker(adjusted_symbol)
                try:
                    info = tk_obj.info or {}
                    def safe_value(key):
                        val = info.get(key)
                        return round(val, 4) if isinstance(val, (int, float)) else "N/A"

                    def safe_pct(key):
                        val = info.get(key)
                        return f"{round(val * 100, 2)}%" if isinstance(val, (int, float)) else "N/A"

                    target_mean = info.get("targetMeanPrice", "N/A")
                    dati_alternativi = {
                        "Consensus Analisti": info.get("recommendationKey", "N/A").replace("_", " ").title(),
                        "Target Price Medio": f"${target_mean}" if target_mean != "N/A" else "N/A",
                        "N. Analisti": info.get("numberOfAnalystOpinions", "N/A"),
                        "Proprietà Istituzionale": safe_pct("heldPercentInstitutions"),
                        "Proprietà Insider": safe_pct("heldPercentInsiders"),
                        "Azioni Shortate (Pessimismo)": safe_pct("shortPercentOfFloat")
                    }
                    
                    m_cap_raw = info.get("marketCap", "N/A")
                    fundamental_data[symbol] = {"Market Cap": m_cap_raw}

                    fondamentali = {
                        "Trailing P/E": safe_value("trailingPE"),
                        "Forward P/E": safe_value("forwardPE"),
                        "EPS Growth (YoY)": safe_value("earningsQuarterlyGrowth"),
                        "Revenue Growth (YoY)": safe_value("revenueGrowth"),
                        "Profit Margins": safe_value("profitMargins"),
                        "Debt to Equity": safe_value("debtToEquity"),
                        "Dividend Yield": safe_value("dividendYield")
                    }

                    # ------------------------------------------------------------------
                    # RIGHE AGGIUNTIVE PER IL FAIR VALUE (solo in CODA: i parser dell'app
                    # leggono per NOME riga, ma NON cambiare ordine/nomi delle righe sopra
                    # e NON creare nuove tabelle: altri parser usano indici/`.last()`).
                    # Valori sempre numerici puri (niente '%'), "N/A" se non disponibili.
                    # Zero chiamate di rete extra: tutto viene dallo stesso `info`.
                    # ------------------------------------------------------------------
                    # Blocco ISOLATO: un errore qui non deve mai far sparire la tabella base.
                    try:
                        def _fv_num(k):
                            v = info.get(k)
                            if isinstance(v, bool) or not isinstance(v, (int, float)):
                                return None
                            return float(v) if (v == v and abs(v) != float("inf")) else None

                        # Dati di bilancio e prezzo possono avere valute diverse (ADR, LSE in pence):
                        # i rapporti tra le due grandezze si calcolano solo se la valuta coincide.
                        _fv_same_ccy = bool(info.get("currency")) and info.get("currency") == info.get("financialCurrency")

                        _fv_beta = _fv_num("beta")
                        _fv_roe = _fv_num("returnOnEquity")
                        _fv_pb = _fv_num("priceToBook")
                        _fv_fcf, _fv_mcap = _fv_num("freeCashflow"), _fv_num("marketCap")
                        _fv_debt, _fv_cash = _fv_num("totalDebt"), _fv_num("totalCash")
                        _fv_ebitda = _fv_num("ebitda")
                        _fv_rev, _fv_opm = _fv_num("totalRevenue"), _fv_num("operatingMargins")
                        _fv_bvps, _fv_sh = _fv_num("bookValue"), _fv_num("sharesOutstanding")

                        # FCF Yield = free cash flow / market cap (frazione)
                        _fv_fcf_yield = None
                        if _fv_same_ccy and _fv_fcf is not None and _fv_mcap and _fv_mcap > 0:
                            _fv_fcf_yield = _fv_fcf / _fv_mcap

                        # Net Debt / EBITDA (multiplo; rapporto tra due grandezze di bilancio)
                        _fv_nd_ebitda = None
                        if _fv_debt is not None and _fv_ebitda and _fv_ebitda > 0:
                            _fv_nd_ebitda = (_fv_debt - (_fv_cash or 0.0)) / _fv_ebitda

                        # ROIC approssimato = NOPAT / capitale investito (debito + patrimonio netto - cassa)
                        # NOPAT = margine operativo * ricavi * (1 - 25% aliquota generica).
                        _fv_roic = None
                        if _fv_same_ccy and _fv_rev and _fv_rev > 0 and _fv_opm is not None and _fv_debt is not None:
                            _eq = (_fv_bvps * _fv_sh) if (_fv_bvps is not None and _fv_sh) else 0.0
                            _base = _fv_debt + max(_eq, 0.0)
                            _invested = max(_base - (_fv_cash or 0.0), 0.25 * _base)  # evita denominatori ~0
                            if _base > 0 and _invested > 0:
                                _fv_roic = (_fv_opm * _fv_rev * 0.75) / _invested

                        def _fv_row(x, lo, hi, nd=4):
                            return round(x, nd) if (x is not None and lo <= x <= hi) else "N/A"

                        fondamentali["Beta"] = _fv_row(_fv_beta, -1.0, 4.0, 2)
                        fondamentali["Return on Equity"] = _fv_row(_fv_roe, -5.0, 5.0)
                        fondamentali["ROIC (approx)"] = _fv_row(_fv_roic, -5.0, 5.0)
                        fondamentali["FCF Yield"] = _fv_row(_fv_fcf_yield, -1.0, 1.0)
                        fondamentali["Net Debt / EBITDA"] = _fv_row(_fv_nd_ebitda, -20.0, 50.0, 2)
                        fondamentali["Price to Book"] = _fv_row(_fv_pb, 0.01, 500.0, 2)
                    except Exception:
                        for _k in ("Beta", "Return on Equity", "ROIC (approx)", "FCF Yield", "Net Debt / EBITDA", "Price to Book"):
                            fondamentali.setdefault(_k, "N/A")

                    tabella_fondamentali = pd.DataFrame(fondamentali.items(), columns=["Fondamentale", "Valore"]).to_html(index=False, border=0)
                except: pass

                tabella_utili = None
                try:
                    try:
                        utili_df = tk_obj.get_earnings_dates(limit=10)
                        if utili_df is not None and not utili_df.empty:
                            if utili_df.index.tz is not None:
                                utili_df.index = utili_df.index.tz_localize(None)
                            oggi = pd.Timestamp.now().tz_localize(None)

                            utili_passati = utili_df[utili_df.index < oggi].head(5)
                            if not utili_passati.empty:
                                colonne_disponibili = utili_passati.columns.tolist()
                                colonne_da_mostrare = []
                                if 'EPS Estimate' in colonne_disponibili: colonne_da_mostrare.append('EPS Estimate')
                                
                                if 'Reported EPS' in colonne_disponibili: colonne_da_mostrare.append('Reported EPS')
                                elif 'EPS Actual' in colonne_disponibili: colonne_da_mostrare.append('EPS Actual')
                                
                                if 'Surprise(%)' in colonne_disponibili: colonne_da_mostrare.append('Surprise(%)')

                                utili_passati_html = utili_passati[colonne_da_mostrare].copy()
                                utili_passati_html.index = utili_passati_html.index.strftime('%Y-%m-%d')
                                tabella_utili = utili_passati_html.to_html(border=1)

                            utili_futuri = utili_df[utili_df.index >= oggi]
                            for idx, row in utili_futuri.iterrows():
                                eps_est = row.get('EPS Estimate', 'N/A')
                                if isinstance(eps_est, (int, float)) and not pd.isna(eps_est):
                                    eps_est = round(eps_est, 2)
                                    
                                calendario_economico_globale.append({
                                    "Data": idx.strftime('%Y-%m-%d'),
                                    "Ticker": symbol,
                                    "Evento": "Rapporto Sugli Utili",
                                    "Dettaglio": f"Est. EPS: {eps_est}"
                                })
                    except Exception as e:
                        pass 

                    cal = tk_obj.calendar
                    if isinstance(cal, dict):
                        if 'Earnings Date' in cal:
                            earnings_dates = cal['Earnings Date']
                            if not isinstance(earnings_dates, list):
                                earnings_dates = [earnings_dates]
                                
                            for e_date in earnings_dates:
                                if pd.notna(e_date):
                                    e_date_clean = pd.to_datetime(e_date).tz_localize(None).date()
                                    if e_date_clean >= pd.Timestamp.now().tz_localize(None).date():
                                        gia_inserito = any(d['Data'] == e_date_clean.strftime('%Y-%m-%d') and d['Ticker'] == symbol and "Utili" in d['Evento'] for d in calendario_economico_globale)
                                        
                                        if not gia_inserito:
                                            est_avg = cal.get('Earnings Average', 'N/A')
                                            dettaglio = f"Est. EPS: {est_avg}" if est_avg != 'N/A' else "Expected earnings report"
                                            
                                            calendario_economico_globale.append({
                                                "Data": e_date_clean.strftime('%Y-%m-%d'),
                                                "Ticker": symbol,
                                                "Evento": "Rapporto Sugli Utili",
                                                "Dettaglio": dettaglio
                                            })
                except Exception as e:
                    pass

                try:
                    oggi_date = pd.Timestamp.now().tz_localize(None).date()
                    limite_passato = oggi_date - timedelta(days=3)

                    actions = tk_obj.actions
                    if not actions.empty and 'Dividends' in actions.columns:
                        if actions.index.tz is not None:
                            actions.index = actions.index.tz_localize(None)
                        
                        futuri_divs = actions[(actions.index >= pd.Timestamp(limite_passato)) & (actions['Dividends'] > 0)]
                        for idx, row in futuri_divs.iterrows():
                            valore_div = round(row['Dividends'], 4)
                            calendario_economico_globale.append({
                                "Data": idx.strftime('%Y-%m-%d'),
                                "Ticker": symbol,
                                "Evento": "Stacco Dividendo",
                                "Dettaglio": f"Payout: ${valore_div}"
                            })
                            
                    cal = tk_obj.calendar
                    if isinstance(cal, dict):
                        
                        if 'Dividend Date' in cal and pd.notna(cal['Dividend Date']):
                            try:
                                div_date_clean = pd.to_datetime(cal['Dividend Date']).tz_localize(None).date()
                                if div_date_clean >= limite_passato:
                                    calendario_economico_globale.append({
                                        "Data": div_date_clean.strftime('%Y-%m-%d'),
                                        "Ticker": symbol,
                                        "Evento": "Pagamento Dividendo",
                                        "Dettaglio": "Payout Day"
                                    })
                            except Exception:
                                pass 
                        
                        if 'Ex-Dividend Date' in cal and pd.notna(cal['Ex-Dividend Date']):
                            try:
                                ex_date_clean = pd.to_datetime(cal['Ex-Dividend Date']).tz_localize(None).date()
                                if ex_date_clean >= limite_passato:
                                    gia_inserito = any(d['Data'] == ex_date_clean.strftime('%Y-%m-%d') and d['Ticker'] == symbol and "Stacco" in d['Evento'] for d in calendario_economico_globale)
                                    if not gia_inserito:
                                        calendario_economico_globale.append({
                                            "Data": ex_date_clean.strftime('%Y-%m-%d'),
                                            "Ticker": symbol,
                                            "Evento": "Stacco Dividendo",
                                            "Dettaglio": "Ex-Dividend Date"
                                        })
                            except Exception:
                                pass
                except Exception as e:
                    pass
                
                hist = data.copy()
                hist['Date'] = hist.index.strftime('%Y-%m-%d')
                if is_micro_price(current_price):
                    # il default di pandas (6 decimali) rimetterebbe i prezzi a gradini
                    for col in ['Close', 'High', 'Low', 'Open']:
                        hist[col] = hist[col].map(fmt_micro)
                dati_storici_html = hist[['Date','Close','High','Low','Open','Volume']].to_html(index=False, border=1)

                sells_data = None
                buys_data = None
                dati_da_cache = False

                if adjusted_symbol in insider_cache and insider_cache[adjusted_symbol].get("date") == oggi_str:
                    sells_data = insider_cache[adjusted_symbol].get("sells")
                    buys_data = insider_cache[adjusted_symbol].get("buys")
                    flow_payload = insider_cache[adjusted_symbol].get("flow")
                    dati_da_cache = True

                if not dati_da_cache:
                    is_crypto_forex_index = any(x in str(adjusted_symbol) for x in ["=", "^", "-USD"])
                    is_international = "." in str(adjusted_symbol) and not is_crypto_forex_index
                    
                    if is_crypto_forex_index:
                        pass
                    elif not is_international:
                        try:
                            url = f"http://openinsider.com/screener?s={adjusted_symbol}&o=&cnt=1000"
                            tables = pd.read_html(url)
                            if len(tables) > 0:
                                insider_trades = max(tables, key=lambda t: t.shape[0])
                                flow_payload = build_openinsider_payload(insider_trades)
                                insider_trades['Value_clean'] = insider_trades['Value'].replace(r'[\$,]', '', regex=True).astype(float)
                                
                                sells = insider_trades[insider_trades['Trade\xa0Type'].str.contains("Sale", na=False)].copy()
                                if not sells.empty:
                                    sells['Trade Date'] = pd.to_datetime(insider_trades['Trade\xa0Date'])
                                    daily_sells = sells.groupby('Trade Date')['Value_clean'].sum().abs().sort_index()
                                    last_day = daily_sells.index.max()
                                    max_daily = daily_sells.max()
                                    variance = ((daily_sells[last_day] - daily_sells.iloc[-2]) / daily_sells.iloc[-2] * 100) if len(daily_sells) >= 2 and daily_sells.iloc[-2] > 0 else 0
                                    sells_data = {
                                        'Last Day': last_day.strftime('%Y-%m-%d'),
                                        'Last Day Total Sells ($)': f"{daily_sells[last_day]:,.2f}",
                                        'Last vs Max (%)': (daily_sells[last_day] / max_daily * 100) if max_daily else 0,
                                        'Number of Sells Last Day': len(sells[sells['Trade Date'] == last_day]),
                                        'Variance': variance 
                                    }

                                buys = insider_trades[insider_trades['Trade\xa0Type'].str.contains("Purchase", na=False)].copy()
                                if not buys.empty:
                                    buys['Trade Date'] = pd.to_datetime(insider_trades['Trade\xa0Date'])
                                    daily_buys = buys.groupby('Trade Date')['Value_clean'].sum().abs().sort_index()
                                    last_day_b = daily_buys.index.max()
                                    max_daily_b = daily_buys.max()
                                    variance_b = ((daily_buys[last_day_b] - daily_buys.iloc[-2]) / daily_buys.iloc[-2] * 100) if len(daily_buys) >= 2 and daily_buys.iloc[-2] > 0 else 0
                                    buys_data = {
                                        'Last Day': last_day_b.strftime('%Y-%m-%d'),
                                        'Last Day Total Buys ($)': f"{daily_buys[last_day_b]:,.2f}",
                                        'Last vs Max (%)': (daily_buys[last_day_b] / max_daily_b * 100) if max_daily_b else 0,
                                        'Number of Buys Last Day': len(buys[buys['Trade Date'] == last_day_b]),
                                        'Variance': variance_b 
                                    }
                        except Exception: pass
                    else:
                        try:
                            API_KEY = FMP_API_KEY
                            if not API_KEY:
                                print("ATTENZIONE: Chiave FMP mancante!")
                            url_api = f"https://financialmodelingprep.com/api/v4/insider-trading?symbol={adjusted_symbol}&page=0&apikey={API_KEY}"
                            
                            response = requests.get(url_api)
                            
                            if response.status_code == 200 and len(response.json()) > 0:
                                api_df = pd.DataFrame(response.json())
                                
                                if 'transactionDate' in api_df.columns and 'transactionType' in api_df.columns:
                                    api_df['Trade Date'] = pd.to_datetime(api_df['transactionDate'])
                                    if 'securitiesTransacted' in api_df.columns and 'price' in api_df.columns:
                                        api_df['Value_clean'] = api_df['securitiesTransacted'] * api_df['price']
                                    else:
                                        api_df['Value_clean'] = 0.0 

                                    sells = api_df[api_df['transactionType'].astype(str).str.contains("Sale|S-Sale", case=False, na=False)].copy()
                                    if not sells.empty:
                                        daily_sells = sells.groupby('Trade Date')['Value_clean'].sum().abs().sort_index()
                                        if not daily_sells.empty:
                                            last_day = daily_sells.index.max()
                                            max_daily = daily_sells.max()
                                            variance = ((daily_sells[last_day] - daily_sells.iloc[-2]) / daily_sells.iloc[-2] * 100) if len(daily_sells) >= 2 and daily_sells.iloc[-2] > 0 else 0
                                            sells_data = {
                                                'Last Day': last_day.strftime('%Y-%m-%d'),
                                                'Last Day Total Sells ($)': f"{daily_sells[last_day]:,.2f}",
                                                'Last vs Max (%)': (daily_sells[last_day] / max_daily * 100) if max_daily else 0,
                                                'Number of Sells Last Day': len(sells[sells['Trade Date'] == last_day]),
                                                'Variance': variance 
                                            }

                                    buys = api_df[api_df['transactionType'].astype(str).str.contains("Buy|P-Purchase", case=False, na=False)].copy()
                                    if not buys.empty:
                                        daily_buys = buys.groupby('Trade Date')['Value_clean'].sum().abs().sort_index()
                                        if not daily_buys.empty:
                                            last_day_b = daily_buys.index.max()
                                            max_daily_b = daily_buys.max()
                                            variance_b = ((daily_buys[last_day_b] - daily_buys.iloc[-2]) / daily_buys.iloc[-2] * 100) if len(daily_buys) >= 2 and daily_buys.iloc[-2] > 0 else 0
                                            buys_data = {
                                                'Last Day': last_day_b.strftime('%Y-%m-%d'),
                                                'Last Day Total Buys ($)': f"{daily_buys[last_day_b]:,.2f}",
                                                'Last vs Max (%)': (daily_buys[last_day_b] / max_daily_b * 100) if max_daily_b else 0,
                                                'Number of Buys Last Day': len(buys[buys['Trade Date'] == last_day_b]),
                                                'Variance': variance_b 
                                            }
                        except Exception as e:
                            print(f"Errore API per {adjusted_symbol}: {e}")

                    if flow_payload is None and not is_crypto_forex_index:
                        try:
                            flow_payload = build_europe_insider_payload(adjusted_symbol)
                        except Exception as e:      # i dati extra non devono mai interrompere la pipeline
                            print(f"Insider extra {adjusted_symbol}: {e}")
                            flow_payload = None

                    insider_cache[adjusted_symbol] = {
                        "date": oggi_str,
                        "sells": sells_data,
                        "buys": buys_data,
                        "flow": flow_payload
                    }

        except Exception as e: print(f"Err {symbol}: {e}")
        
        file_res = f"{TARGET_FOLDER}/{symbol.upper()}_RESULT.html"
        html_content = [
            f"<html><head><title>{symbol} Forecast</title></head><body>",
            f"<h1>Report: {symbol}</h1>",
            f"<h2 style='color:{sig_col}'>{signal_str} (Hybrid Score: {hybrid_prob}%)</h2>",
            "<hr>",
            "<h3>Price Action Analysis (Patterns)</h3>",
            f"<p><strong>Detected Patterns:</strong> {pat_text_names}</p>",
            f"<p><strong>Chart Sentiment:</strong> <span style='color:{pat_color}'><b>{pat_sentiment_str}</b></span> (Score: {pat_score_val:.2f})</p>",
            "<hr>",
            "<h3>Analisi Hybrid (AI + Tech + Delta)</h3>",
            f"<p><strong>Settore:</strong> {sector} (Trend Leader: {'UP' if leader_val>0 else 'DOWN'})</p>",
            f"<p><strong>Delta Score (Momentum News):</strong> {round(delta_val, 2)}</p>",
            "<hr>",
            "<h2>Indicatori Tecnici</h2>",
            tabella_indicatori if tabella_indicatori else "<p>N/A</p>",
            "<h2>Dati Fondamentali</h2>",
            tabella_fondamentali if tabella_fondamentali else "<p>N/A</p>",
            "<h2>Informative Sells</h2>"
        ]
        
        if sells_data:
            html_content += [
                f"<p><strong>Ultimo giorno registrato:</strong> {sells_data['Last Day']}</p>",
                f"<p><strong>Totale vendite ultimo giorno ($):</strong> {sells_data['Last Day Total Sells ($)']}</p>",
                f"<p><strong>% rispetto al massimo storico giornaliero:</strong> {sells_data['Last vs Max (%)']:.2f}%</p>",
                f"<p><strong>Transazioni recenti:</strong> {sells_data['Number of Sells Last Day']}</p>",
                f"<p><strong>Variazione:</strong> {sells_data['Variance']:.2f}%</p>"
            ]
        else:
            html_content.append("<p>Informative Sells non disponibili.</p>")

        html_content.append("<h2>Informative Buys</h2>")
        if buys_data:
            html_content += [
                f"<p><strong>Ultimo giorno registrato:</strong> {buys_data['Last Day']}</p>",
                f"<p><strong>Totale acquisti ultimo giorno ($):</strong> {buys_data['Last Day Total Buys ($)']}</p>",
                f"<p><strong>% rispetto al massimo storico giornaliero:</strong> {buys_data['Last vs Max (%)']:.2f}%</p>",
                f"<p><strong>Transazioni recenti:</strong> {buys_data['Number of Buys Last Day']}</p>",
                f"<p><strong>Variazione:</strong> {buys_data['Variance']:.2f}%</p>"
            ]
        else:
            html_content.append("<p>Informative Buys non disponibili.</p>")

        html_content.append("<h2>Storico Utili (Earnings)</h2>")
        if tabella_utili:
            html_content.append(tabella_utili)
        else:
            html_content.append("<p>Dati sugli utili recenti non disponibili.</p>")
        
        html_content.append("<h2>Analisi Istituzionale & Sentiment</h2>")
        
        try:
            html_content.append("<table border='1' style='border-collapse:collapse; width:100%; text-align:left;'>")
            for chiave, valore in dati_alternativi.items():
                if valore != "N/A" and valore != "N/A%":
                    html_content.append(f"<tr><td style='padding:8px;'><strong>{chiave}:</strong></td><td style='padding:8px;'>{valore}</td></tr>")
            html_content.append("</table>")
        except NameError:
            html_content.append("<p>Dati istituzionali non disponibili.</p>")
        
        html_content.append("<h2>Dati Storici (ultimi 90 giorni)</h2>")
        html_content.append(dati_storici_html if dati_storici_html else "<p>N/A</p>")
        html_content.append("</body></html>")
        
        r2_manager.write_file(file_res, "\n".join(html_content), is_json=False)
        if flow_payload:
            r2_manager.write_file(f"{INSIDER_FOLDER}/{symbol.upper()}.json", flow_payload, is_json=True)

        for title, date, link, src, img in news_data["last_90_days"]:
            sc = (sia.polarity_scores(title)['compound'] + 1) / 2
            all_news_entries.append((symbol, title, sc, link, src, img, date))

    # Aggiornamento e Salvataggio Cache su R2
    r2_manager.write_file(cache_insider_path, insider_cache, is_json=True)

    print("Generazione Calendario Economico JSON in corso...")
    calendar_json_path = f"{TARGET_FOLDER}/calendario_economico.json"

    if calendario_economico_globale:
        df_cal = pd.DataFrame(calendario_economico_globale)
        df_cal = df_cal.sort_values(by="Data").reset_index(drop=True)
        cal_json = df_cal.to_json(orient="records", indent=4)
        r2_manager.write_file(calendar_json_path, cal_json, is_json=False)
    
    print("Elaborazione Forward Testing in corso...")
    backtester.update_daily_tracking(current_prices_map) 
    backtester.save_data()       
    backtester.generate_report() 

    history_mgr.save_data()
    return (sentiment_results, percentuali_combine, all_news_entries, 
            indicator_data, fundamental_data, crescita_settimanale, dati_storici_all, momentum_results, market_breadth_data)

# ==============================================================================
# 6. ESECUZIONE E SALVATAGGIO CLASSIFICHE SU R2
# ==============================================================================

print("Inizio calcolo globale del Sentiment per i simboli...")
sentiment_for_symbols, percentuali_combine, all_news_entries, indicator_data, fundamental_data, crescita_settimanale, dati_storici_all, momentum_results, market_breadth_data = get_sentiment_for_all_symbols(symbol_list)

# --- CLASSIFICA PRINCIPALE ---
sorted_symbols = sorted(((s, v) for s, v in percentuali_combine.items() if not is_excluded_from_rankings(s)), key=lambda x: x[1], reverse=True)

html_classifica = ["<html><head><title>Classifica dei Simboli</title></head><body>",
                   "<h1>Classifica dei Simboli (Hybrid Score)</h1>",
                   "<table border='1'><tr><th>Simbolo</th><th>Probabilità</th><th>Variazione 1G</th><th>Max/Min (52W/5G)</th><th>Cross SMA</th></tr>"]

for symbol, score in sorted_symbols:
    variazione_str = "N/A"
    info_52w = "N/A"
    cross_sma = "N/A"
    
    try:
        if symbol in dati_storici_all:
            df = dati_storici_all[symbol]
            
            close_p = df['Close'].iloc[:, 0] if isinstance(df['Close'], pd.DataFrame) else df['Close']
            high_p = df['High'].iloc[:, 0] if isinstance(df['High'], pd.DataFrame) else df['High']
            low_p = df['Low'].iloc[:, 0] if isinstance(df['Low'], pd.DataFrame) else df['Low']
            
            if len(close_p) >= 2:
                oggi = close_p.iloc[-1]
                ieri = close_p.iloc[-2]
                variazione = ((oggi - ieri) / ieri) * 100
                variazione_str = f"{variazione:+.2f}%"
                
            if len(close_p) >= 6:
                high_today = high_p.iloc[-1]
                low_today = low_p.iloc[-1]
                trovato_52w = False
                
                if len(close_p) >= 252:
                    max_52w = high_p.iloc[-253:-1].max()
                    min_52w = low_p.iloc[-253:-1].min()
                    if high_today >= max_52w:
                        info_52w = f"Nuovo Max 52W ({fmt_micro(high_today) if is_micro_price(high_today) else format(high_today, '.2f')})"
                        trovato_52w = True
                    elif low_today <= min_52w:
                        info_52w = f"Nuovo Min 52W ({fmt_micro(low_today) if is_micro_price(low_today) else format(low_today, '.2f')})"
                        trovato_52w = True
                        
                if not trovato_52w:
                    max_5g = high_p.iloc[-6:-1].max()
                    min_5g = low_p.iloc[-6:-1].min()
                    if high_today >= max_5g:
                        info_52w = f"Nuovo Max 5G ({fmt_micro(high_today) if is_micro_price(high_today) else format(high_today, '.2f')})"
                    elif low_today <= min_5g:
                        info_52w = f"Nuovo Min 5G ({fmt_micro(low_today) if is_micro_price(low_today) else format(low_today, '.2f')})"
                    
            if len(close_p) >= 21:
                s5 = close_p.rolling(window=5).mean()
                s20 = close_p.rolling(window=20).mean()
                
                p_ieri, p_oggi = close_p.iloc[-2], close_p.iloc[-1]
                s5_ieri, s5_oggi = s5.iloc[-2], s5.iloc[-1]
                s20_ieri, s20_oggi = s20.iloc[-2], s20.iloc[-1]
                
                if s5_ieri <= s20_ieri and s5_oggi > s20_oggi:
                    cross_sma = "Cross UP (SMA5 > SMA20)"
                elif s5_ieri >= s20_ieri and s5_oggi < s20_oggi:
                    cross_sma = "Cross DOWN (SMA5 < SMA20)"
                elif p_ieri <= s20_ieri and p_oggi > s20_oggi:
                    cross_sma = "Cross UP (Price > SMA20)"
                elif p_ieri >= s20_ieri and p_oggi < s20_oggi:
                    cross_sma = "Cross DOWN (Price < SMA20)"
                elif p_ieri <= s5_ieri and p_oggi > s5_oggi:
                    cross_sma = "Cross UP (Price > SMA5)"
                elif p_ieri >= s5_ieri and p_oggi < s5_oggi:
                    cross_sma = "Cross DOWN (Price < SMA5)"

    except Exception:
        pass

    mb = market_breadth_data.get(symbol, {"rsi": 50.0, "sma50": 0, "sma200": 0})
    html_classifica.append(f"<tr data-rsi='{mb['rsi']}' data-sma50='{mb['sma50']}' data-sma200='{mb['sma200']}'><td>{symbol}</td><td>{score:.2f}%</td><td>{variazione_str}</td><td>{info_52w}</td><td>{cross_sma}</td></tr>")

html_classifica.append("</table></body></html>")
r2_manager.write_file(file_path, "\n".join(html_classifica), is_json=False)
print("Classifica aggiornata con successo!")

# --- CLASSIFICA PRO ---
html_classifica_pro = ["<html><head><title>Classifica Combinata</title></head><body>",
                       "<h1>Classifica Combinata (Hybrid Logic)</h1>",
                       "<table border='1'><tr><th>Simbolo</th><th>Hybrid Score</th></tr>"]
for symbol, media in sorted_symbols: 
    html_classifica_pro.append(f"<tr><td>{symbol}</td><td>{media:.2f}%</td></tr>")
html_classifica_pro.append("</table></body></html>")
r2_manager.write_file(pro_path, "\n".join(html_classifica_pro), is_json=False)

# --- CLASSIFICA MOMENTUM ---
sorted_momentum = sorted(((s, v) for s, v in momentum_results.items() if not is_excluded_from_rankings(s)), key=lambda x: x[1], reverse=True)
html_mom = [
    "<html><head><title>Classifica Momentum</title>",
    "<style>",
    "table {border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;}",
    "th, td {border: 1px solid #ddd; padding: 8px; text-align: center;}",
    "th {background-color: #f2f2f2;}",
    ".high {color: green; font-weight: bold;}",
    ".low {color: red; font-weight: bold;}",
    ".neutral {color: black;}",
    "</style>",
    "</head><body>",
    "<h1>🔥 Classifica Momentum (Delta Score)</h1>",
    "<p>Indica l'accelerazione del sentiment e delle notizie rispetto alla media storica.</p>",
    "<table><tr><th>Simbolo</th><th>Momentum Score (0-100)</th><th>Stato</th></tr>"
]

for symbol, score in sorted_momentum:
    if score >= 60:
        color_class = "high"
        status = "HYPE / ACCELERAZIONE"
    elif score <= 40:
        color_class = "low"
        status = "DEPRESSIONE / CALO"
    else:
        color_class = "neutral"
        status = "Normale"
    html_mom.append(f"<tr><td><b>{symbol}</b></td><td class='{color_class}'>{score:.2f}</td><td>{status}</td></tr>")

html_mom.append("</table></body></html>")
r2_manager.write_file(mom_path, "\n".join(html_mom), is_json=False)

# --- CLASSIFICA SETTORI ---
sector_assets = defaultdict(list)
for symbol, score in percentuali_combine.items():
    if is_excluded_from_rankings(symbol): continue
    sec = asset_sector_map.get(symbol, "Altro")
    avg_liquidity_old = 0.0
    rvol = 1.0 
    asset_growth = crescita_settimanale.get(symbol, 0.0)
    
    if symbol in dati_storici_all:
        df = dati_storici_all[symbol]
        try:
            last_month = df.tail(20).copy()
            liquidity_series = (last_month['Close'] * last_month['Volume']).fillna(0)
            avg_liquidity_old = liquidity_series.mean()
            if avg_liquidity_old <= 0 or pd.isna(avg_liquidity_old):
                avg_liquidity_old = 1000.0 
                
            vol_today = last_month['Volume'].iloc[-1]
            vol_mean = last_month['Volume'].mean()
            if pd.notna(vol_today) and vol_mean > 0:
                rvol = vol_today / vol_mean
            rvol = max(0.1, min(rvol, 10.0))
        except:
            avg_liquidity_old = 1000.0
            rvol = 1.0
    else:
        avg_liquidity_old = 1000.0
        rvol = 1.0
        
    sector_assets[sec].append({
        'symbol': symbol,
        'score': score,
        'liquidity_old': avg_liquidity_old,
        'rvol': rvol,
        'growth': asset_growth  
    })

sector_final_scores = []
for sec, assets in sector_assets.items():
    total_sector_liquidity_old = sum(a['liquidity_old'] for a in assets)
    total_sector_rvol = sum(a['rvol'] for a in assets)
    total_sector_growth = sum(a['growth'] for a in assets)
    asset_count = len(assets)
    avg_sector_growth = (total_sector_growth / asset_count) if asset_count > 0 else 0.0
    
    weighted_score_sum = 0.0
    top_asset = max(assets, key=lambda x: x['rvol'])
    leader_name = top_asset['symbol']
    
    for asset in assets:
        weight = asset['rvol'] / total_sector_rvol if total_sector_rvol > 0 else (1.0 / asset_count)
        weighted_score_sum += (asset['score'] * weight)
        
    sector_final_scores.append({
        'sector': sec,
        'avg': weighted_score_sum,
        'count': asset_count,
        'leader': leader_name,
        'total_vol_old': total_sector_liquidity_old,
        'sector_rvol': round((total_sector_rvol / asset_count), 2),
        'avg_growth': round(avg_sector_growth, 2) 
    })

sorted_sectors = sorted(sector_final_scores, key=lambda x: x['avg'], reverse=True)

html_sector = [
    "<html><head><title>Classifica Settori</title>",
    "<style>",
    "table {border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;}",
    "th, td {border: 1px solid #ddd; padding: 12px; text-align: left;}",
    "th {background-color: #f2f2f2;}",
    ".bull {color: green; font-weight: bold;}",
    ".bear {color: red; font-weight: bold;}",
    ".neutral {color: #333;}",
    "</style>",
    "</head><body>",
    "<h1>📊 Performance Settoriale (RVOL Weighted)</h1>",
    "<p>Classifica ponderata sul <b>Volume Relativo (RVOL)</b>.</p>",
    "<table><tr><th>Pos</th><th>Settore</th><th>Dominant Asset</th><th>Score Ponderato</th><th>Asset</th><th>Trend</th><th>Volume Movimentato</th><th>RVOL (Nuovo)</th><th>Media Rendimento</th></tr>"
]

for idx, item in enumerate(sorted_sectors, 1):
    avg = item['avg']
    vol_int = int(item['total_vol_old'])
    rvol_val = item['sector_rvol']
    avg_growth_val = item['avg_growth'] 
    
    if avg >= 55:
        style_class = "bull"
        trend_label = "STRONG"
    elif avg >= 50:
        style_class = "bull"
        trend_label = "POSITIVE"
    elif avg <= 45:
        style_class = "bear"
        trend_label = "WEAK"
    elif avg <= 40:
        style_class = "bear"
        trend_label = "CRITICAL"
    else:
        style_class = "neutral"
        trend_label = "NEUTRAL"
        
    growth_class = "bull" if avg_growth_val > 0 else ("bear" if avg_growth_val < 0 else "neutral")
    
    html_sector.append(
        f"<tr>"
        f"<td>{idx}</td>"
        f"<td><b>{item['sector']}</b></td>"
        f"<td>{item['leader']}</td>"
        f"<td class='{style_class}'>{avg:.2f}%</td>"
        f"<td>{item['count']}</td>"
        f"<td class='{style_class}'>{trend_label}</td>"
        f"<td>{vol_int}</td>"
        f"<td><b>{rvol_val}x</b></td>"
        f"<td class='{growth_class}'>{avg_growth_val:+.2f}%</td>" 
        f"</tr>"
    )

html_sector.append("</table></body></html>")
r2_manager.write_file(sector_path, "\n".join(html_sector), is_json=False)

# --- NEWS HTML & ARCHIVE JSON ---
html_news = ["<html><head><title>Notizie e Sentiment</title></head><body>",
             "<h1>Notizie Finanziarie con Sentiment</h1>",
             "<table border='1'><tr><th>Simbolo</th><th>Notizia</th><th>Fonte</th><th>Immagine</th><th>Sentiment</th><th>Link</th><th>Data/Ora</th></tr>"]

news_by_symbol = defaultdict(list)
for symbol, title, sentiment, url, source, image, date in all_news_entries:
    news_by_symbol[symbol].append((title, sentiment, url, source, image, date))

for symbol, entries in news_by_symbol.items():
    unique_dict = {}
    for entry in entries:
        title = entry[0]
        if title not in unique_dict:
            unique_dict[title] = entry
    all_entries_unique = list(unique_dict.values())
    
    chronological_entries = sorted(all_entries_unique, key=lambda x: x[5] if hasattr(x[5], 'strftime') else datetime.min, reverse=True)
    capped_entries = chronological_entries[:80] 
    
    symbol_archive = []
    for title, sentiment, url, source, image, date in capped_entries:
        date_str = date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(date, 'strftime') else "N/A"
        short_url = clean_google_news_url(url)
        symbol_archive.append([title, round(sentiment, 3), short_url, source, date_str])
    
    file_path_json = f"{ARCHIVE_FOLDER}/{symbol.upper()}.json"
    r2_manager.write_file(file_path_json, symbol_archive, is_json=True)

    # L'archivio per-simbolo resta; il feed comune news.html no
    if is_excluded_from_rankings(symbol): continue

    sorted_by_sentiment = sorted(all_entries_unique, key=lambda x: x[1])
    if len(sorted_by_sentiment) > 10:
        app_entries = sorted_by_sentiment[:5] + sorted_by_sentiment[-5:]
        app_entries = list({v[0]:v for v in app_entries}.values()) 
    else:
        app_entries = sorted_by_sentiment
    
    for title, sentiment, url, source, image, date in app_entries:
        img_html = f"<img src='{image}' width='100'>" if image else "N/A"
        date_str = date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(date, 'strftime') else "N/A"
        html_news.append(f"<tr><td>{symbol}</td><td>{title}</td><td>{source}</td><td>{img_html}</td><td>{sentiment:.2f}</td><td><a href='{url}' target='_blank'>Leggi</a></td><td>{date_str}</td></tr>")

html_news.append("</table></body></html>")
r2_manager.write_file(news_path, "\n".join(html_news), is_json=False)

# --- CLASSIFICA FIRE ---
sorted_crescita = sorted([(s, g) for s, g in crescita_settimanale.items() if g is not None and not is_excluded_from_rankings(s)], key=lambda x: (x[1], x[0]), reverse=True)
html_fire = ["<html><head><title>Classifica per Crescita</title></head><body>",
             "<h1>Asset Ordinati per Crescita Settimanale</h1>",
             "<table border='1'><tr><th>Simbolo</th><th>Crescita 7gg (%)</th></tr>"]
for symbol, growth in sorted_crescita:
    html_fire.append(f"<tr><td>{symbol}</td><td>{growth:.2f}%</td></tr>")
html_fire.append("</table></body></html>")
r2_manager.write_file(fire_path, "\n".join(html_fire), is_json=False)


# ==============================================================================
# 7. DAILY BRIEF V2 (FULL DATABASE & DYNAMIC AI COPYWRITING)
# ==============================================================================
def calculate_support_resistance(df):
    if len(df) < 20: return 0.0, 0.0
    recent_low = df['Low'].tail(20).min()
    recent_high = df['High'].tail(20).max()
    if is_micro_price(recent_high):
        return float(recent_low), float(recent_high)
    return round(recent_low, 2), round(recent_high, 2)

all_analyzed_assets = []

for sym, score in percentuali_combine.items():
    if is_excluded_from_rankings(sym): continue
    if sym not in dati_storici_all: continue
    df = dati_storici_all[sym]
    if len(df) < 20: continue
    
    vol_today = df['Volume'].iloc[-1]
    
    if vol_today == 0 and len(df) >= 2:
        vol_today = df['Volume'].iloc[-2]
        vol_avg = df['Volume'].iloc[-21:-1].mean() if len(df) > 20 else df['Volume'].iloc[:-1].mean()
    else:
        vol_avg = df['Volume'].tail(20).mean()
        
    vol_surge = (vol_today / vol_avg) if vol_avg > 0 else 1.0
    
    rsi = indicator_data.get(sym, {}).get("RSI (14)", 50)
    pat_score, _ = PatternAnalyzer(df).get_pattern_info()
    sup, res = calculate_support_resistance(df)
    current_price = df['Close'].iloc[-1]
    
    dist_to_sup = abs(current_price - sup) / current_price if sup > 0 else 1.0
    dist_to_res = abs(current_price - res) / current_price if res > 0 else 1.0

    anomaly_score = 0
    dominant_trait = ""
    
    if vol_surge > 2.0: 
        anomaly_score += 4
        dominant_trait = "vol_breakout"
    elif vol_surge > 1.5: 
        anomaly_score += 2
        dominant_trait = "vol_breakout"
        
    if rsi > 75 or rsi < 25: 
        anomaly_score += 3
        if not dominant_trait: dominant_trait = "rsi_overbought" if rsi > 75 else "rsi_oversold"
    elif rsi > 70 or rsi < 30: 
        anomaly_score += 1
        
    if dist_to_res < 0.015:
        anomaly_score += 3
        if not dominant_trait: dominant_trait = "resistance_break"
    elif dist_to_sup < 0.015:
        anomaly_score += 3
        if not dominant_trait: dominant_trait = "support_test"
        
    if abs(pat_score) >= 0.4:
        anomaly_score += 2

    if anomaly_score == 0:
        anomaly_score = abs(score - 50) / 10.0

    macd_line = indicator_data.get(sym, {}).get("MACD Line", 0)
    macd_sig = indicator_data.get(sym, {}).get("MACD Signal", 0)
    macd_trend = "Bull" if macd_line > macd_sig else "Bear"
    confluence = f"RSI: {round(rsi)} | MACD: {macd_trend} | Vol: {round(vol_surge, 1)}x"
    volatility = "High" if vol_surge > 1.5 or rsi > 70 or rsi < 30 else "Normal"
    
    if not dominant_trait: 
        dominant_trait = "generic_bull" if score >= 50 else "generic_bear"
    elif dominant_trait == "vol_breakout":
        dominant_trait = "vol_breakout_bull" if score >= 50 else "vol_breakout_bear"

    all_analyzed_assets.append({
        'sym': sym, 'score': score, 'anomaly_score': anomaly_score, 
        'trait': dominant_trait, 'confluence': confluence, 
        'volatility': volatility, 'expected_move': "1-3 Days",
        'sup': sup, 'res': res
    })

all_analyzed_assets = sorted(all_analyzed_assets, key=lambda x: x['anomaly_score'], reverse=True)

bullish_count = sum(1 for score in percentuali_combine.values() if score >= 50)
total_assets_count = len(percentuali_combine)
breadth_ratio = bullish_count / total_assets_count if total_assets_count > 0 else 0.5

safe_haven_tickers = ["GC=F", "SI=F", "XAUUSD", "GLD", "SLV"]
safe_haven_surging = False
for ticker in safe_haven_tickers:
    if ticker in percentuali_combine and percentuali_combine[ticker] > 65:
        safe_haven_surging = True
        break

macro_theme = "consolidation" 
if breadth_ratio > 0.65:
    macro_theme = "strong_bull"
elif breadth_ratio < 0.35:
    if safe_haven_surging:
        macro_theme = "safe_haven"
    else:
        macro_theme = "strong_bear"

html_v2 = ["<html><body>"]

def get_randomized_lang_attributes(trait, dictionary_pool):
    variations_list = dictionary_pool.get(trait, dictionary_pool["generic_bull"])
    selected_lang_data = random.choice(variations_list)
    
    attrs = []
    for lang, text in selected_lang_data.items():
        safe_text = text.replace("'", "&apos;")
        attrs.append(f"data-{lang}='{safe_text}'")
    return " ".join(attrs)

selected_macro_data = random.choice(MACRO_SCENARIOS[macro_theme])
macro_attrs_list = []
for lang, text in selected_macro_data.items():
    safe_text = text.replace("'", "&apos;")
    macro_attrs_list.append(f"data-{lang}='{safe_text}'")

macro_attrs = " ".join(macro_attrs_list)
html_v2.append(f"<div id='macro_insight' {macro_attrs}></div>")

for cand in all_analyzed_assets:
    sym = cand['sym']
    name = symbol_name_map.get(sym, [sym])[0]
    lang_attrs = get_randomized_lang_attributes(cand['trait'], INSIGHT_DICT)
    html_v2.append(f"<div class='asset_data' data-ticker='${sym}' data-clean-ticker='{sym}' data-name='{name}' data-score='{int(cand['score'])}' data-anomaly='{cand['anomaly_score']}' {lang_attrs} data-confluence='{cand['confluence']}' data-volatility='{cand['volatility']}' data-move='{cand['expected_move']}' data-sup='{fmt_micro(cand['sup']) if is_micro_price(cand['sup']) else cand['sup']}' data-res='{fmt_micro(cand['res']) if is_micro_price(cand['res']) else cand['res']}'></div>")

html_v2.append("</body></html>")
v2_path = f"{TARGET_FOLDER}/daily_brief_v2_data.html"
r2_manager.write_file(v2_path, "\n".join(html_v2), is_json=False)

print("Daily Brief V2 (Dynamic Copy & Macro) salvato con successo su R2!")

# ==============================================================================
# 8. CORRELAZIONI STATISTICHE
# ==============================================================================
def calcola_correlazioni(dati_storici_all):
    returns = {sym: np.log(df["Close"]).diff().dropna() for sym, df in dati_storici_all.items() if "Close" in df.columns}
    # Data-quality gate: prices quantized by the feed (e.g. SHIB-USD ~0.000012 delivered with
    # 6 decimals) or stale/ffilled series give mostly-zero returns. Those zeros made the
    # direction-concordance term collapse to ~0% (mapped to -1), producing fake "inverse"
    # correlations against every asset. Drop series that are too flat to be meaningful.
    returns = {sym: r for sym, r in returns.items() if len(r) > 0 and (r == 0).mean() <= 0.20}
    results = {}
    assets = list(returns.keys())
    
    for asset1 in assets:
        all_candidates = []
        s1 = returns[asset1]
        
        for asset2 in assets:
            if asset1 == asset2: continue
            if is_excluded_from_rankings(asset2): continue

            s2 = returns[asset2]
            common = s1.index.intersection(s2.index)
            if len(common) < 60: continue
            
            x = s1.loc[common]
            y = s2.loc[common]
            
            try: p_r_std, _ = pearsonr(x, y)
            except: p_r_std = 0.0
            
            y_lagged = s2.shift(1).loc[common].dropna()
            x_aligned = x.loc[y_lagged.index]
            try:
                if len(x_aligned) > 30:
                    p_r_lag, _ = pearsonr(x_aligned, y_lagged)
                else:
                    p_r_lag = 0.0
            except: p_r_lag = 0.0
            
            if abs(p_r_lag) > abs(p_r_std):
                p_r = p_r_lag
                lag_usato = True
            else:
                p_r = p_r_std
                lag_usato = False

            try: s_r, _ = spearmanr(x, y)
            except: s_r = 0.0
            
            # Concordance only on days where both moved: a zero return (holiday ffill,
            # flat close) is "no information", not "opposite direction".
            moved = (x != 0) & (y != 0)
            if moved.sum() < 30: continue
            conc = (np.sign(x[moved]) == np.sign(y[moved])).mean() * 100
            conc_mapped = (conc / 50.0) - 1.0
            score = (p_r + s_r + conc_mapped) / 3.0
            
            giorni_di_crisi = x[x < -0.015].index
            if len(giorni_di_crisi) >= 5: 
                x_crisi = x.loc[giorni_di_crisi]
                y_crisi = y.loc[giorni_di_crisi]
                try: pearson_crisi, _ = pearsonr(x_crisi, y_crisi)
                except: pearson_crisi = None
            else:
                pearson_crisi = None 
                
            all_candidates.append({
                "asset2": asset2,
                "score": score,
                "pearson": p_r,
                "spearman": s_r,
                "concordance": conc,
                "lag_usato": lag_usato,
                "pearson_crisi": pearson_crisi
            })
            
        all_candidates.sort(key=lambda item: item["score"], reverse=True)
        top_direct = [c for c in all_candidates if c["score"] > 0][:10]
        top_inverse = sorted([c for c in all_candidates if c["score"] < 0], key=lambda item: item["score"])[:10]
        
        results[asset1] = {
            "dirette": top_direct,
            "inverse": top_inverse
        }
        
    return results

def salva_correlazioni_html(correlazioni, r2_mgr, file_path=corr_pro_path):
    html_corr = [
        "<html><head><title>Correlazioni PRO</title>",
        "<style>",
        "body {font-family: Arial, sans-serif; padding: 20px; color: #333;}",
        "table {border-collapse: collapse; width: 100%; margin-bottom: 40px; font-size: 14px;}",
        "th, td {border: 1px solid #ddd; padding: 10px; text-align: center;}",
        "th {background-color: #f8f9fa;}",
        ".dir {color: #198754; font-weight: bold;}",
        ".inv {color: #dc3545; font-weight: bold;}",
        ".alert {color: #dc3545;}",
        "h2 {margin-top: 50px; border-bottom: 2px solid #ccc; padding-bottom: 5px; color: #2c3e50;}",
        "h3 {font-size: 16px; margin-bottom: 10px; color: #555;}",
        "</style>",
        "</head><body>",
        "<h1>Analisi Correlazioni Statistiche Avanzate</h1>",
        "<p>Punteggio da -1.0 (Inversa) a +1.0 (Diretta). Include compensazione Fusi Orari (Lag) e Stress Test durante i crolli di mercato (Drop > 1.5%).</p>"
    ]
    
    for sym, data in correlazioni.items():
        html_corr.append(f"<h2>Asset: {sym}</h2>")
        
        html_corr.append("<h3>🔥 Top 10 Dirette (Si muovono all'unisono)</h3>")
        if data['dirette']:
            html_corr.append("<table><tr><th>Partner</th><th>Score Combinato</th><th>Pearson</th><th>Spearman</th><th>Concordanza Direz.</th><th>Stress Test (Crisi)</th><th>Lag Rilevato</th></tr>")
            for info in data['dirette']:
                lag_str = "⚠️ Sì (1g)" if info['lag_usato'] else "No"
                crisi_str = f"{info['pearson_crisi']:.2f}" if info['pearson_crisi'] is not None else "N/A"
                html_corr.append(f"<tr><td><b>{info['asset2']}</b></td><td class='dir'>+{info['score']:.2f}</td><td>{info['pearson']:.2f}</td><td>{info['spearman']:.2f}</td><td>{info['concordance']:.1f}%</td><td>{crisi_str}</td><td>{lag_str}</td></tr>")
            html_corr.append("</table>")
        else:
            html_corr.append("<p>Nessuna correlazione diretta rilevante.</p>")
            
        html_corr.append("<h3>🛡️ Top 10 Inverse (Potenziale Hedging / Copertura)</h3>")
        if data['inverse']:
            html_corr.append("<table><tr><th>Partner</th><th>Score Combinato</th><th>Pearson</th><th>Spearman</th><th>Concordanza Direz.</th><th>Stress Test (Crisi)</th><th>Lag Rilevato</th></tr>")
            for info in data['inverse']:
                lag_str = "⚠️ Sì (1g)" if info['lag_usato'] else "No"
                crisi_val = info['pearson_crisi']
                if crisi_val is None:
                    crisi_str = "N/A"
                elif crisi_val > 0.3:
                    crisi_str = f"<span class='alert'>{crisi_val:.2f} (Falso Sicuro)</span>"
                else:
                    crisi_str = f"{crisi_val:.2f}"
                    
                html_corr.append(f"<tr><td><b>{info['asset2']}</b></td><td class='inv'>{info['score']:.2f}</td><td>{info['pearson']:.2f}</td><td>{info['spearman']:.2f}</td><td>{info['concordance']:.1f}%</td><td>{crisi_str}</td><td>{lag_str}</td></tr>")
            html_corr.append("</table>")
        else:
            html_corr.append("<p>Nessuna correlazione inversa rilevante.</p>")
            
    html_corr.append("</body></html>")
    r2_mgr.write_file(file_path, "\n".join(html_corr), is_json=False)

print("Calcolo Correlazioni...")
correlazioni = calcola_correlazioni(dati_storici_all)
salva_correlazioni_html(correlazioni, r2_manager)

# --- NUOVO: SALVATAGGIO DATA AGGIORNAMENTO PER L'APP ANDROID ---
from datetime import datetime
print("Salvataggio timestamp aggiornamento...")
update_data = {
    # Formato pronto e già impaginato in modo leggibile (es. "Oct 15, 2026 - 14:30")
    "display_date": datetime.now().strftime("%b %d, %Y - %H:%M")
}
r2_manager.write_file(f"{TARGET_FOLDER}/last_update.json", update_data, is_json=True)



# ==============================================================================
# 9. GENERATORE PROGRAMMATIC SEO PROFESSIONALE (FIX BADGE, LEGAL COMPLIANCE & OHLC)
# ==============================================================================
def clean_slug(symbol):
    return re.sub(r'[^a-z0-9]+', '-', symbol.lower()).strip('-')

def format_compact_vol(val):
    try:
        v = float(val)
        if v >= 1_000_000_000: return f"{v/1e9:.1f}B"
        if v >= 1_000_000: return f"{v/1e6:.1f}M"
        if v >= 1_000: return f"{v/1e3:.1f}K"
        return f"{v:.0f}"
    except:
        return "0"

def format_smart_price(val):
    try:
        v = float(val)
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1: return f"{v:.2f}"
        if v >= 0.01: return f"{v:.4f}"
        if is_micro_price(v): return f"{v:.10f}"
        return f"{v:.6f}"
    except:
        return "0.00"

def format_smart_oscillator(val):
    try:
        v = float(val)
        if abs(v) >= 10: return f"{v:+.2f}"
        if abs(v) >= 0.1: return f"{v:+.3f}"
        if 0 < abs(v) < 0.00001: return f"{v:+.10f}"
        return f"{v:+.5f}"
    except:
        return "0.00"

def generate_investing_style_seo_pages(dati_storici_all, crescita_settimanale, news_by_symbol):
    print("Avvio generazione 260+ pagine SEO professionali e compliance...")
    seo_folder = "public_seo/assets"
    today_iso = datetime.utcnow().strftime("%Y-%m-%d")
    app_logo_url = "https://tradegpt-vector.com/tradeGPT_glass_logo.png"
    
    play_svg = '<span style="background:#ffffff; width:20px; height:20px; border-radius:4px; display:inline-flex; align-items:center; justify-content:center; flex-shrink:0; box-shadow:0 1px 3px rgba(0,0,0,0.2);"><img src="https://upload.wikimedia.org/wikipedia/commons/d/d0/Google_Play_Arrow_logo.svg" alt="Google Play" width="13" height="13" style="display:block;"></span>'
    
    prezzi_live_root = r2_manager.read_json("prezzi.json")
    prezzi_live_map = prezzi_live_root.get("data", {}) if isinstance(prezzi_live_root, dict) else {}

    all_valid_syms = [s for s in dati_storici_all.keys() if dati_storici_all[s] is not None and len(dati_storici_all[s]) >= 10]

    home_tickers = [
        "NVDA", "TSLA", "AAPL", "MSFT", "GOOGL", "PLTR",
        "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "BNBUSD", "DOGEUSD",
        "ISP.MI", "RACE.MI", "UCG.MI", "ENEL.MI", "LDO.MI", "ASML.AS",
        "EURUSD", "GBPUSD", "USDJPY", "GOLD", "SILVER", "OIL",
        "SPY", "QQQ", "VOO", "VT", "GLD", "TLT"
    ]
    home_quotes = {}
    for h_sym in home_tickers:
        df_h = dati_storici_all.get(h_sym)
        if df_h is None or len(df_h) < 2:
            continue
        try:
            close_s = df_h['Close'].iloc[:, 0] if isinstance(df_h['Close'], pd.DataFrame) else df_h['Close']
            cp = float(close_s.iloc[-1])
            pp = float(close_s.iloc[-2])
            if h_sym in prezzi_live_map and "price" in prezzi_live_map[h_sym]:
                cp = float(prezzi_live_map[h_sym]["price"])
                pp = float(prezzi_live_map[h_sym].get("prevClose", pp))
            chg = ((cp - pp) / pp * 100.0) if pp else 0.0
            home_quotes[h_sym] = {"p": format_smart_price(cp), "c": round(chg, 2)}
        except Exception:
            pass

    strip_tickers = [
        ("NVDA", "NVDA", "nvda"), ("BTCUSD", "BTC/USD", "btcusd"), ("TSLA", "TSLA", "tsla"),
        ("EURUSD", "EUR/USD", "eurusd"), ("GOLD", "GOLD", "gold"), ("ISP.MI", "ISP.MI", "isp-mi"),
        ("RACE.MI", "RACE.MI", "race-mi"), ("AAPL", "AAPL", "aapl"), ("SPY", "S&P 500 ETF", "spy")
    ]
    strip_items_html = []
    for s_key, s_label, s_slug in strip_tickers:
        q_info = home_quotes.get(s_key)
        if q_info:
            c_val = q_info["c"]
            c_cls = "tape-up" if c_val >= 0 else "tape-down"
            c_str = f"{c_val:+.2f}%"
            p_str = q_info["p"]
        else:
            c_cls = "tape-up"
            c_str = "Chart"
            p_str = ""
        strip_items_html.append(
            f'<a href="https://tradegpt-vector.com/assets/{s_slug}.html" class="strip-item">'
            f'<span class="strip-dot"></span><span class="strip-sym">{s_label}</span>'
            f'<span class="strip-price">{p_str}</span><span class="{c_cls}">{c_str}</span></a>'
        )
    top_strip_html = "\n            ".join(strip_items_html)

    search_index_list = []
    for s_sym in all_valid_syms:
        s_up = s_sym.upper()
        s_fn = symbol_name_map.get(s_sym, [s_sym])[0]
        search_index_list.append([s_up, s_fn])
    search_index_json = json.dumps(search_index_list)

    sitemap_urls = [
        f"""  <url>
    <loc>https://tradegpt-vector.com/assets/</loc>
    <lastmod>{today_iso}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.9</priority>
  </url>"""
    ]
    
    directory_by_sector = defaultdict(list)

    for sym in all_valid_syms:
        try:
            df = dati_storici_all[sym]
            slug = clean_slug(sym)
            upper_sym = sym.upper()
            full_name = symbol_name_map.get(sym, [sym])[0]
            sector_raw = asset_sector_map.get(sym, "Global Markets")
            sector_clean = re.sub(r'^\d+\.\s*', '', sector_raw)

            company_desc = f"{full_name} ({upper_sym}) is tracked by TradeGPT quantitative algorithms across {sector_clean}."
            try:
                gh_info_url = f"https://raw.githubusercontent.com/pammyhouse/dati-finanziari/main/{upper_sym}.html"
                r_info = requests.get(gh_info_url, timeout=3)
                if r_info.status_code == 200:
                    m_p = re.search(r'<p>(.*?)</p>', r_info.text, re.DOTALL | re.IGNORECASE)
                    if m_p:
                        clean_d = re.sub(r'<[^>]+>', '', m_p.group(1)).strip()
                        if len(clean_d) > 20:
                            company_desc = clean_d
            except:
                pass

            close_s = df['Close'].iloc[:, 0] if isinstance(df['Close'], pd.DataFrame) else df['Close']
            high_s = df['High'].iloc[:, 0] if isinstance(df['High'], pd.DataFrame) else df['High']
            low_s = df['Low'].iloc[:, 0] if isinstance(df['Low'], pd.DataFrame) else df['Low']
            open_s = df['Open'].iloc[:, 0] if isinstance(df['Open'], pd.DataFrame) else df['Open']
            vol_s = df['Volume'].iloc[:, 0] if isinstance(df['Volume'], pd.DataFrame) else df['Volume']

            curr_price = float(close_s.iloc[-1])
            prev_price = float(close_s.iloc[-2]) if len(close_s) >= 2 else curr_price
            open_price = float(open_s.iloc[-1]) if pd.notna(open_s.iloc[-1]) else prev_price

            live_obj = prezzi_live_map.get(sym, {})
            if live_obj and "price" in live_obj:
                curr_price = float(live_obj["price"])
                prev_price = float(live_obj.get("prevClose", prev_price))

            abs_change = curr_price - prev_price
            pct_change = ((abs_change / prev_price) * 100.0) if prev_price else 0.0
            week_change = crescita_settimanale.get(sym, 0.0)

            # CALCOLO MATEMATICO PURO (RSI, MACD, VOLATILITÀ)
            try:
                c_clean = close_s.dropna().astype(float)
                delta_c = c_clean.diff()
                up_c = delta_c.clip(lower=0)
                down_c = -1 * delta_c.clip(upper=0)
                ma_up = up_c.ewm(com=13, adjust=False).mean()
                ma_down = down_c.ewm(com=13, adjust=False).mean()
                rs_ratio = ma_up / ma_down
                rsi_series = 100.0 - (100.0 / (1.0 + rs_ratio))
                rsi_val = float(rsi_series.iloc[-1]) if pd.notna(rsi_series.iloc[-1]) else 50.0
                rsi_val = max(1.0, min(99.0, rsi_val))
            except Exception:
                rsi_val = 50.0

            rsi_bar_pos = int(max(4, min(96, round(rsi_val))))
            if rsi_val >= 70.0:
                rsi_zone_txt = "Overbought (>70)"
                rsi_zone_col = "#f59e0b"
            elif rsi_val <= 30.0:
                rsi_zone_txt = "Oversold (<30)"
                rsi_zone_col = "#38bdf8"
            else:
                rsi_zone_txt = "Neutral Zone (30-70)"
                rsi_zone_col = "#10b981"

            try:
                ema12 = c_clean.ewm(span=12, adjust=False).mean()
                ema26 = c_clean.ewm(span=26, adjust=False).mean()
                macd_line_s = ema12 - ema26
                macd_sig_s = macd_line_s.ewm(span=9, adjust=False).mean()
                macd_hist_s = macd_line_s - macd_sig_s
                macd_val = float(macd_line_s.iloc[-1]) if pd.notna(macd_line_s.iloc[-1]) else 0.0
                macd_sig = float(macd_sig_s.iloc[-1]) if pd.notna(macd_sig_s.iloc[-1]) else 0.0
                macd_hist = float(macd_hist_s.iloc[-1]) if pd.notna(macd_hist_s.iloc[-1]) else 0.0
            except Exception:
                macd_val, macd_sig, macd_hist = 0.0, 0.0, 0.0

            macd_hist_col = "#10b981" if macd_hist >= 0 else "#ef4444"

            try:
                ret_s = c_clean.pct_change().dropna().tail(30)
                hv30_val = float(ret_s.std() * (252 ** 0.5) * 100.0) if len(ret_s) >= 5 else 18.5
            except Exception:
                hv30_val = 18.5

            is_open = live_obj.get("isOpen", False)
            status_html = "Latest Active Session" if is_open else "Delayed Data / EOD"
            
            ext_html = ""
            if not is_open:
                if live_obj.get("postMarketPrice") and live_obj.get("postMarketChangePct") is not None:
                    ep = float(live_obj["postMarketPrice"])
                    ec = float(live_obj["postMarketChangePct"])
                    ecol = "#10b981" if ec >= 0 else "#ef4444"
                    ext_html = f"<div style='font-family:\"Space Mono\",monospace; font-size:0.84rem; margin-top:6px; color:#9ca3af;'>Post-market indication: <span style='color:{ecol};'>{format_smart_price(ep)} ({ec:+.2f}%)</span></div>"

            d_min = float(low_s.iloc[-1])
            d_max = float(high_s.iloc[-1])
            day_pct = int(max(5, min(95, ((curr_price - d_min) / (d_max - d_min) * 100)))) if d_max > d_min else 50

            h_min = float(low_s.tail(252).min())
            h_max = float(high_s.tail(252).max())
            yr_pct = int(max(5, min(95, ((curr_price - h_min) / (h_max - h_min) * 100)))) if h_max > h_min else 50

            valid_vols = sorted([float(v) for v in vol_s.tail(252).dropna() if v > 0])
            curr_vol = float(vol_s.iloc[-1]) if pd.notna(vol_s.iloc[-1]) else 0.0
            if valid_vols:
                v_min = valid_vols[int(len(valid_vols) * 0.05)]
                v_max = valid_vols[-1]
                vol_pct = int(max(5, min(95, ((curr_vol - v_min) / (v_max - v_min) * 100)))) if v_max > v_min else 50
            else:
                v_min, v_max, vol_pct = 0.0, 0.0, 0

            chart_records = []
            tail_df = df.tail(365)
            for idx, r in tail_df.iterrows():
                d_str = idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx)[:10]
                o_v = float(r['Open'].iloc[0]) if isinstance(r['Open'], pd.Series) else float(r['Open'])
                h_v = float(r['High'].iloc[0]) if isinstance(r['High'], pd.Series) else float(r['High'])
                l_v = float(r['Low'].iloc[0]) if isinstance(r['Low'], pd.Series) else float(r['Low'])
                c_v = float(r['Close'].iloc[0]) if isinstance(r['Close'], pd.Series) else float(r['Close'])
                nd = 12 if is_micro_price(c_v) else 4
                chart_records.append({"time": d_str, "open": round(o_v, nd), "high": round(h_v, nd), "low": round(l_v, nd), "close": round(c_v, nd)})
            chart_json_str = json.dumps(chart_records)

            play_link = f"https://play.google.com/store/apps/details?id=com.vectordev.einsight&referrer=utm_source%3Dwebsite%26utm_medium%3Dasset_page%26utm_campaign%3D{upper_sym}"

            raw_news = news_by_symbol.get(sym, [])[:4]
            news_cards_html = ""
            if raw_news:
                n_items = []
                for n_title, _, _, n_src, _, n_date in raw_news:
                    d_fmt = n_date.strftime("%b %d, %Y") if hasattr(n_date, 'strftime') else ""
                    n_items.append(f"""
                    <div style="padding:12px 0; border-bottom:1px solid #1a202e; display:flex; justify-content:space-between; align-items:center; gap:14px;">
                        <div>
                            <div style="font-size:0.88rem; color:#f3f4f6; font-weight:500;">{n_title}</div>
                            <div style="font-size:0.75rem; color:#6b7280; margin-top:3px;">{n_src} • {d_fmt}</div>
                        </div>
                        <a href="{play_link}" target="_blank" style="background:rgba(16,185,129,0.1); border:1px solid rgba(16,185,129,0.3); color:#10b981; padding:4px 10px; border-radius:4px; font-size:0.72rem; font-family:'Space Mono',monospace; text-decoration:none; white-space:nowrap;">
                            Sentiment in App
                        </a>
                    </div>""")
                news_cards_html = f"""
                <div class="panel-box">
                    <h2 class="panel-title"><span>Latest {upper_sym} Headlines</span><span style="font-size:0.75rem; color:#9ca3af; font-weight:400;">AI Sentiment Scored</span></h2>
                    {''.join(n_items)}
                </div>"""

            other_syms = [s for s in all_valid_syms if s != sym]
            similar_sample = random.sample(other_syms, min(5, len(other_syms)))
            similar_html_list = []
            for s_sym in similar_sample:
                s_slug = clean_slug(s_sym)
                s_name = symbol_name_map.get(s_sym, [s_sym])[0]
                s_logo = f"https://raw.githubusercontent.com/pammyhouse/dati-finanziari/main/loghi/{s_sym.upper()}_img.png"
                s_fmp = f"https://financialmodelingprep.com/image-stock/{s_sym.upper()}.png"
                similar_html_list.append(f"""
                <a href="https://tradegpt-vector.com/assets/{s_slug}.html" style="background:#181d29; border:1px solid #232a3b; border-radius:6px; padding:10px 12px; text-decoration:none; color:#fff; display:flex; align-items:center; gap:10px; transition:border-color 0.15s;">
                    <img src="{s_logo}" onerror="this.onerror=function(){{this.src='{app_logo_url}'}};this.src='{s_fmp}';" width="32" height="32" style="border-radius:50%; object-fit:cover;">
                    <div style="overflow:hidden;">
                        <div style="font-weight:700; font-size:0.85rem;">{s_sym}</div>
                        <div style="font-size:0.72rem; color:#9ca3af; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{s_name}</div>
                    </div>
                </a>""")

            chg_col = "#10b981" if pct_change >= 0 else "#ef4444"
            wk_col = "#10b981" if week_change >= 0 else "#ef4444"
            gh_logo = f"https://raw.githubusercontent.com/pammyhouse/dati-finanziari/main/loghi/{upper_sym}_img.png"
            fmp_logo = f"https://financialmodelingprep.com/image-stock/{upper_sym}.png"

            html_page = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{upper_sym} ({full_name}) Historical Chart, RSI, MACD & Quantitative Analysis | TradeGPT</title>
    <link rel="icon" type="image/png" href="{app_logo_url}">
    <meta name="description" content="{full_name} ({upper_sym}) delayed reference price ({format_smart_price(curr_price)}), historical chart, RSI(14) at {rsi_val:.1f}, MACD({format_smart_oscillator(macd_val)}), and quantitative AI analysis on TradeGPT.">
    <meta name="robots" content="index, follow">
    <link rel="canonical" href="https://tradegpt-vector.com/assets/{slug}.html">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
    <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        :root {{
            --bg-body: #0b0e14;
            --bg-surface: #12161f;
            --bg-elevated: #181d29;
            --bg-hover: #1e2534;
            --border-color: #232a3b;
            --border-subtle: #1a202e;
            --text-main: #f3f4f6;
            --text-secondary: #9ca3af;
            --text-muted: #6b7280;
            --accent-emerald: #10b981;
            --accent-orange: #f59e0b;
            --accent-blue: #3b82f6;
            --up-green: #10b981;
            --down-red: #ef4444;
        }}
        *, *::before, *::after {{ box-sizing: border-box; }}
        html, body {{
            margin: 0; padding: 0 0 76px 0; width: 100%; max-width: 100vw; overflow-x: hidden;
            background-color: var(--bg-body); color: var(--text-main);
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            -webkit-font-smoothing: antialiased; font-size: 14px;
        }}
        a {{ color: inherit; text-decoration: none; }}

        /* --- 1. TOP TICKER TAPE UNIFICATA --- */
        .top-ticker-strip {{
            background: #080a0f; border-bottom: 1px solid var(--border-color); height: 36px;
            display: flex; align-items: center; overflow-x: auto; white-space: nowrap; padding: 0 16px; scrollbar-width: none;
        }}
        .top-ticker-strip::-webkit-scrollbar {{ display: none; }}
        .strip-inner {{ max-width: 1320px; margin: 0 auto; display: flex; align-items: center; gap: 20px; width: 100%; }}
        .strip-item {{ display: inline-flex; align-items: center; gap: 6px; font-size: 12px; padding: 2px 6px; border-radius: 4px; transition: background 0.15s; }}
        .strip-item:hover {{ background: var(--bg-elevated); }}
        .strip-sym {{ font-weight: 700; color: #fff; }}
        .strip-price {{ font-family: 'Space Mono', monospace; color: var(--text-secondary); font-size: 11.5px; }}
        .tape-up {{ font-family: 'Space Mono', monospace; color: var(--up-green); font-weight: 700; font-size: 11.5px; }}
        .tape-down {{ font-family: 'Space Mono', monospace; color: var(--down-red); font-weight: 700; font-size: 11.5px; }}
        .strip-dot {{ width: 6px; height: 6px; border-radius: 50%; background: var(--up-green); display: inline-block; }}

        /* --- 2. HEADER PRINCIPALE A DOPPIA BARRA UNIFICATO (CON BADGE AI CENTRATO) --- */
        .inv-header {{ background: var(--bg-surface); border-bottom: 1px solid var(--border-color); position: sticky; top: 0; z-index: 1000; }}
        .inv-header-top {{ max-width: 1320px; margin: 0 auto; padding: 10px 20px; display: flex; align-items: center; justify-content: space-between; gap: 20px; }}
        .brand-box {{ display: flex; align-items: center; gap: 10px; flex-shrink: 0; }}
        .brand-box img {{ width: 34px; height: 34px; border-radius: 7px; object-fit: contain; }}
        .brand-title {{
            font-size: 19px; font-weight: 800; letter-spacing: -0.5px; color: #fff;
            display: inline-flex; align-items: center; gap: 6px; line-height: 1; white-space: nowrap;
        }}
        .brand-pro-tag {{
            background: var(--accent-emerald); color: #000; font-size: 10px; font-weight: 800;
            padding: 3px 5px; border-radius: 4px; text-transform: uppercase; letter-spacing: 0.3px; line-height: 1;
        }}

        .header-search-wrap {{ flex: 1; max-width: 520px; position: relative; }}
        .header-search-input {{
            width: 100%; height: 38px; background: var(--bg-body); border: 1px solid var(--border-color);
            border-radius: 6px; padding: 0 14px 0 38px; color: #fff; font-family: 'Inter', sans-serif; font-size: 13.5px; outline: none;
        }}
        .header-search-input:focus {{ border-color: var(--accent-emerald); }}
        .header-search-icon {{ position: absolute; left: 12px; top: 50%; transform: translateY(-50%); color: var(--text-muted); pointer-events: none; }}
        .search-results-dropdown {{
            position: absolute; top: calc(100% + 4px); left: 0; right: 0; background: var(--bg-surface);
            border: 1px solid var(--border-color); border-radius: 6px; max-height: 320px; overflow-y: auto; z-index: 9999; display: none; box-shadow: 0 12px 32px rgba(0,0,0,0.75);
        }}
        .search-res-item {{ display: flex; align-items: center; justify-content: space-between; padding: 10px 14px; border-bottom: 1px solid var(--border-subtle); font-size: 13px; }}
        .search-res-item:hover {{ background: var(--bg-hover); }}

        .btn-header-play {{
            display: inline-flex; align-items: center; gap: 8px; background: var(--accent-emerald);
            color: #000; font-weight: 700; font-size: 12.5px; padding: 7px 14px; border-radius: 6px; flex-shrink: 0;
        }}
        .btn-header-play:hover {{ background: #34d399; }}

        .inv-nav-bar {{ background: #0e121a; border-top: 1px solid var(--border-subtle); }}
        .inv-nav-inner {{
            max-width: 1320px; margin: 0 auto; padding: 0 20px; display: flex; align-items: center;
            justify-content: space-between; height: 40px; overflow-x: auto; white-space: nowrap; scrollbar-width: none;
        }}
        .inv-nav-inner::-webkit-scrollbar {{ display: none; }}
        .inv-nav-links {{ display: flex; align-items: center; gap: 24px; list-style: none; margin: 0; padding: 0; height: 100%; }}
        .inv-nav-links li {{ height: 100%; display: flex; align-items: center; }}
        .inv-nav-links a {{
            font-size: 13px; font-weight: 600; color: var(--text-secondary); height: 100%;
            display: flex; align-items: center; border-bottom: 2px solid transparent;
        }}
        .inv-nav-links a:hover, .inv-nav-links a.active {{ color: #fff; border-bottom-color: var(--accent-emerald); }}
        .quick-sublinks {{ display: flex; align-items: center; gap: 14px; font-size: 12px; }}
        .quick-sublinks a {{ color: var(--text-muted); padding: 2px 8px; border-radius: 4px; background: rgba(255,255,255,0.03); border: 1px solid var(--border-subtle); }}
        .quick-sublinks a:hover {{ color: #fff; border-color: var(--border-color); }}

        /* --- 3. STILI PAGINA TICKER ARRICCHITA --- */
        .container {{ max-width: 1060px; margin: 0 auto; padding: 24px 20px; }}
        .breadcrumb-bar {{ font-size: 12px; color: var(--text-muted); margin-bottom: 14px; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; }}
        .breadcrumb-bar a {{ color: var(--accent-emerald); font-weight: 600; }}
        .instrument-header {{ display: flex; justify-content: space-between; align-items: flex-start; padding-bottom: 18px; border-bottom: 1px solid var(--border-color); margin-bottom: 18px; flex-wrap: wrap; gap: 14px; }}
        .instrument-id {{ display: flex; align-items: center; gap: 14px; }}
        .instrument-logo {{ width: 48px; height: 48px; border-radius: 50%; object-fit: cover; background: var(--bg-elevated); border: 1px solid var(--border-color); }}
        .quote-price {{ font-family: 'Space Mono', monospace; font-size: 2.35rem; font-weight: 700; color: #fff; line-height: 1; }}
        .quote-stats {{ font-family: 'Space Mono', monospace; font-size: 0.88rem; margin-top: 8px; display: flex; flex-wrap: wrap; align-items: center; gap: 14px; }}
        
        .chart-panel {{ background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; padding: 16px; margin-bottom: 18px; }}
        .chart-toolbar {{ display: flex; justify-content: space-between; align-items: center; padding-bottom: 10px; margin-bottom: 10px; border-bottom: 1px solid var(--border-subtle); }}
        .chart-toggle-btn {{ background: var(--bg-elevated); border: 1px solid var(--border-color); color: var(--text-main); width: 34px; height: 34px; border-radius: 6px; display: flex; align-items: center; justify-content: center; cursor: pointer; }}
        #tvchart {{ width: 100%; height: 390px; }}

        /* Griglia 8 Statistiche Chiave (Sessione & Storico) */
        .key-stats-grid {{
            display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 20px;
        }}
        .stat-cell {{
            background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; padding: 11px 14px;
        }}
        .stat-lbl {{ font-size: 11.5px; color: var(--text-muted); text-transform: uppercase; font-weight: 600; margin-bottom: 4px; }}
        .stat-val {{ font-family: 'Space Mono', monospace; font-size: 14px; font-weight: 700; color: #fff; }}

        /* Doppia Card Indicatori Reali (RSI 14 & MACD) */
        .tech-snapshot-grid {{
            display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 20px;
        }}
        .tech-card {{
            background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; padding: 18px 20px; display: flex; flex-direction: column; justify-content: space-between;
        }}
        .tech-card-head {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }}
        .tech-card-title {{ font-size: 13.5px; font-weight: 700; color: #fff; text-transform: uppercase; letter-spacing: 0.3px; }}
        .tech-big-num {{ font-family: 'Space Mono', monospace; font-size: 1.75rem; font-weight: 700; color: #fff; line-height: 1.1; }}
        .rsi-gauge-track {{
            width: 100%; height: 7px; border-radius: 4px; margin: 14px 0 6px 0; position: relative;
            background: linear-gradient(90deg, #38bdf8 0%, #38bdf8 30%, #10b981 30%, #10b981 70%, #f59e0b 70%, #ef4444 100%);
        }}
        .rsi-gauge-thumb {{
            position: absolute; top: -4px; width: 14px; height: 14px; border-radius: 50%;
            background: #fff; border: 3px solid #0b0e14; transform: translateX(-50%); box-shadow: 0 0 6px rgba(255,255,255,0.8);
        }}
        .rsi-scale-labels {{ display: flex; justify-content: space-between; font-family: 'Space Mono', monospace; font-size: 10.5px; color: var(--text-muted); margin-bottom: 12px; }}
        .tech-lock-footer {{
            margin-top: auto; padding-top: 12px; border-top: 1px solid var(--border-subtle);
            display: flex; align-items: center; justify-content: space-between; font-size: 12px; color: var(--text-secondary);
        }}
        .lock-pill-cta {{
            display: inline-flex; align-items: center; gap: 5px; background: rgba(245,158,11,0.12);
            border: 1px solid rgba(245,158,11,0.35); color: var(--accent-orange); padding: 4px 10px;
            border-radius: 4px; font-family: 'Space Mono', monospace; font-size: 11px; font-weight: 700;
        }}
        .lock-pill-cta:hover {{ background: rgba(245,158,11,0.22); }}

        .tools-bar {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px; }}
        .tool-btn {{ background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; padding: 12px 14px; color: var(--text-main); font-size: 0.85rem; font-weight: 600; display: flex; align-items: center; justify-content: space-between; transition: border-color 0.15s; }}
        .tool-btn:hover {{ border-color: var(--accent-emerald); }}
        .lock-tag {{ font-size: 0.68rem; color: var(--accent-emerald); background: rgba(16,185,129,0.1); border: 1px solid rgba(16,185,129,0.25); padding: 2px 6px; border-radius: 4px; font-family: 'Space Mono', monospace; }}

        .panel-box {{ background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; padding: 20px; margin-bottom: 20px; }}
        .panel-title {{ font-size: 0.98rem; font-weight: 700; color: #fff; margin: 0 0 16px 0; padding-bottom: 10px; border-bottom: 1px solid var(--border-subtle); display: flex; justify-content: space-between; align-items: center; }}
        .range-item {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; font-family: 'Space Mono', monospace; font-size: 0.82rem; }}
        .range-item:last-child {{ margin-bottom: 0; }}
        .range-lbl {{ font-family: 'Inter', sans-serif; color: var(--text-secondary); width: 145px; font-size: 0.84rem; }}
        .range-track {{ flex: 1; margin: 0 16px; height: 5px; background: var(--border-color); border-radius: 3px; overflow: hidden; }}
        .range-bar-fill {{ height: 100%; background: var(--accent-blue); border-radius: 3px; }}

        .paywall-panel {{ position: relative; background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; overflow: hidden; margin-bottom: 20px; }}
        .paywall-table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
        .paywall-table th, .paywall-table td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--border-subtle); }}
        .paywall-table th {{ color: var(--text-muted); font-weight: 600; font-size: 11.5px; text-transform: uppercase; }}
        .blurred-val {{ filter: blur(6px); user-select: none; font-family: 'Space Mono', monospace; color: var(--text-main); }}
        .paywall-cta-banner {{ background: linear-gradient(180deg, rgba(18,22,31,0.35) 0%, rgba(18,22,31,0.96) 35%, #12161f 100%); padding: 38px 20px 24px 20px; text-align: center; margin-top: -92px; position: relative; z-index: 5; }}
        .btn-google-play {{ display: inline-flex; align-items: center; gap: 10px; background: var(--accent-emerald); color: #000; font-weight: 700; font-size: 0.92rem; padding: 10px 22px 10px 12px; border-radius: 6px; margin-top: 12px; }}

        /* Disclaimer Legale Rigoroso & Accettazione Implicita */
        .legal-notice-box {{
            background: rgba(239, 68, 68, 0.05); border: 1px solid rgba(239, 68, 68, 0.25);
            border-radius: 6px; padding: 18px 20px; margin-top: 28px; color: var(--text-secondary); font-size: 12px; line-height: 1.65;
        }}
        .legal-notice-box strong {{ color: #fca5a5; }}
        .legal-notice-box a {{ color: var(--accent-emerald); text-decoration: underline; }}

        .inv-footer {{ background: #080a0f; border-top: 1px solid var(--border-color); padding: 32px 20px; margin-top: 36px; color: var(--text-muted); font-size: 12px; line-height: 1.6; }}
        .inv-footer-inner {{ max-width: 1060px; margin: 0 auto; }}

        .sticky-app-footer {{ position: fixed; bottom: 0; left: 0; right: 0; background: #12161f; border-top: 1px solid var(--border-color); padding: 10px 20px; z-index: 9990; }}
        .sticky-inner {{ max-width: 1060px; margin: 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 12px; }}

        /* Menu Mobile Drawer */
        .mobile-menu-btn {{ display: none; background: transparent; border: 1px solid var(--border-color); color: #fff; width: 36px; height: 36px; border-radius: 6px; align-items: center; justify-content: center; cursor: pointer; }}
        .mobile-drawer-backdrop {{ display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.75); z-index: 9998; }}
        .mobile-drawer-backdrop.open {{ display: block; }}
        .mobile-drawer {{ position: fixed; top: 0; left: 0; bottom: 0; width: 270px; background: var(--bg-surface); border-right: 1px solid var(--border-color); z-index: 9999; transform: translateX(-105%); transition: transform 0.25s ease; padding: 20px; display: flex; flex-direction: column; gap: 16px; }}
        .mobile-drawer.open {{ transform: translateX(0); }}
        .mobile-drawer-links {{ list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 6px; }}
        .mobile-drawer-links a {{ display: block; padding: 11px 12px; border-radius: 6px; font-weight: 600; color: var(--text-main); }}
        .mobile-drawer-links a:hover, .mobile-drawer-links a.active {{ background: var(--bg-hover); color: var(--accent-emerald); }}

        @media (max-width: 1080px) {{ .quick-sublinks {{ display: none; }} }}
        @media (max-width: 768px) {{
            .mobile-menu-btn {{ display: inline-flex; }}
            .inv-header-top {{ flex-wrap: wrap; padding: 10px 14px; gap: 10px; }}
            .header-search-wrap {{ order: 3; max-width: 100%; width: 100%; flex: 0 0 100%; }}
            .inv-nav-bar {{ display: none; }}
            .key-stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .tech-snapshot-grid {{ grid-template-columns: 1fr; }}
            .tools-bar {{ grid-template-columns: 1fr; }}
            .quote-price {{ font-size: 1.9rem; }}
            #tvchart {{ height: 300px; }}
            .container {{ padding: 16px 12px; }}
        }}
    </style>
</head>
<body>

    <!-- 1. TOP LIVE TICKER STRIP -->
    <div class="top-ticker-strip">
        <div class="strip-inner">
            {top_strip_html}
        </div>
    </div>

    <!-- DRAWER MOBILE UNIFICATO -->
    <div class="mobile-drawer-backdrop" id="sidebarBackdrop" onclick="closeMobileSidebar()"></div>
    <aside class="mobile-drawer" id="sidebar">
        <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid var(--border-color); padding-bottom:12px;">
            <div class="brand-box">
                <img src="{app_logo_url}" alt="TradeGPT" width="30" height="30">
                <span class="brand-title">TradeGPT <span class="brand-pro-tag">AI</span></span>
            </div>
            <button onclick="closeMobileSidebar()" style="background:none; border:none; color:#9ca3af; font-size:20px; cursor:pointer;">✕</button>
        </div>
        <ul class="mobile-drawer-links">
            <li><a href="https://tradegpt-vector.com/index.html">Markets Home</a></li>
            <li><a href="https://tradegpt-vector.com/assets/" class="active">All 260+ Assets Directory</a></li>
            <li><a href="https://tradegpt-vector.com/index.html#hot-assets-section">Smart Rankings</a></li>
            <li><a href="https://tradegpt-vector.com/index.html#features-section">Features & Tiers</a></li>
            <li><a href="https://tradegpt-vector.com/products.html">API & Pricing</a></li>
            <li><a href="https://tradegpt-vector.com/docs.html">Developer Docs</a></li>
            <li><a href="https://tradegpt-vector.com/about.html">About Us</a></li>
        </ul>
        <div style="margin-top:auto; padding-top:14px; border-top:1px solid var(--border-color);">
            <a href="{play_link}" target="_blank" rel="noopener" class="btn-header-play" style="width:100%; justify-content:center; padding:10px;">
                {play_svg} <span>Get App (Free)</span>
            </a>
        </div>
    </aside>

    <!-- 2. HEADER ORIZZONTALE UNIFICATO -->
    <header class="inv-header">
        <div class="inv-header-top">
            <div style="display:flex; align-items:center; gap:12px;">
                <button class="mobile-menu-btn" onclick="toggleMobileSidebar(event)" aria-label="Menu">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
                </button>
                <a href="https://tradegpt-vector.com/" class="brand-box">
                    <img src="{app_logo_url}" onerror="this.onerror=null;this.src='https://tradegpt-vector.com/favicon.svg';" alt="TradeGPT">
                    <span class="brand-title">TradeGPT <span class="brand-pro-tag">AI</span></span>
                </a>
            </div>

            <div class="header-search-wrap">
                <svg class="header-search-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                <input type="text" id="assetSearchInput" class="header-search-input" placeholder="Search 260+ stocks, crypto, forex, ETFs (e.g., NVDA, Ferrari, Bitcoin, Gold...)" autocomplete="off">
                <div class="search-results-dropdown" id="searchDropdown"></div>
            </div>

            <div style="display:flex; align-items:center; gap:12px;">
                <a href="{play_link}" target="_blank" rel="noopener" class="btn-header-play">
                    {play_svg} <span>Get App (Free)</span>
                </a>
            </div>
        </div>

        <div class="inv-nav-bar">
            <div class="inv-nav-inner">
                <ul class="inv-nav-links">
                    <li><a href="https://tradegpt-vector.com/index.html">Markets Home</a></li>
                    <li><a href="https://tradegpt-vector.com/assets/" class="active">All 260+ Assets Directory</a></li>
                    <li><a href="https://tradegpt-vector.com/index.html#hot-assets-section">Smart Rankings</a></li>
                    <li><a href="https://tradegpt-vector.com/index.html#features-section">Features & Tiers</a></li>
                    <li><a href="https://tradegpt-vector.com/products.html">API & Pricing</a></li>
                    <li><a href="https://tradegpt-vector.com/docs.html">Developer Docs</a></li>
                    <li><a href="https://tradegpt-vector.com/about.html">About Us</a></li>
                </ul>
                <div class="quick-sublinks">
                    <a href="https://tradegpt-vector.com/assets/nvda.html">NVDA</a>
                    <a href="https://tradegpt-vector.com/assets/btcusd.html">Bitcoin</a>
                    <a href="https://tradegpt-vector.com/assets/isp-mi.html">FTSE MIB</a>
                    <a href="https://tradegpt-vector.com/assets/gold.html">Gold</a>
                    <a href="https://tradegpt-vector.com/assets/eurusd.html">EUR/USD</a>
                </div>
            </div>
        </div>
    </header>

    <!-- 3. CONTENUTO PRINCIPALE ASSET -->
    <div class="container">
        <div class="breadcrumb-bar">
            <div>
                <a href="https://tradegpt-vector.com/">Markets Home</a> / <a href="https://tradegpt-vector.com/assets/">Assets Directory</a> / <span>{upper_sym} ({full_name})</span>
            </div>
            <span>Sector: <b style="color:#fff;">{sector_clean}</b></span>
        </div>

        <div class="instrument-header">
            <div class="instrument-id">
                <img src="{gh_logo}" onerror="this.onerror=function(){{this.src='{app_logo_url}'}};this.src='{fmp_logo}';" alt="{upper_sym}" class="instrument-logo">
                <div>
                    <h1 style="font-size:1.45rem; font-weight:800; margin:0; color:#fff; letter-spacing:-0.3px;">{full_name} ({upper_sym})</h1>
                    <p style="font-size:0.82rem; color:var(--text-secondary); margin:4px 0 0 0;">{sector_clean} • Quantitative Market Data & AI Analytics</p>
                </div>
            </div>
            <div style="display:flex; gap:8px; align-items:center;">
                <span style="background:var(--bg-surface); border:1px solid var(--border-color); padding:5px 10px; border-radius:4px; font-family:'Space Mono',monospace; font-size:12px;">
                    RSI(14): <b style="color:{rsi_zone_col};">{rsi_val:.1f}</b>
                </span>
                <a href="{play_link}" target="_blank" class="lock-pill-cta">🔒 AI Forecast in App</a>
            </div>
        </div>

        <div style="margin-bottom:20px;">
            <div class="quote-price">{format_smart_price(curr_price)}</div>
            <div class="quote-stats">
                <span style="color:{chg_col}; font-weight:700;">{abs_change:+.2f} ({pct_change:+.2f}%) 24h</span>
                <span style="color:var(--border-color);">|</span>
                <span style="color:{wk_col}; font-weight:700;">{week_change:+.2f}% 7D</span>
                <span style="color:var(--border-color);">|</span>
                <span style="color:var(--text-muted); font-size:0.8rem;">{status_html}</span>
            </div>
            {ext_html}
        </div>

        <div class="chart-panel">
            <div class="chart-toolbar">
                <span style="font-size:0.8rem; font-weight:700; color:var(--text-secondary); text-transform:uppercase;">{upper_sym} Interactive Historical Chart (1Y Daily)</span>
                <button class="chart-toggle-btn" onclick="toggleChartType()" title="Toggle Candlestick / Line Chart" aria-label="Toggle Chart Type">
                    <svg id="iconLine" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"></polyline><polyline points="16 7 22 7 22 13"></polyline></svg>
                    <svg id="iconCandle" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:none;"><line x1="7" y1="3" x2="7" y2="21"></line><rect x="4" y="7" width="6" height="9" rx="1" fill="currentColor"></rect><line x1="17" y1="4" x2="17" y2="20"></line><rect x="14" y="9" width="6" height="7" rx="1"></rect></svg>
                </button>
            </div>
            <div id="tvchart"></div>
            <div style="font-size:0.72rem; color:var(--text-muted); margin-top:10px; display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px;">
                <span>Historical charts are for educational visualization only.</span>
                <span>Drag or pinch to inspect historical candles</span>
            </div>
        </div>

        <!-- GRIGLIA 8 STATISTICHE CHIAVE DI SESSIONE (AGGIORNATI GLI ID PER IL CROSSHAIR) -->
        <div class="key-stats-grid">
            <div class="stat-cell">
                <div class="stat-lbl">Open</div>
                <div class="stat-val" id="dyn-open">{format_smart_price(open_price)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl" id="dyn-close-lbl">Prev. Close</div>
                <div class="stat-val" id="dyn-close">{format_smart_price(prev_price)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl">Day High</div>
                <div class="stat-val" id="dyn-high">{format_smart_price(d_max)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl">Day Low</div>
                <div class="stat-val" id="dyn-low">{format_smart_price(d_min)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl">52-Week High</div>
                <div class="stat-val">{format_smart_price(h_max)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl">52-Week Low</div>
                <div class="stat-val">{format_smart_price(h_min)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl">Session Volume</div>
                <div class="stat-val">{format_compact_vol(curr_vol)}</div>
            </div>
            <div class="stat-cell">
                <div class="stat-lbl">30D Volatility (HV)</div>
                <div class="stat-val">{hv30_val:.1f}%</div>
            </div>
        </div>

        <!-- BARRA INCENTIVO AI (AGGIUNTA) -->
        <a href="{play_link}" target="_blank" rel="noopener" style="display:flex; align-items:center; justify-content:space-between; background: linear-gradient(90deg, rgba(16,185,129,0.15) 0%, rgba(24,29,41,0.5) 100%); border: 1px solid rgba(16,185,129,0.3); border-radius: 6px; padding: 14px 18px; margin-bottom: 20px; text-decoration: none; transition: all 0.2s;">
            <div style="display:flex; align-items:center; gap: 14px;">
                <div style="background:rgba(16,185,129,0.2); border-radius:50%; width:36px; height:36px; display:flex; align-items:center; justify-content:center; font-size:1.1rem; flex-shrink:0;">✨</div>
                <div>
                    <div style="color:#fff; font-weight:700; font-size:0.95rem; margin-bottom:2px;">Ask AI about {upper_sym}</div>
                    <div style="color:var(--text-secondary); font-size:0.8rem;">Unlock instant AI forecasts, entry signals, and advanced analytics.</div>
                </div>
            </div>
            <div style="background:var(--accent-emerald); color:#000; font-weight:700; font-size:0.75rem; font-family:'Space Mono', monospace; padding:8px 14px; border-radius:4px; display:flex; align-items:center; gap:6px; flex-shrink:0; text-transform:uppercase;">
                {play_svg} Get App
            </div>
        </a>

        <!-- CARD GEMELLE CON IL PURO VALORE DI RSI (14) E MACD (12,26,9) + ANALISI IA BLOCCATA -->
        <div class="tech-snapshot-grid">
            <!-- Card 1: RSI (14) -->
            <div class="tech-card">
                <div>
                    <div class="tech-card-head">
                        <span class="tech-card-title">{upper_sym} Relative Strength Index (RSI 14)</span>
                        <span style="font-size:11.5px; font-weight:700; color:{rsi_zone_col};">{rsi_zone_txt}</span>
                    </div>
                    <div class="tech-big-num">{rsi_val:.2f}</div>
                    <div class="rsi-gauge-track">
                        <div class="rsi-gauge-thumb" style="left:{rsi_bar_pos}%;"></div>
                    </div>
                    <div class="rsi-scale-labels">
                        <span>0 (Oversold &lt;30)</span>
                        <span>50 (Neutral)</span>
                        <span>100 (Overbought &gt;70)</span>
                    </div>
                </div>
                <div class="tech-lock-footer">
                    <span>AI Divergence & Entry Signal:</span>
                    <a href="{play_link}" target="_blank" class="lock-pill-cta">🔒 Unlock AI Analysis in App</a>
                </div>
            </div>

            <!-- Card 2: MACD (12, 26, 9) -->
            <div class="tech-card">
                <div>
                    <div class="tech-card-head">
                        <span class="tech-card-title">{upper_sym} MACD (12, 26, 9) Level</span>
                        <span style="font-size:11.5px; font-weight:600; color:var(--text-secondary);">Daily Oscillator</span>
                    </div>
                    <div class="tech-big-num">{format_smart_oscillator(macd_val)}</div>
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; margin:14px 0 12px 0; background:var(--bg-body); border:1px solid var(--border-subtle); border-radius:6px; padding:10px 12px;">
                        <div>
                            <div style="font-size:11px; color:var(--text-muted);">Signal Line (9)</div>
                            <div style="font-family:'Space Mono',monospace; font-weight:700; font-size:13px; color:#fff; margin-top:2px;">{format_smart_oscillator(macd_sig)}</div>
                        </div>
                        <div>
                            <div style="font-size:11px; color:var(--text-muted);">MACD Histogram</div>
                            <div style="font-family:'Space Mono',monospace; font-weight:700; font-size:13px; color:{macd_hist_col}; margin-top:2px;">{format_smart_oscillator(macd_hist)}</div>
                        </div>
                    </div>
                </div>
                <div class="tech-lock-footer">
                    <span>AI Momentum & Crossover Verdict:</span>
                    <a href="{play_link}" target="_blank" class="lock-pill-cta">🔒 Unlock AI Analysis in App</a>
                </div>
            </div>
        </div>

        <div class="tools-bar">
            <a href="{play_link}" target="_blank" class="tool-btn">
                <span style="display:flex; align-items:center; gap:8px;">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon></svg>
                    Volume & Price Highlights
                </span>
                <span class="lock-tag">APP</span>
            </a>
            <a href="{play_link}" target="_blank" class="tool-btn">
                <span style="display:flex; align-items:center; gap:8px;">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#f59e0b" stroke-width="2"><polygon points="12 2 2 7 12 12 22 7 12 2"></polygon><polyline points="2 17 12 22 22 17"></polyline><polyline points="2 12 12 17 22 12"></polyline></svg>
                    Fibonacci Pivot Points
                </span>
                <span class="lock-tag">APP</span>
            </a>
            <a href="{play_link}" target="_blank" class="tool-btn">
                <span style="display:flex; align-items:center; gap:8px;">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#a855f7" stroke-width="2"><circle cx="18" cy="5" r="3"></circle><circle cx="6" cy="12" r="3"></circle><circle cx="18" cy="19" r="3"></circle><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"></line><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"></line></svg>
                    Chart Pattern Detector
                </span>
                <span class="lock-tag">APP</span>
            </a>
        </div>

        <div class="panel-box">
            <h2 class="panel-title">Market Ranges & Volume</h2>
            <div class="range-item">
                <span class="range-lbl">Volume (Vs History)</span>
                <span>{format_compact_vol(v_min)}</span>
                <div class="range-track"><div class="range-bar-fill" style="width:{vol_pct}%;"></div></div>
                <span>{format_compact_vol(v_max)}</span>
            </div>
            <div class="range-item">
                <span class="range-lbl">Daily Range</span>
                <span>{format_smart_price(d_min)}</span>
                <div class="range-track"><div class="range-bar-fill" style="width:{day_pct}%;"></div></div>
                <span>{format_smart_price(d_max)}</span>
            </div>
            <div class="range-item">
                <span class="range-lbl">52-Week Range</span>
                <span>{format_smart_price(h_min)}</span>
                <div class="range-track"><div class="range-bar-fill" style="width:{yr_pct}%;"></div></div>
                <span>{format_smart_price(h_max)}</span>
            </div>
        </div>

        <!-- TABELLA RIEPILOGO: RSI E MACD VISIBILI SOLO COME NUMERO, INTERPRETAZIONE E PROIEZIONI IA SFOCATE -->
        <div class="paywall-panel">
            <div style="padding:16px 20px; border-bottom:1px solid var(--border-subtle); display:flex; justify-content:space-between; align-items:center;">
                <span style="font-weight:700; font-size:0.95rem; color:#fff;">Quantitative Technical Ratings & AI Signals ({upper_sym})</span>
                <span style="font-size:0.75rem; color:var(--accent-emerald); font-family:'Space Mono',monospace; font-weight:700;">PREMIUM / ULTRA IN APP</span>
            </div>
            <table class="paywall-table">
                <thead>
                    <tr><th>Metric / Indicator</th><th>Current Reading</th><th>AI Action Verdict</th></tr>
                </thead>
                <tbody>
                    <tr>
                        <td><b>RSI (14)</b> Relative Strength</td>
                        <td style="font-family:'Space Mono',monospace; font-weight:700; color:#fff;">{rsi_val:.2f}</td>
                        <td><a href="{play_link}" target="_blank" class="lock-pill-cta">🔒 Unlock AI Verdict</a></td>
                    </tr>
                    <tr>
                        <td><b>MACD (12, 26, 9)</b> Level / Signal</td>
                        <td style="font-family:'Space Mono',monospace; font-weight:700; color:#fff;">{format_smart_oscillator(macd_val)} / {format_smart_oscillator(macd_sig)}</td>
                        <td><a href="{play_link}" target="_blank" class="lock-pill-cta">🔒 Unlock AI Verdict</a></td>
                    </tr>
                    <tr>
                        <td>Bollinger Bands (20, 2) & EMA (50/200)</td>
                        <td class="blurred-val">{format_smart_price(curr_price * 0.96)} - {format_smart_price(curr_price * 1.04)}</td>
                        <td class="blurred-val" style="color:var(--accent-emerald);">BULLISH TREND</td>
                    </tr>
                    <tr>
                        <td>AI News Sentiment & Momentum Score</td>
                        <td class="blurred-val">82.5 / 100</td>
                        <td class="blurred-val">High Accumulation</td>
                    </tr>
                    <tr>
                        <td>AI Hybrid Probability & 30D Target Range</td>
                        <td class="blurred-val">{format_smart_price(curr_price * 0.94)} - {format_smart_price(curr_price * 1.12)}</td>
                        <td class="blurred-val" style="color:var(--accent-emerald);">STRONG BUY</td>
                    </tr>
                </tbody>
            </table>
            <div class="paywall-cta-banner">
                <h3 style="margin:0 0 6px 0; font-size:1.15rem; color:#fff;">Unlock Full AI Analysis & Statistical Projections for {upper_sym}</h3>
                <p style="margin:0 auto; color:var(--text-secondary); max-width:540px; font-size:0.86rem; line-height:1.5;">
                    You are viewing raw mathematical RSI ({rsi_val:.1f}) and MACD ({format_smart_oscillator(macd_val)}) computations. To unlock AI interpretation, Bullish/Bearish ratings, Bollinger Bands, Executive Insider Trades, and 30-Day Statistical Trend Charts for <b>{full_name}</b>, download the <b>TradeGPT</b> Android app.
                </p>
                <a href="{play_link}" target="_blank" rel="noopener" class="btn-google-play">
                    {play_svg} Download on Google Play
                </a>
            </div>
        </div>

        <div class="panel-box" style="cursor:pointer;" onclick="toggleProfile()">
            <h2 class="panel-title">About {full_name} ({upper_sym})</h2>
            <p id="profileText" style="margin:0; font-size:0.88rem; color:var(--text-secondary); line-height:1.6; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden;">
                {company_desc}
            </p>
            <span id="profileMore" style="color:var(--accent-emerald); font-size:0.82rem; font-weight:600; display:inline-block; margin-top:8px;">Read more</span>
        </div>

        {news_cards_html}

        <div class="panel-box">
            <h2 class="panel-title">Related Instruments</h2>
            <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)); gap:10px;">
                {''.join(similar_html_list)}
            </div>
        </div>

        <!-- BOX LEGALE OBBLIGATORIO DI ACCETTAZIONE IMPLICITA DEI TERMINI E POLICY -->
        <div class="legal-notice-box">
            <strong>⚖️ Legal Notice, Risk Disclosure & Implicit Acceptance of Terms:</strong><br>
            By accessing, viewing, or utilizing any data, charts, quotes, or indicators displayed on this page, you expressly acknowledge and agree that your use is subject to the strict and unconditional acceptance of the TradeGPT & VectorDEV <a href="https://tradegpt-vector.com/privacy.html">Privacy Policy</a> and <a href="https://tradegpt-vector.com/docs.html">Terms of Use & API/Data Policies</a>. All market prices, candlestick charts, RSI, MACD, and AI-generated statistical metrics for <b>{full_name} ({upper_sym})</b> are provided strictly for informational and educational purposes only. They do NOT constitute financial advice, investment recommendations, or an offer to buy or sell any financial instrument. Any prices shown are delayed or end-of-day reference values and should not be used for trade execution. Trading in stocks, cryptocurrencies, forex, and commodities involves a high degree of risk. Automated scraping, unauthorized redistribution, or commercial use of this data is strictly prohibited.
        </div>
    </div>

    <!-- 4. FOOTER ISTITUZIONALE UNIFICATO -->
    <footer class="inv-footer">
        <div class="inv-footer-inner">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:16px; padding-bottom:20px; border-bottom:1px solid var(--border-color); margin-bottom:20px;">
                <div class="brand-box">
                    <img src="{app_logo_url}" alt="TradeGPT" width="28" height="28">
                    <span style="font-weight:700; color:#fff; font-size:15px;">TradeGPT by VectorDEV</span>
                </div>
                <div style="display:flex; gap:20px; flex-wrap:wrap; font-weight:600; color:var(--text-secondary);">
                    <a href="https://tradegpt-vector.com/index.html">Markets Home</a>
                    <a href="https://tradegpt-vector.com/assets/">All 260+ Assets Directory</a>
                    <a href="https://tradegpt-vector.com/products.html">API & Pricing</a>
                    <a href="https://tradegpt-vector.com/docs.html">API Documentation</a>
                    <a href="https://tradegpt-vector.com/about.html">About Us</a>
                    <a href="https://tradegpt-vector.com/privacy.html">Privacy Policy</a>
                </div>
            </div>
            <div style="color:var(--text-secondary);">
                © 2026 <b>TradeGPT</b> by VectorDEV. All Rights Reserved. Use of this website constitutes acceptance of our Terms of Use and Privacy Policy.
            </div>
        </div>
    </footer>

    <div class="sticky-app-footer">
        <div class="sticky-inner">
            <div>
                <div style="font-weight:700; font-size:0.9rem; color:#fff;">TradeGPT Financial Analysis</div>
                <div style="font-size:0.76rem; color:var(--text-secondary);">Unlock AI Signal for {upper_sym} (RSI: {rsi_val:.1f} • MACD: {format_smart_oscillator(macd_val)})</div>
            </div>
            <a href="{play_link}" target="_blank" rel="noopener" class="btn-header-play">
                {play_svg} <span>Google Play</span>
            </a>
        </div>
    </div>

    <script>
        function toggleMobileSidebar(e) {{
            if (e) e.stopPropagation();
            const sb = document.getElementById('sidebar');
            const bd = document.getElementById('sidebarBackdrop');
            const isOpen = sb.classList.contains('open');
            sb.classList.toggle('open', !isOpen);
            bd.classList.toggle('open', !isOpen);
        }}
        function closeMobileSidebar() {{
            document.getElementById('sidebar').classList.remove('open');
            document.getElementById('sidebarBackdrop').classList.remove('open');
        }}
        function toSlug(sym) {{
            return sym.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
        }}

        const SEARCH_INDEX = {search_index_json};
        const searchInput = document.getElementById('assetSearchInput');
        const searchDropdown = document.getElementById('searchDropdown');

        searchInput.addEventListener('input', function() {{
            const q = this.value.trim().toLowerCase();
            if (!q) {{ searchDropdown.style.display = 'none'; return; }}
            const matches = SEARCH_INDEX.filter(arr => arr[0].toLowerCase().includes(q) || arr[1].toLowerCase().includes(q)).slice(0, 8);
            if (matches.length === 0) {{
                searchDropdown.innerHTML = `<a href="https://tradegpt-vector.com/assets/" class="search-res-item" style="color:var(--accent-emerald);">Browse all 260+ instruments in Directory →</a>`;
            }} else {{
                searchDropdown.innerHTML = matches.map(m => `
                    <a href="https://tradegpt-vector.com/assets/${{toSlug(m[0])}}.html" class="search-res-item">
                        <div><b>${{m[0]}}</b> <span style="color:var(--text-secondary); margin-left:6px;">${{m[1]}}</span></div>
                        <span style="color:var(--accent-emerald); font-size:12px; font-family:'Space Mono',monospace;">Open Chart →</span>
                    </a>
                `).join('');
            }}
            searchDropdown.style.display = 'block';
        }});

        searchInput.addEventListener('keydown', function(e) {{
            if (e.key === 'Enter') {{
                const q = this.value.trim().toLowerCase();
                const firstMatch = SEARCH_INDEX.find(a => a[0].toLowerCase() === q) || SEARCH_INDEX.find(a => a[0].toLowerCase().includes(q) || a[1].toLowerCase().includes(q));
                if (firstMatch) {{
                    window.location.href = `https://tradegpt-vector.com/assets/${{toSlug(firstMatch[0])}}.html`;
                }}
            }}
        }});

        document.addEventListener('click', function(e) {{
            if (!searchInput.contains(e.target) && !searchDropdown.contains(e.target)) {{
                searchDropdown.style.display = 'none';
            }}
        }});

        const rawData = {chart_json_str};
        const container = document.getElementById('tvchart');
        const totalBars = rawData.length;
        const calcMinSpacing = Math.max(1.5, container.clientWidth / Math.max(totalBars, 1));

        const chart = LightweightCharts.createChart(container, {{
            layout: {{ background: {{ type: 'solid', color: '#12161f' }}, textColor: '#9ca3af', fontFamily: 'Space Mono, monospace', fontSize: 11 }},
            grid: {{ vertLines: {{ color: '#1a202e' }}, horzLines: {{ color: '#1a202e' }} }},
            rightPriceScale: {{ borderColor: '#232a3b' }},
            timeScale: {{
                borderColor: '#232a3b',
                fixLeftEdge: true,
                fixRightEdge: true,
                minBarSpacing: calcMinSpacing
            }},
            handleScroll: {{ mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false }},
            handleScale: {{ axisPressedMouseMove: true, mouseWheel: true, pinch: true }}
        }});

        const candleSeries = chart.addCandlestickSeries({{
            upColor: '#10b981', downColor: '#ef4444',
            borderUpColor: '#10b981', borderDownColor: '#ef4444',
            wickUpColor: '#10b981', wickDownColor: '#ef4444'
        }});

        const areaSeries = chart.addAreaSeries({{
            lineColor: '#10b981',
            topColor: 'rgba(16, 185, 129, 0.25)',
            bottomColor: 'rgba(16, 185, 129, 0.0)',
            lineWidth: 2,
            visible: false
        }});

        candleSeries.setData(rawData);
        areaSeries.setData(rawData.map(item => ({{ time: item.time, value: item.close }})));

        if (totalBars > 0 && rawData[totalBars - 1].close < 0.001) {{
            const microFormat = {{ type: 'price', precision: 10, minMove: 0.0000000001 }};
            candleSeries.applyOptions({{ priceFormat: microFormat }});
            areaSeries.applyOptions({{ priceFormat: microFormat }});
        }}

        if (totalBars > 90) {{
            chart.timeScale().setVisibleLogicalRange({{ from: totalBars - 90, to: totalBars - 1 }});
        }} else {{
            chart.timeScale().fitContent();
        }}

        let isCandleMode = true;
        function toggleChartType() {{
            isCandleMode = !isCandleMode;
            candleSeries.applyOptions({{ visible: isCandleMode }});
            areaSeries.applyOptions({{ visible: !isCandleMode }});
            document.getElementById('iconLine').style.display = isCandleMode ? 'block' : 'none';
            document.getElementById('iconCandle').style.display = isCandleMode ? 'none' : 'block';
        }}

        // --- INIZIO AGGIORNAMENTO DINAMICO OHLC (CROSSHAIR) ---
        function formatJsPrice(v) {{
            if (v >= 1000) return v.toLocaleString('en-US', {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
            if (v >= 1) return v.toFixed(2);
            if (v >= 0.01) return v.toFixed(4);
            if (v > 0 && v < 0.001) return v.toFixed(10);
            return v.toFixed(6);
        }}

        const origOpen = "{format_smart_price(open_price)}";
        const origClose = "{format_smart_price(prev_price)}";
        const origHigh = "{format_smart_price(d_max)}";
        const origLow = "{format_smart_price(d_min)}";

        const elOpen = document.getElementById('dyn-open');
        const elClose = document.getElementById('dyn-close');
        const elHigh = document.getElementById('dyn-high');
        const elLow = document.getElementById('dyn-low');
        const elCloseLbl = document.getElementById('dyn-close-lbl');

        chart.subscribeCrosshairMove(function(param) {{
            if (
                param.point === undefined ||
                !param.time ||
                param.point.x < 0 ||
                param.point.x > container.clientWidth ||
                param.point.y < 0 ||
                param.point.y > container.clientHeight
            ) {{
                // Ripristina i valori di base della sessione
                elOpen.innerText = origOpen;
                elClose.innerText = origClose;
                elHigh.innerText = origHigh;
                elLow.innerText = origLow;
                elCloseLbl.innerText = "Prev. Close";
            }} else {{
                // Estrae i dati della candela selezionata
                const candleData = param.seriesData.get(candleSeries);
                const areaData = param.seriesData.get(areaSeries);
                
                if (isCandleMode && candleData) {{
                    elOpen.innerText = formatJsPrice(candleData.open);
                    elHigh.innerText = formatJsPrice(candleData.high);
                    elLow.innerText = formatJsPrice(candleData.low);
                    elClose.innerText = formatJsPrice(candleData.close);
                    elCloseLbl.innerText = "Close";
                }} else if (!isCandleMode && areaData) {{
                    // In modalità linea abbiamo solo il valore di chiusura
                    const valStr = formatJsPrice(areaData.value);
                    elOpen.innerText = valStr;
                    elHigh.innerText = valStr;
                    elLow.innerText = valStr;
                    elClose.innerText = valStr;
                    elCloseLbl.innerText = "Close";
                }}
            }}
        }});
        // --- FINE AGGIORNAMENTO DINAMICO OHLC ---

        let profExp = false;
        function toggleProfile() {{
            profExp = !profExp;
            document.getElementById('profileText').style.webkitLineClamp = profExp ? 'unset' : '2';
            document.getElementById('profileMore').innerText = profExp ? 'Show less' : 'Read more';
        }}

        window.addEventListener('resize', () => {{
            const newMinSpacing = Math.max(1.5, container.clientWidth / Math.max(totalBars, 1));
            chart.applyOptions({{
                width: container.clientWidth,
                timeScale: {{ minBarSpacing: newMinSpacing }}
            }});
        }});
    </script>
</body>
</html>"""

            r2_manager.write_file(f"{seo_folder}/{slug}.html", html_page, is_json=False)
            directory_by_sector[sector_clean].append((upper_sym, full_name, slug, curr_price, pct_change))
            sitemap_urls.append(f"""  <url>
    <loc>https://tradegpt-vector.com/assets/{slug}.html</loc>
    <lastmod>{today_iso}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>""")
        except Exception as e:
            print(f"Errore SEO {sym}: {e}")

    sec_html = []
    for sec in sorted(directory_by_sector.keys()):
        cards = []
        for sym, fn, sl, pr, ch in sorted(directory_by_sector[sec]):
            c_col = "#10b981" if ch >= 0 else "#ef4444"
            gh_l = f"https://raw.githubusercontent.com/pammyhouse/dati-finanziari/main/loghi/{sym}_img.png"
            fmp_l = f"https://financialmodelingprep.com/image-stock/{sym}.png"
            cards.append(f"<a href='https://tradegpt-vector.com/assets/{sl}.html' style='background:#12161f; padding:12px 14px; border-radius:6px; color:#fff; text-decoration:none; border:1px solid #232a3b; display:flex; align-items:center; justify-content:space-between; transition:border-color 0.15s;'><div style='display:flex; align-items:center; gap:10px; overflow:hidden;'><img src='{gh_l}' onerror=\"this.onerror=function(){{this.src='{app_logo_url}'}};this.src='{fmp_l}';\" width='30' height='30' style='border-radius:50%; object-fit:cover; flex-shrink:0;'><div style='overflow:hidden;'><b>{sym}</b><br><small style='color:#9ca3af; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; display:block;'>{fn}</small></div></div><span style='color:{c_col}; font-family:\"Space Mono\",monospace; font-weight:700; font-size:0.85rem; margin-left:8px;'>{ch:+.2f}%</span></a>")
        sec_html.append(f"<h2 style='color:#f3f4f6; font-size:1.15rem; font-weight:700; margin-top:32px; border-bottom:1px solid #232a3b; padding-bottom:10px;'>{sec}</h2><div style='display:grid; grid-template-columns:repeat(auto-fill, minmax(250px,1fr)); gap:12px;'>{''.join(cards)}</div>")

    # Pagina Directory /assets/index.html con lo stesso Header Orizzontale, Ricerca e Disclaimer Legale
    index_assets_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>All 260+ Covered Financial Assets - Reference Charts & Data | TradeGPT</title>
    <link rel="icon" type="image/png" href="{app_logo_url}">
    <meta name="description" content="Browse the complete directory of 260+ global stocks, cryptocurrencies, forex pairs, commodities, and ETFs covered by TradeGPT quantitative AI analysis.">
    <link rel="canonical" href="https://tradegpt-vector.com/assets/">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-body: #0b0e14; --bg-surface: #12161f; --bg-elevated: #181d29; --bg-hover: #1e2534;
            --border-color: #232a3b; --border-subtle: #1a202e; --text-main: #f3f4f6;
            --text-secondary: #9ca3af; --text-muted: #6b7280; --accent-emerald: #10b981;
            --up-green: #10b981; --down-red: #ef4444;
        }}
        *, *::before, *::after {{ box-sizing: border-box; }}
        html, body {{ margin: 0; padding: 0; width: 100%; max-width: 100vw; overflow-x: hidden; background: var(--bg-body); color: var(--text-main); font-family: 'Inter', sans-serif; font-size: 14px; -webkit-font-smoothing: antialiased; }}
        a {{ color: inherit; text-decoration: none; }}
        .top-ticker-strip {{ background: #080a0f; border-bottom: 1px solid var(--border-color); height: 36px; display: flex; align-items: center; overflow-x: auto; white-space: nowrap; padding: 0 16px; scrollbar-width: none; }}
        .top-ticker-strip::-webkit-scrollbar {{ display: none; }}
        .strip-inner {{ max-width: 1320px; margin: 0 auto; display: flex; align-items: center; gap: 20px; width: 100%; }}
        .strip-item {{ display: inline-flex; align-items: center; gap: 6px; font-size: 12px; padding: 2px 6px; border-radius: 4px; }}
        .strip-sym {{ font-weight: 700; color: #fff; }}
        .strip-price {{ font-family: 'Space Mono', monospace; color: var(--text-secondary); font-size: 11.5px; }}
        .tape-up {{ font-family: 'Space Mono', monospace; color: var(--up-green); font-weight: 700; font-size: 11.5px; }}
        .tape-down {{ font-family: 'Space Mono', monospace; color: var(--down-red); font-weight: 700; font-size: 11.5px; }}
        .strip-dot {{ width: 6px; height: 6px; border-radius: 50%; background: var(--up-green); display: inline-block; }}
        .inv-header {{ background: var(--bg-surface); border-bottom: 1px solid var(--border-color); position: sticky; top: 0; z-index: 1000; }}
        .inv-header-top {{ max-width: 1320px; margin: 0 auto; padding: 10px 20px; display: flex; align-items: center; justify-content: space-between; gap: 20px; }}
        .brand-box {{ display: flex; align-items: center; gap: 10px; flex-shrink: 0; }}
        .brand-box img {{ width: 34px; height: 34px; border-radius: 7px; object-fit: contain; }}
        .brand-title {{ font-size: 19px; font-weight: 800; letter-spacing: -0.5px; color: #fff; display: inline-flex; align-items: center; gap: 6px; line-height: 1; }}
        .brand-pro-tag {{ background: var(--accent-emerald); color: #000; font-size: 10px; font-weight: 800; padding: 3px 5px; border-radius: 4px; margin-left: 2px; text-transform: uppercase; line-height: 1; }}
        .header-search-wrap {{ flex: 1; max-width: 520px; position: relative; }}
        .header-search-input {{ width: 100%; height: 38px; background: var(--bg-body); border: 1px solid var(--border-color); border-radius: 6px; padding: 0 14px 0 38px; color: #fff; font-size: 13.5px; outline: none; }}
        .header-search-input:focus {{ border-color: var(--accent-emerald); }}
        .header-search-icon {{ position: absolute; left: 12px; top: 50%; transform: translateY(-50%); color: var(--text-muted); pointer-events: none; }}
        .search-results-dropdown {{ position: absolute; top: calc(100% + 4px); left: 0; right: 0; background: var(--bg-surface); border: 1px solid var(--border-color); border-radius: 6px; max-height: 320px; overflow-y: auto; z-index: 9999; display: none; box-shadow: 0 12px 32px rgba(0,0,0,0.75); }}
        .search-res-item {{ display: flex; align-items: center; justify-content: space-between; padding: 10px 14px; border-bottom: 1px solid var(--border-subtle); font-size: 13px; }}
        .search-res-item:hover {{ background: var(--bg-hover); }}
        .btn-header-play {{ display: inline-flex; align-items: center; gap: 8px; background: var(--accent-emerald); color: #000; font-weight: 700; font-size: 12.5px; padding: 7px 14px; border-radius: 6px; flex-shrink: 0; }}
        .inv-nav-bar {{ background: #0e121a; border-top: 1px solid var(--border-subtle); }}
        .inv-nav-inner {{ max-width: 1320px; margin: 0 auto; padding: 0 20px; display: flex; align-items: center; justify-content: space-between; height: 40px; overflow-x: auto; white-space: nowrap; scrollbar-width: none; }}
        .inv-nav-links {{ display: flex; align-items: center; gap: 24px; list-style: none; margin: 0; padding: 0; height: 100%; }}
        .inv-nav-links li {{ height: 100%; display: flex; align-items: center; }}
        .inv-nav-links a {{ font-size: 13px; font-weight: 600; color: var(--text-secondary); height: 100%; display: flex; align-items: center; border-bottom: 2px solid transparent; }}
        .inv-nav-links a:hover, .inv-nav-links a.active {{ color: #fff; border-bottom-color: var(--accent-emerald); }}
        .legal-notice-box {{ background: rgba(239, 68, 68, 0.05); border: 1px solid rgba(239, 68, 68, 0.25); border-radius: 6px; padding: 18px 20px; margin-top: 36px; color: var(--text-secondary); font-size: 12px; line-height: 1.65; }}
        .legal-notice-box strong {{ color: #fca5a5; }}
        .legal-notice-box a {{ color: var(--accent-emerald); text-decoration: underline; }}
        .inv-footer {{ background: #080a0f; border-top: 1px solid var(--border-color); padding: 32px 20px; margin-top: 36px; color: var(--text-muted); font-size: 12px; }}
        @media (max-width: 768px) {{ .inv-header-top {{ flex-wrap: wrap; padding: 10px 14px; }} .header-search-wrap {{ order: 3; max-width: 100%; flex: 0 0 100%; }} }}
    </style>
</head>
<body>
    <div class="top-ticker-strip"><div class="strip-inner">{top_strip_html}</div></div>
    <header class="inv-header">
        <div class="inv-header-top">
            <a href="https://tradegpt-vector.com/" class="brand-box">
                <img src="{app_logo_url}" alt="TradeGPT">
                <span class="brand-title">TradeGPT <span class="brand-pro-tag">AI</span></span>
            </a>
            <div class="header-search-wrap">
                <svg class="header-search-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                <input type="text" id="assetSearchInput" class="header-search-input" placeholder="Search 260+ stocks, crypto, forex, ETFs..." autocomplete="off">
                <div class="search-results-dropdown" id="searchDropdown"></div>
            </div>
            <a href="https://play.google.com/store/apps/details?id=com.vectordev.einsight&referrer=utm_source%3Dwebsite%26utm_medium%3Ddirectory_header" target="_blank" rel="noopener" class="btn-header-play">{play_svg} <span>Get App (Free)</span></a>
        </div>
        <div class="inv-nav-bar">
            <div class="inv-nav-inner">
                <ul class="inv-nav-links">
                    <li><a href="https://tradegpt-vector.com/index.html">Markets Home</a></li>
                    <li><a href="https://tradegpt-vector.com/assets/" class="active">All 260+ Assets Directory</a></li>
                    <li><a href="https://tradegpt-vector.com/index.html#hot-assets-section">Smart Rankings</a></li>
                    <li><a href="https://tradegpt-vector.com/index.html#features-section">Features & Tiers</a></li>
                    <li><a href="https://tradegpt-vector.com/products.html">API & Pricing</a></li>
                    <li><a href="https://tradegpt-vector.com/docs.html">Developer Docs</a></li>
                    <li><a href="https://tradegpt-vector.com/about.html">About Us</a></li>
                </ul>
            </div>
        </div>
    </header>
    <main style="max-width:1320px; margin:0 auto; padding:28px 20px;">
        <h1 style="margin:0 0 8px 0; font-size:1.75rem; font-weight:800;">TradeGPT Global Markets Directory (260+ Assets)</h1>
        <p style="color:#9ca3af; font-size:0.92rem; margin:0 0 24px 0;">Select any financial instrument below to inspect interactive historical candlestick charts, RSI(14), MACD, 52-week ranges, and unlock AI forecasts in the app.</p>
        {''.join(sec_html)}
        <div class="legal-notice-box">
            <strong>⚖️ Legal Notice, Risk Disclosure & Implicit Acceptance of Terms:</strong><br>
            By accessing or using this directory and any associated market data pages, you expressly agree to be bound by the TradeGPT & VectorDEV <a href="https://tradegpt-vector.com/privacy.html">Privacy Policy</a> and <a href="https://tradegpt-vector.com/docs.html">Terms of Use</a>. All financial quotes, charts, and AI indicators are delayed reference values provided for informational and educational purposes only and do not constitute financial advice.
        </div>
    </main>
    <footer class="inv-footer">
        <div style="max-width:1320px; margin:0 auto;">
            © 2026 <b>TradeGPT</b> by VectorDEV. All Rights Reserved.
        </div>
    </footer>
    <script>
        function toSlug(sym) {{ return sym.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, ''); }}
        const SEARCH_INDEX = {search_index_json};
        const searchInput = document.getElementById('assetSearchInput');
        const searchDropdown = document.getElementById('searchDropdown');
        searchInput.addEventListener('input', function() {{
            const q = this.value.trim().toLowerCase();
            if (!q) {{ searchDropdown.style.display = 'none'; return; }}
            const matches = SEARCH_INDEX.filter(arr => arr[0].toLowerCase().includes(q) || arr[1].toLowerCase().includes(q)).slice(0, 8);
            searchDropdown.innerHTML = matches.map(m => `<a href="https://tradegpt-vector.com/assets/${{toSlug(m[0])}}.html" class="search-res-item"><div><b>${{m[0]}}</b> <span style="color:#9ca3af; margin-left:6px;">${{m[1]}}</span></div><span style="color:#10b981; font-size:12px; font-family:'Space Mono',monospace;">Open Chart →</span></a>`).join('');
            searchDropdown.style.display = 'block';
        }});
    </script>
</body>
</html>"""
    r2_manager.write_file(f"{seo_folder}/index.html", index_assets_html, is_json=False)

    sitemap_xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + "\n".join(sitemap_urls) + "\n</urlset>"
    r2_manager.s3.put_object(
        Bucket=r2_manager.bucket_name,
        Key="public_seo/sitemap_assets.xml",
        Body=sitemap_xml.encode('utf-8'),
        ContentType='application/xml; charset=utf-8',
        CacheControl='max-age=3600'
    )

    home_news = []
    for focus_sym in ["NVDA", "BTCUSD", "TSLA", "AAPL", "ISP.MI", "GOLD"]:
        n_list = news_by_symbol.get(focus_sym, [])
        if n_list:
            t, _, _, src, _, dt = n_list[0]
            d_str = dt.strftime("%b %d") if hasattr(dt, 'strftime') else ""
            home_news.append({"sym": focus_sym, "title": t, "src": src, "date": d_str})

    home_pulse_payload = {
        "updated": datetime.utcnow().strftime("%b %d, %H:%M UTC"),
        "quotes": home_quotes,
        "news": home_news[:5]
    }
    r2_manager.write_file("public_seo/home_pulse.json", home_pulse_payload, is_json=True)
    
    print("Pagine SEO professionali e sitemap_assets.xml salvate su R2!")

generate_investing_style_seo_pages(dati_storici_all, crescita_settimanale, news_by_symbol)




# ==============================================================================
# 10. GENERATORE "PRO TIPS" MULTILINGUA (STILE INVESTING.COM) CON CLOUDFLARE AI
# ==============================================================================

CF_API_TOKEN = os.environ.get("CF_API_TOKEN") # Token API Cloudflare con permessi Workers AI
CF_AI_MODEL = "@cf/google/gemma-4-26b-a4b-it"
pro_tips_path = f"{TARGET_FOLDER}/pro_tips.json"
TARGET_LANGS = ["ar", "de", "en", "es", "fr", "hi", "id", "it", "ja", "ko", "nl", "pt", "ru", "zh-rCN"]

def generate_random_timestamp(base_dt, window_minutes=20):
    """Genera un orario casuale nel range [-window_minutes, +window_minutes] rispetto a base_dt."""
    offset_seconds = random.randint(-window_minutes * 60, window_minutes * 60)
    random_dt = base_dt + timedelta(seconds=offset_seconds)
    return random_dt

def _series(df, col):
    s = df[col]
    return s.iloc[:, 0] if isinstance(s, pd.DataFrame) else s


def _load_previous_tips():
    """Tip dell'ultima generazione: servono per non ripetere ticker e aperture."""
    try:
        prev = r2_manager.read_json(pro_tips_path)
        if isinstance(prev, dict) and isinstance(prev.get("tips"), list):
            return prev["tips"]
    except Exception:
        pass
    return []


def _opening_words(text, n=3):
    return " ".join(str(text).split()[:n]).lower()


def select_top_8_hot_tickers(dati_storici_all, indicator_data, percentuali_combine, momentum_results,
                             news_by_symbol):
    """
    Seleziona gli 8 ticker piu' 'caldi'. Firma originale invariata.
    I tips sono un contenuto Premium: qui NON entrano dati Ultra (insider, short float, target analisti).
    Novita': metriche di trend (SMA50/200, rendimenti 5/20gg), diversificazione per settore
    (max 2 per settore) e rotazione rispetto ai ticker dell'ultima generazione.
    """
    candidates = []

    for sym, df in dati_storici_all.items():
        if df is None or len(df) < 21:
            continue
        if is_excluded_from_rankings(sym):
            continue

        try:
            close_s = _series(df, 'Close')
            high_s = _series(df, 'High')
            low_s = _series(df, 'Low')
            vol_s = _series(df, 'Volume')

            curr_price = float(close_s.iloc[-1])
            prev_price = float(close_s.iloc[-2])
            if prev_price <= 0:
                continue

            chg_24h = ((curr_price - prev_price) / prev_price) * 100.0

            vol_today = float(vol_s.iloc[-1])
            if vol_today == 0 and len(vol_s) >= 2:
                vol_today = float(vol_s.iloc[-2])
            vol_avg_20 = float(vol_s.iloc[-21:-1].mean())
            rvol = min((vol_today / vol_avg_20) if vol_avg_20 > 0 else 1.0, 10.0)

            lookback = min(252, len(close_s))
            high_52w = float(high_s.tail(lookback).max())
            low_52w = float(low_s.tail(lookback).min())
            dist_52w_high = ((curr_price - high_52w) / high_52w) * 100.0 if high_52w > 0 else 0.0
            dist_52w_low = ((curr_price - low_52w) / low_52w) * 100.0 if low_52w > 0 else 0.0

            dist_sma50 = None
            if len(close_s) >= 50:
                sma50 = float(close_s.tail(50).mean())
                dist_sma50 = ((curr_price - sma50) / sma50) * 100.0 if sma50 > 0 else None
            dist_sma200 = None
            if len(close_s) >= 200:
                sma200 = float(close_s.tail(200).mean())
                dist_sma200 = ((curr_price - sma200) / sma200) * 100.0 if sma200 > 0 else None
            ret_5d = ((curr_price / float(close_s.iloc[-6])) - 1) * 100.0 if len(close_s) >= 6 else 0.0
            ret_20d = ((curr_price / float(close_s.iloc[-21])) - 1) * 100.0 if len(close_s) >= 21 else 0.0

            ind = indicator_data.get(sym, {})
            rsi = float(ind.get("RSI (14)", 50.0))
            macd_line = float(ind.get("MACD Line", 0.0))
            macd_sig = float(ind.get("MACD Signal", 0.0))

            hybrid_score = float(percentuali_combine.get(sym, 50.0))
            mom_score = float(momentum_results.get(sym, 50.0))

            raw_news = news_by_symbol.get(sym, [])
            recent_headlines = [n[0] for n in raw_news[:2]] if raw_news else []

            heat_score = (abs(chg_24h) * 1.8) + (max(0.0, rvol - 1.0) * 3.5) + (max(0.0, abs(rsi - 50.0) - 15.0) * 0.4) + (abs(mom_score - 50.0) * 0.15) + (2.0 if recent_headlines else 0.0)

            candidates.append({
                "symbol": sym,
                "name": symbol_name_map.get(sym, [sym])[0],
                "sector": re.sub(r'^\d+\.\s*', '', asset_sector_map.get(sym, "Global Markets")),
                "price": round(curr_price, 12) if is_micro_price(curr_price) else (round(curr_price, 4) if curr_price < 1 else round(curr_price, 2)),
                "change_24h_pct": round(chg_24h, 2),
                "rvol": round(rvol, 2),
                "rsi_14": round(rsi, 1),
                "macd_bullish": bool(macd_line > macd_sig),
                "dist_52w_high_pct": round(dist_52w_high, 2),
                "dist_52w_low_pct": round(dist_52w_low, 2),
                "dist_sma50_pct": None if dist_sma50 is None else round(dist_sma50, 2),
                "dist_sma200_pct": None if dist_sma200 is None else round(dist_sma200, 2),
                "ret_5d_pct": round(ret_5d, 2),
                "ret_20d_pct": round(ret_20d, 2),
                "hybrid_score": round(hybrid_score, 1),
                "headlines": recent_headlines,
                "heat_score": round(heat_score, 2)
            })
        except Exception:
            continue

    candidates.sort(key=lambda x: x["heat_score"], reverse=True)

    # Diversificazione: max 2 per settore, e ruota i ticker usati nella generazione precedente.
    prev_symbols = {t.get("symbol") for t in _load_previous_tips()}
    picked, per_sector = [], defaultdict(int)
    for c in candidates:
        if len(picked) == 8:
            break
        if c["symbol"] in prev_symbols or per_sector[c["sector"]] >= 2:
            continue
        picked.append(c)
        per_sector[c["sector"]] += 1
    for c in candidates:                      # se mancano candidati "freschi", completa
        if len(picked) == 8:
            break
        if c not in picked:
            picked.append(c)
    return picked


def _get_fundamentals_for_tip_isolated(symbol):
    """Helper ISOLATO per scaricare al volo i fondamentali solo degli 8 asset vincitori."""
    try:
        yf_tick = TICKER_MAP.get(symbol, symbol)
        info = yf.Ticker(yf_tick).info or {}

        def s_val(k):
            v = info.get(k)
            return round(v, 2) if isinstance(v, (int, float)) else "N/A"
        def s_pct(k):
            v = info.get(k)
            return f"{round(v*100, 2)}%" if isinstance(v, (int, float)) else "N/A"

        fund_data = {
            "P/E (Trailing)": s_val("trailingPE"),
            "P/E (Forward)": s_val("forwardPE"),
            "Revenue Growth (YoY)": s_pct("revenueGrowth"),
            "Profit Margin": s_pct("profitMargins"),
            "Debt to Equity": s_val("debtToEquity"),
        }   # niente Short Float / target analisti: sono dati Ultra
        return {k: v for k, v in fund_data.items() if v != "N/A" and v != "N/A%"}
    except Exception:
        return {}


def _num(v):
    """'12.5%' / 12.5 / '-3' -> float oppure None."""
    try:
        return float(str(v).replace("%", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ------------------------------------------------------------------------------
# MOTORE DI ANALISI: trasforma i dati in una TESI (non in un elenco di numeri)
# ------------------------------------------------------------------------------
def _build_insight(a):
    """Deduzione deterministica: tesi, sentiment, hashtag e fatti rilevanti con la loro lettura."""
    rsi, chg, rvol = a["rsi_14"], a["change_24h_pct"], a["rvol"]
    dist_hi, dist_lo = a["dist_52w_high_pct"], a.get("dist_52w_low_pct", 50.0)
    d50, d200 = a.get("dist_sma50_pct"), a.get("dist_sma200_pct")
    above50 = d50 is not None and d50 >= 0
    macd_bull = a["macd_bullish"]
    f = a.get("fundamentals") or {}
    pe = _num(f.get("P/E (Forward)")) or _num(f.get("P/E (Trailing)"))
    growth = _num(f.get("Revenue Growth (YoY)"))

    thesis, sentiment, why = "momentum", ("bull" if chg >= 0 else "bear"), ""
    if dist_hi >= -3 and rvol >= 1.5 and chg > 0:
        thesis, sentiment = "breakout", "bull"
        why = f"Price sits {abs(dist_hi):.1f}% from its 52-week high on {rvol}x volume: a breakout backed by participation, not a drift."
    elif rsi <= 32:
        thesis, sentiment = "oversold", "bull"
        why = f"RSI at {rsi} is washed-out territory" + (f", only {dist_lo:.0f}% above the 52-week low" if dist_lo < 15 else "") + ": selling pressure tends to fade from here, but a rebound needs confirmation."
    elif rsi >= 72:
        thesis, sentiment = "overbought", "bear"
        why = f"RSI at {rsi} after a {a.get('ret_20d_pct', 0):+.1f}% run in 20 days: the move is stretched and vulnerable to a pause."
    elif rvol >= 2 and abs(chg) < 1.2:
        thesis, sentiment = "accumulation", ("bull" if (macd_bull or above50) else "bear")
        why = f"Volume is {rvol}x average yet price barely moves ({chg:+.1f}%): large players are positioning, a decisive move often follows."
    elif pe is not None and 0 < pe <= 15 and growth is not None and growth >= 8:
        thesis, sentiment = "value", "bull"
        why = f"P/E of {pe:.1f} paired with {growth:.0f}% revenue growth: earnings power is not fully priced in."
    elif chg <= -2 and rvol >= 1.3 and not above50:
        thesis, sentiment = "selloff", "bear"
        why = f"A {chg:+.1f}% drop on {rvol}x volume while below the 50-day average: distribution, not a random dip."
    elif chg <= -1 and above50 and macd_bull:
        thesis, sentiment = "pullback", "bull"
        why = f"A {chg:+.1f}% dip while price holds above its 50-day average and MACD stays positive: a pullback inside an uptrend."
    elif chg >= 2 and d50 is not None and not above50:
        thesis, sentiment = "recovery", "bull"
        why = f"A {chg:+.1f}% rebound that still leaves price {abs(d50):.1f}% under its 50-day average: relief, not yet a trend change."

    facts = [f"Move: {chg:+.2f}% today, {a.get('ret_5d_pct', 0):+.1f}% in 5 days, {a.get('ret_20d_pct', 0):+.1f}% in 20 days",
             f"Volume: {rvol}x the 20-day average", f"RSI(14): {rsi}; MACD {'above' if macd_bull else 'below'} its signal line"]
    if d50 is not None:
        facts.append(f"Price is {d50:+.1f}% vs the 50-day average" + (f" and {d200:+.1f}% vs the 200-day" if d200 is not None else ""))
    facts.append(f"Distance from 52-week high: {dist_hi:+.1f}%")
    for k, v in f.items():
        facts.append(f"{k}: {v}")

    hashtags = {"breakout": "#BREAKOUT",
                "oversold": "#OVERSOLD", "overbought": "#OVERBOUGHT", "accumulation": "#VOLUME_SPIKE",
                "value": "#VALUE_PLAY", "selloff": "#SELLOFF", "pullback": "#PULLBACK",
                "recovery": "#RECOVERY", "momentum": "#MOMENTUM"}
    return {"thesis": thesis, "sentiment": sentiment, "hashtag": hashtags[thesis], "why": why, "facts": facts}


# ------------------------------------------------------------------------------
# FALLBACK DETERMINISTICO (solo se l'AI non risponde): testi nativi in 6 lingue
# ------------------------------------------------------------------------------
_FB_GROUP = {"oversold": "oversold", "overbought": "overbought", "breakout": "breakout",
             "accumulation": "flow", "selloff": "selloff",
             "pullback": "pullback", "recovery": "pullback"}

_FB = {
    "oversold": {
        "en": ("{sym}: selling looks stretched", "{name} has slipped to an RSI of {rsi}, a zone where sellers tend to run out of steam. With volume at {rvol}x average, a relief bounce is plausible, but it needs a close above recent highs to confirm."),
        "it": ("{sym}: le vendite appaiono tirate", "{name} è scesa a un RSI di {rsi}, zona in cui i venditori tendono a esaurire la spinta. Con volumi a {rvol}x la media un rimbalzo tecnico è plausibile, ma serve una chiusura sopra i massimi recenti per confermarlo."),
        "es": ("{sym}: la presión vendedora se agota", "{name} cae hasta un RSI de {rsi}, zona donde los vendedores suelen quedarse sin fuerza. Con un volumen de {rvol}x la media, un rebote técnico es plausible, aunque necesita un cierre por encima de los máximos recientes."),
        "fr": ("{sym} : la pression vendeuse s'essouffle", "{name} recule jusqu'à un RSI de {rsi}, zone où les vendeurs s'épuisent souvent. Avec des volumes à {rvol}x la moyenne, un rebond technique est plausible, mais il faut une clôture au-dessus des plus hauts récents."),
        "de": ("{sym}: Verkaufsdruck lässt nach", "{name} ist auf ein RSI von {rsi} gefallen – eine Zone, in der den Verkäufern oft die Luft ausgeht. Bei {rvol}x Durchschnittsvolumen ist eine technische Erholung plausibel, braucht aber einen Schlusskurs über den jüngsten Hochs."),
        "pt": ("{sym}: pressão vendedora perde força", "{name} caiu para um RSI de {rsi}, zona em que os vendedores costumam perder fôlego. Com volume {rvol}x a média, um repique técnico é plausível, mas precisa de um fecho acima das máximas recentes."),
    },
    "overbought": {
        "en": ("{sym}: the run looks stretched", "{name} trades at an RSI of {rsi} after {ret20}% in 20 days. Rallies this extended often pause or retrace, so chasing here carries more risk than the headline move suggests."),
        "it": ("{sym}: il rialzo appare tirato", "{name} viaggia con un RSI di {rsi} dopo il {ret20}% in 20 giorni. Rialzi così estesi spesso si fermano o ritracciano: inseguire il prezzo ora è più rischioso di quanto sembri."),
        "es": ("{sym}: la subida parece estirada", "{name} cotiza con un RSI de {rsi} tras un {ret20}% en 20 días. Subidas tan extendidas suelen detenerse o corregir, así que perseguir el precio ahora implica más riesgo del que parece."),
        "fr": ("{sym} : la hausse paraît tendue", "{name} affiche un RSI de {rsi} après {ret20}% en 20 jours. Des hausses aussi étendues marquent souvent une pause ou un repli : courir après le prix comporte plus de risque qu'il n'y paraît."),
        "de": ("{sym}: Der Anstieg wirkt überdehnt", "{name} notiert bei einem RSI von {rsi} nach {ret20}% in 20 Tagen. Derart ausgedehnte Rallyes pausieren oder korrigieren oft – dem Kurs hinterherzulaufen ist riskanter, als es aussieht."),
        "pt": ("{sym}: a alta parece esticada", "{name} negocia com RSI de {rsi} após {ret20}% em 20 dias. Altas tão extensas costumam pausar ou recuar, e perseguir o preço agora traz mais risco do que parece."),
    },
    "breakout": {
        "en": ("{sym}: breakout with real volume", "{name} is only {dist}% below its 52-week high and volume runs at {rvol}x average. Strength on participation is what separates a durable breakout from a false start."),
        "it": ("{sym}: rottura con volumi veri", "{name} è a solo il {dist}% dai massimi a 52 settimane e i volumi sono a {rvol}x la media. La forza accompagnata dalla partecipazione distingue una rottura solida da una falsa partenza."),
        "es": ("{sym}: ruptura con volumen real", "{name} está a solo un {dist}% de sus máximos de 52 semanas y el volumen va a {rvol}x la media. La fuerza con participación distingue una ruptura sólida de un falso arranque."),
        "fr": ("{sym} : cassure avec de vrais volumes", "{name} n'est qu'à {dist}% de son plus haut sur 52 semaines, avec des volumes à {rvol}x la moyenne. La force accompagnée de participation distingue une cassure durable d'un faux départ."),
        "de": ("{sym}: Ausbruch mit echtem Volumen", "{name} liegt nur {dist}% unter dem 52-Wochen-Hoch, das Volumen beträgt {rvol}x des Durchschnitts. Stärke mit Beteiligung unterscheidet einen tragfähigen Ausbruch von einem Fehlstart."),
        "pt": ("{sym}: rompimento com volume real", "{name} está a apenas {dist}% da máxima de 52 semanas, com volume a {rvol}x a média. Força com participação separa um rompimento sólido de um falso início."),
    },
    "flow": {
        "en": ("{sym}: heavy volume, quiet price", "{name} trades {rvol}x its usual volume yet price moves only {chg}%. That mismatch often means large players are positioning before the next directional move."),
        "it": ("{sym}: volumi alti, prezzo fermo", "{name} scambia {rvol}x i volumi abituali ma il prezzo si muove solo del {chg}%. Questo scollamento indica spesso grandi operatori che si posizionano prima del prossimo movimento direzionale."),
        "es": ("{sym}: volumen alto, precio quieto", "{name} mueve {rvol}x su volumen habitual, pero el precio solo varía un {chg}%. Ese desajuste suele indicar grandes operadores posicionándose antes del siguiente movimiento."),
        "fr": ("{sym} : gros volumes, prix calme", "{name} échange {rvol}x ses volumes habituels alors que le prix ne bouge que de {chg}%. Cet écart signale souvent de gros acteurs qui se positionnent avant le prochain mouvement."),
        "de": ("{sym}: hohes Volumen, ruhiger Kurs", "{name} handelt mit {rvol}x dem üblichen Volumen, doch der Kurs bewegt sich nur {chg}%. Diese Diskrepanz deutet oft auf große Akteure hin, die sich vor der nächsten Bewegung positionieren."),
        "pt": ("{sym}: volume alto, preço parado", "{name} negocia {rvol}x o volume habitual, mas o preço varia só {chg}%. Essa divergência costuma indicar grandes players se posicionando antes do próximo movimento."),
    },
    "selloff": {
        "en": ("{sym}: selling with conviction", "{name} drops {chg}% on {rvol}x average volume while below its 50-day average. Heavy volume on the way down points to distribution rather than a routine dip."),
        "it": ("{sym}: vendite convinte", "{name} cede il {chg}% con volumi a {rvol}x la media, restando sotto la media a 50 giorni. Volumi alti in discesa indicano distribuzione più che un semplice storno."),
        "es": ("{sym}: ventas con convicción", "{name} cae un {chg}% con un volumen de {rvol}x la media, por debajo de su media de 50 días. Un volumen alto en la caída apunta a distribución más que a un simple recorte."),
        "fr": ("{sym} : des ventes appuyées", "{name} perd {chg}% avec des volumes à {rvol}x la moyenne, sous sa moyenne à 50 jours. Un fort volume à la baisse évoque de la distribution plutôt qu'un simple repli."),
        "de": ("{sym}: Verkäufe mit Überzeugung", "{name} verliert {chg}% bei {rvol}x Durchschnittsvolumen und liegt unter dem 50-Tage-Schnitt. Hohes Volumen beim Rückgang spricht eher für Distribution als für eine normale Delle."),
        "pt": ("{sym}: vendas com convicção", "{name} cai {chg}% com volume {rvol}x a média, abaixo da média de 50 dias. Volume alto na queda sugere distribuição, não uma simples correção."),
    },
    "pullback": {
        "en": ("{sym}: dip inside an uptrend", "{name} eases {chg}% but holds above its 50-day average with MACD still positive. Pullbacks like this are where trend followers usually look for a better entry."),
        "it": ("{sym}: storno dentro un trend rialzista", "{name} cede il {chg}% ma resta sopra la media a 50 giorni con MACD ancora positivo. Gli storni di questo tipo sono dove chi segue il trend cerca spesso un ingresso migliore."),
        "es": ("{sym}: corrección dentro de una tendencia alcista", "{name} cede un {chg}% pero se mantiene sobre su media de 50 días con el MACD aún positivo. Correcciones así son donde quien sigue la tendencia busca una mejor entrada."),
        "fr": ("{sym} : repli dans une tendance haussière", "{name} recule de {chg}% mais reste au-dessus de sa moyenne à 50 jours, MACD toujours positif. C'est dans ce genre de repli que les suiveurs de tendance cherchent un meilleur point d'entrée."),
        "de": ("{sym}: Rücksetzer im Aufwärtstrend", "{name} gibt {chg}% nach, hält aber den 50-Tage-Schnitt und das MACD bleibt positiv. Genau in solchen Rücksetzern suchen Trendfolger oft den besseren Einstieg."),
        "pt": ("{sym}: recuo dentro de uma tendência de alta", "{name} cede {chg}%, mas segue acima da média de 50 dias com o MACD ainda positivo. É em recuos assim que quem segue a tendência costuma buscar uma entrada melhor."),
    },
    "momentum": {
        "en": ("{sym}: momentum is building", "{name} moves {chg}% with {rvol}x average volume and RSI at {rsi}. The combination of price and participation is worth watching for follow-through."),
        "it": ("{sym}: il momentum prende forma", "{name} si muove del {chg}% con volumi a {rvol}x la media e RSI a {rsi}. L'abbinamento tra prezzo e partecipazione va tenuto d'occhio per capire se c'è continuazione."),
        "es": ("{sym}: el impulso gana forma", "{name} se mueve un {chg}% con un volumen de {rvol}x la media y RSI en {rsi}. La combinación de precio y participación merece seguimiento para ver si hay continuidad."),
        "fr": ("{sym} : l'élan se construit", "{name} bouge de {chg}% avec des volumes à {rvol}x la moyenne et un RSI à {rsi}. L'association du prix et de la participation mérite d'être suivie pour juger de la continuation."),
        "de": ("{sym}: Dynamik baut sich auf", "{name} bewegt sich {chg}% bei {rvol}x Durchschnittsvolumen und einem RSI von {rsi}. Die Kombination aus Kurs und Beteiligung verdient Beobachtung auf Anschlusskäufe bzw. -verkäufe."),
        "pt": ("{sym}: o ímpeto ganha forma", "{name} varia {chg}% com volume {rvol}x a média e RSI em {rsi}. A combinação de preço e participação merece acompanhamento para ver se há continuidade."),
    },
}


def _build_fallback_tip(asset_ctx, insight=None, only_langs=None):
    """Fallback deterministico con testo nativo (en/it/es/fr/de/pt; le altre lingue ricevono l'inglese)."""
    insight = insight or _build_insight(asset_ctx)
    group = _FB_GROUP.get(insight["thesis"], "momentum")
    vals = {
        "sym": asset_ctx["symbol"], "name": asset_ctx["name"], "chg": f"{asset_ctx['change_24h_pct']:+.1f}",
        "rvol": asset_ctx["rvol"], "rsi": asset_ctx["rsi_14"], "dist": f"{abs(asset_ctx['dist_52w_high_pct']):.1f}",
        "ret20": f"{asset_ctx.get('ret_20d_pct', 0):+.0f}",
    }
    out = {}
    for lang in (only_langs or TARGET_LANGS):
        tpl = _FB[group].get(lang) or _FB[group]["en"]
        out[lang] = {"title": tpl[0].format(**vals), "body": tpl[1].format(**vals)}
    return {"hashtag": insight["hashtag"], "sentiment": insight["sentiment"], "translations": out,
            "thesis": insight["thesis"], "source": "fallback"}


# ------------------------------------------------------------------------------
# GENERAZIONE AI IN DUE FASI (testo EN + traduzioni a lotti), con log diagnostici
# ------------------------------------------------------------------------------
_LANG_NAMES = {"ar": "Arabic", "de": "German", "en": "English", "es": "Spanish", "fr": "French", "hi": "Hindi",
               "id": "Indonesian", "it": "Italian", "ja": "Japanese", "ko": "Korean", "nl": "Dutch",
               "pt": "Portuguese", "ru": "Russian", "zh-rCN": "Simplified Chinese"}

_VOICES = [
    "lead with the most surprising number",
    "lead with the risk to the thesis",
    "lead with what volume says that price does not",
    "desk-note style: claim, reason, thing to watch",
    "lead with two signals that disagree",
]

_LANG_CODES = " ".join(TARGET_LANGS)


def _cf_text(j):
    """Estrae il testo dalla risposta Workers AI: stile OpenAI (choices[0].message.content),
    con envelope opzionale 'result', oppure il vecchio campo 'response'."""
    res = j.get("result", j) if isinstance(j, dict) else {}
    if not isinstance(res, dict):
        return "", ""
    finish = ""
    content = ""
    choices = res.get("choices")
    if isinstance(choices, list) and choices:
        finish = str(choices[0].get("finish_reason") or "")
        msg = choices[0].get("message") or {}
        content = msg.get("content") or ""
        if isinstance(content, list):                       # content a parti: [{type:'text', text:'...'}]
            content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    if not content:
        content = res.get("response") or ""
    return str(content), finish


def _cf_chat(messages, max_out, temperature, account_id, api_token, tag=""):
    """Chiamata Workers AI (Gemma 4 e' un modello 'reasoning': il thinking e' ATTIVO di default e consuma
    i token di output lasciando content vuoto -> prima causa del fallback). Qui il thinking e' disattivato
    con chat_template_kwargs.enable_thinking=false. Se l'API rifiuta un parametro (400/422) si ripiega su
    varianti piu' semplici. Ritorna il testo oppure ''."""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{CF_AI_MODEL}"
    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    base = {"messages": messages, "temperature": temperature}
    variants = [
        {**base, "max_completion_tokens": max_out, "response_format": {"type": "json_object"},
         "chat_template_kwargs": {"enable_thinking": False}},
        {**base, "max_completion_tokens": max_out, "chat_template_kwargs": {"enable_thinking": False}},
        {**base, "max_tokens": max_out},
    ]
    vi, tries = 0, 0
    while vi < len(variants) and tries < 5:
        tries += 1
        try:
            resp = requests.post(url, headers=headers, json=variants[vi], timeout=90)
            if resp.status_code == 200:
                j = resp.json()
                text, finish = _cf_text(j)
                if text.strip():
                    if finish == "length":
                        print(f"[tips] {tag}: output troncato (finish_reason=length)")
                    return text
                usage = (j.get("result", j) or {}).get("usage") if isinstance(j, dict) else None
                print(f"[tips] {tag}: content vuoto, finish={finish!r}, usage={usage}")
                vi += 1                                       # riprova con una variante piu' semplice
            elif resp.status_code in (400, 422):
                print(f"[tips] {tag}: HTTP {resp.status_code} (variante {vi}): {resp.text[:240]}")
                vi += 1
            elif resp.status_code in (429, 500, 502, 503, 504):
                print(f"[tips] {tag}: HTTP {resp.status_code}, riprovo")
                time.sleep(2 + tries * 2)
            else:
                print(f"[tips] {tag}: HTTP {resp.status_code}: {resp.text[:240]}")
                break
        except Exception as e:
            print(f"[tips] {tag}: eccezione {e}")
            time.sleep(2)
    return ""


def _extract_json_obj(text):
    clean = re.sub(r"^```(?:json)?|```$", "", str(text).strip(), flags=re.MULTILINE).strip()
    m = re.search(r"\{.*\}", clean, re.DOTALL)
    if not m:
        return None
    raw = m.group(0)
    for candidate in (raw, re.sub(r",\s*([}\]])", r"\1", raw)):
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def _parse_lang_map(obj, langs):
    """{lang: [title, body]} oppure {lang: {title, body}} -> {lang: {title, body}} per le lingue valide."""
    out = {}
    if not isinstance(obj, dict):
        return out
    for l in langs:
        v = obj.get(l)
        t = b = ""
        if isinstance(v, (list, tuple)) and len(v) >= 2:
            t, b = str(v[0]).strip(), str(v[1]).strip()
        elif isinstance(v, dict):
            t, b = str(v.get("title", "")).strip(), str(v.get("body", "")).strip()
        if t and b:
            out[l] = {"title": t[:90], "body": b[:300]}
    return out


def _compact_data(a):
    """Dati minimi e solo livelli Free/Premium (mai insider, short float o target analisti: sono Ultra)."""
    f = a.get("fundamentals") or {}
    parts = [f"chg {a['change_24h_pct']:+.1f}%", f"vol {a['rvol']}x", f"RSI {a['rsi_14']}",
             f"52wH {a['dist_52w_high_pct']:+.0f}%"]
    if a.get("dist_sma50_pct") is not None:
        parts.append(f"vs50d {a['dist_sma50_pct']:+.0f}%")
    if a.get("ret_20d_pct") is not None:
        parts.append(f"20d {a['ret_20d_pct']:+.0f}%")
    pe = f.get("P/E (Forward)") or f.get("P/E (Trailing)")
    if pe is not None:
        parts.append(f"PE {pe}")
    if f.get("Revenue Growth (YoY)"):
        parts.append(f"revG {f['Revenue Growth (YoY)']}")
    return ", ".join(parts)


def call_cloudflare_gemma_for_tip(asset_ctx, account_id, api_token, avoid_openings=None):
    """Una sola chiamata compatta: tesi deterministica in input, 14 lingue in output (array [titolo, corpo]).
    Le eventuali lingue mancanti vengono richieste con una seconda chiamata minima, poi fallback nativo."""
    insight = _build_insight(asset_ctx)
    if not api_token:
        print("[tips] CF_API_TOKEN assente: uso il fallback deterministico.")
        return _build_fallback_tip(asset_ctx, insight)

    avoid = sorted(avoid_openings or [])[:6]
    system_prompt = (
        "Markets analyst writing one AI Market Tip. Explain what the signals MEAN and what to watch next; "
        "never recap the numbers (max 2 figures, woven in). "
        "Title <=55 chars. Body: 2 sentences, <=190 chars, the 2nd says what would confirm or invalidate. "
        "No buy/sell advice, no price targets. "
        f"Style: {random.choice(_VOICES)}." + (f" Don't open with: {'; '.join(avoid)}." if avoid else "") + " "
        f"Reply JSON only: {{\"<code>\":[\"title\",\"body\"]}} for codes: {_LANG_CODES} "
        "(zh-rCN=Simplified Chinese, id=Indonesian). Native natural wording; keep tickers and numbers."
    )
    news = (" | news: " + asset_ctx["headlines"][0][:110]) if asset_ctx.get("headlines") else ""
    user_prompt = (
        f"{asset_ctx['symbol']} {asset_ctx['name']} ({asset_ctx['sector']}) | thesis: {insight['thesis']}, "
        f"{'bullish' if insight['sentiment'] == 'bull' else 'bearish'} | why: {insight['why'] or 'mixed signals, no standout anomaly'} | "
        f"data: {_compact_data(asset_ctx)}{news}"
    )
    text = _cf_chat([{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                    max_out=2400, temperature=0.7, account_id=account_id, api_token=api_token,
                    tag=asset_ctx["symbol"])
    translations = _parse_lang_map(_extract_json_obj(text), TARGET_LANGS) if text else {}

    if "en" not in translations:
        print(f"[tips] {asset_ctx['symbol']}: risposta AI non utilizzabile, uso il fallback deterministico.")
        return _build_fallback_tip(asset_ctx, insight)

    missing = [l for l in TARGET_LANGS if l not in translations]
    if missing:                                              # richiesta minima solo per le lingue mancanti
        en = translations["en"]
        text2 = _cf_chat(
            [{"role": "system", "content": f"Translate title and body naturally. Reply JSON only: {{\"<code>\":[\"title\",\"body\"]}} for codes: {' '.join(missing)} (zh-rCN=Simplified Chinese, id=Indonesian)."},
             {"role": "user", "content": f"{en['title']} || {en['body']}"}],
            max_out=1400, temperature=0.2, account_id=account_id, api_token=api_token,
            tag=f"{asset_ctx['symbol']} +{len(missing)}")
        translations.update(_parse_lang_map(_extract_json_obj(text2), missing) if text2 else {})
        missing = [l for l in TARGET_LANGS if l not in translations]
        if missing:
            print(f"[tips] {asset_ctx['symbol']}: lingue senza traduzione {missing}, uso il fallback nativo.")
            translations.update(_build_fallback_tip(asset_ctx, insight, only_langs=missing)["translations"])

    return {"hashtag": insight["hashtag"], "sentiment": insight["sentiment"], "translations": translations,
            "thesis": insight["thesis"], "source": "ai"}


def already_generated_today(today_str):
    """Controlla su R2 se pro_tips.json esiste già ed è stato generato oggi (UTC)."""
    try:
        existing = r2_manager.read_json(pro_tips_path)
        if existing and isinstance(existing, dict):
            gen_at = existing.get("generated_at_utc", "")
            if gen_at.startswith(today_str) and existing.get("count", 0) > 0:
                return True
    except Exception:
        pass
    return False


def generate_and_save_pro_tips():
    now_utc = datetime.utcnow()
    today_str = now_utc.strftime("%Y-%m-%d")

    # 1. Orario target casuale ma deterministico per giorno (stesso valore in tutte le run
    #    del giorno), scelto tra 07:00 e 13:59 UTC. La generazione parte alla prima run
    #    utile a partire da quell'orario; se le run nella finestra saltano o ritardano
    #    (GitHub Actions cron e' best-effort), le run successive recuperano comunque
    #    (catch-up) cosi' da avere almeno un aggiornamento al giorno.
    _rng = random.Random(f"protips-{today_str}")
    target_minute = _rng.randint(7 * 60, 14 * 60 - 1)
    now_minute = now_utc.hour * 60 + now_utc.minute
    if now_minute < target_minute:
        print(f"ProTips skippati: ora UTC attuale ({now_utc.strftime('%H:%M')}) prima dell'orario target odierno "
              f"({target_minute // 60:02d}:{target_minute % 60:02d} UTC).")
        return

    # 2. Controllo esecuzione giornaliera su R2
    if already_generated_today(today_str):
        print(f"ProTips già generati oggi ({today_str} UTC). Nessuna azione richiesta.")
        return

    print("Avvio selezione 8 Hot Tickers e generazione ProTips multilingua con Gemma 4 26B...")
    
    # Usa esattamente la tua chiamata originale, nessun parametro aggiunto o rimosso
    top_8_assets = select_top_8_hot_tickers(
        dati_storici_all, indicator_data, percentuali_combine, momentum_results, news_by_symbol
    )

    account_id = r2_manager.account_id
    tips_list = []
    # Aperture gia' usate (ultima generazione + questo lotto): l'AI viene istruita a non ripeterle
    avoid_openings = set()
    for _pt in _load_previous_tips():
        _en = (_pt.get("translations") or {}).get("en") or {}
        for _k in ("title", "body"):
            if _en.get(_k):
                avoid_openings.add(_opening_words(_en[_k]))

    for asset_ctx in top_8_assets:
        # Novità Isolato: Scarica i fondamentali SOLO per questi 8 asset prima di passarlo all'IA
        asset_ctx['fundamentals'] = _get_fundamentals_for_tip_isolated(asset_ctx['symbol'])
        
        ai_tip = call_cloudflare_gemma_for_tip(asset_ctx, account_id, CF_API_TOKEN, avoid_openings)
        _en_new = ai_tip["translations"].get("en") or {}
        for _k in ("title", "body"):
            if _en_new.get(_k):
                avoid_openings.add(_opening_words(_en_new[_k]))
        random_dt = generate_random_timestamp(now_utc, window_minutes=20)

        tips_list.append({
            "symbol": asset_ctx["symbol"],
            "name": asset_ctx["name"],
            "hashtag": ai_tip["hashtag"],
            "status": ai_tip["sentiment"],
            "timestamp": random_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "display_time": random_dt.strftime("%H:%M"),
            "translations": ai_tip["translations"],
            "thesis": ai_tip.get("thesis", ""),
            "source": ai_tip.get("source", ""),
            "metrics_snapshot": {
                "price": asset_ctx["price"],
                "change_24h_pct": asset_ctx["change_24h_pct"],
                "rvol": asset_ctx["rvol"],
                "rsi_14": asset_ctx["rsi_14"]
            },
            "_sort_dt": random_dt
        })

    tips_list.sort(key=lambda x: x["_sort_dt"], reverse=True)
    for t in tips_list:
        del t["_sort_dt"]

    final_payload = {
        "generated_at_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(tips_list),
        "tips": tips_list
    }

    r2_manager.write_file(pro_tips_path, final_payload, is_json=True)
    print(f"Salvati {len(tips_list)} ProTips multilingua su Cloudflare R2 in: {pro_tips_path} "
          f"(AI: {sum(1 for t in tips_list if t['source'] == 'ai')}, fallback: {sum(1 for t in tips_list if t['source'] == 'fallback')})")

# Esecuzione
generate_and_save_pro_tips()




print("Tutte le operazioni completate con successo!")
