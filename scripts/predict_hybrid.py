import os
import re
import feedparser
import math
import json
import time
import requests # AGGIUNTO
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
    # --- 1. Big Tech, Software & Internet ---
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
    # --- 2. Semiconductors & AI ---
    "NVDA": "2. Semiconductors & AI", "INTC": "2. Semiconductors & AI",
    "QCOM": "2. Semiconductors & AI", "ADI": "2. Semiconductors & AI",
    "ARM": "2. Semiconductors & AI", "CSCO": "2. Semiconductors & AI",
    "ACN": "2. Semiconductors & AI", "FIS": "2. Semiconductors & AI",
    "AVGO": "2. Semiconductors & AI", "STM.MI": "2. Semiconductors & AI",
    "ASML.AS": "2. Semiconductors & AI", "TSM": "2. Semiconductors & AI",
    # --- 3. Financial Services ---
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
    # --- 4. Automotive & Mobility ---
    "TSLA": "4. Automotive & Mobility", "GM": "4. Automotive & Mobility",
    "NIO": "4. Automotive & Mobility", "STLAM.MI": "4. Automotive & Mobility",
    "HTZ": "4. Automotive & Mobility", "LCID": "4. Automotive & Mobility",
    "RIVN": "4. Automotive & Mobility", "UBER": "4. Automotive & Mobility",
    "LYFT": "4. Automotive & Mobility", "NAAS": "4. Automotive & Mobility",
    "BA": "4. Automotive & Mobility", "AIR.PA": "4. Automotive & Mobility",
    "RACE.MI": "4. Automotive & Mobility", "P911.DE": "4. Automotive & Mobility",
    "TM": "4. Automotive & Mobility", "VOW3.DE": "4. Automotive & Mobility",
    "MBG.DE": "4. Automotive & Mobility",
    # --- 5. Healthcare & Pharma ---
    "LLY": "5. Healthcare & Pharma", "JNJ": "5. Healthcare & Pharma",
    "PFE": "5. Healthcare & Pharma", "MRK": "5. Healthcare & Pharma",
    "ABT": "5. Healthcare & Pharma", "BMY": "5. Healthcare & Pharma",
    "AMGN": "5. Healthcare & Pharma", "CVS": "5. Healthcare & Pharma",
    "BDX": "5. Healthcare & Pharma", "ZTS": "5. Healthcare & Pharma",
    "EW": "5. Healthcare & Pharma", "LNTH": "5. Healthcare & Pharma",
    "SYK": "5. Healthcare & Pharma", "UNH": "5. Healthcare & Pharma",
    "NVO": "5. Healthcare & Pharma", "AZN.L": "5. Healthcare & Pharma",
    "SAN.PA": "5. Healthcare & Pharma",
    # --- 6. Consumer Goods & Retail ---
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
    # --- 7. Industrials & Defense ---
    "CAT": "7. Industrials & Defense", "LMT": "7. Industrials & Defense",
    "ITW": "7. Industrials & Defense", "FDX": "7. Industrials & Defense",
    "NSC": "7. Industrials & Defense", "GE": "7. Industrials & Defense",
    "HON": "7. Industrials & Defense", "DE": "7. Industrials & Defense",
    "LDO.MI": "7. Industrials & Defense", "BKNG": "7. Industrials & Defense",
    "SIE.DE": "7. Industrials & Defense", "EMAAR.AE": "7. Industrials & Defense",
    # --- 8. Energy (Oil & Gas) ---
    "OIL": "8. Energy (Oil & Gas)", "NATGAS": "8. Energy (Oil & Gas)",
    "XOM": "8. Energy (Oil & Gas)", "CVX": "8. Energy (Oil & Gas)",
    "PBR": "8. Energy (Oil & Gas)", "NRG": "8. Energy (Oil & Gas)",
    "ENI.MI": "8. Energy (Oil & Gas)", "SHEL.L": "8. Energy (Oil & Gas)",
    "BP.L": "8. Energy (Oil & Gas)", "TTE.PA": "8. Energy (Oil & Gas)",
    # --- 9. Utilities & Green ---
    "SO": "9. Utilities & Green", "ENEL.MI": "9. Utilities & Green",
    "DUK": "9. Utilities & Green", "AEP": "9. Utilities & Green",
    "D": "9. Utilities & Green", "HE": "9. Utilities & Green",
    "APD": "9. Utilities & Green", "IBE.MC": "9. Utilities & Green",
    # --- 10. Precious Metals & Materials ---
    "GOLD": "10. Precious Metals & Materials", "SILVER": "10. Precious Metals & Materials",
    "COPPER": "10. Precious Metals & Materials", "VALE": "10. Precious Metals & Materials",
    "RIO.L": "10. Precious Metals & Materials",
    # --- 11. Media & Telecom ---
    "NFLX": "11. Media & Telecom", "DIS": "11. Media & Telecom",
    "T": "11. Media & Telecom", "TMUS": "11. Media & Telecom",
    "AMX": "11. Media & Telecom", "ROKU": "11. Media & Telecom",
    "SONY": "11. Media & Telecom", "DTE.DE": "11. Media & Telecom",
    "TEF.MC": "11. Media & Telecom",
    # --- 12. Indices (Global) ---
    "SPX500": "12. Indices (Global)", "DJ30": "12. Indices (Global)",
    "NAS100": "12. Indices (Global)", "NASCOMP": "12. Indices (Global)",
    "RUS2000": "12. Indices (Global)", "VIX": "12. Indices (Global)",
    "EU50": "12. Indices (Global)", "ITA40": "12. Indices (Global)",
    "GER40": "12. Indices (Global)", "UK100": "12. Indices (Global)",
    "FRA40": "12. Indices (Global)", "SWI20": "12. Indices (Global)",
    "ESP35": "12. Indices (Global)", "NETH25": "12. Indices (Global)",
    "JPN225": "12. Indices (Global)", "HKG50": "12. Indices (Global)",
    "CHN50": "12. Indices (Global)", "IND50": "12. Indices (Global)",
    "KOR200": "12. Indices (Global)",
    # --- 13. Forex (Currencies) ---
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
    # --- 14. Crypto Assets ---
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
    # --- 15. ETFs & Funds ---
    "SPY": "15. ETFs & Funds", "VOO": "15. ETFs & Funds", "QQQ": "15. ETFs & Funds",
    "IWM": "15. ETFs & Funds", "EFA": "15. ETFs & Funds",
    "EEM": "15. ETFs & Funds", "TLT": "15. ETFs & Funds",
    "GLD": "15. ETFs & Funds", "XLF": "15. ETFs & Funds",
    "XLK": "15. ETFs & Funds", "XLE": "15. ETFs & Funds",
    # --- 16. Agricultural Commodities ---
    "COCOA": "16. Agricultural Commodities", "WHEAT": "16. Agricultural Commodities",
    "CORN": "16. Agricultural Commodities", "SOYBEANS": "16. Agricultural Commodities",
    "SUGAR": "16. Agricultural Commodities"
}

TICKER_MAP = {
    "AAPL": "AAPL", "MSFT": "MSFT", "GOOGL": "GOOGL", "AMZN": "AMZN", "META": "META",
    "TSLA": "TSLA", "V": "V", "JPM": "JPM", "JNJ": "JNJ", "WMT": "WMT",
    "NVDA": "NVDA", "PYPL": "PYPL", "DIS": "DIS", "NFLX": "NFLX", "NIO": "NIO",
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
    "SPY": "SPY", "VOO": "VOO", "QQQ": "QQQ", "IWM": "IWM", "EFA": "EFA", "EEM": "EEM", 
    "TLT": "TLT", "GLD": "GLD", "XLF": "XLF", "XLK": "XLK", "XLE": "XLE",
    "EURUSD": "EURUSD=X", "USDJPY": "USDJPY=X", "GBPUSD": "GBPUSD=X",
    "AUDUSD": "AUDUSD=X", "USDCAD": "USDCAD=X", "USDCHF": "USDCHF=X",
    "NZDUSD": "NZDUSD=X", "EURGBP": "EURGBP=X", "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X", "AUDJPY": "AUDJPY=X", "CADJPY": "CADJPY=X",
    "CHFJPY": "CHFJPY=X", "EURAUD": "EURAUD=X", "EURNZD": "EURNZD=X",
    "EURCAD": "EURCAD=X", "EURCHF": "EURCHF=X", "GBPCHF": "GBPCHF=X",
    "AUDCAD": "AUDCAD=X",
    "SPX500": "^GSPC", "DJ30": "^DJI", "NAS100": "^NDX", "NASCOMP": "^IXIC",
    "RUS2000": "^RUT", "VIX": "^VIX", "EU50": "^STOXX50E", "ITA40": "FTSEMIB.MI",
    "GER40": "^GDAXI", "UK100": "^FTSE", "FRA40": "^FCHI", "SWI20": "^SSMI",
    "ESP35": "^IBEX", "NETH25": "^AEX", "JPN225": "^N225", "HKG50": "^HSI",
    "CHN50": "000001.SS", "IND50": "^NSEI", "KOR200": "^KS200",
    "BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD", "LTCUSD": "LTC-USD",
    "XRPUSD": "XRP-USD", "BCHUSD": "BCH-USD", "EOSUSD": "EOS-USD",
    "XLMUSD": "XLM-USD", "ADAUSD": "ADA-USD", "TRXUSD": "TRX-USD",
    "NEOUSD": "NEO-USD", "DASHUSD": "DASH-USD", "XMRUSD": "XMR-USD",
    "ETCUSD": "ETC-USD", "ZECUSD": "ZEC-USD", "BNBUSD": "BNB-USD",
    "DOGEUSD": "DOGE-USD", "USDTUSD": "USDT-USD", "LINKUSD": "LINK-USD",
    "ATOMUSD": "ATOM-USD", "XTZUSD": "XTZ-USD",
    "USDCUSD": "USDC-USD", "SOLUSD": "SOL-USD", "TONUSD": "TON11419-USD",
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
    # Stocks
    "AAPL": ["Apple", "Apple Inc."],
    "MSFT": ["Microsoft", "Microsoft Corporation"],
    "GOOGL": ["Google", "Alphabet", "Alphabet Inc."],
    "AMZN": ["Amazon", "Amazon.com"],
    "META": ["Meta", "Facebook", "Meta Platforms"],
    "TSLA": ["Tesla", "Tesla Inc."],
    "V": ["Visa", "Visa Inc."],
    "JPM": ["JPMorgan", "JPMorgan Chase"],
    "JNJ": ["Johnson & Johnson", "JNJ"],
    "WMT": ["Walmart"],
    "NVDA": ["NVIDIA", "Nvidia Corp."],
    "PYPL": ["PayPal"],
    "DIS": ["Disney", "The Walt Disney Company"],
    "NFLX": ["Netflix"],
    "NIO": ["NIO Inc."],
    "NRG": ["NRG Energy"],
    "ADBE": ["Adobe", "Adobe Inc."],
    "INTC": ["Intel", "Intel Corporation"],
    "CSCO": ["Cisco", "Cisco Systems"],
    "PFE": ["Pfizer"],
    "KO": ["Coca-Cola", "The Coca-Cola Company"],
    "PEP": ["Pepsi", "PepsiCo"],
    "MA": ["Mastercard", "Mastercard Inc.", "Mastercard Incorporated", "Master Card"],
    "BRK-B": ["Berkshire Hathaway", "Berkshire", "Warren Buffett company", "BRK.B", "BRK-B", "Berkshire Hathaway Inc."],
    "AVGO": ["Broadcom", "Broadcom Inc.", "Broadcom Corporation"],
    "UNH": ["UnitedHealth", "UnitedHealth Group", "United Health", "UNH"],
    "MRK": ["Merck"],
    "ABT": ["Abbott", "Abbott Laboratories"],
    "XOM": ["ExxonMobil", "Exxon"],
    "CVX": ["Chevron"],
    "T": ["AT&T"],
    "MCD": ["McDonald's"],
    "NKE": ["Nike"],
    "HD": ["Home Depot"],
    "IBM": ["IBM", "International Business Machines"],
    "CRM": ["Salesforce"],
    "BMY": ["Bristol-Myers", "Bristol-Myers Squibb"],
    "ORCL": ["Oracle"],
    "ACN": ["Accenture"],
    "LLY": ["Eli Lilly"],
    "QCOM": ["Qualcomm"],
    "HON": ["Honeywell"],
    "COST": ["Costco"],
    "SBUX": ["Starbucks"],
    "CAT": ["Caterpillar"],
    "LOW": ["Lowe's"],
    "MS": ["Morgan Stanley", "Morgan Stanley Bank", "MS bank", "MS financial"],
    "GS": ["Goldman Sachs"],
    "AXP": ["American Express"],
    "INTU": ["Intuit"],
    "AMGN": ["Amgen"],
    "GE": ["General Electric"],
    "FIS": ["Fidelity National Information Services"],
    "CVS": ["CVS Health"],
    "DE": ["Deere", "John Deere"],
    "BDX": ["Becton Dickinson"],
    "NOW": ["ServiceNow"],
    "SCHW": ["Charles Schwab"],
    "LMT": ["Lockheed Martin"],
    "ADP": ["ADP", "Automatic Data Processing"],
    "C": ["Citigroup"],
    "PLD": ["Prologis"],
    "NSC": ["Norfolk Southern"],
    "TMUS": ["T-Mobile"],
    "ITW": ["Illinois Tool Works"],
    "FDX": ["FedEx"],
    "PNC": ["PNC Financial"],
    "SO": ["Southern Company"],
    "APD": ["Air Products & Chemicals"],
    "ADI": ["Analog Devices"],
    "ICE": ["Intercontinental Exchange"],
    "ZTS": ["Zoetis"],
    "TJX": ["TJX Companies"],
    "CL": ["Colgate-Palmolive"],
    "MMC": ["Marsh & McLennan"],
    "EL": ["Estée Lauder"],
    "GM": ["General Motors"],
    "CME": ["CME Group"],
    "EW": ["Edwards Lifesciences"],
    "AON": ["Aon plc"],
    "D": ["Dominion Energy"],
    "PSA": ["Public Storage"],
    "AEP": ["American Electric Power"],
    "TROW": ["T. Rowe Price"],
    "LNTH": ["Lantheus"],
    "HE": ["Hawaiian Electric"],
    "BTDR": ["Bitdeer"],
    "NAAS": ["NaaS Technology"],
    "SCHL": ["Scholastic"],
    "TGT": ["Target"],
    "SYK": ["Stryker"],
    "BKNG": ["Booking Holdings", "Booking.com"],
    "DUK": ["Duke Energy"],
    "USB": ["U.S. Bancorp"],
    "BABA": ["Alibaba", "Alibaba Group", "阿里巴巴"],
    "HTZ": ["Hertz", "Hertz Global", "Hertz Global Holdings"],
    "UBER": ["Uber", "Uber Technologies", "Uber Technologies Inc."],
    "LYFT": ["Lyft", "Lyft Inc."],
    "PLTR": ["Palantir", "Palantir Technologies", "Palantir Technologies Inc."],
    "SNOW": ["Snowflake", "Snowflake Inc."],
    "ROKU": ["Roku", "Roku Inc."],
    "TWLO": ["Twilio", "Twilio Inc."],
    "COIN": ["Coinbase", "Coinbase Global", "Coinbase Global Inc."],
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
    "BA": ["Boeing", "The Boeing Company"],
    "AIR.PA": ["Airbus", "Airbus SE"],
    "SAP.DE": ["SAP", "SAP SE"],
    "SIE.DE": ["Siemens", "Siemens AG"],
    "ALV.DE": ["Allianz", "Allianz SE"],
    "P911.DE": ["Porsche", "Porsche AG", "Dr. Ing. h.c. F. Porsche AG", "Porsche Automobile"],
    "VOW3.DE": ["Volkswagen", "Volkswagen AG"],
    "MBG.DE": ["Mercedes-Benz", "Mercedes-Benz Group"],
    "DTE.DE": ["Deutsche Telekom", "Deutsche Telekom AG"],
    "NVO": ["Novo Nordisk", "Novo", "Novo Nordisk A/S"],
    "ASML.AS": ["ASML", "ASML Holding", "ASML NV", "ASML Holding N.V."],
    "SHEL.L": ["Shell", "Shell plc"],
    "BP.L": ["BP", "BP p.l.c."],
    "HSBA.L": ["HSBC", "HSBC Holdings"],
    "AZN.L": ["AstraZeneca", "AstraZeneca PLC"],
    "ULVR.L": ["Unilever", "Unilever PLC"],
    "RIO.L": ["Rio Tinto", "Rio Tinto Group"],
    "MC.PA": ["LVMH", "Moët Hennessy Louis Vuitton"],
    "TTE.PA": ["TotalEnergies", "TotalEnergies SE"],
    "OR.PA": ["L'Oréal", "L'Oreal"],
    "SAN.PA": ["Sanofi", "Sanofi S.A."],
    "BNP.PA": ["BNP Paribas", "BNP Paribas S.A."],
    "SAN.MC": ["Santander", "Banco Santander"],
    "IBE.MC": ["Iberdrola", "Iberdrola S.A."],
    "ITX.MC": ["Inditex", "Zara"],
    "BBVA.MC": ["BBVA", "Banco Bilbao Vizcaya Argentaria"],
    "TEF.MC": ["Telefónica", "Telefonica"],
    "ITUB": ["Itaú", "Itaú Unibanco"],
    "NU": ["Nubank", "Nu Holdings"],
    "ABEV": ["Ambev", "Ambev S.A."],
    "EMAAR.AE": ["Emaar", "Emaar Properties"],
    "DIB.AE": ["DIB", "Dubai Islamic Bank P.J.S.C.", "Dubai Bank", "Dubai Islamic Bank"],
    "EMIRATESNBD.AE": ["Emirates NBD", "Emirates NBD Bank"],
    "RIVN": ["Rivian", "Rivian Automotive", "Rivian Automotive Inc."],
    "LCID": ["Lucid", "Lucid Motors", "Lucid Group", "Lucid Group Inc."],
    "DDOG": ["Datadog", "Datadog Inc."],
    "NET": ["Cloudflare", "Cloudflare Inc."],
    "SHOP": ["Shopify", "Shopify Inc."],
    "ZM": ["Zoom", "Zoom Video", "Zoom Video Communications", "Zoom Video Communications Inc."],
    "BIDU": ["Baidu", "百度"],
    "PDD": ["Pinduoduo", "PDD Holdings", "Pinduoduo Inc.", "拼多多"],
    "JD": ["JD.com", "京东"],
    "ARM": ["Arm", "Arm Holdings", "Arm Holdings plc"],
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
    
    # ETF
    "SPY": ["SPY", "SPDR S&P 500", "SPY ETF", "SPDR ETF", "S&P 500 ETF"],
    "QQQ": ["QQQ", "Invesco QQQ", "Nasdaq ETF", "QQQ ETF", "Triple Q", "Nasdaq 100 ETF"],
    "VOO": ["VOO", "Vanguard S&P 500", "VOO ETF", "Vanguard ETF", "S&P 500 Vanguard", "Vanguard S&P 500 ETF"],
    "IWM": ["IWM", "iShares Russell 2000", "Russell 2000 ETF", "IWM ETF", "Small Cap ETF"],
    "EFA": ["EFA", "MSCI EAFE", "MSCI EAFE ETF", "iShares MSCI EAFE"],
    "EEM": ["EEM", "Emerging Markets ETF", "MSCI Emerging Markets", "iShares EEM", "ETF Paesi Emergenti"],
    "TLT": ["TLT", "20+ Year Treasury Bond ETF", "Treasury ETF", "Long-term Treasury", "iShares TLT", "ETF Titoli di Stato USA"],
    "GLD": ["GLD", "SPDR Gold Shares", "Gold ETF", "GLD ETF", "ETF Oro"],
    "XLF": ["XLF", "Financial Select Sector SPDR", "Financial ETF", "Bank ETF", "ETF Finanziario", "XLF ETF"],
    "XLK": ["XLK", "Technology Select Sector SPDR", "Tech ETF", "Technology ETF", "ETF Tecnologico", "XLK ETF"],
    "XLE": ["XLE", "Energy Select Sector SPDR", "Energy ETF", "ETF Energia", "XLE ETF"],
    
    # Forex
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

    #Index
    "SPX500": ["S&P 500", "SPX", "S&P", "S&P 500 Index", "Standard & Poor's 500"],
    "DJ30": ["Dow Jones", "DJIA", "Dow Jones Industrial", "Dow 30", "Dow Jones Industrial Average"],
    "NAS100": ["Nasdaq 100", "NDX", "Nasdaq100", "NASDAQ 100 Index"],
    "NASCOMP": ["Nasdaq Composite", "IXIC", "Nasdaq", "Nasdaq Composite Index"],
    "RUS2000": ["Russell 2000", "RUT", "Russell Small Cap", "Russell 2K"],
    "VIX": ["VIX", "Volatility Index", "Fear Gauge", "CBOE Volatility Index"],
    "EU50": ["Euro Stoxx 50", "Euro Stoxx", "STOXX50", "Euro Stoxx 50 Index"],
    "ITA40": ["FTSE MIB", "MIB", "FTSE MIB Index", "Italy 40"],
    "GER40": ["DAX", "DAX 40", "German DAX", "Frankfurt DAX"],
    "UK100": ["FTSE 100", "FTSE", "UK FTSE 100", "FTSE Index"],
    "FRA40": ["CAC 40", "CAC", "France CAC 40", "CAC40 Index"],
    "SWI20": ["Swiss Market Index", "SMI", "Swiss SMI", "Swiss Market"],
    "ESP35": ["IBEX 35", "IBEX", "Spanish IBEX", "IBEX 35 Index"],
    "NETH25": ["AEX", "Dutch AEX", "Amsterdam Exchange", "AEX Index"],
    "JPN225": ["Nikkei 225", "Nikkei", "Japan Nikkei", "Nikkei Index"],
    "HKG50": ["Hang Seng", "Hong Kong Hang Seng", "Hang Seng Index"],
    "CHN50": ["Shanghai Composite", "SSEC", "China Shanghai", "Shanghai Composite Index"],
    "IND50": ["Nifty 50", "Nifty", "India Nifty", "Nifty 50 Index"],
    "KOR200": ["KOSPI", "KOSPI 200", "Korea KOSPI", "KOSPI Index"],
    
    # Crypto
    "BTCUSD": ["Bitcoin", "BTC"],
    "ETHUSD": ["Ethereum", "ETH"],
    "LTCUSD": ["Litecoin", "LTC"],
    "XRPUSD": ["Ripple", "XRP"],
    "BCHUSD": ["Bitcoin Cash", "BCH"],
    "EOSUSD": ["EOS"],
    "XLMUSD": ["Stellar", "XLM"],
    "ADAUSD": ["Cardano", "ADA"],
    "TRXUSD": ["Tron", "TRX"],
    "NEOUSD": ["NEO"],
    "DASHUSD": ["Dash crypto", "Dash cryptocurrency", "DASH coin", "DASH token", "Digital Cash", "Dash blockchain", "Dash digital currency"],
    "XMRUSD": ["Monero", "XMR"],
    "ETCUSD": ["Ethereum Classic", "ETC"],
    "ZECUSD": ["Zcash", "ZEC"],
    "BNBUSD": ["Binance Coin", "BNB"],
    "DOGEUSD": ["Dogecoin", "DOGE"],
    "USDTUSD": ["Tether", "USDT"],
    "LINKUSD": ["Chainlink", "LINK"],
    "ATOMUSD": ["Cosmos", "ATOM"],
    "XTZUSD": ["Tezos", "XTZ"],
    "USDCUSD": ["USD Coin", "USDC", "USDC Coin"],
    "SOLUSD": ["Solana", "SOL", "Solana token"],
    "TONUSD": ["Toncoin", "The Open Network", "TON"],
    "AVAXUSD": ["Avalanche", "AVAX", "Avalanche network"],
    "DOTUSD": ["Polkadot", "DOT", "Polkadot network"],
    "NEARUSD": ["Near Protocol", "NEAR", "Near"],
    "APTUSD": ["Aptos", "APT", "Aptos network"],
    "SUIUSD": ["Sui", "Sui Network", "SUI token"],
    "ICPUSD": ["Internet Computer", "ICP", "Internet Computer Protocol"],
    "KASUSD": ["Kaspa", "KAS", "Kaspa network"],
    "STXUSD": ["Stacks", "STX", "Stacks network"],
    "SEIUSD": ["Sei", "Sei Network", "SEI token"],
    "HYPEUSD": ["Hyperliquid", "HYPE"],
    "POLUSD": ["Polygon", "POL", "MATIC", "Polygon Ecosystem Token"],
    "OPUSD": ["Optimism", "OP", "Optimism network"],
    "ARBUSD": ["Arbitrum", "ARB", "Arbitrum network"],
    "RENDERUSD": ["Render", "RNDR", "Render token"],
    "IMXUSD": ["Immutable", "IMX", "Immutable X"],
    "SKYUSD": ["Sky", "Maker", "MKR", "Sky token"],
    "UNIUSD": ["Uniswap", "UNI", "Uniswap token"],
    "AAVEUSD": ["Aave", "AAVE token"],
    "ORCAUSD": ["Orca", "ORCA token"],
    "DAIUSD": ["Dai", "DAI stablecoin"],
    "SHIBUSD": ["Shiba Inu", "SHIB", "Shiba token"],

    # Commodities
    "COCOA": ["Cocoa", "Cocoa Futures"],
    "GOLD": ["Gold", "XAU/USD", "Gold price", "Gold spot"],
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
# 2. CLASSI LOGICA NUOVA (History & Hybrid)
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
        
        # --- BLOCCO MIGRAZIONI (Mantiene compatibilità) ---
        if "log" in raw_data and isinstance(raw_data["log"], list):
            print(f"⚠️ Migrazione formato LISTA -> DIZIONARIO...")
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
# 3. HELPER FUNCTIONS
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
# 4. MAIN LOGIC (FUSIONE COMPLETA)
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
        
        sector = asset_sector_map.get(symbol, "General")
        leader_sym = sector_leaders.get(sector, "SPX500")
        leader_val = leader_trends.get(leader_sym, 0.0)
        is_leader = (symbol == leader_sym)

        try:
            ticker = str(adjusted_symbol).strip().upper()
            data = yf.download(ticker, period="5y", auto_adjust=True, progress=False)

            if not data.empty:
                if isinstance(data.columns, pd.MultiIndex):
                     try: data = data.xs(ticker, axis=1, level=1)
                     except: pass
                
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
                indicator_data[symbol] = indicators
                tabella_indicatori = pd.DataFrame(indicators.items(), columns=["Indicatore", "Valore"]).to_html(index=False, border=0)
                
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
                dati_storici_html = hist[['Date','Close','High','Low','Open','Volume']].to_html(index=False, border=1)

                sells_data = None
                buys_data = None
                dati_da_cache = False

                if adjusted_symbol in insider_cache and insider_cache[adjusted_symbol].get("date") == oggi_str:
                    sells_data = insider_cache[adjusted_symbol].get("sells")
                    buys_data = insider_cache[adjusted_symbol].get("buys")
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

                    insider_cache[adjusted_symbol] = {
                        "date": oggi_str,
                        "sells": sells_data,
                        "buys": buys_data
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
# 5. ESECUZIONE
# ==============================================================================

print("Inizio calcolo globale del Sentiment per i simboli...")
sentiment_for_symbols, percentuali_combine, all_news_entries, indicator_data, fundamental_data, crescita_settimanale, dati_storici_all, momentum_results, market_breadth_data = get_sentiment_for_all_symbols(symbol_list)

# --- CLASSIFICA PRINCIPALE ---
sorted_symbols = sorted(percentuali_combine.items(), key=lambda x: x[1], reverse=True)

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
                        info_52w = f"Nuovo Max 52W ({high_today:.2f})"
                        trovato_52w = True
                    elif low_today <= min_52w:
                        info_52w = f"Nuovo Min 52W ({low_today:.2f})"
                        trovato_52w = True
                        
                if not trovato_52w:
                    max_5g = high_p.iloc[-6:-1].max()
                    min_5g = low_p.iloc[-6:-1].min()
                    if high_today >= max_5g:
                        info_52w = f"Nuovo Max 5G ({high_today:.2f})"
                    elif low_today <= min_5g:
                        info_52w = f"Nuovo Min 5G ({low_today:.2f})"
                    
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
for symbol, media in sorted_symbols: # Usa sorted_symbols invece di sorted_symbols_pro (era questo l'errore NameError!)
    html_classifica_pro.append(f"<tr><td>{symbol}</td><td>{media:.2f}%</td></tr>")
html_classifica_pro.append("</table></body></html>")
r2_manager.write_file(pro_path, "\n".join(html_classifica_pro), is_json=False)

# --- CLASSIFICA MOMENTUM ---
sorted_momentum = sorted(momentum_results.items(), key=lambda x: x[1], reverse=True)
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
sorted_crescita = sorted([(s, g) for s, g in crescita_settimanale.items() if g is not None], key=lambda x: (x[1], x[0]), reverse=True)
html_fire = ["<html><head><title>Classifica per Crescita</title></head><body>",
             "<h1>Asset Ordinati per Crescita Settimanale</h1>",
             "<table border='1'><tr><th>Simbolo</th><th>Crescita 7gg (%)</th></tr>"]
for symbol, growth in sorted_crescita:
    html_fire.append(f"<tr><td>{symbol}</td><td>{growth:.2f}%</td></tr>")
html_fire.append("</table></body></html>")
r2_manager.write_file(fire_path, "\n".join(html_fire), is_json=False)


# ==============================================================================
# 6. DAILY BRIEF V2 (FULL DATABASE & DYNAMIC AI COPYWRITING)
# ==============================================================================
def calculate_support_resistance(df):
    if len(df) < 20: return 0.0, 0.0
    recent_low = df['Low'].tail(20).min()
    recent_high = df['High'].tail(20).max()
    return round(recent_low, 2), round(recent_high, 2)

all_analyzed_assets = []

for sym, score in percentuali_combine.items():
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
    html_v2.append(f"<div class='asset_data' data-ticker='${sym}' data-clean-ticker='{sym}' data-name='{name}' data-score='{int(cand['score'])}' data-anomaly='{cand['anomaly_score']}' {lang_attrs} data-confluence='{cand['confluence']}' data-volatility='{cand['volatility']}' data-move='{cand['expected_move']}' data-sup='{cand['sup']}' data-res='{cand['res']}'></div>")

html_v2.append("</body></html>")
v2_path = f"{TARGET_FOLDER}/daily_brief_v2_data.html"
r2_manager.write_file(v2_path, "\n".join(html_v2), is_json=False)

print("Daily Brief V2 (Dynamic Copy & Macro) salvato con successo su R2!")

# ==============================================================================
# 7. CORRELAZIONI STATISTICHE
# ==============================================================================
def calcola_correlazioni(dati_storici_all):
    returns = {sym: np.log(df["Close"]).diff().dropna() for sym, df in dati_storici_all.items() if "Close" in df.columns}
    results = {}
    assets = list(returns.keys())
    
    for asset1 in assets:
        all_candidates = []
        s1 = returns[asset1]
        
        for asset2 in assets:
            if asset1 == asset2: continue
            
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
            
            conc = (np.sign(x) == np.sign(y)).mean() * 100
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
print("Tutte le operazioni completate con successo!")
