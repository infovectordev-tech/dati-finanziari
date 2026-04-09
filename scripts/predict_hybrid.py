from supabase import create_client, Client
import re
import feedparser
import os
from datetime import datetime, timedelta
import math
#import spacy
import yfinance as yf
import ta
import pandas as pd
import numpy as np
import random
import unicodedata
import json
import base64
import requests
import argostranslate.package
import argostranslate.translate
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from financial_lexicon import LEXICON

# Indicatori tecnici e statistica
from ta.momentum import RSIIndicator, StochasticOscillator, WilliamsRIndicator
from ta.trend import MACD, EMAIndicator, CCIIndicator
from ta.volatility import BollingerBands
from urllib.parse import quote_plus
from collections import defaultdict
from scipy.stats import spearmanr, pearsonr, binomtest
from statsmodels.stats.multitest import multipletests
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# Inizializza Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Funzione universale per salvare i file sul database
def save_to_supabase(filename, content):
    data = {"filename": filename, "content": content}
    try:
        supabase.table("app_files").upsert(data).execute()
        print(f"Salvato: {filename}")
    except Exception as e:
        print(f"Errore salvataggio {filename}: {e}")


# --- SETUP AI: TURBO-VADER (VADER + Expanded Financial Lexicon) ---
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

try:
    nlp = spacy.load("en_core_web_sm")
except:
    print("Spacy model not found, proceeding without lemmatization for compatibility.")

# AGGIORNA IL LESSICO UNA VOLTA SOLA
sia = SentimentIntensityAnalyzer()
sia.lexicon.update(LEXICON)




# --- CONFIGURAZIONE CARTELLA OUTPUT ---
TARGET_FOLDER = "hybrid_results"
TEST_FOLDER = "forward_testing"  # dedicata al test


# Paths
file_path = f"{TARGET_FOLDER}/classifica.html"
news_path = f"{TARGET_FOLDER}/news.html"
history_path = f"{TARGET_FOLDER}/history.json"
fire_path = f"{TARGET_FOLDER}/fire.html"
pro_path = f"{TARGET_FOLDER}/classificaPRO.html"
corr_path = f"{TARGET_FOLDER}/correlations.html"
corr_pro_path = f"{TARGET_FOLDER}/correlations_pro.html"
mom_path = f"{TARGET_FOLDER}/classifica_momentum.html"
sector_path = f"{TARGET_FOLDER}/classifica_settori.html"


# 📌 Lingue da generare
LANGUAGES = {
    "ar": "daily_brief_ar.html", "de": "daily_brief_de.html", "es": "daily_brief_es.html",
    "fr": "daily_brief_fr.html", "hi": "daily_brief_hi.html", "it": "daily_brief_it.html",
    "ko": "daily_brief_ko.html", "nl": "daily_brief_nl.html", "pt": "daily_brief_pt.html",
    "ru": "daily_brief_ru.html", "zh": "daily_brief_zh.html", "zh-rCN": "daily_brief_zh-rCN.html",
}

# ==============================================================================
# 1. MAPPE E LISTE COMPLETE
# ==============================================================================

sector_leaders = {
    "1. Big Tech, Software & Internet": "MSFT",
    "2. Semiconductors & AI": "NVDA",
    "3. Financial Services": "JPM",
    "4. Automotive & Mobility": "TSLA",
    "5. Healthcare & Pharma": "LLY",
    "6. Consumer Goods & Retail": "WMT",
    "7. Industrials & Defense": "CAT",
    "8. Energy (Oil & Gas)": "OIL",
    "9. Utilities & Green": "IBE.MC",
    "10. Precious Metals & Materials": "GOLD",
    "11. Media & Telecom": "NFLX",
    "12. Indices (Global)": "SPX500",
    "13. Forex (Currencies)": "EURUSD",
    "14. Crypto Assets": "BTCUSD"
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
    "ZM": "1. Big Tech, Software & Internet",
    "NVDA": "2. Semiconductors & AI", "INTC": "2. Semiconductors & AI",
    "QCOM": "2. Semiconductors & AI", "ADI": "2. Semiconductors & AI",
    "ARM": "2. Semiconductors & AI", "CSCO": "2. Semiconductors & AI",
    "ACN": "2. Semiconductors & AI", "FIS": "2. Semiconductors & AI",
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
    "ISP.MI": "3. Financial Services",
    "TSLA": "4. Automotive & Mobility", "GM": "4. Automotive & Mobility",
    "NIO": "4. Automotive & Mobility", "STLAM.MI": "4. Automotive & Mobility",
    "HTZ": "4. Automotive & Mobility", "LCID": "4. Automotive & Mobility",
    "RIVN": "4. Automotive & Mobility", "UBER": "4. Automotive & Mobility",
    "LYFT": "4. Automotive & Mobility", "NAAS": "4. Automotive & Mobility",
    "LLY": "5. Healthcare & Pharma", "JNJ": "5. Healthcare & Pharma",
    "PFE": "5. Healthcare & Pharma", "MRK": "5. Healthcare & Pharma",
    "ABT": "5. Healthcare & Pharma", "BMY": "5. Healthcare & Pharma",
    "AMGN": "5. Healthcare & Pharma", "CVS": "5. Healthcare & Pharma",
    "BDX": "5. Healthcare & Pharma", "ZTS": "5. Healthcare & Pharma",
    "EW": "5. Healthcare & Pharma", "LNTH": "5. Healthcare & Pharma",
    "SYK": "5. Healthcare & Pharma",
    "WMT": "6. Consumer Goods & Retail", "KO": "6. Consumer Goods & Retail",
    "PEP": "6. Consumer Goods & Retail", "MCD": "6. Consumer Goods & Retail",
    "NKE": "6. Consumer Goods & Retail", "HD": "6. Consumer Goods & Retail",
    "COST": "6. Consumer Goods & Retail", "SBUX": "6. Consumer Goods & Retail",
    "LOW": "6. Consumer Goods & Retail", "TGT": "6. Consumer Goods & Retail",
    "TJX": "6. Consumer Goods & Retail", "CL": "6. Consumer Goods & Retail",
    "EL": "6. Consumer Goods & Retail", "SCHL": "6. Consumer Goods & Retail",
    "COCOA": "6. Consumer Goods & Retail",
    "CAT": "7. Industrials & Defense", "LMT": "7. Industrials & Defense",
    "ITW": "7. Industrials & Defense", "FDX": "7. Industrials & Defense",
    "NSC": "7. Industrials & Defense", "GE": "7. Industrials & Defense",
    "HON": "7. Industrials & Defense", "DE": "7. Industrials & Defense",
    "LDO.MI": "7. Industrials & Defense", "BKNG": "7. Industrials & Defense",
    "BA": "4. Automotive & Mobility", "AIR.PA": "4. Automotive & Mobility",
    "OIL": "8. Energy (Oil & Gas)", "NATGAS": "8. Energy (Oil & Gas)",
    "XOM": "8. Energy (Oil & Gas)", "CVX": "8. Energy (Oil & Gas)",
    "PBR": "8. Energy (Oil & Gas)", "NRG": "8. Energy (Oil & Gas)",
    "SO": "9. Utilities & Green", "ENEL.MI": "9. Utilities & Green",
    "DUK": "9. Utilities & Green", "AEP": "9. Utilities & Green",
    "D": "9. Utilities & Green", "HE": "9. Utilities & Green",
    "APD": "9. Utilities & Green",
    "GOLD": "10. Precious Metals & Materials", "SILVER": "10. Precious Metals & Materials",
    "VALE": "10. Precious Metals & Materials",
    "NFLX": "11. Media & Telecom", "DIS": "11. Media & Telecom",
    "T": "11. Media & Telecom", "TMUS": "11. Media & Telecom",
    "AMX": "11. Media & Telecom", "ROKU": "11. Media & Telecom",
    "SAP.DE": "1. Big Tech, Software & Internet", "SIE.DE": "7. Industrials & Defense",
    "ALV.DE": "3. Financial Services", "VOW3.DE": "4. Automotive & Mobility",
    "MBG.DE": "4. Automotive & Mobility", "DTE.DE": "11. Media & Telecom",
    "SHEL.L": "8. Energy (Oil & Gas)", "BP.L": "8. Energy (Oil & Gas)",
    "HSBA.L": "3. Financial Services", "AZN.L": "5. Healthcare & Pharma",
    "ULVR.L": "6. Consumer Goods & Retail", "RIO.L": "10. Precious Metals & Materials",
    "MC.PA": "6. Consumer Goods & Retail", "TTE.PA": "8. Energy (Oil & Gas)",
    "OR.PA": "6. Consumer Goods & Retail", "SAN.PA": "5. Healthcare & Pharma",
    "BNP.PA": "3. Financial Services", "SAN.MC": "3. Financial Services",
    "IBE.MC": "9. Utilities & Green", "ITX.MC": "6. Consumer Goods & Retail",
    "BBVA.MC": "3. Financial Services", "TEF.MC": "11. Media & Telecom",
    "ITUB": "3. Financial Services", "NU": "3. Financial Services",
    "ABEV": "6. Consumer Goods & Retail", "EMAAR.AE": "7. Industrials & Defense",
    "DIB.AE": "3. Financial Services", "EMIRATESNBD.AE": "3. Financial Services",
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
}

TICKER_MAP = {
    "AAPL": "AAPL", "MSFT": "MSFT", "GOOGL": "GOOGL", "AMZN": "AMZN", "META": "META",
    "TSLA": "TSLA", "V": "V", "JPM": "JPM", "JNJ": "JNJ", "WMT": "WMT",
    "NVDA": "NVDA", "PYPL": "PYPL", "DIS": "DIS", "NFLX": "NFLX", "NIO": "NIO",
    "NRG": "NRG", "ADBE": "ADBE", "INTC": "INTC", "CSCO": "CSCO", "PFE": "PFE",
    "KO": "KO", "PEP": "PEP", "MRK": "MRK", "ABT": "ABT", "XOM": "XOM",
    "CVX": "CVX", "T": "T", "MCD": "MCD", "NKE": "NKE", "HD": "HD",
    "IBM": "IBM", "CRM": "CRM", "BMY": "BMY", "ORCL": "ORCL", "ACN": "ACN",
    "LLY": "LLY", "QCOM": "QCOM", "HON": "HON", "COST": "COST", "SBUX": "SBUX",
    "CAT": "CAT", "LOW": "LOW", "MS": "MS", "GS": "GS", "AXP": "AXP",
    "INTU": "INTU", "AMGN": "AMGN", "GE": "GE", "FIS": "FIS", "CVS": "CVS",
    "DE": "DE", "BDX": "BDX", "NOW": "NOW", "SCHW": "SCHW", "LMT": "LMT",
    "ADP": "ADP", "C": "C", "PLD": "PLD", "NSC": "NSC", "TMUS": "TMUS",
    "ITW": "ITW", "FDX": "FDX", "PNC": "PNC", "SO": "SO", "APD": "APD",
    "ADI": "ADI", "ICE": "ICE", "ZTS": "ZTS", "TJX": "TJX", "CL": "CL",
    "MMC": "MMC", "EL": "EL", "GM": "GM", "CME": "CME", "EW": "EW",
    "AON": "AON", "D": "D", "PSA": "PSA", "AEP": "AEP", "TROW": "TROW",
    "LNTH": "LNTH", "HE": "HE", "BTDR": "BTDR", "NAAS": "NAAS", "SCHL": "SCHL",
    "TGT": "TGT", "SYK": "SYK", "BKNG": "BKNG", "DUK": "DUK", "USB": "USB",
    "ARM": "ARM", "BABA": "BABA", "BIDU": "BIDU", "COIN": "COIN",
    "DDOG": "DDOG", "HTZ": "HTZ", "JD": "JD", "LCID": "LCID", "LYFT": "LYFT", "NET": "NET",
    "PDD": "PDD", "PLTR": "PLTR", "RIVN": "RIVN", "ROKU": "ROKU", "SHOP": "SHOP",
    "SNOW": "SNOW", "TWLO": "TWLO", "UBER": "UBER",
    "ZM": "ZM", "DUOL": "DUOL", "PBR": "PBR", "VALE": "VALE", "AMX": "AMX",
    "ISP.MI": "ISP.MI", "ENEL.MI": "ENEL.MI", "STLAM.MI": "STLAM.MI",
    "LDO.MI": "LDO.MI", "PST.MI": "PST.MI", "UCG.MI": "UCG.MI",
    "BA": "BA", "AIR.PA": "AIR.PA", "SAP.DE": "SAP.DE", "SIE.DE": "SIE.DE",
    "ALV.DE": "ALV.DE", "VOW3.DE": "VOW3.DE", "MBG.DE": "MBG.DE", "DTE.DE": "DTE.DE",
    "SHEL.L": "SHEL.L", "BP.L": "BP.L", "HSBA.L": "HSBA.L", "AZN.L": "AZN.L",
    "ULVR.L": "ULVR.L", "RIO.L": "RIO.L", "MC.PA": "MC.PA", "TTE.PA": "TTE.PA",
    "OR.PA": "OR.PA", "SAN.PA": "SAN.PA", "BNP.PA": "BNP.PA", "SAN.MC": "SAN.MC",
    "IBE.MC": "IBE.MC", "ITX.MC": "ITX.MC", "BBVA.MC": "BBVA.MC", "TEF.MC": "TEF.MC",
    "ITUB": "ITUB", "NU": "NU", "ABEV": "ABEV", "EMAAR.AE": "EMAAR.AE", "DIB.AE": "DIB.AE", "EMIRATESNBD.AE": "EMIRATESNBD.AE",
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
    "COCOA": "CC=F", "GOLD": "GC=F", "SILVER": "SI=F", "OIL": "CL=F", "NATGAS": "NG=F"
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
    "BA": ["Boeing", "The Boeing Company"],
    "AIR.PA": ["Airbus", "Airbus SE"],
    "SAP.DE": ["SAP", "SAP SE"],
    "SIE.DE": ["Siemens", "Siemens AG"],
    "ALV.DE": ["Allianz", "Allianz SE"],
    "VOW3.DE": ["Volkswagen", "Volkswagen AG"],
    "MBG.DE": ["Mercedes-Benz", "Mercedes-Benz Group"],
    "DTE.DE": ["Deutsche Telekom", "Deutsche Telekom AG"],
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
    "NATGAS": ["Natural gas", "Gas price", "Natgas", "Henry Hub", "NG=F", "Natural gas futures"]
}

indicator_data = {}
fundamental_data = {}

# ==============================================================================
# 2. CLASSI LOGICA NUOVA (History & Hybrid)
# ==============================================================================

class HistoryManager:
    def __init__(self, filename=history_path):
        self.filename = filename
        self.data = self._load_data_from_supabase() # <-- CORRETTO IL NOME DEL METODO
        self._clean_old_data()

    def _load_data_from_supabase(self):
        try:
            response = supabase.table("app_files").select("content").eq("filename", self.filename).execute()
            if response.data and len(response.data) > 0:
                return json.loads(response.data[0]["content"])
            return {}
        except Exception:
            return {}

    def save_data_to_supabase(self):
        json_content = json.dumps(self.data, indent=4)
        save_to_supabase(self.filename, json_content)

    def _clean_old_data(self):
        # SETUP REATTIVO:
        # Teniamo solo 21 giorni (circa 1 mese di borsa). 
        # Questo rende la "media" molto più sensibile ai cambiamenti recenti.
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
        # Salviamo il sentiment normalizzato (0-1) e il conteggio
        self.data[ticker][today] = { "sentiment": float(sentiment), "news_count": int(news_count) }

    def calculate_delta_score(self, ticker, current_sent, current_count):
        if ticker not in self.data: return 50.0 
        
        history = self.data[ticker]
        today = datetime.now().strftime("%Y-%m-%d")
        
        past_sentiments = [v['sentiment'] for k, v in history.items() if k != today]
        past_counts = [v['news_count'] for k, v in history.items() if k != today]
        
        if not past_sentiments: return 50.0 
        
        # 1. Calcolo Sentiment
        avg_sent = sum(past_sentiments) / len(past_sentiments)
        sent_diff = current_sent - avg_sent
        raw_delta = (sent_diff * 100)
        
        # --- LOGICA REATTIVA (FAST MOMENTUM) ---
        multiplier = 1.0
        
        # SOGLIA BASSA: Basta poco per attivare l'analisi (3 news)
        MIN_NEWS_FLOOR = 3  
        
        # Bastano 2 giorni di storico per iniziare a calcolare (molto aggressivo)
        if len(past_counts) >= 2 and current_count >= MIN_NEWS_FLOOR:
            
            avg_count = np.mean(past_counts)
            std_dev = np.std(past_counts)
            
            # Deviazione standard minima più bassa per essere più sensibili
            if std_dev < 0.2: std_dev = 0.2
            
            z_score = (current_count - avg_count) / std_dev
            
            # TRIGGER PIÙ FACILI DA RAGGIUNGERE
            # Z=2.0 (Top 5%) invece di 3.0 (Top 0.3%) per il massimo boost
            if z_score >= 2.0:      
                multiplier = 2.0    # Massimo Boost
            elif z_score >= 1.5:    
                multiplier = 1.75   # Boost Alto
            elif z_score >= 1.0:    
                multiplier = 1.25   # Boost Medio (basta essere 1 sigma sopra la media)
                
        else:
            # Fallback Aggressivo per nuovi asset o pochissimi dati
            # Se le news sono il doppio della media semplice, spingiamo già.
            avg_simple = sum(past_counts)/len(past_counts) if past_counts else 0
            # Se oggi ho più di 5 news e sono il doppio della media -> Boost
            if current_count >= 5 and current_count >= (avg_simple * 2):
                multiplier = 1.5

        final_delta = 50 + (raw_delta * multiplier)
        return max(min(final_delta, 100), 0)

class BacktestSystem:
    def __init__(self, folder_name="forward_testing"):
        self.folder = folder_name
        self.json_filename = f"{self.folder}/backtest_log.json"
        self.html_filename = f"{self.folder}/reliability_curve.html"
        
        # Flag per sapere se il caricamento è andato a buon fine
        self.load_success = False 
        self.data = self._load_data()
        
    def _load_data(self):
        try:
            response = supabase.table("app_files").select("content").eq("filename", self.json_filename).execute()
            if response.data and len(response.data) > 0:
                self.load_success = True
                return json.loads(response.data[0]["content"])
            self.load_success = True
            return {}
        except Exception:
            self.load_success = False
            return {}

    def save_data(self):
        if not self.load_success and len(self.data) == 0: return
        content = json.dumps(self.data, indent=4)
        save_to_supabase(self.json_filename, content)

    def log_new_prediction(self, symbol, score, current_price):
        """Salva nel formato Dizionario [SIMBOLO][DATA]"""
        # Se il caricamento è fallito, non logghiamo nulla per non sporcare la memoria
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
        if not self.load_success: return

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
        if not hasattr(self, 'stats_cache') or not self.load_success: 
            if not self.load_success: return # Niente report se dati corrotti
            self._analyze_stats()
            
        stats = self.stats_cache
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
        
        save_to_supabase(self.html_filename, "\n".join(html))



# ==============================================================================
# CLASSE PATTERN ANALYZER (PROFESSIONALE)
# ==============================================================================
class PatternAnalyzer:
    def __init__(self, df):
        if hasattr(df.columns, 'levels'): # Check più sicuro per MultiIndex
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
        """
        Returns a score between -1.0 (Strong Bearish) and +1.0 (Strong Bullish).
        """
        score, _ = self._analyze_logic()
        return score

    def get_pattern_info(self):
        """
        Returns: (Numeric Score, String with English pattern names)
        """
        score, patterns = self._analyze_logic()
        pattern_text = ", ".join(patterns) if patterns else "No significant patterns"
        return score, pattern_text

    def _analyze_logic(self):
        score = 0.0
        patterns_found = []
        limit = len(self.c)
        
        if limit < 20: return 0.0, ["Insufficient Data"]
        
        # --- A. CONTEXT & TREND IDENTIFICATION ---
        i = limit - 1
        
        # Simple trend detection (10 periods lookback)
        sma_10 = np.mean(self.c[i-9:i+1])
        trend_up = self.c[i] > sma_10 and self.c[i-1] > self.c[i-5]
        trend_down = self.c[i] < sma_10 and self.c[i-1] < self.c[i-5]

        # Current and previous candles
        c1, c2, c3 = self.c[i-2], self.c[i-1], self.c[i]
        o1, o2, o3 = self.o[i-2], self.o[i-1], self.o[i]
        h1, h2, h3 = self.h[i-2], self.h[i-1], self.h[i]
        l1, l2, l3 = self.l[i-2], self.l[i-1], self.l[i]
        
        body1 = abs(c1 - o1)
        body2 = abs(c2 - o2)
        body3 = abs(c3 - o3)
        
        # --- B. CANDLESTICK PATTERNS (Context-Aware) ---

        # 1. Bullish Engulfing (Valid only in Downtrend)
        if trend_down and c2 < o2 and c3 > o3 and c3 > o2 and o3 < c2: 
            score += 0.4
            patterns_found.append("Bullish Engulfing")
        
        # 2. Bearish Engulfing (Valid only in Uptrend)
        if trend_up and c2 > o2 and c3 < o3 and c3 < o2 and o3 > c2: 
            score -= 0.4
            patterns_found.append("Bearish Engulfing")

        # 3. Hammer & Hanging Man
        lower_shadow3 = min(c3, o3) - l3
        upper_shadow3 = h3 - max(c3, o3)
        if lower_shadow3 > (body3 * 2.0) and upper_shadow3 < (body3 * 0.5):
            if trend_down:
                score += 0.3
                patterns_found.append("Hammer")
            elif trend_up:
                score -= 0.3
                patterns_found.append("Hanging Man")

        # 4. Shooting Star & Inverted Hammer
        if upper_shadow3 > (body3 * 2.0) and lower_shadow3 < (body3 * 0.5):
            if trend_up:
                score -= 0.3
                patterns_found.append("Shooting Star")
            elif trend_down:
                score += 0.3
                patterns_found.append("Inverted Hammer")

        # 5. Morning Star (Strong Bullish Reversal)
        if trend_down and c1 < o1 and body1 > (self.h[i-2]-self.l[i-2])*0.5: # 1st red, strong
            if body2 < body1 * 0.3: # 2nd small body (star)
                if c3 > o3 and c3 > (o1 + c1) / 2: # 3rd green, pierces 1st
                    score += 0.5
                    patterns_found.append("Morning Star")

        # 6. Evening Star (Strong Bearish Reversal)
        if trend_up and c1 > o1 and body1 > (self.h[i-2]-self.l[i-2])*0.5: 
            if body2 < body1 * 0.3: 
                if c3 < o3 and c3 < (o1 + c1) / 2: 
                    score -= 0.5
                    patterns_found.append("Evening Star")

        # 7. Three White Soldiers / Three Black Crows
        if c1 > o1 and c2 > o2 and c3 > o3 and c3 > c2 > c1 and trend_down:
            score += 0.4
            patterns_found.append("3 White Soldiers")
        elif c1 < o1 and c2 < o2 and c3 < o3 and c3 < c2 < c1 and trend_up:
            score -= 0.4
            patterns_found.append("3 Black Crows")

        # 8. Doji (Indecision)
        if body3 <= (h3 - l3) * 0.1 and (h3 - l3) > 0:
            patterns_found.append("Doji")
            # Doji non dà punteggio da sola, indica solo indecisione

        # --- C. STRUCTURAL PATTERNS & S/R ---
        curr_price = c3
        lookback = min(126, limit) 
        recent_h = self.h[-lookback:]
        recent_l = self.l[-lookback:]
        max_h = np.max(recent_h)
        min_l = np.min(recent_l)
        threshold = 0.02
        
        # Double Top / Double Bottom Detection (Simplified for backend scoring)
        # Check if current price is near the max, but we've seen a drop in between
        if abs(curr_price - max_h) / curr_price <= threshold:
            # Siamo vicino ai massimi. È un Double Top?
            if trend_up and self.h[i-1] < max_h * 0.98: # C'è stato un ritracciamento recente
                score -= 0.5
                patterns_found.append("Double Top Resistance")
            else:
                score -= 0.3
                patterns_found.append("At Resistance Level")

        elif abs(curr_price - min_l) / curr_price <= threshold:
            # Siamo vicino ai minimi. È un Double Bottom?
            if trend_down and self.l[i-1] > min_l * 1.02: # C'è stato un rimbalzo recente
                score += 0.5
                patterns_found.append("Double Bottom Support")
            else:
                score += 0.3
                patterns_found.append("At Support Level")

        # Normalizza il punteggio finale tra -1.0 e 1.0
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
        # Questo è il tuo vecchio metodo (RSI + SMA)
        # Lo teniamo come "Trend Score" di base
        if len(df) < 50: return 0.0
        
        # Gestione MultiIndex se necessario
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
        
        # Nota: RSI qui serve per Ipercomprato/Ipervenduto generico
        if rsi < 30: score += 0.5 
        elif rsi > 70: score -= 0.5 
        return max(min(score, 1.0), -1.0)

    def calculate_probability(self, df, sent_raw, news_n, lead, is_lead, delta_score):
        # 1. Analisi Tecnica Standard (RSI, SMA)
        tech_score = self._get_technical_score(df)
        
        # 2. Analisi Pattern Avanzata (Nuova aggiunta)
        analyzer = PatternAnalyzer(df)
        pattern_score = analyzer.get_pattern_score()

        curr_lead = 0.0 if is_lead else lead
        delta_factor = (delta_score - 50) / 50.0 
        
        # --- DEFINIZIONE PESI (WEIGHTS) ---
        # w_n = News Sentiment
        # w_t = Technical Trend (SMA/RSI)
        # w_p = Pattern (Candele + Supporti) -> NUOVO
        # w_l = Leader di settore
        # w_d = Delta Momentum (Hype recente)

        if is_lead:
            if news_n == 0:     
                # Senza news, ci affidiamo a Tecnica, Pattern e Momentum
                w_n, w_l, w_t, w_p, w_d = 0.00, 0.00, 0.40, 0.40, 0.20
            elif news_n <= 3:   
                w_n, w_l, w_t, w_p, w_d = 0.20, 0.00, 0.35, 0.30, 0.15
            else:               
                # Con tante news, il Sentiment pesa di più
                w_n, w_l, w_t, w_p, w_d = 0.40, 0.00, 0.25, 0.20, 0.15
        else:
            if news_n == 0:     
                w_n, w_l, w_t, w_p, w_d = 0.00, 0.20, 0.35, 0.35, 0.10
            elif news_n <= 3:   
                w_n, w_l, w_t, w_p, w_d = 0.15, 0.20, 0.30, 0.25, 0.10
            else:               
                w_n, w_l, w_t, w_p, w_d = 0.35, 0.15, 0.20, 0.20, 0.10
        
        # Calcolo Finale Ponderato
        final = (sent_raw * w_n) + \
                (tech_score * w_t) + \
                (pattern_score * w_p) + \
                (curr_lead * w_l) + \
                (delta_factor * w_d)
        
        # Limita tra -1 e 1
        final = max(min(final, 1.0), -1.0)
        
        # Converte in scala 0-100
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
    """
    Calcola il sentiment usando Turbo-VADER.
    Mantiene il peso temporale per dare più importanza alle news recenti.
    """
    # Se non ci sono news, neutro
    if not news_items: 
        return 0.5 if not return_raw else 0.0

    scores = []
    now = datetime.utcnow()
    
    for item in news_items:
        title = item[0]
        date = item[1]
        
        # Analisi Sentiment
        # 'compound' è il punteggio aggregato (-1 molto negativo, +1 molto positivo)
        score = sia.polarity_scores(title)['compound']
        
        # Peso temporale: le notizie di oggi pesano più di quelle di 3 mesi fa
        # Formula: e^(-0.03 * giorni_passati)
        days = (now - date).days
        weight = math.exp(-0.03 * days)
        
        scores.append(score * weight)
        
    # Media ponderata
    avg = sum(scores) / len(scores) if scores else 0
    
    if return_raw: 
        return avg # Restituisce da -1 a 1 (per calcoli matematici)
        
    # Normalizzazione finale da [-1, 1] a [0, 1] (per percentuali 0-100%)
    return (avg + 1) / 2

# ==============================================================================
# 4. MAIN LOGIC (FUSIONE COMPLETA)
# ==============================================================================

def get_sentiment_for_all_symbols(symbol_list):
    history_mgr = HistoryManager(history_path)
    scorer = HybridScorer()

    # --- SETUP BACKTESTER (Cartella Separata) ---
    # Questo inizializza il sistema puntando a "forward_testing/"
    # Se la cartella non esiste, GitHub la creerà col primo file.
    backtester = BacktestSystem(folder_name=TEST_FOLDER)
    current_prices_map = {} # Serve per il controllo bulk finale
    # --------------------------------------------
    
    sentiment_results = {}
    percentuali_combine = {} 
    all_news_entries = []
    crescita_settimanale = {}
    dati_storici_all = {}
    indicator_data = {}
    fundamental_data = {}
    momentum_results = {}
    
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

    # --- SETUP CACHE INSIDER (Eseguito una sola volta) ---
    import os
    CACHE_FILE = "insider_cache.json"
    oggi_str = datetime.now().strftime("%Y-%m-%d")
    insider_cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                insider_cache = json.load(f)
        except Exception: pass
    # -----------------------------------------------------
    
    # Loop Principale
    for symbol, adjusted_symbol in zip(symbol_list, symbol_list_for_yfinance):
        # 1. News & Sentiment
        news_data = get_stock_news(symbol)
        s7_raw = calculate_sentiment_vader(news_data["last_7_days"], return_raw=True)
        s7_norm = calculate_sentiment_vader(news_data["last_7_days"], return_raw=False) # 0-1 range for history
        news_count_7 = len(news_data["last_7_days"])
        s90 = calculate_sentiment_vader(news_data["last_90_days"])
        sentiment_results[symbol] = {"90_days": s90}
        
        # 2. Delta Score
        history_mgr.update_history(symbol, s7_norm, news_count_7)
        delta_val = history_mgr.calculate_delta_score(symbol, s7_norm, news_count_7)
        
        # SALVIAMO IL VALORE NEL DIZIONARIO
        momentum_results[symbol] = delta_val
        
        # 3. Dati Tecnici & Hybrid Score
        hybrid_prob = 50.0
        signal_str = "HOLD"
        sig_col = "black"
        tabella_indicatori = None
        dati_storici_html = None
        tabella_fondamentali = None
        holders_html = "<p>Dati sulla struttura azionaria non disponibili.</p>"
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
                
                # --- PULIZIA DATI (Elimina gli Zeri e riempie i buchi) ---
                # 1. Sostituisce eventuali zeri (0) con NaN (Not a Number) in tutte le colonne
                data = data.replace(0, np.nan)
                # 2. Usa il "forward fill" (ffill) per copiare l'ultimo dato valido noto al posto del NaN
                data = data.ffill()
                # 3. Se per assurdo i primissimi giorni fossero NaN, usa il "backward fill" (bfill)
                data = data.bfill()
                # ---------------------------------------------------------
                
                close = data['Close']
                high = data['High']
                low = data['Low']
                dati_storici_all[symbol] = data.copy()

                # Estrazione Pattern per HTML (usiamo PatternAnalyzer esplicitamente per ottenere il TESTO)
                analyzer = PatternAnalyzer(data)
                pat_score_val, pat_text_names = analyzer.get_pattern_info()
                
                # Logica Colori e Stati (In Inglese per output internazionale)
                pat_sentiment_str = "NEUTRAL"
                pat_color = "black"
                if pat_score_val >= 0.3: 
                    pat_sentiment_str = "BULLISH"
                    pat_color = "green"
                elif pat_score_val <= -0.3: 
                    pat_sentiment_str = "BEARISH"
                    pat_color = "red"
                    
                # Calcolo Hybrid Score
                hybrid_prob = scorer.calculate_probability(data, s7_raw, news_count_7, leader_val, is_leader, delta_val)
                percentuali_combine[symbol] = hybrid_prob 
                signal_str, sig_col = scorer.get_signal(hybrid_prob)

                current_price = float(close.iloc[-1])
                current_prices_map[symbol] = current_price
    
                # --- LOGGING SILENZIOSO ---
                # Questo salva i dati in memoria, non tocca file, non stampa nulla.
                # Non influenza i tuoi report "classifica.html" o altro.
                backtester.log_new_prediction(symbol, hybrid_prob, current_price)
                # --------------------------
            
                # Crescita Settimanale
                try:
                    # 1. Prezzo Attuale
                    last_price = close.iloc[-1]
                    last_date = close.index[-1]

                    # 2. Data Target: Esattamente 7 giorni fa
                    target_date = last_date - timedelta(days=7)

                    # 3. Trova il prezzo in quella data (o il giorno di borsa aperta precedente)
                    # 'asof' cerca il valore all'indice specificato o quello immediatamente precedente
                    prev_price = close.asof(target_date)

                    # Se non trova nulla (es. storico troppo breve), usa un fallback a 5 candele fa
                    if pd.isna(prev_price):
                        idx = max(0, len(close) - 6)
                        prev_price = close.iloc[idx]

                    # 4. Calcolo Variazione
                    growth = ((last_price - prev_price) / prev_price) * 100
                    crescita_settimanale[symbol] = round(growth, 2)
                except: 
                    crescita_settimanale[symbol] = 0.0

                # --- INDICATORI TECNICI COMPLETI ---
                rsi = RSIIndicator(close).rsi().iloc[-1]
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
                
                # --- FONDAMENTALI COMPLETI ---
                tk_obj = yf.Ticker(adjusted_symbol)
                try:
                    info = tk_obj.info or {}
                    def safe_value(key):
                        val = info.get(key)
                        return round(val, 4) if isinstance(val, (int, float)) else "N/A"

                    # --- DATI ISTITUZIONALI ---
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
                    # --- FINE DATI ISTITUZIONALI ---
                    
                    # Salviamo la Market Cap nei dati globali ma la teniamo fuori dalla tabella HTML singola
                    m_cap_raw = info.get("marketCap", "N/A")
                    fundamental_data[symbol] = {"Market Cap": m_cap_raw}

                    # Dizionario per il file SINGOLO (senza Market Cap)
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
                
                # Storico HTML
                hist = data.copy()
                hist['Date'] = hist.index.strftime('%Y-%m-%d')
                dati_storici_html = hist[['Date','Close','High','Low','Open','Volume']].to_html(index=False, border=1)

                # --- INSIDER TRADING (SELLS & BUYS CON CACHE GIORNALIERA) ---
                sells_data = None
                buys_data = None
                dati_da_cache = False

                # 1. CONTROLLO CACHE
                if adjusted_symbol in insider_cache and insider_cache[adjusted_symbol].get("date") == oggi_str:
                    sells_data = insider_cache[adjusted_symbol].get("sells")
                    buys_data = insider_cache[adjusted_symbol].get("buys")
                    dati_da_cache = True

                # 2. SCARICAMENTO (Solo se non in cache)
                if not dati_da_cache:
                    # --- LOGICA DI SMISTAMENTO BLINDATA ---
                    is_crypto_forex_index = any(x in str(adjusted_symbol) for x in ["=", "^", "-USD"])
                    # Un'azione USA tipicamente NON ha punti nel ticker (es: AAPL, MSFT, TSLA)
                    # Le azioni internazionali hanno sempre il suffisso dopo il punto (es: ISP.MI, SAP.DE)
                    is_international = "." in str(adjusted_symbol) and not is_crypto_forex_index
                    
                    if is_crypto_forex_index:
                        # SKIP TOTALE: Crypto, Forex e Indici NON hanno insider trading.
                        pass

                    elif not is_international:
                        # --- A. SOLO OPENINSIDER (Azioni USA) ---
                        try:
                            url = f"http://openinsider.com/screener?s={adjusted_symbol}&o=&cnt=1000"
                            tables = pd.read_html(url)
                            if len(tables) > 0:
                                insider_trades = max(tables, key=lambda t: t.shape[0])
                                insider_trades['Value_clean'] = insider_trades['Value'].replace(r'[\$,]', '', regex=True).astype(float)
                                
                                # Processa SELLS
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

                                # Processa BUYS
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
                        # --- B. SOLO API PROFESSIONALE (Europa, Asia, ecc.) ---
                        try:
                            API_KEY = FMP_API_KEY
                            if not API_KEY:
                                print("ATTENZIONE: Chiave FMP mancante!")
                            url_api = f"https://financialmodelingprep.com/api/v4/insider-trading?symbol={adjusted_symbol}&page=0&apikey={API_KEY}"
                            
                            import requests
                            response = requests.get(url_api)
                            
                            if response.status_code == 200 and len(response.json()) > 0:
                                api_df = pd.DataFrame(response.json())
                                
                                if 'transactionDate' in api_df.columns and 'transactionType' in api_df.columns:
                                    api_df['Trade Date'] = pd.to_datetime(api_df['transactionDate'])
                                    if 'securitiesTransacted' in api_df.columns and 'price' in api_df.columns:
                                        api_df['Value_clean'] = api_df['securitiesTransacted'] * api_df['price']
                                    else:
                                        api_df['Value_clean'] = 0.0 

                                    # Processa SELLS 
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

                                    # Processa BUYS
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

                    # 3. SALVA IN CACHE
                    insider_cache[adjusted_symbol] = {
                        "date": oggi_str,
                        "sells": sells_data,
                        "buys": buys_data
                    }
                    try:
                        with open(CACHE_FILE, "w") as f:
                            json.dump(insider_cache, f)
                    except Exception: pass

        except Exception as e: print(f"Err {symbol}: {e}")
        
        # 4. Generazione HTML Singolo (Struttura Aggiornata + Dati Completi)
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
        
        # RENDERING SELLS
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

        # RENDERING BUYS
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

        # --- TABELLA HTML ISTITUZIONALE ---
        html_content.append("<h2>Analisi Istituzionale & Sentiment</h2>")
        
        # Usiamo try-except nel caso in cui i dati alternativi non siano stati valorizzati per un errore API
        try:
            html_content.append("<table border='1' style='border-collapse:collapse; width:100%; text-align:left;'>")
            for chiave, valore in dati_alternativi.items():
                # Evitiamo di stampare le righe vuote se Yahoo non ha i dati
                if valore != "N/A" and valore != "N/A%":
                    html_content.append(f"<tr><td style='padding:8px;'><strong>{chiave}:</strong></td><td style='padding:8px;'>{valore}</td></tr>")
            html_content.append("</table>")
        except NameError:
            html_content.append("<p>Dati istituzionali non disponibili.</p>")
        # --- FINE TABELLA ISTITUZIONALE ---
        
        html_content.append("<h2>Dati Storici (ultimi 90 giorni)</h2>")
        html_content.append(dati_storici_html if dati_storici_html else "<p>N/A</p>")
        html_content.append("</body></html>")
        
        try:
            full_html = "\n".join(html_content)
            save_to_supabase(file_res, full_html)
        except: pass

        # Raccolta News
        for title, date, link, src, img in news_data["last_90_days"]:
            # Calcolo sentiment usando l'oggetto 'sia' globale già inizializzato a inizio script
            sc = (sia.polarity_scores(title)['compound'] + 1) / 2
            # IMPORTANTE: Aggiungiamo 'date' alla fine della tupla salvata
            all_news_entries.append((symbol, title, sc, link, src, img, date))

    
    # --- SALVATAGGIO TEST (Alla fine del loop, prima del return) ---
    print("Elaborazione Forward Testing in corso...")
    backtester.update_daily_tracking(current_prices_map) # Calcola risultati
    backtester.save_data()       # Scrive il JSON in forward_testing/
    backtester.generate_report() # Scrive l'HTML in forward_testing/
    # ---------------------------------------------------------------

    history_mgr.save_data_to_supabase()
    return (sentiment_results, percentuali_combine, all_news_entries, 
            indicator_data, fundamental_data, crescita_settimanale, dati_storici_all, momentum_results)


# ==============================================================================
# 5. ESECUZIONE
# ==============================================================================

sentiment_for_symbols, percentuali_combine, all_news_entries, indicator_data, fundamental_data, crescita_settimanale, dati_storici_all, momentum_results = get_sentiment_for_all_symbols(symbol_list)

# --- CLASSIFICA PRINCIPALE (BASATA SU HYBRID SCORE) ---
sorted_symbols = sorted(percentuali_combine.items(), key=lambda x: x[1], reverse=True)

# Ho aggiornato il nome delle due nuove intestazioni per riflettere la nuova logica
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
            
            # Estrazione sicura
            close_p = df['Close'].iloc[:, 0] if isinstance(df['Close'], pd.DataFrame) else df['Close']
            high_p = df['High'].iloc[:, 0] if isinstance(df['High'], pd.DataFrame) else df['High']
            low_p = df['Low'].iloc[:, 0] if isinstance(df['Low'], pd.DataFrame) else df['Low']
            
            # 1. VARIAZIONE 1G
            if len(close_p) >= 2:
                oggi = close_p.iloc[-1]
                ieri = close_p.iloc[-2]
                variazione = ((oggi - ieri) / ieri) * 100
                variazione_str = f"{variazione:+.2f}%"
                
            # 2. NUOVO MAX/MIN (52 Settimane con fallback a 5 Giorni)
            if len(close_p) >= 6:
                high_today = high_p.iloc[-1]
                low_today = low_p.iloc[-1]
                trovato_52w = False
                
                # Test 52 settimane (252 giorni borsa)
                # Calcola il max/min escludendo la candela di oggi, poi confronta
                if len(close_p) >= 252:
                    max_52w = high_p.iloc[-253:-1].max()
                    min_52w = low_p.iloc[-253:-1].min()
                    if high_today >= max_52w:
                        info_52w = f"Nuovo Max 52W ({high_today:.2f})"
                        trovato_52w = True
                    elif low_today <= min_52w:
                        info_52w = f"Nuovo Min 52W ({low_today:.2f})"
                        trovato_52w = True
                        
                # Se non trovato a 52W (o se l'asset ha meno di 1 anno di vita), testa i 5 giorni
                if not trovato_52w:
                    max_5g = high_p.iloc[-6:-1].max()
                    min_5g = low_p.iloc[-6:-1].min()
                    if high_today >= max_5g:
                        info_52w = f"Nuovo Max 5G ({high_today:.2f})"
                    elif low_today <= min_5g:
                        info_52w = f"Nuovo Min 5G ({low_today:.2f})"
                    
            # 3. CROSS DOWN / UP SMA (5 e 20)
            if len(close_p) >= 21:
                s5 = close_p.rolling(window=5).mean()
                s20 = close_p.rolling(window=20).mean()
                
                p_ieri, p_oggi = close_p.iloc[-2], close_p.iloc[-1]
                s5_ieri, s5_oggi = s5.iloc[-2], s5.iloc[-1]
                s20_ieri, s20_oggi = s20.iloc[-2], s20.iloc[-1]
                
                # Logica incroci: <= prima e > dopo (UP) | >= prima e < dopo (DOWN)
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

    # Aggiunta in tabella preservando i vecchi TD
    html_classifica.append(f"<tr><td>{symbol}</td><td>{score:.2f}%</td><td>{variazione_str}</td><td>{info_52w}</td><td>{cross_sma}</td></tr>")

html_classifica.append("</table></body></html>")

save_to_supabase(file_path, "\n".join(html_classifica))

print("Classifica aggiornata con successo!")



# --- CLASSIFICA PRO ---
sorted_symbols_pro = sorted(percentuali_combine.items(), key=lambda x: x[1], reverse=True)
html_classifica_pro = ["<html><head><title>Classifica Combinata</title></head><body>",
                       "<h1>Classifica Combinata (Hybrid Logic)</h1>",
                       "<table border='1'><tr><th>Simbolo</th><th>Hybrid Score</th></tr>"]
for symbol, media in sorted_symbols_pro:
    html_classifica_pro.append(f"<tr><td>{symbol}</td><td>{media:.2f}%</td></tr>")
html_classifica_pro.append("</table></body></html>")

save_to_supabase(pro_path, "\n".join(html_classifica_pro))

print("Classifica PRO aggiornata!")


# --- CLASSIFICA MOMENTUM ---
print("Generazione Classifica Momentum...")

# Ordina dal più alto al più basso
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
    # Definizione colore e stato
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

# Salvataggio su GitHub
save_to_supabase(mom_path, "\n".join(html_mom))

print("Classifica Momentum creata con successo!")


# --- CLASSIFICA SETTORI (RVOL & RETRO-COMPATIBILITA') ---
print("Generazione Classifica Settori (RVOL Weighted)...")

# 1. Raccogliamo i dati grezzi per calcolare i pesi
sector_assets = defaultdict(list)

for symbol, score in percentuali_combine.items():
    sec = asset_sector_map.get(symbol, "Altro")
    
    avg_liquidity_old = 0.0
    rvol = 1.0 # Default neutro
    
    if symbol in dati_storici_all:
        df = dati_storici_all[symbol]
        try:
            # Prendiamo gli ultimi 20 giorni
            last_month = df.tail(20).copy()
            
            # --- VECCHIO CALCOLO (Mantenuto per NON far crashare l'app in produzione) ---
            liquidity_series = (last_month['Close'] * last_month['Volume']).fillna(0)
            avg_liquidity_old = liquidity_series.mean()
            if avg_liquidity_old <= 0 or pd.isna(avg_liquidity_old):
                avg_liquidity_old = 1000.0 
                
            # --- NUOVO CALCOLO PROFESSIONALE (RVOL) ---
            vol_today = last_month['Volume'].iloc[-1]
            vol_mean = last_month['Volume'].mean()
            if pd.notna(vol_today) and vol_mean > 0:
                rvol = vol_today / vol_mean
            # Limitiamo gli eccessi tra 0.1 e 10.0 per evitare distorsioni matematiche
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
        'rvol': rvol
    })

# 2. Calcolo Score Ponderato per Settore
sector_final_scores = []

for sec, assets in sector_assets.items():
    # Somma della VECCHIA liquidità (per la colonna letta dalla vecchia App)
    total_sector_liquidity_old = sum(a['liquidity_old'] for a in assets)
    
    # Somma del NUOVO RVOL (Per la nuova colonna e per il calcolo dello score)
    total_sector_rvol = sum(a['rvol'] for a in assets)
    
    weighted_score_sum = 0.0
    asset_count = len(assets)
    
    # Trova il leader in base a chi sta spingendo di più oggi (RVOL)
    top_asset = max(assets, key=lambda x: x['rvol'])
    leader_name = top_asset['symbol']
    
    for asset in assets:
        # Ponderiamo il settore sul NUOVO RVOL (molto più professionale e veritiero)
        weight = asset['rvol'] / total_sector_rvol if total_sector_rvol > 0 else (1.0 / asset_count)
        weighted_score_sum += (asset['score'] * weight)
        
    sector_final_scores.append({
        'sector': sec,
        'avg': weighted_score_sum,
        'count': asset_count,
        'leader': leader_name,
        'total_vol_old': total_sector_liquidity_old,
        'sector_rvol': round((total_sector_rvol / asset_count), 2)
    })

# 3. Ordinamento
sorted_sectors = sorted(sector_final_scores, key=lambda x: x['avg'], reverse=True)

# 4. Generazione HTML
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
    "<p>Classifica ponderata sul <b>Volume Relativo (RVOL)</b>. I settori con volumi in accelerazione rispetto alla media dominano il punteggio.</p>",
    "<table><tr><th>Pos</th><th>Settore</th><th>Dominant Asset</th><th>Score Ponderato</th><th>Asset</th><th>Trend</th><th>Volume Movimentato</th><th>RVOL (Nuovo)</th></tr>"
]

for idx, item in enumerate(sorted_sectors, 1):
    avg = item['avg']
    vol_int = int(item['total_vol_old']) # Il vecchio valore per non far crashare l'App!
    rvol_val = item['sector_rvol']
    
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
    
    # Attenzione all'ordine dei td: Volume Movimentato resta il 7° elemento (indice 6 in java)
    # RVOL viene aggiunto alla fine come 8° elemento (indice 7 in java)
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
        f"</tr>"
    )

html_sector.append("</table></body></html>")

# 5. Salvataggio
save_to_supabase(sector_path, "\n".join(html_sector))

print("Classifica Settori (RVOL) aggiornata con successo in totale sicurezza!")



# --- NEWS HTML ---
html_news = ["<html><head><title>Notizie e Sentiment</title></head><body>",
             "<h1>Notizie Finanziarie con Sentiment</h1>",
             "<table border='1'><tr><th>Simbolo</th><th>Notizia</th><th>Fonte</th><th>Immagine</th><th>Sentiment</th><th>Link</th><th>Data/Ora</th></tr>"]
news_by_symbol = defaultdict(list)

# Estraiamo anche la 'date' dalla tupla appena aggiornata
for symbol, title, sentiment, url, source, image, date in all_news_entries:
    news_by_symbol[symbol].append((title, sentiment, url, source, image, date))

for symbol, entries in news_by_symbol.items():
    sorted_entries = sorted(entries, key=lambda x: x[1])
    selected_entries = sorted_entries[:5] + sorted_entries[-5:]
    selected_entries = list(dict.fromkeys(selected_entries))
    
    for title, sentiment, url, source, image, date in selected_entries:
        img_html = f"<img src='{image}' width='100'>" if image else "N/A"
        
        # Formattazione della data in Anno-Mese-Giorno Ora:Minuto:Secondo
        date_str = date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(date, 'strftime') else "N/A"
        
        # Aggiunta della colonna data/ora in coda (7° colonna)
        html_news.append(f"<tr><td>{symbol}</td><td>{title}</td><td>{source}</td><td>{img_html}</td><td>{sentiment:.2f}</td><td><a href='{url}' target='_blank'>Leggi</a></td><td>{date_str}</td></tr>")

html_news.append("</table></body></html>")

save_to_supabase(news_path, "\n".join(html_news))

print("News aggiornata (con data e ora)!")

# --- CLASSIFICA FIRE ---
sorted_crescita = sorted([(s, g) for s, g in crescita_settimanale.items() if g is not None], key=lambda x: (x[1], x[0]), reverse=True)
html_fire = ["<html><head><title>Classifica per Crescita</title></head><body>",
             "<h1>Asset Ordinati per Crescita Settimanale</h1>",
             "<table border='1'><tr><th>Simbolo</th><th>Crescita 7gg (%)</th></tr>"]
for symbol, growth in sorted_crescita:
    html_fire.append(f"<tr><td>{symbol}</td><td>{growth:.2f}%</td></tr>")
html_fire.append("</table></body></html>")

save_to_supabase(fire_path, "\n".join(html_fire))

print("Fire aggiornato!")



# ==============================================================================
# 6. NEW DAILY BRIEF V2 (FULL DATABASE & DYNAMIC AI COPYWRITING)
# ==============================================================================
import random
print("Generazione Daily Brief V2 Data...")

# --- DIZIONARIO INSIGHT DINAMICI (Molteplici varianti in 14 Lingue) ---
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

# --- MOTORE MACRO DINAMICO (Pre-tradotto in 14 Lingue con Varianti) ---
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
            "hi": "सुरक्षा की ओर उड़ान जारी है। वैश्विक बाजार की चिंता बढ़ने से सोना और चांदी में प्रवाह देखा जा रहा है।",
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

def calculate_support_resistance(df):
    if len(df) < 20: return 0.0, 0.0
    recent_low = df['Low'].tail(20).min()
    recent_high = df['High'].tail(20).max()
    return round(recent_low, 2), round(recent_high, 2)

all_analyzed_assets = []

# --- 1. CALCOLO ANALISI ASSET GLOBALI ---
for sym, score in percentuali_combine.items():
    if sym not in dati_storici_all: continue
    df = dati_storici_all[sym]
    if len(df) < 20: continue
    
    # 1. Estrai l'ultimo volume disponibile
    vol_today = df['Volume'].iloc[-1]
    
    # 2. Controllo Mercato Chiuso / Dati Assenti
    if vol_today == 0 and len(df) >= 2:
        # Prendi il volume di ieri (ultima sessione valida)
        vol_today = df['Volume'].iloc[-2]
        # Calcola la media dei 20 giorni saltando la giornata odierna "vuota"
        vol_avg = df['Volume'].iloc[-21:-1].mean() if len(df) > 20 else df['Volume'].iloc[:-1].mean()
    else:
        # Se il volume c'è, calcola normalmente
        vol_avg = df['Volume'].tail(20).mean()
        
    # 3. Calcolo finale del Volume Relativo (RVOL)
    vol_surge = (vol_today / vol_avg) if vol_avg > 0 else 1.0
    
    rsi = indicator_data.get(sym, {}).get("RSI (14)", 50)
    pat_score, _ = PatternAnalyzer(df).get_pattern_info()
    sup, res = calculate_support_resistance(df)
    current_price = df['Close'].iloc[-1]
    
    dist_to_sup = abs(current_price - sup) / current_price if sup > 0 else 1.0
    dist_to_res = abs(current_price - res) / current_price if res > 0 else 1.0

    # CALCOLO ANOMALY SCORE E TRAIT
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

    # Score base per asset non anomali (più si allontanano da 50, più sono rilevanti)
    if anomaly_score == 0:
        anomaly_score = abs(score - 50) / 10.0

    # Dati Extra
    macd_line = indicator_data.get(sym, {}).get("MACD Line", 0)
    macd_sig = indicator_data.get(sym, {}).get("MACD Signal", 0)
    macd_trend = "Bull" if macd_line > macd_sig else "Bear"
    confluence = f"RSI: {round(rsi)} | MACD: {macd_trend} | Vol: {round(vol_surge, 1)}x"
    volatility = "High" if vol_surge > 1.5 or rsi > 70 or rsi < 30 else "Normal"
    
    # Assegnazione Testo
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

# ORDINAMENTO ASSOLUTO PER ANOMALIA (Dal maggiore impatto al minore)
all_analyzed_assets = sorted(all_analyzed_assets, key=lambda x: x['anomaly_score'], reverse=True)


# --- 2. MOTORE MACRO DINAMICO (Market Breadth) ---
bullish_count = sum(1 for score in percentuali_combine.values() if score >= 50)
total_assets_count = len(percentuali_combine)
breadth_ratio = bullish_count / total_assets_count if total_assets_count > 0 else 0.5

safe_haven_tickers = ["GC=F", "SI=F", "XAUUSD", "GLD", "SLV"]
safe_haven_surging = False
for ticker in safe_haven_tickers:
    if ticker in percentuali_combine and percentuali_combine[ticker] > 65:
        safe_haven_surging = True
        break

macro_theme = "consolidation" # Default
if breadth_ratio > 0.65:
    macro_theme = "strong_bull"
elif breadth_ratio < 0.35:
    if safe_haven_surging:
        macro_theme = "safe_haven"
    else:
        macro_theme = "strong_bear"


# ==============================================================================
# 3. GENERAZIONE HTML CON SELEZIONE CASUALE MODELLI DI TESTO
# ==============================================================================
html_v2 = ["<html><body>"]

# Helper che estrae a caso una variante di testo dalla lista nel dizionario
def get_randomized_lang_attributes(trait, dictionary_pool):
    # Recupera l'array di varianti per quel trait, oppure il generic_bull come fallback
    variations_list = dictionary_pool.get(trait, dictionary_pool["generic_bull"])
    # Sceglie una singola variante (dizionario lingue) in modo casuale
    selected_lang_data = random.choice(variations_list)
    
    attrs = []
    for lang, text in selected_lang_data.items():
        safe_text = text.replace("'", "&apos;")
        attrs.append(f"data-{lang}='{safe_text}'")
    return " ".join(attrs)


# --- Stampa Macro Insight ---
# Sceglie casualmente un dizionario di lingue dall'array di MACRO_SCENARIOS
selected_macro_data = random.choice(MACRO_SCENARIOS[macro_theme])
macro_attrs_list = []
for lang, text in selected_macro_data.items():
    safe_text = text.replace("'", "&apos;")
    macro_attrs_list.append(f"data-{lang}='{safe_text}'")

macro_attrs = " ".join(macro_attrs_list)
html_v2.append(f"<div id='macro_insight' {macro_attrs}></div>")


# --- Stampa Database Asset ---
for cand in all_analyzed_assets:
    sym = cand['sym']
    name = symbol_name_map.get(sym, [sym])[0]
    
    # Per ogni asset genera un copy dinamico richiamando l'helper
    lang_attrs = get_randomized_lang_attributes(cand['trait'], INSIGHT_DICT)
    
    html_v2.append(f"<div class='asset_data' data-ticker='${sym}' data-clean-ticker='{sym}' data-name='{name}' data-score='{int(cand['score'])}' data-anomaly='{cand['anomaly_score']}' {lang_attrs} data-confluence='{cand['confluence']}' data-volatility='{cand['volatility']}' data-move='{cand['expected_move']}' data-sup='{cand['sup']}' data-res='{cand['res']}'></div>")

html_v2.append("</body></html>")


# --- SALVATAGGIO SU GITHUB ---
v2_path = f"{TARGET_FOLDER}/daily_brief_v2_data.html"
save_to_supabase(v2_path, "\n".join(html_v2))

print("Daily Brief V2 (Dynamic Copy & Macro) salvato con successo!")



# --- CORRELAZIONI STATISTICHE (COMPLETA) ---
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
            # Allineamento serie temporali standard
            common = s1.index.intersection(s2.index)
            
            # Filtro robustezza: servono almeno 60 giorni di borsa in comune
            if len(common) < 60: continue
            
            x = s1.loc[common]
            y = s2.loc[common]
            
            # --- 1. GESTIONE EFFETTO LAG (Ritardo Fusi Orari) ---
            # Calcolo Pearson standard
            try: p_r_std, _ = pearsonr(x, y)
            except: p_r_std = 0.0
            
            # Calcolo Pearson con Lag di 1 giorno per l'Asset 2
            y_lagged = s2.shift(1).loc[common].dropna()
            x_aligned = x.loc[y_lagged.index]
            try:
                if len(x_aligned) > 30:
                    p_r_lag, _ = pearsonr(x_aligned, y_lagged)
                else:
                    p_r_lag = 0.0
            except: p_r_lag = 0.0
            
            # Il sistema sceglie automaticamente la correlazione più forte
            if abs(p_r_lag) > abs(p_r_std):
                p_r = p_r_lag
                lag_usato = True
            else:
                p_r = p_r_std
                lag_usato = False

            # Spearman (calcolato sulla serie standard)
            try: s_r, _ = spearmanr(x, y)
            except: s_r = 0.0
            
            # --- 2. VALORE ASSOLUTO E DIREZIONALITÀ ---
            conc = (np.sign(x) == np.sign(y)).mean() * 100
            # Mappa la concordanza da [0, 100] a [-1.0, 1.0] per il punteggio combinato
            conc_mapped = (conc / 50.0) - 1.0
            
            # Score finale combinato che rispetta la direzione (da -1.0 a +1.0)
            score = (p_r + s_r + conc_mapped) / 3.0
            
            # --- 3. CORRELAZIONE IN TEMPO DI CRISI (Downside Risk) ---
            # Filtra solo i giorni in cui l'Asset 1 ha perso più dell'1.5%
            giorni_di_crisi = x[x < -0.015].index
            
            if len(giorni_di_crisi) >= 5: # Requisito minimo statistico
                x_crisi = x.loc[giorni_di_crisi]
                y_crisi = y.loc[giorni_di_crisi]
                try: pearson_crisi, _ = pearsonr(x_crisi, y_crisi)
                except: pearson_crisi = None
            else:
                pearson_crisi = None # Non ci sono stati abbastanza crolli per calcolarla
                
            all_candidates.append({
                "asset2": asset2,
                "score": score,
                "pearson": p_r,
                "spearman": s_r,
                "concordance": conc,
                "lag_usato": lag_usato,
                "pearson_crisi": pearson_crisi
            })
            
        # --- SUDDIVISIONE TOP 10 DIRETTE E INVERSE ---
        all_candidates.sort(key=lambda item: item["score"], reverse=True)
        
        # Le 10 più forti in concordanza
        top_direct = [c for c in all_candidates if c["score"] > 0][:10]
        # Le 10 più forti in opposizione (dal più negativo verso lo zero)
        top_inverse = sorted([c for c in all_candidates if c["score"] < 0], key=lambda item: item["score"])[:10]
        
        results[asset1] = {
            "dirette": top_direct,
            "inverse": top_inverse
        }
        
    return results


def salva_correlazioni_html(correlazioni, file_path=corr_pro_path):
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
        
        # --- TOP 10 DIRETTE ---
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
            
        # --- TOP 10 INVERSE ---
        html_corr.append("<h3>🛡️ Top 10 Inverse (Potenziale Hedging / Copertura)</h3>")
        if data['inverse']:
            html_corr.append("<table><tr><th>Partner</th><th>Score Combinato</th><th>Pearson</th><th>Spearman</th><th>Concordanza Direz.</th><th>Stress Test (Crisi)</th><th>Lag Rilevato</th></tr>")
            for info in data['inverse']:
                lag_str = "⚠️ Sì (1g)" if info['lag_usato'] else "No"
                
                # Formattazione per evidenziare se un asset inverso smette di esserlo durante le crisi
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
    
    save_to_supabase(file_path, "\n".join(html_corr))

print("Calcolo Correlazioni...")
correlazioni = calcola_correlazioni(dati_storici_all)
salva_correlazioni_html(correlazioni)
print("Finito!")
