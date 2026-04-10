import os
import time
import json
import traceback
from datetime import datetime
import requests
from supabase import create_client, Client

# --- CONFIGURAZIONE SUPABASE ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti!")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- LA TUA LISTA COMPLETA ---
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
    "EL": "EL", "GM": "GM", "CME": "CME", "EW": "EW",
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

# 1. Creiamo una mappa inversa per ritrovare facilmente la chiave della tua app
REVERSE_MAP = {}
for app_sym, yf_sym in TICKER_MAP.items():
    if yf_sym not in REVERSE_MAP:
        REVERSE_MAP[yf_sym] = []
    REVERSE_MAP[yf_sym].append(app_sym)

# Lista unica dei simboli per Yahoo (così evitiamo richieste duplicate se due asset puntano allo stesso ticker)
unique_yf_symbols = list(set(TICKER_MAP.values()))

# Simuliamo un browser reale
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
})

def fetch_and_upload():
    try:
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Download snapshot via Quote API...")
        snapshot = {}
        
        # Dividiamo in blocchi da 80 ticker per evitare URL troppo lunghi
        chunk_size = 80
        for i in range(0, len(unique_yf_symbols), chunk_size):
            chunk = unique_yf_symbols[i:i + chunk_size]
            symbols_string = ",".join(chunk)
            
            # API velocissima e ufficiale di Yahoo Finance (restituisce solo un JSON con i prezzi attuali)
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_string}"
            
            response = session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("quoteResponse", {}).get("result", [])
                
                for quote in results:
                    yf_sym = quote.get("symbol")
                    price = quote.get("regularMarketPrice")
                    open_price = quote.get("regularMarketPreviousClose", price) # Usiamo la chiusura precedente come base per le %
                    
                    if price is not None and yf_sym in REVERSE_MAP:
                        for app_sym in REVERSE_MAP[yf_sym]:
                            snapshot[app_sym] = {
                                "price": float(price),
                                "open": float(open_price)
                            }
            else:
                print(f"Errore API Yahoo sul blocco {i}: HTTP {response.status_code}")

        if not snapshot:
            print("Avviso: Nessun dato valido recuperato.")
            return
        
        # Salviamo il "mega-dizionario" su Supabase in una volta sola
        payload = {"id": 1, "data": snapshot, "updated_at": datetime.utcnow().isoformat()}
        supabase.table("market_snapshot").upsert(payload).execute()
        
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Supabase aggiornato con successo ({len(snapshot)} asset).")
        
    except Exception as e:
        print(f"Errore critico durante l'aggiornamento: {e}")
        traceback.print_exc()

def run_loop():
    # Riduciamo il loop totale a 14 minuti per sicurezza del job GitHub
    timeout = time.time() + (14 * 60)
    print(f"Avvio Fast Updater (Quote API). Aggiornamento ogni 60 secondi...")
    
    while time.time() < timeout:
        start_time = time.time()
        fetch_and_upload()
        
        # Aspetta 60 secondi prima di ripetere
        elapsed = time.time() - start_time
        sleep_time = max(5, 60 - elapsed) 
        
        if time.time() + sleep_time < timeout:
            time.sleep(sleep_time)
        else:
            break

if __name__ == "__main__":
    run_loop()
