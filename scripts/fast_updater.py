import os
import time
import traceback
import json
from datetime import datetime
import requests
import boto3
from botocore.config import Config

# --- CONFIGURAZIONE CLOUDFLARE R2 ---
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
BUCKET_NAME = "trading-data"  # Assicurati che sia esattamente il nome che hai dato al secchio su R2

if not R2_ACCOUNT_ID or not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
    print("ERRORE: Chiavi Cloudflare R2 mancanti nei Secrets di GitHub!")
    exit(1)

# Inizializzazione del Client R2 tramite Boto3
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4'),
    region_name='auto'  # R2 richiede 'auto' o 'weur' come standard
)

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

# Mappa inversa per trovare il nome dell'app partendo dal simbolo Yahoo
REVERSE_MAP = {}
for app_sym, yf_sym in TICKER_MAP.items():
    if yf_sym not in REVERSE_MAP:
        REVERSE_MAP[yf_sym] = []
    REVERSE_MAP[yf_sym].append(app_sym)

unique_yf_symbols = list(set(TICKER_MAP.values()))

# --- FUNZIONE PER GENERARE COOKIE E CRUMB ---
def get_yahoo_session_and_crumb():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    })
    
    try:
        session.get("https://fc.yahoo.com", timeout=5)
    except: pass
    
    try:
        session.get("https://finance.yahoo.com", timeout=5)
    except: pass

    crumb = ""
    try:
        res = session.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=5)
        if res.status_code == 200:
            crumb = res.text.strip()
            print(f"[OK] Yahoo Crumb ottenuto: {crumb}")
    except:
        print("[WARNING] Errore di rete durante la richiesta del Crumb.")
        
    return session, crumb

# Inizializziamo sessione e crumb
yahoo_session, yahoo_crumb = get_yahoo_session_and_crumb()

def fetch_and_upload():
    global yahoo_session, yahoo_crumb
    
    try:
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Download snapshot via Quote API...")
        
        # Dizionario temporaneo per raccogliere i dati disordinati
        temp_snapshot = {}
        
        chunk_size = 80
        for i in range(0, len(unique_yf_symbols), chunk_size):
            chunk = unique_yf_symbols[i:i + chunk_size]
            symbols_string = ",".join(chunk)
            
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_string}"
            if yahoo_crumb:
                url += f"&crumb={yahoo_crumb}"
            url += f"&_t={int(time.time())}" # <--- QUESTA RIGA FREGA LA CACHE DI YAHOO
            
            response = yahoo_session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("quoteResponse", {}).get("result", [])
                
                for quote in results:
                    yf_sym = quote.get("symbol")
                    
                    # Estrazione OCHLV + Timestamp
                    price = quote.get("regularMarketPrice")
                    open_price = quote.get("regularMarketOpen", quote.get("regularMarketPreviousClose", price))
                    high_price = quote.get("regularMarketDayHigh", price)
                    low_price = quote.get("regularMarketDayLow", price)
                    volume = quote.get("regularMarketVolume", 0)
                    timestamp = quote.get("regularMarketTime", int(time.time()))
                    
                    if price is not None and yf_sym in REVERSE_MAP:
                        for app_sym in REVERSE_MAP[yf_sym]:
                            temp_snapshot[app_sym] = {
                                "price": float(price),
                                "open": float(open_price),
                                "high": float(high_price),
                                "low": float(low_price),
                                "volume": float(volume),
                                "timestamp": int(timestamp)
                            }
            elif response.status_code == 401:
                print(f"Errore 401 sul blocco {i}. Rigenero il Crumb...")
                yahoo_session, yahoo_crumb = get_yahoo_session_and_crumb()
            else:
                print(f"Errore API Yahoo sul blocco {i}: HTTP {response.status_code}")

        if not temp_snapshot:
            print("Avviso: Nessun dato valido recuperato.")
            return
            
        # RIORDINO DEI DATI SECONDO LA TICKER_MAP ORIGINALE
        ordered_snapshot = {}
        for app_sym in TICKER_MAP.keys():
            if app_sym in temp_snapshot:
                ordered_snapshot[app_sym] = temp_snapshot[app_sym]
        
        # Preparazione del payload JSON in formato testo
        payload = {
            "data": ordered_snapshot, 
            "updated_at": datetime.utcnow().isoformat()
        }
        json_data = json.dumps(payload)
        
        # Upload diretto del file JSON su Cloudflare R2
        # Upload diretto del file JSON su Cloudflare R2
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='prezzi.json',
            Body=json_data,
            ContentType='application/json',
            # 🚀 FORZA CLOUDFLARE E IL TELEFONO A SCADERE DOPO 45 SECONDI
            CacheControl='max-age=45' 
        )
        
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Cloudflare R2 aggiornato ({len(ordered_snapshot)} asset).")
        
    except Exception as e:
        print(f"Errore critico durante l'aggiornamento: {e}")
        traceback.print_exc()

def run_loop():
    # Timeout di 4 ore e 45 minuti per coincidere con il cron di GitHub
    timeout_minutes = 285 
    timeout = time.time() + (timeout_minutes * 60)
    
    print(f"Avvio Fast Updater (R2 API). Durata massima: {timeout_minutes} minuti.")
    print("Aggiornamento ogni 50 secondi...") # 🚀 Coerenza con lo sleep
    
    while time.time() < timeout:
        start_time = time.time()
        fetch_and_upload()
        
        elapsed = time.time() - start_time
        
        # 🚀 50 secondi è perfetto: Yahoo respira e tu hai dati freschi
        sleep_time = max(5, 50 - elapsed)
        
        if time.time() + sleep_time < timeout:
            time.sleep(sleep_time)
        else:
            print("Tempo limite raggiunto. Il job termina.")
            break

if __name__ == "__main__":
    run_loop()
