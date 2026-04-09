# financial_lexicon.py

# Dizionario esteso per Turbo-VADER
# Include varianti verbali (passato, presente, gerundio) e termini specifici di mercato.

LEXICON = {
    # --- HYPE, CRESCITA & POSITIVITÀ (Score: +1.5 a +4.0) ---
    'surge': 3.5, 'surges': 3.5, 'surged': 3.5, 'surging': 3.5,
    'soar': 3.5, 'soars': 3.5, 'soared': 3.5, 'soaring': 3.5,
    'rocket': 4.0, 'rocketing': 4.0, 'rocketed': 4.0,
    'jump': 2.5, 'jumps': 2.5, 'jumped': 2.5, 'jumping': 2.5,
    'rally': 3.0, 'rallies': 3.0, 'rallied': 3.0, 'rallying': 3.0,
    'boom': 3.0, 'booms': 3.0, 'booming': 3.0, 'boomed': 3.0,
    'spike': 2.5, 'spikes': 2.5, 'spiked': 2.5, 'spiking': 2.5,
    'explode': 3.0, 'explodes': 3.0, 'exploded': 3.0, 'exploding': 3.0,
    'breakout': 3.0, 'breaking out': 3.0,
    'all-time high': 3.0, 'ath': 3.0, 'record high': 3.0,
    'moon': 3.0, 'mooning': 3.0, # Crypto slang
    'bull': 2.5, 'bulls': 2.5, 'bullish': 2.5,
    'beat': 2.5, 'beats': 2.5, 'beating': 2.5,
    'outperform': 3.0, 'outperforms': 3.0, 'outperformed': 3.0, 'outperforming': 3.0,
    'upgrade': 3.0, 'upgrades': 3.0, 'upgraded': 3.0, 'upgrading': 3.0,
    'buy': 2.0, 'buys': 2.0, 'buying': 2.0, 'bought': 2.0,
    'long': 1.5, # "Go long"
    'green': 1.5,
    'profit': 2.0, 'profits': 2.0, 'profitable': 2.5, 'profitability': 2.5,
    'revenue': 1.5, 'revenues': 1.5,
    'gain': 2.0, 'gains': 2.0, 'gained': 2.0, 'gaining': 2.0,
    'growth': 2.0, 'grow': 2.0, 'grows': 2.0, 'growing': 2.0, 'grew': 2.0,
    'expand': 2.0, 'expands': 2.0, 'expanded': 2.0, 'expanding': 2.0, 'expansion': 2.0,
    'strong': 2.0, 'strength': 2.0, 'strengthen': 2.0, 'strengthens': 2.0,
    'rebound': 2.5, 'rebounds': 2.5, 'rebounded': 2.5, 'rebounding': 2.5,
    'recover': 2.0, 'recovers': 2.0, 'recovered': 2.0, 'recovering': 2.0, 'recovery': 2.0,
    'partnership': 2.0, 'partner': 1.5, 'partners': 1.5, 'partnered': 1.5,
    'agreement': 1.5, 'agreed': 1.5, 'deal': 1.5,
    'merger': 1.5, 'merged': 1.5, 'merging': 1.5,
    'acquisition': 1.5, 'acquire': 1.5, 'acquires': 1.5, 'acquired': 1.5, 'acquiring': 1.5,
    'approval': 2.5, 'approve': 2.5, 'approves': 2.5, 'approved': 2.5,
    'dividend': 1.5, 'dividends': 1.5,
    'split': 1.5, # Stock split usually bullish hype
    'buyback': 2.5, 'repurchase': 2.5,
    'innovation': 2.0, 'innovative': 2.0,
    'launch': 1.5, 'launches': 1.5, 'launched': 1.5, 'launching': 1.5,
    
    # --- CRASH, PANICO & RISCHIO (Score: -1.5 a -4.0) ---
    'crash': -4.0, 'crashes': -4.0, 'crashed': -4.0, 'crashing': -4.0,
    'plunge': -3.5, 'plunges': -3.5, 'plunged': -3.5, 'plunging': -3.5,
    'collapse': -4.0, 'collapses': -4.0, 'collapsed': -4.0, 'collapsing': -4.0,
    'tank': -3.0, 'tanks': -3.0, 'tanked': -3.0, 'tanking': -3.0,
    'dive': -3.0, 'dives': -3.0, 'dived': -3.0, 'diving': -3.0,
    'tumble': -3.0, 'tumbles': -3.0, 'tumbled': -3.0, 'tumbling': -3.0,
    'plummet': -3.5, 'plummets': -3.5, 'plummeted': -3.5, 'plummeting': -3.5,
    'slump': -3.0, 'slumps': -3.0, 'slumped': -3.0, 'slumping': -3.0,
    'drop': -2.5, 'drops': -2.5, 'dropped': -2.5, 'dropping': -2.5,
    'fall': -2.0, 'falls': -2.0, 'fell': -2.0, 'falling': -2.0, 'fallen': -2.0,
    'sell': -2.0, 'sells': -2.0, 'selling': -2.0, 'sold': -2.0,
    'selloff': -3.5, 'sell-off': -3.5,
    'bear': -2.5, 'bears': -2.5, 'bearish': -2.5,
    'short': -1.5, 'shorts': -1.5, 'shorting': -1.5, 'shorted': -1.5,
    'miss': -2.5, 'misses': -2.5, 'missed': -2.5, 'missing': -2.5,
    'loss': -2.5, 'losses': -3.0, 'lost': -2.5, 'lose': -2.5, 'losing': -2.5,
    'lower': -1.5, 'lowest': -1.5,
    'weak': -2.0, 'weakness': -2.0, 'weakens': -2.0, 'weakened': -2.0, 'weaker': -2.0,
    'fail': -3.0, 'fails': -3.0, 'failed': -3.0, 'failing': -3.0, 'failure': -3.5,
    'bankrupt': -4.0, 'bankruptcy': -4.0, 'bankruptcies': -4.0, 'insolvent': -4.0,
    'debt': -1.5, 'debts': -1.5, 'indebted': -2.0,
    'risk': -2.0, 'risks': -2.0, 'risky': -2.5,
    'uncertainty': -1.5, 'uncertain': -1.5,
    'volatile': -2.0, 'volatility': -2.0,
    'warning': -2.5, 'warn': -2.5, 'warns': -2.5, 'warned': -2.5,
    'downgrade': -3.0, 'downgrades': -3.0, 'downgraded': -3.0, 'downgrading': -3.0,
    'cut': -2.0, 'cuts': -2.0, 'cutting': -2.0,
    'halt': -2.5, 'halts': -2.5, 'halted': -2.5, 'halting': -2.5,
    'delist': -3.0, 'delisted': -3.0, 'delisting': -3.0,
    'investigation': -2.5, 'investigated': -2.5, 'investigating': -2.5, 'probe': -2.5,
    'lawsuit': -2.5, 'lawsuits': -2.5, 'sue': -2.5, 'sues': -2.5, 'sued': -2.5, 'suing': -2.5,
    'fraud': -4.0, 'frauds': -4.0, 'fraudulent': -4.0,
    'scandal': -3.5, 'scandals': -3.5,
    'breach': -3.0, 'hack': -3.0, 'hacked': -3.0,
    'sanction': -2.5, 'sanctions': -2.5, 'sanctioned': -2.5,
    'recession': -3.5, 'depression': -4.0,
    'inflation': -2.0, 'inflationary': -2.0,
    'headwind': -2.0, 'headwinds': -2.0,
    'correction': -2.0,
    
    # --- CORREZIONI NEUTRE (Per evitare errori di VADER) ---
    'vice': 0.0, 'president': 0.0, 
    'gross': 0.0, # Gross profit
    'mine': 0.0, # Gold mine
    'share': 0.0, 'shares': 0.0,
    'stock': 0.0, 'stocks': 0.0,
    'bond': 0.0, 'bonds': 0.0,
    'security': 0.0, 'securities': 0.0,
    'equity': 0.0, 'equities': 0.0,
    'fund': 0.0, 'funds': 0.0,
    'holding': 0.0, 'holdings': 0.0,
    'crude': 0.0, # Crude oil
    'fed': 0.0, 'federal': 0.0, 'reserve': 0.0,
    'yield': 0.0, 'yields': 0.0,
    'liability': -0.5, 'liabilities': -0.5, # Contabile, non tragico
    'expense': -0.5, 'expenses': -0.5,
    'cost': -0.5, 'costs': -0.5,
    'mature': 0.0, 'maturity': 0.0, # Bond maturity
    'volume': 0.0,
    'operations': 0.0,
    'company': 0.0, 'corp': 0.0, 'inc': 0.0
}
