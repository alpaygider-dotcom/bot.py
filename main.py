import asyncio
import aiohttp
import os
import time
import random
import json
from statistics import mean, median, stdev
from collections import deque
import traceback
from datetime import datetime, timedelta

# =========================================================
# AYARLAR
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ BOT_TOKEN veya CHAT_ID ortam değişkeni eksik!")
    exit(1)

FAPI_URL = "https://fapi.binance.com"
SPOT_GLOBAL_URL = "https://api.binance.com"

SCAN_INTERVAL = 20
COOLDOWN_BASE = 3600
GLOBAL_COOLDOWN = 90
MAX_SIGNALS_PER_ROUND = 2
MIN_SCORE = 24          # Kullanıcı isteğiyle 24 yapıldı

TP_MULT = 10
SL_MULT = 5
MAX_TP_PCT = 8.0

CACHE_5M = 35
CACHE_1M = 20
CACHE_1H = 300
CACHE_OI = 120
CACHE_FUNDING = 300
CACHE_LS_5M = 60

BATCH_SIZE = 25
MAX_CONSECUTIVE_ERRORS = 15
SEMAPHORE = asyncio.Semaphore(20)

STABLECOIN_BLACKLIST = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT", "USDTUSDT", "FDUSDUSDT"}
MAJOR_COINS_BLACKLIST = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}
COMMODITY_BLACKLIST = {"PAXGUSDT"}

# Sabit Binance TR coin listesi (kullanıcı tarafından sağlanmıştır)
TR_COIN_LIST = sorted([
    "PORTAL", "NFP", "HEI", "ALLO", "PSG", "WLD",
    "XLM", "VIC", "FET", "GENIUS", "NOM", "ID",
    "HIVE", "IO", "EDEN", "MBOX", "INIT", "MEME",
    "TRX", "1MBABYDOGE", "ASTER", "FORM", "INJ",
    "AVNT", "SAGA", "GUN", "HEMI", "1000CAT", "PEPE",
    "GIGGLE", "OPG", "AI", "LUNC", "MEGA", "ONDO",
    "STRAX", "SUI", "D", "LAZIO", "ENA", "EIGEN",
    "THE", "HBAR", "RENDER", "BEL", "PIXEL", "TIA",
    "AVAX", "NEAR", "CAKE", "ALGO", "LUMIA", "MOVE",
    "CHZ", "MUBARAK", "USUAL", "HIGH", "NIL", "NIGHT",
    "CHIP", "ARKM", "TLM", "DOGE", "TAO", "DYM",
    "JTO", "ALT", "RESOLV", "PENDLE", "SHIB", "ETHFI",
    "ORDI", "SANTOS", "KAT", "FLOKI", "PHA", "BIO",
    "BONK", "PENGU", "FIDA", "SCR", "TON", "TOWNS",
    "ADA", "DASH", "TRUMP", "NEIRO", "1000SATS", "ALPINE",
    "EUL", "STO", "DYDX", "ZK", "AXS", "BB",
    "NEWT", "EDU", "GRT", "DODO", "LA", "PORTO",
    "CFG", "APE", "ACH", "SEI", "USTC", "BMT",
    "HYPER", "TST", "LISTA", "ARB", "ICP", "HOME",
    "IOTA", "AXL", "VANRY", "LUNA", "SHELL", "ROBO",
    "PARTI", "SXT", "AR", "SAHARA", "XAI", "TNSR",
    "RARE", "LAYER", "YB", "VIRTUAL", "PLUME", "DOGS",
    "AMP", "VET", "LINK", "W", "MITO", "CYBER",
    "OG", "REZ", "MANTRA", "PNUT", "COS", "BAR",
    "C", "SOLV", "ALICE", "RAD", "BERA", "FOGO",
    "HAEDAL", "PUMP", "ME", "ATOM", "LDO", "JUV",
    "FIL", "XVG", "STRK", "NXPC", "GPS", "XPL",
    "PYTH", "AAVE", "FF", "BANK", "HMSTR", "GALA",
    "KITE", "VANA", "LTC", "AEVO", "MMT", "MAGIC",
    "ENJ", "CITY", "NOT", "ACT", "RONIN", "ACE",
    "TURBO", "ZRO", "KERNEL", "SIGN", "MIRA", "ZBT",
    "AUDIO", "DOT", "ROSE", "SAPIEN", "GMT", "SLP",
    "PROVE", "MAV", "AIXBT", "MORPHO", "OP", "OPN",
    "OPEN", "BOME", "HOT", "SUPER", "WLFI", "LINEA",
    "UNI", "JASMY", "SNX", "ASR", "KAITO", "ENS",
    "MINA", "2Z", "BLUR", "APT", "ONT", "S",
    "API3", "G", "BABY", "HUMA", "MOVR", "METIS",
    "BARD", "ACM", "LPT", "ARPA", "ARK", "BANANA",
    "ORCA", "COMP", "SPELL", "ENSO", "ETC", "DOLO",
    "WAL", "WIF", "COTI", "TRB", "POL", "CFX",
    "BREV", "MANTA", "NMR", "POLYX", "F", "NEO",
    "BTTC", "PEOPLE", "ERA", "SYRUP", "AT", "MET",
    "STORJ", "BCH", "JUP", "SENT", "UMA", "ONE",
    "ZKC", "RED", "STX", "SPK", "ESP", "ATM",
    "SUSHI", "CETUS", "MANA", "XEC", "HOLO", "TREE",
    "RVN", "CRV", "TWT", "RAY", "TURTLE", "SAND",
    "CKB", "SKL", "EGLD", "0G", "OGN", "SOMI",
    "WCT", "ANIME", "ANKR", "KSM", "THETA", "RSR",
    "SUN", "GAS", "ZIL", "CATI", "ZKP", "QTUM",
    "BEAMX", "COW", "CELO", "SOPH", "AUCTION", "SKY",
    "U", "MASK", "ACX", "A", "PHB"
])

cache = {"funding": {}, "oi": {}, "ls_5m": {}, "klines_1m": {}, "klines_5m": {}, "klines_1h": {}}
last_signals = {}
bot_running = True
pending_command = None
consecutive_errors = 0
signal_history = deque(maxlen=100)
signal_tracker = {}
recent_signal_coins = deque(maxlen=50)

# YENİ: Günlük takip ve raporlama
daily_tracker = {}       # {symbol: {"price": ..., "time": ...}}
daily_report_lock = asyncio.Lock()

# =========================================================
# TELEGRAM
# =========================================================
async def send_telegram(session, text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        await session.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"Telegram error: {e}")

async def telegram_polling(session):
    global bot_running, pending_command
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            resp = await fetch(session, url, {"offset": offset, "timeout": 30})
            if resp and resp.get("ok"):
                for update in resp["result"]:
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip().lower()
                    if text == "/status":
                        await send_telegram(session, f"🤖 Bot aktif\nSon sinyaller: {len(signal_history)}")
                    elif text == "/stop":
                        bot_running = False
                        await send_telegram(session, "🛑 Bot durduruldu")
                    elif text == "/start":
                        bot_running = True
                        await send_telegram(session, "✅ Bot yeniden aktif")
                    elif text == "/next":
                        pending_command = "FORCE_NEXT"
                        await send_telegram(session, "⏩ Cooldown bypass")
                    elif text == "/ping":
                        await send_telegram(session, "🏓 Pong")
                    elif text == "/report":
                        await generate_report(session)
        except Exception:
            pass
        await asyncio.sleep(1)

async def generate_report(session):
    now = time.time()
    to_remove = [s for s, d in signal_tracker.items() if now - d["time"] > 259200]
    for s in to_remove:
        del signal_tracker[s]
    if not signal_tracker:
        await send_telegram(session, "📊 Henüz takip edilen sinyal yok.")
        return
    report_lines = ["📊 <b>Sinyal Performans Raporu</b>"]
    for symbol, data in signal_tracker.items():
        entry_price = data['price']
        try:
            ticker = await fetch_api(session, FAPI_URL, "/fapi/v1/ticker/price", {"symbol": symbol})
            current_price = float(ticker['price']) if ticker else entry_price
            change_pct = ((current_price - entry_price) / entry_price) * 100
            elapsed = (time.time() - data['time']) / 3600
            emoji = "🟢" if change_pct > 0 else "🔴"
            report_lines.append(f"{emoji} <b>{symbol}</b> | Giriş: {entry_price} | Güncel: {current_price} | %{change_pct:+.2f} | ({elapsed:.1f}saat)")
        except:
            report_lines.append(f"⚪ <b>{symbol}</b> | Giriş: {entry_price} | Fiyat alınamadı")
    await send_telegram(session, "\n".join(report_lines))

# =========================================================
# GECE YARISI RAPORLAMA
# =========================================================
async def midnight_reporter(session):
    """Her gün saat 00:00'da daily_tracker'daki coin'lerin son durumunu raporlar."""
    global daily_tracker
    while True:
        now = datetime.now()
        # Bir sonraki gece yarısına kadar bekle
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait_seconds = (next_midnight - now).total_seconds()
        await asyncio.sleep(wait_seconds)

        async with daily_report_lock:
            if not daily_tracker:
                continue

            report_lines = ["🌙 <b>GECE YARISI GÜNLÜK RAPOR</b>"]
            for symbol, data in list(daily_tracker.items()):
                entry_price = data["price"]
                signal_time = data["time"]
                try:
                    ticker = await fetch_api(session, FAPI_URL, "/fapi/v1/ticker/price", {"symbol": symbol})
                    if ticker:
                        current_price = float(ticker["price"])
                        change_pct = ((current_price - entry_price) / entry_price) * 100
                        emoji = "🟢" if change_pct > 0 else "🔴"
                        report_lines.append(
                            f"{emoji} <b>{symbol}</b> | Giriş: {entry_price:.4f} | "
                            f"00:00 Fiyatı: {current_price:.4f} | %{change_pct:+.2f}"
                        )
                    else:
                        report_lines.append(f"⚪ <b>{symbol}</b> | Giriş: {entry_price:.4f} | Fiyat alınamadı")
                except Exception as e:
                    report_lines.append(f"⚠️ <b>{symbol}</b> hata: {e}")

            # Özet satır
            report_lines.append(f"\n📌 Toplam {len(daily_tracker)} coin takip edildi.")
            await send_telegram(session, "\n".join(report_lines))

            # Günlük listeyi sıfırla
            daily_tracker.clear()

# =========================================================
# API
# =========================================================
async def fetch(session, url, params=None):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    backoff = 2
    for attempt in range(3):
        try:
            async with SEMAPHORE:
                async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After", str(backoff))
                        try: wait = int(retry_after)
                        except ValueError: wait = backoff
                        await asyncio.sleep(wait)
                        backoff *= 2
                        continue
                    if resp.status != 200: return None
                    return await resp.json()
        except:
            await asyncio.sleep(backoff)
            backoff *= 2
    return None

async def fetch_api(session, base, endpoint, params=None):
    return await fetch(session, f"{base}{endpoint}", params)

async def get_cached(session, cache_name, key, base, endpoint, params, duration):
    now = time.time()
    if key in cache[cache_name] and now - cache[cache_name][key]["time"] < duration:
        return cache[cache_name][key]["data"]
    data = await fetch_api(session, base, endpoint, params)
    if data is not None:
        cache[cache_name][key] = {"time": now, "data": data}
        return data
    return cache[cache_name].get(key, {}).get("data")

# =========================================================
# İNDİKATÖRLER
# =========================================================
def calculate_ema(prices, period):
    if len(prices) < period: return None
    m = 2 / (period + 1)
    e = mean(prices[:period])
    for p in prices[period:]: e = (p - e) * m + e
    return e

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    avg_gain, avg_loss = mean(gains[:period]), mean(losses[:period])
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period-1) + gains[i]) / period
        avg_loss = (avg_loss * (period-1) + losses[i]) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else 100
    return 100 - (100 / (1 + rs))

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period: return None, None, None
    sma = mean(prices[-period:])
    std = stdev(prices[-period:])
    return sma, sma + std_dev * std, sma - std_dev * std

def calculate_atr(highs, lows, closes, period=10):
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1: return None
    highs = highs[-n:]
    lows = lows[-n:]
    closes = closes[-n:]
    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, n)]
    return mean(tr[-period:]) if len(tr) >= period else None

# =========================================================
# COIN LİSTESİ (SADECE SABİT TR LİSTESİ)
# =========================================================
def get_tr_coin_list():
    """Sabit Binance TR coin listesini USDT formatına çevir, filtrele."""
    symbols = []
    for c in TR_COIN_LIST:
        sym = f"{c}USDT"
        if sym not in STABLECOIN_BLACKLIST and sym not in MAJOR_COINS_BLACKLIST and sym not in COMMODITY_BLACKLIST:
            symbols.append(sym)
    return symbols

async def get_futures_symbols(session):
    info = await fetch_api(session, FAPI_URL, "/fapi/v1/exchangeInfo")
    if not info: return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING" and s["symbol"] not in STABLECOIN_BLACKLIST
            and s["symbol"] not in MAJOR_COINS_BLACKLIST}

async def get_daily_change_map(session, symbols):
    """
    Hem futures hem spot 24 saatlik değişimleri alır.
    """
    cmap = {}
    f_data = await fetch_api(session, FAPI_URL, "/fapi/v1/ticker/24hr")
    if f_data:
        for item in f_data:
            s = item.get("symbol", "")
            if s in symbols:
                try: cmap[s] = float(item["priceChangePercent"])
                except: pass

    missing = [s for s in symbols if s not in cmap]
    if missing:
        spot_data = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/ticker/24hr")
        if spot_data:
            for item in spot_data:
                s = item.get("symbol", "")
                if s in missing:
                    try: cmap[s] = float(item["priceChangePercent"])
                    except: pass
    return cmap

# =========================================================
# SCAN COIN
# =========================================================
async def scan_coin(session, symbol, is_futures, kl_5m, btc_change, klines_1h, daily_change):
    global recent_signal_coins

    now = time.time()
    if symbol in last_signals:
        highs_dyn = [float(k[2]) for k in kl_5m[-30:-1]]
        lows_dyn = [float(k[3]) for k in kl_5m[-30:-1]]
        closes_dyn = [float(k[4]) for k in kl_5m[-30:-1]]
        atr_dyn = calculate_atr(highs_dyn, lows_dyn, closes_dyn)
        close_p_rel = float(kl_5m[-2][4])
        if atr_dyn and atr_dyn > 0 and close_p_rel > 0:
            atr_pct = (atr_dyn / close_p_rel) * 100
            dynamic_cooldown = int(COOLDOWN_BASE * (2.0 / max(atr_pct, 0.3)))
            dynamic_cooldown = max(1800, min(10800, dynamic_cooldown))
        else:
            dynamic_cooldown = COOLDOWN_BASE
        if now - last_signals[symbol] < dynamic_cooldown:
            return None

    if daily_change is not None and daily_change > 4.0:
        return None

    if not kl_5m or len(kl_5m) < 40:
        return None

    closed = kl_5m[:-1]
    last = closed[-1]
    open_p, close_p, high, low, vol, quote_vol, tbuy = (
        float(last[1]), float(last[4]), float(last[2]), float(last[3]),
        float(last[5]), float(last[7]), float(last[9])
    )
    change_pct = ((close_p - open_p) / open_p) * 100

    if len(closed) >= 5:
        open_5 = float(closed[-5][1])
        change_5 = ((close_p - open_5) / open_5) * 100
        if change_5 > 3.5:
            return None
    else:
        change_5 = 0.0

    if change_pct > 2.0:
        return None

    if 0.99 < close_p < 1.01 and abs(change_pct) < 0.1: return None

    vol_history = [float(k[5]) for k in kl_5m[-20:-1]]
    coin_median_vol = median(vol_history) if vol_history else vol
    min_quote = max(30_000, coin_median_vol * 0.30)
    if quote_vol < min_quote or abs(change_pct) > 8.0: return None

    taker_hist = [float(k[9]) for k in kl_5m[-7:-2]]
    avg_taker = mean(taker_hist) if taker_hist else tbuy
    rel_vol = round(tbuy / avg_taker, 2) if avg_taker > 0 else 0
    if rel_vol < 0.8:
        return None

    closes = [float(k[4]) for k in closed[-40:]]
    highs = [float(k[2]) for k in closed[-40:]]
    lows = [float(k[3]) for k in closed[-40:]]
    volumes = [float(k[5]) for k in closed[-40:]]

    if len(closes) >= 12:
        coin_mom = (closes[-1] - closes[-12]) / closes[-12] * 100
        rs = coin_mom - btc_change
    else:
        rs = 0.0
    if rs < -0.5:
        return None

    cvd_30 = sum([float(k[9]) - (float(k[5]) - float(k[9])) for k in kl_5m[-7:]])

    bb_mid, bb_upper, bb_lower = calculate_bollinger(closes, 20, 2)
    bb_widths = []
    for i in range(20, len(closes)):
        bb = calculate_bollinger(closes[:i], 20, 2)
        if bb[0] is not None:
            bb_widths.append((bb[1] - bb[2]) / bb[0] if bb[0] > 0 else 1)
    is_squeezing = len(bb_widths) >= 10 and min(bb_widths[-10:]) < median(bb_widths) * 0.6

    atr_now = calculate_atr(highs[-20:], lows[-20:], closes[-20:], 14)
    atr_old = calculate_atr(highs[-34:-14], lows[-34:-14], closes[-34:-14], 14)
    volatility_squeeze = atr_now and atr_old and atr_old > 0 and atr_now < atr_old * 0.75

    recent_vols = [float(k[5]) for k in kl_5m[-10:-1]]
    avg_vol = mean(recent_vols) if recent_vols else vol
    volume_dryup = (vol < avg_vol * 0.5) and (rel_vol > 1.2)

    high_30 = max([float(k[2]) for k in kl_5m[-31:-1]]) if len(kl_5m) >= 31 else high
    distance_to_high = ((high_30 - close_p) / close_p) * 100 if close_p > 0 else 100
    near_breakout = distance_to_high < 2.5
    breakout_pressure = near_breakout and (rel_vol > 1.5)

    last_3_vol = sum(volumes[-3:])
    prev_12_vol = sum(volumes[-15:-3]) if len(volumes) >= 15 else last_3_vol * 4
    vol_acceleration = last_3_vol > prev_12_vol * 0.35

    ls_5m_change = 0.0
    if is_futures:
        try:
            ls5m = await get_cached(session, "ls_5m", symbol, FAPI_URL,
                                    "/futures/data/globalLongShortAccountRatio",
                                    {"symbol": symbol, "period": "5m", "limit": 2}, CACHE_LS_5M)
            if ls5m and len(ls5m) >= 2:
                ls_5m_prev = float(ls5m[-2]["longShortRatio"])
                ls_5m_curr = float(ls5m[-1]["longShortRatio"])
                ls_5m_change = ((ls_5m_curr - ls_5m_prev) / ls_5m_prev) * 100
        except: pass

    oi_change_pct = 0.0
    if is_futures:
        try:
            oi_data = await get_cached(session, "oi", symbol, FAPI_URL,
                                       "/futures/data/openInterestHist",
                                       {"symbol": symbol, "period": "5m", "limit": 4}, CACHE_OI)
            if oi_data and len(oi_data) >= 4:
                oi_prev = float(oi_data[-4]["sumOpenInterest"])
                oi_curr = float(oi_data[-1]["sumOpenInterest"])
                oi_change_pct = ((oi_curr - oi_prev) / oi_prev) * 100
        except: pass

    # ========== SKORLAMA ==========
    score = 0
    reasons = []

    if rel_vol > 3.0: score += 5; reasons.append("🔥🔥 RelVol (alıcı oranının yüksekliği)")
    elif rel_vol > 2.0: score += 3; reasons.append("🔥 RelVol (alıcı oranının yüksekliği)")
    elif rel_vol > 1.5: score += 1; reasons.append("RelVol (alıcı oranının yüksekliği)")

    if rs > 2.0: score += 4; reasons.append(f"🚀 RS {rs:.2f} (BTC'den bağımsız güç)")
    elif rs > 1.0: score += 2; reasons.append(f"✅ RS {rs:.2f} (BTC'den bağımsız güç)")

    if cvd_30 > 0: score += 2; reasons.append("📈 CVD Trend (son 7 mumda güçlülük)")

    if is_squeezing: score += 6; reasons.append("🎯 BB Sıkışma")
    if volatility_squeeze: score += 5; reasons.append("📉 Volatilite Daralması")
    if volume_dryup: score += 4; reasons.append("🤫 Hacim Kuruması (sessiz alım)")
    if breakout_pressure: score += 6; reasons.append("⚡ Kırılım Baskısı (tepe + hacim)")
    elif near_breakout: score += 3; reasons.append("📌 Tepeye Yakın")
    if vol_acceleration: score += 3; reasons.append("🚀 Hacim İvmesi")
    if ls_5m_change < -2: score += 3; reasons.append("📉 LS squeeze")
    if oi_change_pct > 1.0: score += 5; reasons.append(f"📊 OI Artışı %{oi_change_pct:.1f}")
    elif oi_change_pct < -1.0: score -= 2

    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)
    if ema9 and ema21 and (abs(ema9 - ema21) / close_p * 100) < 0.8:
        score += 5; reasons.append("🌀 EMA Sıkışması")

    if symbol in klines_1h and klines_1h[symbol] is not None:
        ema20_1h = calculate_ema([float(k[4]) for k in klines_1h[symbol]], 20)
        if ema20_1h and close_p > ema20_1h: score += 2; reasons.append("1h↑")

    rsi = calculate_rsi(closes, 14)
    if rsi and 30 < rsi < 50: score += 3; reasons.append(f"RSI{rsi:.0f} dip")

    if any(c == symbol for c, _ in recent_signal_coins): score -= 3

    if score < MIN_SCORE: return None

    atr_val = calculate_atr(highs, lows, closes, 10) or close_p * 0.02
    conf = min(95, 55 + score * 2)
    tp_price = round(close_p + atr_val * 10, 4)
    sl_price = round(close_p - atr_val * 5, 4)
    tp_pct = min(round((tp_price - close_p) / close_p * 100, 2), MAX_TP_PCT)
    sl_pct = round((close_p - sl_price) / close_p * 100, 2)
    if tp_pct >= MAX_TP_PCT: tp_price = round(close_p * (1 + MAX_TP_PCT / 100), 4)

    return {
        "symbol": symbol, "score": score, "conf": conf,
        "price": round(close_p, 4), "change": round(change_pct, 2),
        "rs": round(rs, 2), "rel_vol": rel_vol,
        "tp": tp_price, "sl": sl_price, "tp_pct": tp_pct, "sl_pct": sl_pct,
        "reasons": reasons
    }

# =========================================================
# MAIN
# =========================================================
async def main():
    global bot_running, pending_command, consecutive_errors, recent_signal_coins, daily_tracker
    print("🚀 PATLAMA ÖNCESİ SİNYAL BOTU (MIN_SCORE=24 + Gece Raporu)")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        asyncio.create_task(telegram_polling(session))
        # Gece yarısı raporlama görevi
        asyncio.create_task(midnight_reporter(session))

        COIN_LIST = get_tr_coin_list()
        futures_set = await get_futures_symbols(session)
        await send_telegram(session, f"🎯 Patlama Öncesi Bot ({len(COIN_LIST)} TR coin) | /report")
        print(f"✅ {len(COIN_LIST)} coin taranıyor ({len(futures_set)} futures)")

        last_global = 0
        while True:
            if not bot_running:
                await asyncio.sleep(1); continue

            now = time.time()
            while recent_signal_coins and now - recent_signal_coins[0][1] > COOLDOWN_BASE:
                recent_signal_coins.popleft()

            try:
                t0 = time.time()
                daily_map = await get_daily_change_map(session, set(COIN_LIST))
                btc = await fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                      {"symbol": "BTCUSDT", "interval": "15m", "limit": 10})
                btc_change = 0.0
                if btc:
                    bo, bc = float(btc[-2][1]), float(btc[-2][4])
                    btc_change = ((bc - bo) / bo) * 100

                fut_list = [s for s in COIN_LIST if s in futures_set]
                tasks_1h = [get_cached(session, "klines_1h", s, FAPI_URL, "/fapi/v1/klines",
                                       {"symbol": s, "interval": "1h", "limit": 20}, CACHE_1H) for s in fut_list]
                r1 = await asyncio.gather(*tasks_1h)
                k1 = {s: r for s, r in zip(fut_list, r1) if r is not None}

                tasks_5m = []
                for s in COIN_LIST:
                    base = FAPI_URL if s in futures_set else SPOT_GLOBAL_URL
                    ep = "/fapi/v1/klines" if s in futures_set else "/api/v3/klines"
                    tasks_5m.append(get_cached(session, "klines_5m", s, base, ep,
                                               {"symbol": s, "interval": "5m", "limit": 50}, CACHE_5M))
                resp_5m = await asyncio.gather(*tasks_5m)
                valid = {}
                for s, r in zip(COIN_LIST, resp_5m):
                    if r and len(r) >= 40: valid[s] = r

                scan_tasks = [scan_coin(session, s, s in futures_set, valid[s], btc_change, k1, daily_map.get(s))
                              for s in COIN_LIST if s in valid]
                all_res = []
                for i in range(0, len(scan_tasks), BATCH_SIZE):
                    batch = scan_tasks[i:i+BATCH_SIZE]
                    all_res.extend([r for r in await asyncio.gather(*batch) if r])

                all_res.sort(key=lambda x: x['score'], reverse=True)

                now_ts = time.time()
                is_forced = (pending_command == "FORCE_NEXT")
                sent = 0
                if (now_ts - last_global >= GLOBAL_COOLDOWN) or is_forced:
                    for r in all_res:
                        if sent >= MAX_SIGNALS_PER_ROUND: break
                        if r['symbol'] in last_signals and now_ts - last_signals[r['symbol']] < COOLDOWN_BASE: continue
                        reasons = ", ".join(r['reasons'])
                        msg = (
                            f"🟢 <b>{r['symbol']} (LONG)</b>\n"
                            f"Puan: {r['score']} | Güven: %{r['conf']}\n"
                            f"Giriş: {r['price']} | %{r['change']}\n"
                            f"🎯 TP: {r['tp']} (%{r['tp_pct']}) | 🛑 SL: {r['sl']} (%{r['sl_pct']})\n"
                            f"RelVol: {r['rel_vol']}x | RS: {r['rs']:.2f}\n"
                            f"Sebep: {reasons}"
                        )
                        await send_telegram(session, msg)
                        last_signals[r['symbol']] = now_ts
                        recent_signal_coins.append((r['symbol'], now_ts))
                        signal_history.append({"symbol": r['symbol'], "score": r['score'], "time": now_ts})
                        if r['symbol'] not in signal_tracker:
                            signal_tracker[r['symbol']] = {'price': r['price'], 'time': now_ts}
                        # Günlük rapor takibi (ilk sinyali kaydet)
                        async with daily_report_lock:
                            if r['symbol'] not in daily_tracker:
                                daily_tracker[r['symbol']] = {"price": r['price'], "time": now_ts}
                        sent += 1
                        await asyncio.sleep(random.uniform(0.5, 1.0))
                    if sent > 0: last_global = now_ts
                if is_forced: pending_command = None

                print(f"🔍 {len(all_res)} aday (Min Skor: {MIN_SCORE})")
                consecutive_errors = 0
                elapsed = time.time() - t0
                await asyncio.sleep(max(0, 20 - elapsed))
            except Exception as e:
                consecutive_errors += 1
                print(f"Hata: {e}")
                traceback.print_exc()
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    await send_telegram(session, "❌ Çok fazla hata, 2 dk dinlenme.")
                    await asyncio.sleep(120)
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
