import asyncio
import aiohttp
import os
import time
import random
from statistics import mean, median, stdev
from collections import deque
import traceback

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

SCAN_INTERVAL = 40
COOLDOWN = 600
GLOBAL_COOLDOWN = 90
MAX_SIGNALS_PER_ROUND = 3
BATCH_SIZE = 25

# Kalibre edilmiş skor eşikleri
MIN_SCORE_BASE = 8
MIN_SCORE_HIGH_VOLATILITY = 10
MIN_SCORE_LOW_VOLATILITY = 6

TP_MULT = 10
SL_MULT = 5

CACHE_5M = 35
CACHE_15M = 180
CACHE_1H = 300
CACHE_4H = 900
CACHE_OI = 120
CACHE_FUNDING = 300

MAX_CONSECUTIVE_ERRORS = 15
SEMAPHORE = asyncio.Semaphore(20)

STABLECOIN_BLACKLIST = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "USDPUSDT", "FDUSDUSDT", "USTCUSDT", "EURSUSDT"
}

EXCLUDE_FROM_VOLUME_CALC = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT", "USDPUSDT", "FDUSDUSDT"
}

# Altın gibi emtia/usd çiftleri - tarama dışı
COMMODITY_BLACKLIST = {"PAXGUSDT"}

# =========================================================
# BINANCE TR SPOT LİSTESİ (TEMİZ)
# =========================================================
TR_COIN_LIST = sorted([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "SOLUSDT", "AVAXUSDT",
    "DOTUSDT", "LINKUSDT", "UNIUSDT", "MATICUSDT", "SHIBUSDT", "DOGEUSDT",
    "LTCUSDT", "TRXUSDT", "ATOMUSDT", "NEARUSDT", "ALGOUSDT", "FTMUSDT",
    "SANDUSDT", "MANAUSDT", "AXSUSDT", "GALAUSDT", "CHZUSDT", "ENJUSDT",
    "HOTUSDT", "ZILUSDT", "BATUSDT", "CELOUSDT", "COMPUSDT", "CRVUSDT",
    "DYDXUSDT", "EGLDUSDT", "FLOWUSDT", "GRTUSDT", "ICPUSDT", "KSMUSDT",
    "LRUSDT", "MKRUSDT", "OMGUSDT", "QNTUSDT", "RENUSDT", "RSRUSDT",
    "SKLUSDT", "SNXUSDT", "STORJUSDT", "SUSHIUSDT", "SXPUSDT", "UMAUSDT",
    "YFIUSDT", "ZENUSDT", "ZRXUSDT", "1INCHUSDT", "AAVEUSDT",
    "ACHUSDT", "AGLDUSDT", "AKROUSDT", "ALICEUSDT", "ALPHAUSDT", "ANKRUSDT",
    "APEUSDT", "API3USDT", "ARPAUSDT", "AUDIOUSDT", "BAKEUSDT", "BANDUSDT",
    "BELUSDT", "BLURUSDT", "BNTUSDT", "C98USDT", "CAKEUSDT", "COTIUSDT",
    "CTSIUSDT", "CTXCUSDT", "CVCUSDT", "DARUSDT", "DENTUSDT", "DGBUSDT",
    "DOCKUSDT", "DODOUSDT", "DUSKUSDT", "EDUUSDT", "ERNUSDT", "FETUSDT",
    "FIDAUSDT", "FORTHUSDT", "FRONTUSDT", "FXSUSDT", "GTCUSDT", "HARDUSDT",
    "HIGHUSDT", "ICXUSDT", "IDUSDT", "ILVUSDT", "IMXUSDT", "INJUSDT",
    "IOSTUSDT", "IOTXUSDT", "JASMYUSDT", "JOEUSDT", "KAVAUSDT", "KDAUSDT",
    "KLAYUSDT", "KNCUSDT", "LDOUSDT", "LINAUSDT", "LOOMUSDT",
    "LPTUSDT", "LQTYUSDT", "LRCUSDT", "MAGICUSDT", "MASKUSDT", "MDTUSDT",
    "MINAUSDT", "MLNUSDT", "MTLUSDT", "NKNUSDT", "NMRUSDT",
    "OCEANUSDT", "OGNUSDT", "ONEUSDT", "ONTUSDT", "OPUSDT", "ORBSUSDT",
    "OXTUSDT", "PENDLEUSDT", "PEOPLEUSDT", "PEPEUSDT", "PERLUSDT",
    "PHAUSDT", "POLSUSDT", "PONDUSDT", "POWRUSDT", "PROMUSDT", "PYRUSDT",
    "QIUSDT", "RADUSDT", "RAREUSDT", "REEFUSDT", "REIUSDT", "RLCUSDT",
    "RNDRUSDT", "ROSEUSDT", "RPLUSDT", "RVNUSDT", "SCUSDT", "SFPUSDT",
    "SLPUSDT", "SNTUSDT", "SPELLUSDT", "STGUSDT", "STMXUSDT", "STPTUSDT",
    "STRAXUSDT", "SUIUSDT", "SUNUSDT", "SUPERUSDT", "TFUELUSDT",
    "THETAUSDT", "TLMUSDT", "TOMOUSDT", "TRBUSDT", "TROYUSDT",
    "TVKUSDT", "UNFIUSDT", "UTKUSDT", "VETUSDT", "VGXUSDT",
    "VIDTUSDT", "VITEUSDT", "VOXELUSDT", "VTHOUSDT", "WAVESUSDT", "WAXPUSDT",
    "WBTCUSDT", "WINUSDT", "WLDUSDT", "WOOUSDT", "WRXUSDT", "XECUSDT",
    "XEMUSDT", "XLMUSDT", "XMRUSDT", "XTZUSDT", "XVGUSDT", "YGGUSDT"
])

cache = {
    "funding": {}, "oi": {},
    "klines_5m": {}, "klines_15m": {}, "klines_1h": {}, "klines_4h": {}
}
last_signals = {}
bot_running = True
pending_command = None
consecutive_errors = 0
signal_history = deque(maxlen=100)

# =========================================================
# TELEGRAM
# =========================================================
async def send_telegram(session, text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        await session.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
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
        except Exception:
            pass
        await asyncio.sleep(1)

# =========================================================
# API
# =========================================================
async def fetch(session, url, params=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    backoff = 2
    for attempt in range(3):
        try:
            async with SEMAPHORE:
                async with session.get(url, params=params, headers=headers, timeout=20) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    if resp.status != 200:
                        return None
                    return await resp.json()
        except Exception:
            if attempt == 2:
                traceback.print_exc()
            await asyncio.sleep(1)
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
    ema = mean(prices[:period])
    for p in prices[period:]:
        ema = (p - ema) * m + ema
    return ema

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return None
    gains = [max(0, prices[i] - prices[i-1]) for i in range(1, len(prices))]
    losses = [max(0, prices[i-1] - prices[i]) for i in range(1, len(prices))]
    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period-1) + gains[i]) / period
        avg_loss = (avg_loss * (period-1) + losses[i]) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else 100
    return 100 - (100 / (1 + rs))

def calculate_real_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return None, None, None
    fm = 2 / (fast + 1)
    sm = 2 / (slow + 1)
    ema_fast = [mean(prices[:fast])]
    ema_slow = [mean(prices[:slow])]
    for i in range(fast, len(prices)):
        ema_fast.append((prices[i] - ema_fast[-1]) * fm + ema_fast[-1])
    for i in range(slow, len(prices)):
        ema_slow.append((prices[i] - ema_slow[-1]) * sm + ema_slow[-1])
    offset = slow - fast
    macd_line = [ema_fast[offset + i] - ema_slow[i] for i in range(len(ema_slow))]
    sigm = 2 / (signal + 1)
    signal_line = [mean(macd_line[:signal])]
    for i in range(signal, len(macd_line)):
        signal_line.append((macd_line[i] - signal_line[-1]) * sigm + signal_line[-1])
    histogram = macd_line[-1] - signal_line[-1]
    return macd_line[-1], signal_line[-1], histogram

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period: return None, None, None
    sma = mean(prices[-period:])
    std = stdev(prices[-period:])
    return sma, sma + std_dev * std, sma - std_dev * std

def calculate_atr(highs, lows, closes, period=10):
    if len(highs) < period + 1: return None
    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(highs))]
    return mean(tr[-period:])

# =========================================================
# COIN LİSTESİ
# =========================================================
async def get_spot_symbols(session):
    # Sadece TR listesindeki coin'leri kullan, PAXG gibi emtiaları çıkar
    return {s for s in TR_COIN_LIST if s not in COMMODITY_BLACKLIST}

async def get_futures_symbols(session):
    info = await fetch_api(session, FAPI_URL, "/fapi/v1/exchangeInfo")
    if not info: return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING" and s["symbol"] not in STABLECOIN_BLACKLIST}

async def get_daily_change_map(session, symbols):
    cmap = {}
    data = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/ticker/24hr")
    if data:
        for item in data:
            s = item.get("symbol", "")
            if s in symbols:
                try: cmap[s] = float(item["priceChangePercent"])
                except: cmap[s] = 0.0
    return cmap

# =========================================================
# DİNAMİK HACİM FİLTRESİ
# =========================================================
def calculate_dynamic_volume_threshold(volumes):
    if not volumes:
        return 100_000
    sorted_vols = sorted(volumes)
    n = len(sorted_vols)
    if n >= 10:
        trimmed = sorted_vols[n//10:-n//10]
    else:
        trimmed = sorted_vols
    if not trimmed:
        return 100_000
    med = median(trimmed)
    threshold = max(100_000, min(500_000, med * 0.25))
    return threshold

# =========================================================
# SCAN COIN (KALİBRE EDİLMİŞ SKORLAMA)
# =========================================================
async def scan_coin(session, symbol, is_futures, kl_5m, market_median,
                    btc_change, min_score, dynamic_min_volume,
                    klines_1h, klines_4h, klines_15m, daily_change):
    if symbol in last_signals and time.time() - last_signals[symbol] < COOLDOWN:
        return None
    if daily_change is not None and daily_change > 10.0:
        return None
    if not kl_5m or len(kl_5m) < 30:
        return None

    closed = kl_5m[:-1]
    last = closed[-1]
    open_p, close_p, high, low, vol, quote_vol, tbuy = (
        float(last[1]), float(last[4]), float(last[2]), float(last[3]),
        float(last[5]), float(last[7]), float(last[9])
    )
    change_pct = ((close_p - open_p) / open_p) * 100

    if 0.99 < close_p < 1.01 and abs(change_pct) < 0.1: return None
    if len(closed) >= 13:
        if (close_p - float(closed[-13][4])) / float(closed[-13][4]) * 100 > 8.0: return None

    # Dinamik hacim filtresi
    if quote_vol < dynamic_min_volume or abs(change_pct) > 8.0: return None

    prev_vols = [float(k[5]) for k in kl_5m[-7:-2]]
    avg_vol = mean(prev_vols) if prev_vols else vol
    speed = vol / avg_vol if avg_vol > 0 else 0
    rel_vol = round(speed, 2)

    heavy = speed > 1.2 or vol > market_median * 1.5
    taker_r = tbuy / vol if vol > 0 else 0
    delta = tbuy - (vol - tbuy)
    delta_r = delta / vol if vol > 0 else 0
    body_r = abs(close_p - open_p) / (high - low) if (high - low) > 0 else 0
    wick_r = 1 - body_r

    highs = [float(k[2]) for k in closed[-30:]]
    lows = [float(k[3]) for k in closed[-30:]]
    closes = [float(k[4]) for k in closed[-30:]]
    atr_val = calculate_atr(highs, lows, closes)
    if atr_val is None: return None

    rsi = calculate_rsi(closes, 14)
    macd_l, sig_l, hist = calculate_real_macd(closes)
    bb_mid, bb_upper, bb_lower = calculate_bollinger(closes, 20, 2)

    oi_change = 0.0
    has_oi = False
    funding_rate = 0.0
    if is_futures:
        try:
            oi_data = await get_cached(session, "oi", symbol, FAPI_URL,
                                       "/futures/data/openInterestHist",
                                       {"symbol": symbol, "period": "5m", "limit": 2}, CACHE_OI)
            if oi_data and len(oi_data) >= 2:
                prev_oi = float(oi_data[-2]["sumOpenInterestValue"])
                curr_oi = float(oi_data[-1]["sumOpenInterestValue"])
                if prev_oi > 0:
                    oi_change = ((curr_oi - prev_oi) / prev_oi) * 100
                    has_oi = True
        except: pass
        try:
            fund = await get_cached(session, "funding", symbol, FAPI_URL,
                                    "/fapi/v1/premiumIndex", {"symbol": symbol}, CACHE_FUNDING)
            if fund: funding_rate = float(fund.get("lastFundingRate", 0))
        except: pass

    ema20_1h = ema50_4h = None
    bull_struct = False
    if symbol in klines_1h and klines_1h[symbol] is not None:
        ema20_1h = calculate_ema([float(k[4]) for k in klines_1h[symbol]], 20)
    if symbol in klines_4h and klines_4h[symbol] is not None:
        ema50_4h = calculate_ema([float(k[4]) for k in klines_4h[symbol]], 50)
    if symbol in klines_15m and klines_15m[symbol] is not None:
        kl15 = klines_15m[symbol]
        if len(kl15) >= 4:
            hh = [float(k[2]) for k in kl15[-4:]]
            ll = [float(k[3]) for k in kl15[-4:]]
            if hh[-1] > hh[-2] and ll[-1] > ll[-2]:
                bull_struct = True

    # ========== YENİ KALİBRE EDİLMİŞ SKORLAMA ==========
    score = 0
    reasons = []
    squeeze = False

    # Hacim patlaması - kalibre edildi
    if speed > 2.0 and change_pct > 0:
        score += 3; reasons.append("💪 Yüksek hacim")
    elif speed > 1.5 and change_pct > 0:
        score += 2; reasons.append("Hacim")

    # Normalize hareket - daha sıkı
    if high > low:
        norm = change_pct / ((high - low) / close_p * 100) if ((high - low) / close_p * 100) > 0 else 0
        if 0.3 < norm < 4 and norm > 0:
            score += 2; reasons.append("Norm")

    # Taker ve Delta - daha sıkı
    if taker_r > 0.58: score += 2; reasons.append("Taker")
    if delta_r > 0.15: score += 2; reasons.append("Delta")

    # Funding squeeze
    if funding_rate < -0.005 and change_pct > 0:
        score += 2; squeeze = True; reasons.append("Squeeze")

    # Gerçek RS - daha sıkı
    if len(closes) >= 12:
        coin_mom = (closes[-1] - closes[-12]) / closes[-12] * 100
        rs = coin_mom - btc_change
        if rs > 1.5: score += 2; reasons.append(f"RS {rs:.2f}")
    else:
        rs = 0.0

    # Sıkışma kırılımı - daha sıkı
    if heavy and len(kl_5m) > 6:
        r_high = max(float(k[2]) for k in kl_5m[-7:-2])
        r_low = min(float(k[3]) for k in kl_5m[-7:-2])
        comp = (r_high - r_low) / close_p * 100 if close_p > 0 else 0
        if comp < 1.0 and speed > 2.0 and delta_r > 0.15:
            score += 4; reasons.append("Kırılım")

    # BTC'ye rağmen güçlü
    if has_oi and btc_change <= 0 and oi_change > 2 and delta_r > 0.15:
        score += 3; reasons.append("BTC↓ güçlü")

    # Trend - azaltıldı
    if ema20_1h is not None and close_p > ema20_1h: score += 1; reasons.append("1h↑")
    if ema50_4h is not None and close_p > ema50_4h: score += 1; reasons.append("4h↑")
    if bull_struct: score += 1; reasons.append("15m↑")

    # RelVol - azaltıldı
    if rel_vol > 2.0: score += 2; reasons.append("RelVol")
    elif rel_vol > 1.5: score += 1

    # Wick cezası - aynı
    if wick_r > 0.6: score -= 1

    # BTC etkisi - aynı
    if btc_change <= -0.8: score -= 4; reasons.append("BTC↓")
    if btc_change > 1.5 and symbol != "BTCUSDT": score -= 2

    # Multi-TF uyumu - azaltıldı
    if ema50_4h is not None and ema20_1h is not None and close_p > ema50_4h and close_p > ema20_1h and bull_struct:
        score += 2; reasons.append("MTF")

    # RSI - daha sıkı
    if rsi is not None and 40 < rsi < 60 and close_p > open_p:
        score += 1; reasons.append(f"RSI{rsi:.0f}")

    # MACD - aynı
    if macd_l is not None and sig_l is not None and macd_l > sig_l and hist > 0:
        score += 2; reasons.append("MACD")

    # Bollinger - daha sıkı
    if bb_lower is not None and close_p <= bb_lower * 1.005 and change_pct > 0:
        score += 2; reasons.append("BB")

    # YENİ: Kalite kontrolü - en az 2 farklı kategoriden puan almalı
    categories = set()
    if speed > 1.2: categories.add("hacim")
    if taker_r > 0.55: categories.add("taker")
    if delta_r > 0.12: categories.add("delta")
    if "RS" in "".join(reasons): categories.add("rs")
    if bull_struct or (ema20_1h is not None and close_p > ema20_1h): categories.add("trend")
    if bb_lower is not None and close_p <= bb_lower * 1.01: categories.add("bb")
    if macd_l is not None and sig_l is not None and macd_l > sig_l: categories.add("macd")

    # En az 3 farklı kategoriden sinyal gelmeli
    if len(categories) < 3:
        return None

    if score < min_score: return None

    conf = min(95, 55 + score * 3)
    tp_price = round(close_p + atr_val * TP_MULT, 4)
    sl_price = round(close_p - atr_val * SL_MULT, 4)
    tp_pct = round((tp_price - close_p) / close_p * 100, 2)
    sl_pct = round((close_p - sl_price) / close_p * 100, 2)

    return {
        "symbol": symbol, "score": score, "conf": conf,
        "price": round(close_p, 4), "change": round(change_pct, 2),
        "oi": round(oi_change, 2) if has_oi else -999,
        "funding": funding_rate, "delta": delta_r,
        "rel_vol": rel_vol, "squeeze": squeeze, "rs": round(rs, 2),
        "tp": tp_price, "sl": sl_price, "tp_pct": tp_pct, "sl_pct": sl_pct,
        "reasons": reasons
    }

# =========================================================
# MAIN
# =========================================================
async def main():
    global bot_running, pending_command, consecutive_errors
    print(f"🚀 KALİBRE SNIPER BOT ({len(TR_COIN_LIST)} coin)")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        asyncio.create_task(telegram_polling(session))
        await send_telegram(session, f"🎯 Kalibre Sniper Bot başlatıldı ({len(TR_COIN_LIST)} coin)")

        spot_symbols = await get_spot_symbols(session)
        futures_set = await get_futures_symbols(session)
        COIN_LIST = sorted(spot_symbols)
        print(f"✅ {len(COIN_LIST)} TR coin taranıyor ({len(futures_set)} futures'ta var)")

        last_global = 0

        while True:
            if not bot_running:
                await asyncio.sleep(1)
                continue

            try:
                t0 = time.time()
                daily_map = await get_daily_change_map(session, spot_symbols)

                btc = await fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                      {"symbol": "BTCUSDT", "interval": "15m", "limit": 10})
                btc_change = 0.0; btc_atr_pct = 0.0
                if btc:
                    bo, bc = float(btc[-2][1]), float(btc[-2][4])
                    btc_change = ((bc - bo) / bo) * 100
                    bh = [float(k[2]) for k in btc[-5:]]
                    bl = [float(k[3]) for k in btc[-5:]]
                    btc_atr_pct = ((max(bh) - min(bl)) / min(bl)) * 100

                min_score = MIN_SCORE_LOW_VOLATILITY if btc_atr_pct < 1.0 else (MIN_SCORE_HIGH_VOLATILITY if btc_atr_pct > 2.5 else MIN_SCORE_BASE)

                fut_list = [s for s in COIN_LIST if s in futures_set]

                tasks_1h = [get_cached(session, "klines_1h", s, FAPI_URL, "/fapi/v1/klines",
                                       {"symbol": s, "interval": "1h", "limit": 20}, CACHE_1H) for s in fut_list]
                tasks_4h = [get_cached(session, "klines_4h", s, FAPI_URL, "/fapi/v1/klines",
                                       {"symbol": s, "interval": "4h", "limit": 60}, CACHE_4H) for s in fut_list]
                tasks_15m = [get_cached(session, "klines_15m", s, FAPI_URL, "/fapi/v1/klines",
                                        {"symbol": s, "interval": "15m", "limit": 6}, CACHE_15M) for s in fut_list]
                r1, r4, r15 = await asyncio.gather(asyncio.gather(*tasks_1h), asyncio.gather(*tasks_4h), asyncio.gather(*tasks_15m))

                k1 = {s: r for s, r in zip(fut_list, r1) if r is not None}
                k4 = {s: r for s, r in zip(fut_list, r4) if r is not None}
                k15 = {s: r for s, r in zip(fut_list, r15) if r is not None}

                tasks_5m = []
                for s in COIN_LIST:
                    base = FAPI_URL if s in futures_set else SPOT_GLOBAL_URL
                    ep = "/fapi/v1/klines" if s in futures_set else "/api/v3/klines"
                    tasks_5m.append(get_cached(session, "klines_5m", s, base, ep,
                                               {"symbol": s, "interval": "5m", "limit": 50}, CACHE_5M))
                resp_5m = await asyncio.gather(*tasks_5m)

                valid = {}
                vols = []
                altcoin_volumes = []

                for s, r in zip(COIN_LIST, resp_5m):
                    if r and len(r) >= 30:
                        valid[s] = r
                        try:
                            vol_val = float(r[-2][5])
                            vols.append(vol_val)
                            if s not in EXCLUDE_FROM_VOLUME_CALC:
                                quote_vol = float(r[-2][7])
                                altcoin_volumes.append(quote_vol)
                        except: pass

                fvols = [v for v in vols if v > 100000]
                market_median = median(sorted(fvols)[2:-2]) if len(fvols) > 4 else (median(fvols) if fvols else 1)

                dynamic_min_volume = calculate_dynamic_volume_threshold(altcoin_volumes)
                print(f"📊 Dinamik hacim eşiği: {dynamic_min_volume:.0f} USDT (Altcoin: {len(altcoin_volumes)})")

                scan_tasks = []
                for s in COIN_LIST:
                    if s not in valid: continue
                    scan_tasks.append(scan_coin(session, s, s in futures_set, valid[s], market_median,
                                                btc_change, min_score, dynamic_min_volume,
                                                k1, k4, k15, daily_map.get(s)))

                all_res = []
                for i in range(0, len(scan_tasks), BATCH_SIZE):
                    batch = scan_tasks[i:i+BATCH_SIZE]
                    all_res.extend([r for r in await asyncio.gather(*batch) if r])

                for r in all_res:
                    r['rank'] = (r['score'] * 0.4) + (r['rel_vol'] * 0.3) + (abs(r['delta']) * 0.3)
                all_res.sort(key=lambda x: x['rank'], reverse=True)

                now = time.time()
                is_forced = (pending_command == "FORCE_NEXT")

                if (now - last_global >= GLOBAL_COOLDOWN) or is_forced:
                    sent = 0
                    for r in all_res:
                        if sent >= MAX_SIGNALS_PER_ROUND: break
                        if r['symbol'] in last_signals and now - last_signals[r['symbol']] < COOLDOWN: continue
                        reasons = ", ".join(r['reasons'])
                        oi_str = f"%{r['oi']:.2f}" if r['oi'] != -999 else "N/A"
                        msg = (
                            f"🟢 *{r['symbol']} (LONG)*\n"
                            f"Puan: {r['score']} | Güven: %{r['conf']}\n"
                            f"Giriş: {r['price']} | %{r['change']}\n"
                            f"🎯 TP: {r['tp']} (%{r['tp_pct']}) | 🛑 SL: {r['sl']} (%{r['sl_pct']})\n"
                            f"OI: {oi_str} | RelVol: {r['rel_vol']}x\n"
                            f"RS: {r['rs']:.2f} | Funding: {r['funding']*100:.4f}%\n"
                            f"Delta: {r['delta']:.2f} | Sebep: {reasons}"
                        )
                        await send_telegram(session, msg)
                        print(f"✅ {r['symbol']} LONG (Puan: {r['score']})")
                        last_signals[r['symbol']] = now
                        signal_history.append(r['symbol'])
                        sent += 1
                        await asyncio.sleep(random.uniform(0.5, 1.0))
                    if sent > 0: last_global = now
                else:
                    if all_res:
                        print(f"⏳ Cooldown devrede. {all_res[0]['symbol']} bekliyor.")

                if is_forced: pending_command = None

                print(f"🔍 {len(all_res)} aday (Min Skor: {min_score})")

                consecutive_errors = 0
                elapsed = time.time() - t0
                await asyncio.sleep(max(0, 35 - elapsed))

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
