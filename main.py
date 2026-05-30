import asyncio
import aiohttp
import os
import time
import random
from statistics import mean, median, stdev
from collections import deque
import traceback

# =========================================================
# AYARLAR (AZ & KALİTELİ – SESSİZ BİRİKİM İÇİN OPTİMİZE)
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
MAX_SIGNALS_PER_ROUND = 3

# Min skor yükseltildi
MIN_SCORE_BASE = 8
MIN_SCORE_HIGH_VOLATILITY = 10
MIN_SCORE_LOW_VOLATILITY = 6

TP_MULT = 10
SL_MULT = 5
MAX_TP_PCT = 8.0

CACHE_5M = 35
CACHE_1M = 20
CACHE_15M = 180
CACHE_1H = 300
CACHE_4H = 900
CACHE_OI = 120
CACHE_FUNDING = 300
CACHE_LS_5M = 60
CACHE_LS_1H = 300
CACHE_LS_6H = 1800
CACHE_LS_12H = 3600

BATCH_SIZE = 25
MAX_CONSECUTIVE_ERRORS = 15
SEMAPHORE = asyncio.Semaphore(20)

STABLECOIN_BLACKLIST = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "USDPUSDT", "FDUSDUSDT", "USTCUSDT", "EURSUSDT"
}

MAJOR_COINS_BLACKLIST = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "LTCUSDT", "TRXUSDT", "DOTUSDT"
}

COMMODITY_BLACKLIST = {"PAXGUSDT"}

SECTOR_GROUPS = {
    "ai": ["FETUSDT", "RNDRUSDT", "WLDUSDT", "OCEANUSDT"],
    "l1": ["AVAXUSDT", "NEARUSDT", "FTMUSDT", "EGLDUSDT", "KLAYUSDT", "SUIUSDT"],
    "defi": ["UNIUSDT", "LINKUSDT", "AAVEUSDT", "CRVUSDT", "MKRUSDT"],
    "gaming": ["SANDUSDT", "MANAUSDT", "AXSUSDT", "GALAUSDT", "ENJUSDT"],
    "storage": ["FILUSDT", "ARUSDT", "STORJUSDT"]
}

TR_COIN_LIST = sorted([
    "AVAXUSDT", "LINKUSDT", "UNIUSDT", "MATICUSDT", "SHIBUSDT",
    "ATOMUSDT", "NEARUSDT", "ALGOUSDT", "FTMUSDT",
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
    "XEMUSDT", "XLMUSDT", "XTZUSDT", "XVGUSDT", "YGGUSDT"
])

TR_COIN_LIST = [s for s in TR_COIN_LIST if s not in MAJOR_COINS_BLACKLIST]

cache = {
    "funding": {}, "oi": {},
    "ls_5m": {}, "ls_1h": {}, "ls_6h": {}, "ls_12h": {},
    "klines_1m": {}, "klines_5m": {}, "klines_15m": {}, "klines_1h": {}, "klines_4h": {}
}
last_signals = {}
watchlist = {}
bot_running = True
pending_command = None
consecutive_errors = 0
signal_history = deque(maxlen=100)
signal_tracker = {}
recent_signal_coins = deque(maxlen=50)

# =========================================================
# TELEGRAM (HTML)
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
            ticker = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/ticker/price", {"symbol": symbol})
            current_price = float(ticker['price']) if ticker else entry_price
            change_pct = ((current_price - entry_price) / entry_price) * 100
            elapsed = (time.time() - data['time']) / 3600
            emoji = "🟢" if change_pct > 0 else "🔴"
            report_lines.append(f"{emoji} <b>{symbol}</b> | Giriş: {entry_price} | Güncel: {current_price} | %{change_pct:+.2f} | ({elapsed:.1f}saat)")
        except:
            report_lines.append(f"⚪ <b>{symbol}</b> | Giriş: {entry_price} | Fiyat alınamadı")
    await send_telegram(session, "\n".join(report_lines))

# =========================================================
# API
# =========================================================
async def fetch(session, url, params=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    backoff = 2
    for attempt in range(3):
        try:
            async with SEMAPHORE:
                async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After", str(backoff))
                        try: wait = int(retry_after)
                        except ValueError: wait = backoff
                        print(f"⚠️ HTTP 429: {wait}s bekleniyor...")
                        await asyncio.sleep(wait)
                        backoff *= 2
                        continue
                    if resp.status != 200:
                        return None
                    return await resp.json()
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await asyncio.sleep(backoff)
            backoff *= 2
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
    e = mean(prices[:period])
    for p in prices[period:]:
        e = (p - e) * m + e
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

def calculate_real_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return None, None, None
    fm, sm = 2/(fast+1), 2/(slow+1)
    ema_fast, ema_slow = [mean(prices[:fast])], [mean(prices[:slow])]
    for i in range(fast, len(prices)):
        ema_fast.append((prices[i] - ema_fast[-1]) * fm + ema_fast[-1])
    for i in range(slow, len(prices)):
        ema_slow.append((prices[i] - ema_slow[-1]) * sm + ema_slow[-1])
    offset = slow - fast
    macd_line = [ema_fast[offset+i] - ema_slow[i] for i in range(len(ema_slow))]
    sigm = 2/(signal+1)
    signal_line = [mean(macd_line[:signal])]
    for i in range(signal, len(macd_line)):
        signal_line.append((macd_line[i] - signal_line[-1]) * sigm + signal_line[-1])
    return macd_line[-1], signal_line[-1], macd_line[-1] - signal_line[-1]

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period: return None, None, None
    sma = mean(prices[-period:])
    std = stdev(prices[-period:])
    return sma, sma + std_dev * std, sma - std_dev * std

def calculate_atr(highs, lows, closes, period=10):
    if len(highs) < period + 1: return None
    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(highs))]
    return mean(tr[-period:])

def calculate_vwap(klines):
    cum_tp_vol, cum_vol = 0, 0
    for k in klines:
        tp = (float(k[2]) + float(k[3]) + float(k[4])) / 3
        vol = float(k[5])
        cum_tp_vol += tp * vol
        cum_vol += vol
    return cum_tp_vol / cum_vol if cum_vol > 0 else None

# =========================================================
# COIN LİSTESİ
# =========================================================
async def get_spot_symbols(session):
    return {s for s in TR_COIN_LIST if s not in COMMODITY_BLACKLIST}

async def get_futures_symbols(session):
    info = await fetch_api(session, FAPI_URL, "/fapi/v1/exchangeInfo")
    if not info: return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING" and s["symbol"] not in STABLECOIN_BLACKLIST
            and s["symbol"] not in MAJOR_COINS_BLACKLIST}

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
# SCAN COIN (AZ & KALİTELİ – OPTİMİZE)
# =========================================================
async def scan_coin(session, symbol, is_futures, kl_5m, kl_1m, market_median,
                    btc_change, min_score,
                    klines_1h, klines_4h, klines_15m, daily_change, sector_strength):
    global watchlist, recent_signal_coins

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

    if daily_change is not None and daily_change > 5.0:
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
        price_1h_ago = float(closed[-13][4])
        if ((close_p - price_1h_ago) / price_1h_ago * 100) > 2.0: return None

    # Coin bazlı hacim (düşük hacimli coinler için 50k ve 0.40 çarpan korundu)
    vol_history = [float(k[5]) for k in kl_5m[-20:-1]]
    coin_median_vol = median(vol_history) if vol_history else vol
    min_quote = max(50_000, coin_median_vol * 0.40)
    if quote_vol < min_quote or abs(change_pct) > 8.0: return None

    # RelVol = Taker Buy Volume (eşik 1.3)
    taker_hist = [float(k[9]) for k in kl_5m[-7:-2]]
    avg_taker = mean(taker_hist) if taker_hist else tbuy
    speed = tbuy / avg_taker if avg_taker > 0 else 0
    rel_vol = round(speed, 2)

    total_speed = vol / mean([float(k[5]) for k in kl_5m[-7:-2]]) if mean([float(k[5]) for k in kl_5m[-7:-2]]) > 0 else 0

    if rel_vol < 1.3:  # 1.0 → 1.3
        return None

    taker_r = tbuy / vol if vol > 0 else 0
    delta = tbuy - (vol - tbuy)
    delta_r = delta / vol if vol > 0 else 0
    body_r = abs(close_p - open_p) / (high - low) if (high - low) > 0 else 0
    wick_r = 1 - body_r
    upper_wick = (high - max(open_p, close_p)) / (high - low) if (high - low) > 0 else 0
    lower_wick = (min(open_p, close_p) - low) / (high - low) if (high - low) > 0 else 0

    highs = [float(k[2]) for k in closed[-30:]]
    lows = [float(k[3]) for k in closed[-30:]]
    closes = [float(k[4]) for k in closed[-30:]]
    atr_val = calculate_atr(highs, lows, closes)
    if atr_val is None: return None

    vwap = calculate_vwap(kl_5m[-50:])

    rsi = calculate_rsi(closes, 14)
    macd_l, sig_l, hist = calculate_real_macd(closes)
    bb_mid, bb_upper, bb_lower = calculate_bollinger(closes, 20, 2)
    bb_width = (bb_upper - bb_lower) / bb_mid if bb_mid > 0 else 1

    cvd_5 = sum([float(k[9]) - (float(k[5]) - float(k[9])) for k in kl_5m[-6:-1]])
    cvd_30 = sum([float(k[9]) - (float(k[5]) - float(k[9])) for k in kl_5m[-7:]])

    bb_widths = []
    for i in range(20, len(closes)):
        bb = calculate_bollinger(closes[:i], 20, 2)
        if bb[0] is not None:
            bb_widths.append((bb[1] - bb[2]) / bb[0] if bb[0] > 0 else 1)
    is_squeezing = len(bb_widths) >= 10 and min(bb_widths[-10:]) < median(bb_widths) * 0.6

    recent_20_high = max(highs[-20:]) if len(highs) >= 20 else high
    recent_20_low = min(lows[-20:]) if len(lows) >= 20 else low
    consolidation_range = (recent_20_high - recent_20_low) / close_p * 100 if close_p > 0 else 100
    is_consolidating = consolidation_range < 1.5

    long_consolidation = False
    if symbol in klines_1h and klines_1h[symbol] is not None and len(klines_1h[symbol]) >= 48:
        h48_highs = [float(k[2]) for k in klines_1h[symbol][-48:]]
        h48_lows = [float(k[3]) for k in klines_1h[symbol][-48:]]
        h48_range = (max(h48_highs) - min(h48_lows)) / close_p * 100 if close_p > 0 else 100
        if h48_range < 6.0:
            long_consolidation = True

    higher_lows = len(lows) >= 4 and lows[-1] > lows[-2] > lows[-3]

    volume_dryup_explosion = False
    if kl_1m and len(kl_1m) >= 12:
        recent_10_vols = [float(k[5]) for k in kl_1m[-11:-1]]
        last_vol = float(kl_1m[-1][5])
        avg_10_vol = mean(recent_10_vols) if recent_10_vols else last_vol
        if avg_10_vol > 0 and last_vol / avg_10_vol > 2.0 and mean(recent_10_vols[:5]) < avg_10_vol * 0.6:
            volume_dryup_explosion = True

    score = 0
    reasons = []
    squeeze = False

    # Yeni yüz bonusu (hafif)
    new_face = not any(c == symbol for c, _ in recent_signal_coins)
    if new_face:
        score += 1
        reasons.append("🆕")

    if btc_change < -0.5 and taker_r > 0.60:
        score += 10
        reasons.append("🛡️ BTC'ye Direnen")

    if abs(change_pct) < 0.5 and taker_r > 0.70:
        score += 5  # 4 → 5
        reasons.append("🤫 Sessiz Kurumsal Birikim")

    if is_squeezing:
        score += 5
        reasons.append("🎯 BB Sıkışma")
    if is_consolidating:
        score += 4
        reasons.append("📦 Yatay Toplama")
    if higher_lows:
        score += 3
        reasons.append("📈 Artan Taban")
    if volume_dryup_explosion:
        score += 6
        reasons.append("💥 Hacim Patlaması")
    if long_consolidation:
        score += 6
        reasons.append("⏳ Uzun Sıkışma (48s)")
    if vwap is not None and close_p > vwap:
        score += 3
        reasons.append("📊 VWAP Üstü")
    if abs(change_pct) < 0.5 and cvd_5 > 0:
        score += 4
        reasons.append("🔍 Gizli Birikim")
    if cvd_30 > 0 and abs(change_pct) < 1.0:
        score += 5
        reasons.append("📈 CVD Trend")

    buying_pressure = 0
    vol_acceleration = 0

    if kl_1m and len(kl_1m) >= 13:
        recent_3m_taker = sum([float(k[9]) for k in kl_1m[-3:]])
        older_10m_taker = sum([float(k[9]) for k in kl_1m[-13:-3]])
        avg_10m_taker = older_10m_taker / 10 if older_10m_taker > 0 else 1
        buying_pressure = recent_3m_taker / (avg_10m_taker * 3) if avg_10m_taker > 0 else 0
        if buying_pressure > 1.5:
            score += 3
            reasons.append("⚡ 1m Alım Hızı")

    if len(closed) >= 13:
        recent_3 = mean([float(k[5]) for k in closed[-3:]])
        older_10 = mean([float(k[5]) for k in closed[-13:-3]])
        vol_acceleration = recent_3 / older_10 if older_10 > 0 else 0
        if vol_acceleration > 1.8:
            score += 4
            reasons.append("🚀 Vol Acceleration")

    if kl_1m and len(kl_1m) >= 5:
        m1_change = ((float(kl_1m[-1][4]) - float(kl_1m[-5][1])) / float(kl_1m[-5][1])) * 100
        if m1_change > 0.5:
            score += 3
            reasons.append("⚡ 1m Momentum")

    prev_low = min([float(k[3]) for k in closed[-6:-1]])
    liquidity_sweep = (low < prev_low and close_p > prev_low and delta_r > 0.15)
    if liquidity_sweep:
        score += 4
        reasons.append("🩸 Liquidity Sweep")

    absorption = (close_p >= open_p and lower_wick > 0.35 and delta_r > 0.18 and taker_r > 0.58)
    if absorption:
        score += 4
        reasons.append("🧲 Bid Absorption")

    whale_candle = (body_r > 0.7 and taker_r > 0.65 and delta_r > 0.25 and total_speed > 2)
    if whale_candle:
        score += 5
        reasons.append("🐋 Whale Candle")

    if bb_width < 0.015:
        score += 3
        reasons.append("🎯 Sıkışma/Patlama Öncesi")

    recent_ranges = [(float(k[2]) - float(k[3])) / float(k[4]) * 100 for k in closed[-10:]]
    older_ranges = [(float(k[2]) - float(k[3])) / float(k[4]) * 100 for k in closed[-30:-10]]
    recent_volatility = mean(recent_ranges) if recent_ranges else 0
    older_volatility = mean(older_ranges) if older_ranges else 0

    if recent_volatility < older_volatility * 0.7 and total_speed > 1.6:
        score += 4
        reasons.append("⚡ Volatilite sıkışması")

    if total_speed > 2.5 and taker_r > 0.65:
        score += 4; reasons.append("💪 Kaliteli Hacim")
    elif total_speed > 2.0:
        score += 2; reasons.append("Hacim")

    red_absorb = (close_p < open_p and taker_r > 0.55 and delta_r > 0.12 and total_speed > 1.8)
    if red_absorb:
        score += 4
        reasons.append("🩸 Absorption")

    if high > low:
        norm = change_pct / ((high - low) / close_p * 100) if ((high - low) / close_p * 100) > 0 else 0
        if 0.5 < norm < 3.5 and norm > 0:
            score += 2; reasons.append("Norm")

    if taker_r > 0.62: score += 2; reasons.append("Taker")
    if delta_r > 0.18: score += 2; reasons.append("Delta")

    if wick_r > 0.55: score -= 2
    if upper_wick > 0.40 and change_pct > 1.5:
        score -= 5; reasons.append("Fake breakout")
    if change_pct > 3.0:
        score -= 6; reasons.append("Aşırı ısınmış")
    elif change_pct > 1.5:
        score -= 3
    if btc_change <= -0.5 and not ("BTC'ye Direnen" in "".join(reasons)):
        score -= 5; reasons.append("BTC↓")
    if btc_change > 1.2 and symbol != "BTCUSDT": score -= 3

    if rel_vol > 4.0:
        score += 4; reasons.append("🔥🔥 RelVol Patlaması")
    elif rel_vol > 2.5:
        score += 3; reasons.append("🔥 RelVol Yüksek")
    elif rel_vol > 1.8:
        score += 2; reasons.append("RelVol")
    elif rel_vol > 1.3:
        score += 1

    # === İKİ AŞAMALI TARAMA ===
    heavy = total_speed > 2.0
    oi_change, has_oi, funding_rate, mark_price = 0.0, False, 0.0, 0.0
    book_imbalance, spot_premium = 0, 0
    iceberg = False
    ls_5m_str, ls_1h_str, ls_6h_str, ls_12h_str = "N/A", "N/A", "N/A", "N/A"
    ls_5m_change, ls_1h_change, ls_6h_change, ls_12h_change = 0.0, 0.0, 0.0, 0.0

    if is_futures and (score >= min_score - 2 or heavy):
        try:
            fund_data = await get_cached(session, "funding", symbol, FAPI_URL,
                                         "/fapi/v1/premiumIndex", {"symbol": symbol}, CACHE_FUNDING)
            if fund_data:
                funding_rate = float(fund_data.get("lastFundingRate", 0))
                mark_price = float(fund_data.get("markPrice", 0))
        except: pass

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

        if heavy:
            try:
                depth = await fetch_api(session, FAPI_URL, "/fapi/v1/depth",
                                        {"symbol": symbol, "limit": 10})
                if depth:
                    bid_vol = sum([float(b[1]) for b in depth.get("bids", [])[:10]])
                    ask_vol = sum([float(a[1]) for a in depth.get("asks", [])[:10]])
                    if ask_vol > 0:
                        book_imbalance = bid_vol / ask_vol
            except: pass

            if mark_price > 0:
                spot_premium = (close_p - mark_price) / mark_price * 100

        if abs(change_pct) < 0.3 and taker_r > 0.65 and delta_r > 0:
            iceberg = True
            for i in range(1, 6):
                prev_taker = float(kl_5m[-1-i][9]) / float(kl_5m[-1-i][5]) if float(kl_5m[-1-i][5]) > 0 else 0
                if prev_taker < 0.55:
                    iceberg = False
                    break
            if iceberg:
                score += 5
                reasons.append("🧊 Iceberg Alım")

        try:
            ls5m = await get_cached(session, "ls_5m", symbol, FAPI_URL,
                                    "/futures/data/globalLongShortAccountRatio",
                                    {"symbol": symbol, "period": "5m", "limit": 2}, CACHE_LS_5M)
            if ls5m and len(ls5m) >= 2:
                ls_5m_prev = float(ls5m[-2]["longShortRatio"])
                ls_5m_curr = float(ls5m[-1]["longShortRatio"])
                ls_5m_change = ((ls_5m_curr - ls_5m_prev) / ls_5m_prev) * 100
                ls_5m_str = f"5m:{ls_5m_prev:.1f}→{ls_5m_curr:.1f}"
        except: pass

        if heavy:
            for period, cache_key, dur, var_prefix in [
                ("1h", "ls_1h", CACHE_LS_1H, "ls_1h"),
                ("6h", "ls_6h", CACHE_LS_6H, "ls_6h"),
                ("12h", "ls_12h", CACHE_LS_12H, "ls_12h")
            ]:
                try:
                    ls_data = await get_cached(session, cache_key, symbol, FAPI_URL,
                                               "/futures/data/globalLongShortAccountRatio",
                                               {"symbol": symbol, "period": period, "limit": 2}, dur)
                    if ls_data and len(ls_data) >= 2:
                        prev = float(ls_data[-2]["longShortRatio"])
                        curr = float(ls_data[-1]["longShortRatio"])
                        change = ((curr - prev) / prev) * 100
                        if var_prefix == "ls_1h":
                            ls_1h_prev, ls_1h_curr, ls_1h_change = prev, curr, change
                            ls_1h_str = f"1h:{prev:.1f}→{curr:.1f}"
                        elif var_prefix == "ls_6h":
                            ls_6h_prev, ls_6h_curr, ls_6h_change = prev, curr, change
                            ls_6h_str = f"6h:{prev:.1f}→{curr:.1f}"
                        elif var_prefix == "ls_12h":
                            ls_12h_prev, ls_12h_curr, ls_12h_change = prev, curr, change
                            ls_12h_str = f"12h:{prev:.1f}→{curr:.1f}"
                except: pass

    if funding_rate < 0 and oi_change > 5:
        score += 6
        reasons.append("💣 Funding Squeeze Hazırlığı")
        squeeze = True
    if funding_rate < -0.008 and change_pct > 0:
        score += 2; squeeze = True; reasons.append("Squeeze")

    if book_imbalance > 1.5:
        score += 4
        reasons.append("📚 Order Book Imbalance")
    if spot_premium > 0.1:
        score += 3
        reasons.append("📊 Spot Premium")

    if has_oi and btc_change <= 0 and oi_change > 3 and delta_r > 0.18:
        score += 3; reasons.append("BTC↓ güçlü")

    ls_details = []
    if ls_5m_change < -2:
        score += 2; reasons.append(f"LS 5m↓ {ls_5m_str}")
        ls_details.append(ls_5m_str)
    if ls_1h_change < -5:
        score += 3; reasons.append(f"LS 1h↓ {ls_1h_str}")
        ls_details.append(ls_1h_str)
    if ls_6h_change < -8:
        score += 5; reasons.append(f"LS 6h↓↓ {ls_6h_str}")
        ls_details.append(ls_6h_str)
    if ls_12h_change < -10:
        score += 8; reasons.append(f"LS 12h↓↓↓ {ls_12h_str}")
        ls_details.append(ls_12h_str)
    if len(ls_details) >= 2:
        score += 3; reasons.append("Multi-TF LS squeeze")

    if sector_strength and symbol in sector_strength:
        sec_bonus = max(0, min(2, sector_strength[symbol]))
        if sec_bonus > 0:
            score += sec_bonus
            reasons.append(f"🔥 Sektör Gücü +{sec_bonus}")

    # RS (0.1 kapısı)
    if len(closes) >= 24:
        coin_mom = (closes[-1] - closes[-24]) / closes[-24] * 100
        rs = coin_mom - btc_change
    elif len(closes) >= 12:
        coin_mom = (closes[-1] - closes[-12]) / closes[-12] * 100
        rs = coin_mom - btc_change
    else:
        rs = 0.0

    if rs <= 0.1:  # HAFİF KAPI
        return None

    if rs > 1.5:
        score += 4; reasons.append(f"🚀 RS {rs:.2f}")
    elif rs > 0.5:
        score += 2; reasons.append(f"✅ RS {rs:.2f}")

    ema20_1h = calculate_ema([float(k[4]) for k in klines_1h[symbol]], 20) if symbol in klines_1h and klines_1h[symbol] is not None else None
    ema50_4h = calculate_ema([float(k[4]) for k in klines_4h[symbol]], 50) if symbol in klines_4h and klines_4h[symbol] is not None else None
    bull_struct = False
    if symbol in klines_15m and klines_15m[symbol] is not None:
        kl15 = klines_15m[symbol]
        if len(kl15) >= 4:
            hh = [float(k[2]) for k in kl15[-4:]]
            ll = [float(k[3]) for k in kl15[-4:]]
            bull_struct = hh[-1] > hh[-2] and ll[-1] > ll[-2]

    if ema20_1h is not None and close_p > ema20_1h: score += 1; reasons.append("1h↑")
    if ema50_4h is not None and close_p > ema50_4h: score += 1; reasons.append("4h↑")
    if bull_struct: score += 1; reasons.append("15m↑")
    if ema50_4h is not None and ema20_1h is not None and close_p > ema50_4h and close_p > ema20_1h and bull_struct:
        score += 2; reasons.append("MTF")

    if rsi is not None and 35 < rsi < 50 and close_p > open_p:
        score += 2; reasons.append(f"RSI{rsi:.0f} dip")
    elif rsi is not None and 50 <= rsi < 65 and close_p > open_p:
        score += 1; reasons.append(f"RSI{rsi:.0f}")

    if macd_l is not None and sig_l is not None and macd_l > sig_l and hist > 0:
        score += 2; reasons.append("MACD")

    if bb_mid is not None and bb_lower is not None and close_p < bb_mid:
        if close_p > bb_lower and change_pct > 0.8:
            score += 3; reasons.append("BB-Dönüş")
    elif bb_lower is not None and close_p <= bb_lower * 1.01 and change_pct > 0:
        score += 2; reasons.append("BB")

    categories = set()
    if total_speed > 1.5: categories.add("hacim")
    if taker_r > 0.58: categories.add("taker")
    if delta_r > 0.15: categories.add("delta")
    if rs > 0: categories.add("rs")
    if bull_struct or (ema20_1h is not None and close_p > ema20_1h): categories.add("trend")
    if bb_lower is not None and close_p <= bb_mid: categories.add("bb")
    if macd_l is not None and sig_l is not None and macd_l > sig_l: categories.add("macd")
    if rsi is not None and 30 < rsi < 70: categories.add("rsi")
    if recent_volatility < older_volatility * 0.7: categories.add("squeeze_setup")
    if ls_details: categories.add("ls_squeeze")
    if vol_acceleration > 1.8: categories.add("vol_accel")
    if liquidity_sweep: categories.add("liq_sweep")
    if absorption: categories.add("absorption")
    if volume_dryup_explosion: categories.add("dryup")
    if whale_candle: categories.add("whale")
    if vwap is not None and close_p > vwap: categories.add("vwap")
    if abs(change_pct) < 0.5 and cvd_5 > 0: categories.add("cvd")
    if buying_pressure > 1.5: categories.add("buying_pressure")
    if is_squeezing: categories.add("bb_squeeze")
    if is_consolidating: categories.add("consolidation")
    if higher_lows: categories.add("higher_lows")
    if volume_dryup_explosion: categories.add("dryup_explosion")
    if abs(change_pct) < 0.5 and taker_r > 0.70: categories.add("silent_accumulation")
    if funding_rate < 0 and oi_change > 5: categories.add("funding_squeeze")
    if book_imbalance > 1.5: categories.add("book_imbalance")
    if iceberg: categories.add("iceberg")
    if spot_premium > 0.1: categories.add("spot_premium")
    if long_consolidation: categories.add("long_consolidation")
    if btc_change < -0.5 and taker_r > 0.60: categories.add("btc_resistance")
    if new_face: categories.add("new_face")

    if len(categories) < 3:
        return None

    # Watchlist (45 dakika)
    if score >= min_score - 2:
        if symbol not in watchlist:
            watchlist[symbol] = {"time": time.time(), "score": score, "rs": rs}
            return None
        else:
            if score < min_score:
                return None
            del watchlist[symbol]
    else:
        if score < min_score:
            return None

    conf = min(95, 55 + score * 3)

    if score >= 25:
        tp_mult, sl_mult = 14, 7
    elif score >= 18:
        tp_mult, sl_mult = 12, 6
    elif score >= 12:
        tp_mult, sl_mult = 10, 5
    else:
        tp_mult, sl_mult = 8, 4

    tp_price = round(close_p + atr_val * tp_mult, 4)
    sl_price = round(close_p - atr_val * sl_mult, 4)
    tp_pct = round((tp_price - close_p) / close_p * 100, 2)
    sl_pct = round((close_p - sl_price) / close_p * 100, 2)

    if tp_pct > MAX_TP_PCT:
        tp_pct = MAX_TP_PCT
        tp_price = round(close_p * (1 + MAX_TP_PCT / 100), 4)

    ls_full_str = " | ".join(ls_details) if ls_details else "N/A"

    return {
        "symbol": symbol, "score": score, "conf": conf,
        "price": round(close_p, 4), "change": round(change_pct, 2),
        "oi": round(oi_change, 2) if has_oi else -999,
        "funding": funding_rate, "delta": delta_r,
        "rel_vol": rel_vol, "squeeze": squeeze, "rs": round(rs, 2),
        "ls": ls_full_str,
        "tp": tp_price, "sl": sl_price, "tp_pct": tp_pct, "sl_pct": sl_pct,
        "reasons": reasons
    }

# =========================================================
# MAIN
# =========================================================
async def main():
    global bot_running, pending_command, consecutive_errors, watchlist, recent_signal_coins
    print(f"🚀 AZ & KALİTELİ SİNYAL BOTU")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        asyncio.create_task(telegram_polling(session))
        await send_telegram(session, f"🎯 Az & Kaliteli Sinyal Botu başlatıldı ({len(TR_COIN_LIST)} coin) | /report")

        spot_symbols = await get_spot_symbols(session)
        futures_set = await get_futures_symbols(session)
        COIN_LIST = sorted(spot_symbols)
        print(f"✅ {len(COIN_LIST)} altcoin taranıyor ({len(futures_set)} futures'ta var)")

        last_global = 0

        while True:
            if not bot_running:
                await asyncio.sleep(1)
                continue

            now = time.time()
            while recent_signal_coins and now - recent_signal_coins[0][1] > COOLDOWN_BASE:
                recent_signal_coins.popleft()

            to_remove = [s for s, d in watchlist.items() if now - d["time"] > 2700]  # 45 dakika
            for s in to_remove:
                del watchlist[s]

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
                                       {"symbol": s, "interval": "4h", "limit": 120}, CACHE_4H) for s in fut_list]
                tasks_15m = [get_cached(session, "klines_15m", s, FAPI_URL, "/fapi/v1/klines",
                                        {"symbol": s, "interval": "15m", "limit": 6}, CACHE_15M) for s in fut_list]
                r1, r4, r15 = await asyncio.gather(asyncio.gather(*tasks_1h), asyncio.gather(*tasks_4h), asyncio.gather(*tasks_15m))

                k1 = {s: r for s, r in zip(fut_list, r1) if r is not None}
                k4 = {s: r for s, r in zip(fut_list, r4) if r is not None}
                k15 = {s: r for s, r in zip(fut_list, r15) if r is not None}

                tasks_1m = {}
                for s in fut_list:
                    tasks_1m[s] = get_cached(session, "klines_1m", s, FAPI_URL,
                                             "/fapi/v1/klines",
                                             {"symbol": s, "interval": "1m", "limit": 20}, CACHE_1M)
                keys_1m = list(tasks_1m.keys())
                vals_1m = await asyncio.gather(*[tasks_1m[k] for k in keys_1m])
                k1m = {k: v for k, v in zip(keys_1m, vals_1m) if v is not None}

                tasks_5m = []
                for s in COIN_LIST:
                    base = FAPI_URL if s in futures_set else SPOT_GLOBAL_URL
                    ep = "/fapi/v1/klines" if s in futures_set else "/api/v3/klines"
                    tasks_5m.append(get_cached(session, "klines_5m", s, base, ep,
                                               {"symbol": s, "interval": "5m", "limit": 50}, CACHE_5M))
                resp_5m = await asyncio.gather(*tasks_5m)

                valid = {}
                vols = []
                for s, r in zip(COIN_LIST, resp_5m):
                    if r and len(r) >= 30:
                        valid[s] = r
                        try:
                            vols.append(float(r[-2][5]))
                        except: pass

                fvols = [v for v in vols if v > 100000]
                market_median = median(sorted(fvols)[2:-2]) if len(fvols) > 4 else (median(fvols) if fvols else 1)

                sector_strength = {}
                for sector, coins in SECTOR_GROUPS.items():
                    sector_rs = []
                    for coin in coins:
                        if coin in valid:
                            coin_closes = [float(k[4]) for k in valid[coin][-20:-1]]
                            if len(coin_closes) >= 12 and coin_closes[-12] != 0:
                                coin_mom = (coin_closes[-1] - coin_closes[-12]) / coin_closes[-12] * 100
                                sector_rs.append(coin_mom - btc_change)
                    if sector_rs and mean(sector_rs) > 0.3:
                        for coin in coins:
                            if coin in valid:
                                coin_closes = [float(k[4]) for k in valid[coin][-20:-1]]
                                if len(coin_closes) >= 12 and coin_closes[-12] != 0:
                                    coin_mom = (coin_closes[-1] - coin_closes[-12]) / coin_closes[-12] * 100
                                    sector_strength[coin] = max(0, min(2, int(mean(sector_rs) * 3)))

                scan_tasks = []
                for s in COIN_LIST:
                    if s not in valid: continue
                    kl_1m = k1m.get(s) if s in futures_set else None
                    scan_tasks.append(scan_coin(session, s, s in futures_set, valid[s], kl_1m, market_median,
                                                btc_change, min_score,
                                                k1, k4, k15, daily_map.get(s), sector_strength))

                all_res = []
                for i in range(0, len(scan_tasks), BATCH_SIZE):
                    batch = scan_tasks[i:i+BATCH_SIZE]
                    all_res.extend([r for r in await asyncio.gather(*batch) if r])

                for r in all_res:
                    r['relvol_score'] = (r['rel_vol'] * 0.5) + (r['score'] * 0.3) + (r['rs'] * 0.2)
                all_res.sort(key=lambda x: x['relvol_score'], reverse=True)
                relvol_leader = all_res[0] if all_res else None
                if relvol_leader:
                    all_res.remove(relvol_leader)

                for r in all_res:
                    r['rank'] = (
                        (r['score'] * 0.35) +
                        (min(r['rel_vol'], 4) * 0.20) +
                        (abs(r['delta']) * 0.20) +
                        (r['rs'] * 0.15) +
                        (max(r['oi'], 0) * 0.10 if r['oi'] != -999 else 0)
                    )
                all_res.sort(key=lambda x: x['rank'], reverse=True)

                now = time.time()
                is_forced = (pending_command == "FORCE_NEXT")

                sent = 0
                if (now - last_global >= GLOBAL_COOLDOWN) or is_forced:
                    if relvol_leader and sent < MAX_SIGNALS_PER_ROUND:
                        if not (relvol_leader['symbol'] in last_signals and now - last_signals[relvol_leader['symbol']] < COOLDOWN_BASE):
                            reasons = ", ".join(relvol_leader['reasons'])
                            oi_str = f"%{relvol_leader['oi']:.2f}" if relvol_leader['oi'] != -999 else "N/A"
                            msg = (
                                f"🔥 <b>HACİM LİDERİ: {relvol_leader['symbol']} (LONG)</b>\n"
                                f"Puan: {relvol_leader['score']} | Güven: %{relvol_leader['conf']}\n"
                                f"Giriş: {relvol_leader['price']} | %{relvol_leader['change']}\n"
                                f"🎯 TP: {relvol_leader['tp']} (%{relvol_leader['tp_pct']}) | 🛑 SL: {relvol_leader['sl']} (%{relvol_leader['sl_pct']})\n"
                                f"OI: {oi_str} | RelVol: {relvol_leader['rel_vol']}x\n"
                                f"🚀 RS: {relvol_leader['rs']:.2f} | LS: {relvol_leader['ls']}\n"
                                f"Funding: {relvol_leader['funding']*100:.4f}% | Delta: {relvol_leader['delta']:.2f}\n"
                                f"Sebep: {reasons}"
                            )
                            await send_telegram(session, msg)
                            last_signals[relvol_leader['symbol']] = now
                            recent_signal_coins.append((relvol_leader['symbol'], now))
                            sent += 1
                            await asyncio.sleep(random.uniform(0.5, 1.0))

                    for r in all_res:
                        if sent >= MAX_SIGNALS_PER_ROUND: break
                        if r['symbol'] in last_signals and now - last_signals[r['symbol']] < COOLDOWN_BASE: continue
                        reasons = ", ".join(r['reasons'])
                        oi_str = f"%{r['oi']:.2f}" if r['oi'] != -999 else "N/A"
                        msg = (
                            f"🟢 <b>{r['symbol']} (LONG)</b>\n"
                            f"Puan: {r['score']} | Güven: %{r['conf']}\n"
                            f"Giriş: {r['price']} | %{r['change']}\n"
                            f"🎯 TP: {r['tp']} (%{r['tp_pct']}) | 🛑 SL: {r['sl']} (%{r['sl_pct']})\n"
                            f"OI: {oi_str} | RelVol: {r['rel_vol']}x\n"
                            f"🚀 RS: {r['rs']:.2f} | LS: {r['ls']}\n"
                            f"Funding: {r['funding']*100:.4f}% | Delta: {r['delta']:.2f}\n"
                            f"Sebep: {reasons}"
                        )
                        await send_telegram(session, msg)
                        last_signals[r['symbol']] = now
                        recent_signal_coins.append((r['symbol'], now))
                        if r['symbol'] not in signal_tracker:
                            signal_tracker[r['symbol']] = {
                                'price': r['price'],
                                'time': now,
                                'score': r['score'],
                                'rs': r['rs'],
                                'ls': r['ls']
                            }
                        sent += 1
                        await asyncio.sleep(random.uniform(0.5, 1.0))

                    if sent > 0: last_global = now

                if is_forced: pending_command = None

                print(f"🔍 {len(all_res)} aday + {'1' if relvol_leader else '0'} RelVol lideri (Min Skor: {min_score})")

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
