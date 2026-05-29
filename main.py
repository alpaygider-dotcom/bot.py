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
SPOT_TR_URL = "https://api.trbinance.com"
SPOT_GLOBAL_URL = "https://api.binance.com"  # YEDEK

# Önbellek süreleri (saniye)
CACHE_5M = 35
CACHE_15M = 180
CACHE_1H = 300
CACHE_4H = 900
CACHE_OI = 120
CACHE_FUNDING = 300

COOLDOWN = 600
GLOBAL_COOLDOWN = 90
MAX_SIGNALS_PER_ROUND = 3
BATCH_SIZE = 25

MAX_CONSECUTIVE_ERRORS = 15

SEMAPHORE = asyncio.Semaphore(20)

STABLECOIN_BLACKLIST = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "USDPUSDT", "FDUSDUSDT", "USTCUSDT", "EURSUSDT"
}

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
        async with session.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }) as r:
            return await r.text()
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
                for update in resp.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip().lower()

                    if text == "/status":
                        await send_telegram(session,
                            f"🤖 Bot aktif\n"
                            f"Bekleyen coin: {len(last_signals)}\n"
                            f"Son sinyaller: {len(signal_history)}"
                        )
                    elif text == "/stop":
                        bot_running = False
                        await send_telegram(session, "🛑 Bot durduruldu")
                    elif text == "/start":
                        bot_running = True
                        await send_telegram(session, "✅ Bot yeniden aktif")
                    elif text == "/next":
                        pending_command = "FORCE_NEXT"
                        await send_telegram(session, "⏩ Cooldown bypass aktif")
                    elif text == "/ping":
                        await send_telegram(session, "🏓 Pong")
        except Exception:
            pass
        await asyncio.sleep(1)

# =========================================================
# API (EXPONENTIAL BACKOFF)
# =========================================================
async def fetch(session, url, params=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    backoff = 2
    for attempt in range(3):
        try:
            async with SEMAPHORE:
                async with session.get(url, params=params, headers=headers, timeout=20) as resp:
                    if resp.status == 429:
                        print(f"⚠️ HTTP 429: {backoff}s bekleniyor...")
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
# İNDİKATÖRLER (GERÇEK MACD + is not None KONTROLLERİ)
# =========================================================
def calculate_ema(prices, period):
    if len(prices) < period: return None
    multiplier = 2 / (period + 1)
    ema = mean(prices[:period])
    for p in prices[period:]:
        ema = (p - ema) * multiplier + ema
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
    fast_multiplier = 2 / (fast + 1)
    slow_multiplier = 2 / (slow + 1)
    ema_fast_vals = [mean(prices[:fast])]
    ema_slow_vals = [mean(prices[:slow])]
    for i in range(fast, len(prices)):
        ema_fast_vals.append((prices[i] - ema_fast_vals[-1]) * fast_multiplier + ema_fast_vals[-1])
    for i in range(slow, len(prices)):
        ema_slow_vals.append((prices[i] - ema_slow_vals[-1]) * slow_multiplier + ema_slow_vals[-1])
    start_index = slow - fast
    fast_vals = ema_fast_vals[start_index:]
    slow_vals = ema_slow_vals
    macd_line = [fast_vals[i] - slow_vals[i] for i in range(len(fast_vals))]
    signal_multiplier = 2 / (signal + 1)
    signal_line_vals = [mean(macd_line[:signal])]
    for i in range(signal, len(macd_line)):
        signal_line_vals.append((macd_line[i] - signal_line_vals[-1]) * signal_multiplier + signal_line_vals[-1])
    histogram = macd_line[-1] - signal_line_vals[-1]
    return macd_line[-1], signal_line_vals[-1], histogram

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period: return None, None, None
    sma = mean(prices[-period:])
    std = stdev(prices[-period:])
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    return sma, upper, lower

def calculate_atr(highs, lows, closes, period=14):
    if len(highs) < period + 1: return None
    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(highs))]
    return mean(tr[-period:]) if tr else None

# =========================================================
# SEMBOL LİSTESİ (ÖNCE TR, BAŞARISIZSA GLOBAL)
# =========================================================
async def get_spot_symbols(session):
    """Önce Binance TR'yi dener, başarısız olursa Global Binance'e düşer."""
    # 1. Binance TR dene
    info = await fetch_api(session, SPOT_TR_URL, "/api/v3/exchangeInfo")
    if info:
        symbols = {s["symbol"] for s in info.get("symbols", [])
                   if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
                   and s["symbol"] not in STABLECOIN_BLACKLIST}
        if symbols:
            await send_telegram(session, f"✅ Binance TR: {len(symbols)} coin taranacak.")
            return symbols

    # 2. Global Binance'e düş
    await send_telegram(session, "⚠️ Binance TR API'sine erişilemedi. Global Binance üzerinden devam ediliyor.")
    info = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/exchangeInfo")
    if not info:
        await send_telegram(session, "❌ Global Binance API'sine de erişilemedi!")
        return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
            and s["symbol"] not in STABLECOIN_BLACKLIST}

async def get_futures_symbols(session):
    info = await fetch_api(session, FAPI_URL, "/fapi/v1/exchangeInfo")
    if not info:
        return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING" and s["symbol"] not in STABLECOIN_BLACKLIST}

async def get_daily_change_map(session, symbols):
    change_map = {}
    # Önce TR spot, sonra global spot
    for base in (SPOT_TR_URL, SPOT_GLOBAL_URL):
        data = await fetch_api(session, base, "/api/v3/ticker/24hr")
        if data:
            for item in data:
                sym = item.get("symbol", "")
                if sym in symbols:
                    try: change_map[sym] = float(item["priceChangePercent"])
                    except: change_map[sym] = 0.0
            if change_map:
                break
    return change_map

# =========================================================
# SCAN COIN (TÜM PROFESYONEL FİLTRELER KORUNDU)
# =========================================================
async def scan_coin(session, symbol, is_futures, kl_5m, market_median,
                    btc_change, min_score_atr, klines_1h_cache, klines_4h_cache, klines_15m_cache, daily_change):
    if symbol in last_signals and time.time() - last_signals[symbol] < COOLDOWN:
        return None
    if daily_change is not None and daily_change > 10.0:
        return None
    if not kl_5m or len(kl_5m) < 45:
        return None

    closed = kl_5m[:-1]
    last_closed = closed[-1]
    open_price, close_price, high, low, volume, quote_volume, taker_buy = (
        float(last_closed[1]), float(last_closed[4]), float(last_closed[2]), float(last_closed[3]),
        float(last_closed[5]), float(last_closed[7]), float(last_closed[9])
    )
    change_pct = ((close_price - open_price) / open_price) * 100

    if 0.99 < close_price < 1.01 and abs(change_pct) < 0.1:
        return None
    if len(closed) >= 13:
        price_1h_ago = float(closed[-13][4])
        hour_change = (close_price - price_1h_ago) / price_1h_ago * 100
        if hour_change > 8.0:
            return None

    if quote_volume < 3_000_000: return None
    if abs(change_pct) > 8.0: return None

    prev_vols = [float(k[5]) for k in kl_5m[-7:-2]]
    avg_vol = mean(prev_vols) if prev_vols else volume
    speed_ratio = volume / avg_vol if avg_vol > 0 else 0
    rel_vol = round(speed_ratio, 2)

    heavy_check = speed_ratio > 1.2 or volume > market_median * 1.5
    taker_ratio = taker_buy / volume if volume > 0 else 0
    delta = taker_buy - (volume - taker_buy)
    delta_ratio = delta / volume if volume > 0 else 0
    body_ratio = abs(close_price - open_price) / (high - low) if (high - low) > 0 else 0
    wick_ratio = 1 - body_ratio

    highs = [float(k[2]) for k in closed[-45:]]
    lows = [float(k[3]) for k in closed[-45:]]
    closes = [float(k[4]) for k in closed[-45:]]
    atr_val = calculate_atr(highs, lows, closes)
    if atr_val is None: return None

    rsi = calculate_rsi(closes, 14)
    macd_line, signal_line, histogram = calculate_real_macd(closes)
    bb_mid, bb_upper, bb_lower = calculate_bollinger(closes, 20, 2)

    # OI / Funding (tüm futures coinler için, cache ile)
    oi_change = 0.0
    funding_rate = 0.0
    if is_futures:
        oi_data = await get_cached(session, "oi", symbol, FAPI_URL,
                                   "/fapi/v1/openInterestHist",
                                   {"symbol": symbol, "period": "5m", "limit": 2}, CACHE_OI)
        if oi_data and len(oi_data) >= 2:
            prev_oi = float(oi_data[-2]["sumOpenInterestValue"])
            curr_oi = float(oi_data[-1]["sumOpenInterestValue"])
            if prev_oi > 0: oi_change = round(((curr_oi - prev_oi) / prev_oi) * 100, 2)

        funding = await get_cached(session, "funding", symbol, FAPI_URL,
                                   "/fapi/v1/premiumIndex", {"symbol": symbol}, CACHE_FUNDING)
        if funding: funding_rate = float(funding.get("lastFundingRate", 0))

    # Trend (cache'ten)
    ema20_1h = ema50_4h = None
    bullish_structure = False

    if symbol in klines_1h_cache and klines_1h_cache[symbol] is not None:
        closes_1h = [float(k[4]) for k in klines_1h_cache[symbol]]
        ema20_1h = calculate_ema(closes_1h, 20)

    if symbol in klines_4h_cache and klines_4h_cache[symbol] is not None:
        closes_4h = [float(k[4]) for k in klines_4h_cache[symbol]]
        ema50_4h = calculate_ema(closes_4h, 50)

    if symbol in klines_15m_cache and klines_15m_cache[symbol] is not None:
        kl_15m = klines_15m_cache[symbol]
        if len(kl_15m) >= 4:
            h_list = [float(k[2]) for k in kl_15m[-4:]]
            l_list = [float(k[3]) for k in kl_15m[-4:]]
            if h_list[-1] > h_list[-2] and l_list[-1] > l_list[-2]:
                bullish_structure = True

    # ========== LONG SKORLAMA ==========
    long_score = 0
    reasons = []
    squeeze = False

    if speed_ratio > 1.8 and change_pct > 0 and rel_vol > 0.5:
        long_score += 2; reasons.append("Hacim patlaması")
    recent_range = high - low
    if recent_range > 0:
        norm_ch = change_pct / (recent_range / close_price * 100) if (recent_range / close_price * 100) > 0 else 0
        if 0.1 < norm_ch < 5 and norm_ch > 0:
            long_score += 2; reasons.append("Normalize hareket")

    if taker_ratio > 0.55: long_score += 2; reasons.append("Taker alım")
    if delta_ratio > 0.15: long_score += 2; reasons.append("Delta pozitif")
    if oi_change > 1 and delta_ratio > 0.15 and close_price > open_price:
        long_score += 2; reasons.append("OI + delta")
    if funding_rate < -0.005 and change_pct > 0:
        long_score += 2; squeeze = True; reasons.append("Funding squeeze")

    if btc_change > 0: rs = change_pct - btc_change
    else: rs = change_pct + abs(btc_change)
    if rs > 1.0: long_score += 2; reasons.append(f"RS {rs:.1f}")

    if heavy_check and len(kl_5m) > 6:
        recent_high = max(float(k[2]) for k in kl_5m[-7:-2])
        recent_low = min(float(k[3]) for k in kl_5m[-7:-2])
        comp_range = recent_high - recent_low
        comp = (comp_range / close_price) * 100 if close_price > 0 else 0
        bk_strength = (speed_ratio > 1.5 and delta_ratio > 0.12 and taker_ratio > 0.54)
        if comp < 1.2 and bk_strength and delta_ratio > 0.12:
            long_score += 3; reasons.append("Sıkışma kırılımı")

    if btc_change <= 0 and oi_change > 2 and delta_ratio > 0.15:
        long_score += 3; reasons.append("BTC'ye rağmen güçlü")

    if ema20_1h is not None and close_price > ema20_1h: long_score += 1; reasons.append("1h EMA20 üstü")
    if ema50_4h is not None and close_price > ema50_4h: long_score += 2; reasons.append("4h EMA50 üstü")
    if bullish_structure: long_score += 2; reasons.append("15m yükselen yapı")

    if rel_vol > 1.8: long_score += 2; reasons.append("Yüksek rel vol")
    elif rel_vol > 1.3: long_score += 1

    if wick_ratio > 0.5: long_score -= 1
    if btc_change <= -0.8: long_score -= 4; reasons.append("BTC düşüş baskısı")
    if btc_change > 1.5 and symbol != "BTCUSDT": long_score -= 2

    if ema50_4h is not None and ema20_1h is not None and close_price > ema50_4h and close_price > ema20_1h and bullish_structure:
        long_score += 3; reasons.append("Multi-TF uyumu")

    if rsi is not None and 30 < rsi < 70 and close_price > open_price:
        long_score += 1; reasons.append(f"RSI {rsi:.0f}")
    if macd_line is not None and signal_line is not None and histogram is not None and macd_line > signal_line and histogram > 0:
        long_score += 2; reasons.append("MACD bullish")
    if bb_lower is not None and close_price <= bb_lower * 1.01 and change_pct > 0:
        long_score += 2; reasons.append("Bollinger alt dönüş")

    effective_min_score = min_score_atr if is_futures else min_score_atr - 1
    if long_score < effective_min_score:
        return None

    confidence = min(95, 45 + int(long_score * 3))

    # Spot uyumlu TP/SL
    tp_mult = max(4.0, min(10.0, 5.0 + rel_vol * 0.5))
    sl_mult = max(2.0, min(5.0, 2.5 + rel_vol * 0.3))
    sl_price = round(close_price - atr_val * sl_mult, 4)
    tp_price = round(close_price + atr_val * tp_mult, 4)

    return {
        "symbol": symbol, "direction": "LONG", "score": long_score, "confidence": confidence,
        "price": round(close_price, 4), "change": round(change_pct, 2), "oi": oi_change,
        "funding": funding_rate, "delta": delta_ratio, "rel_vol": rel_vol, "trend": "Bullish",
        "squeeze": squeeze, "rs": round(rs, 1), "sl": sl_price, "tp": tp_price, "reasons": reasons
    }

# =========================================================
# MAIN (HATA SAYACI VE SENTEZ EKLENDİ)
# =========================================================
async def main():
    global bot_running, pending_command, consecutive_errors
    print("🚀 FİNAL SENTEZ BOT (Tüm Profesyonel Filtreler + Hata Koruması)")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        asyncio.create_task(telegram_polling(session))
        await send_telegram(session, "🎯 Final Sentez Bot Başlatıldı. /status /stop /start /next /ping")

        spot_symbols = await get_spot_symbols(session)
        if not spot_symbols:
            await send_telegram(session, "❌ Sembol listesi alınamadı, bot 60 saniye sonra tekrar deneyecek.")
            await asyncio.sleep(60)
            return  # Railway otomatik restart edecektir

        futures_set = await get_futures_symbols(session)
        COIN_LIST = sorted(spot_symbols)
        print(f"✅ {len(COIN_LIST)} coin taranacak. ({len(futures_set)} tanesi futures'ta var)")

        last_global_signal = 0

        while True:
            if not bot_running:
                await asyncio.sleep(1)
                continue

            try:
                start_time = time.time()

                daily_map = await get_daily_change_map(session, spot_symbols)

                btc_klines = await fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                             {"symbol": "BTCUSDT", "interval": "15m", "limit": 10})
                btc_change = 0.0; btc_atr_percent = 0.0
                if btc_klines:
                    btc_open, btc_close = float(btc_klines[-2][1]), float(btc_klines[-2][4])
                    btc_change = ((btc_close - btc_open) / btc_open) * 100
                    btc_highs = [float(k[2]) for k in btc_klines[-5:]]; btc_lows = [float(k[3]) for k in btc_klines[-5:]]
                    btc_atr_percent = ((max(btc_highs) - min(btc_lows)) / min(btc_lows)) * 100

                min_score_atr = 7 if btc_atr_percent < 1.0 else (9 if btc_atr_percent > 2.5 else 8)

                futures_list = [s for s in COIN_LIST if s in futures_set]

                # Toplu cache (akıllı sürelerle)
                tasks_1h = [get_cached(session, "klines_1h", sym, FAPI_URL, "/fapi/v1/klines",
                                       {"symbol": sym, "interval": "1h", "limit": 20}, CACHE_1H) for sym in futures_list]
                tasks_4h = [get_cached(session, "klines_4h", sym, FAPI_URL, "/fapi/v1/klines",
                                       {"symbol": sym, "interval": "4h", "limit": 60}, CACHE_4H) for sym in futures_list]
                tasks_15m = [get_cached(session, "klines_15m", sym, FAPI_URL, "/fapi/v1/klines",
                                        {"symbol": sym, "interval": "15m", "limit": 6}, CACHE_15M) for sym in futures_list]

                responses_1h, responses_4h, responses_15m = await asyncio.gather(
                    asyncio.gather(*tasks_1h), asyncio.gather(*tasks_4h), asyncio.gather(*tasks_15m)
                )

                klines_1h_cache = {sym: r for sym, r in zip(futures_list, responses_1h) if r is not None}
                klines_4h_cache = {sym: r for sym, r in zip(futures_list, responses_4h) if r is not None}
                klines_15m_cache = {sym: r for sym, r in zip(futures_list, responses_15m) if r is not None}

                # 5m (her tur güncel)
                tasks_5m = []
                for sym in COIN_LIST:
                    base_url = FAPI_URL if sym in futures_set else SPOT_TR_URL
                    endpoint = "/fapi/v1/klines" if sym in futures_set else "/api/v3/klines"
                    tasks_5m.append(get_cached(session, "klines_5m", sym, base_url, endpoint,
                                               {"symbol": sym, "interval": "5m", "limit": 60}, CACHE_5M))
                responses_5m = await asyncio.gather(*tasks_5m)

                valid_responses = {}
                vols = []
                for sym, r in zip(COIN_LIST, responses_5m):
                    if r is not None and len(r) >= 45:
                        valid_responses[sym] = r
                        try: vols.append(float(r[-2][5]))
                        except: pass

                filtered_vols = [v for v in vols if v > 100000]
                market_median = median(sorted(filtered_vols)[2:-2]) if len(filtered_vols) > 4 else (median(filtered_vols) if filtered_vols else 1)

                # Tarama görevleri
                scan_tasks = []
                for sym in COIN_LIST:
                    if sym not in valid_responses: continue
                    is_fut = sym in futures_set
                    task = scan_coin(session, sym, is_fut, valid_responses[sym], market_median,
                                    btc_change, min_score_atr, klines_1h_cache, klines_4h_cache, klines_15m_cache,
                                    daily_map.get(sym))
                    scan_tasks.append(task)

                # Batch işlem
                all_results = []
                for i in range(0, len(scan_tasks), BATCH_SIZE):
                    batch = scan_tasks[i:i+BATCH_SIZE]
                    batch_results = await asyncio.gather(*batch)
                    all_results.extend([r for r in batch_results if r is not None])

                for coin in all_results:
                    coin['final_rank'] = (coin['score'] * 0.4) + (coin['rel_vol'] * 0.2) + (abs(coin['delta']) * 0.2) + (abs(coin['oi']) * 0.2)
                all_results.sort(key=lambda x: x['final_rank'], reverse=True)

                # =========================================================
                # SİNYAL GÖNDERİM (GLOBAL COOLDOWN VE /next)
                # =========================================================
                now = time.time()
                is_forced = (pending_command == "FORCE_NEXT")

                if (now - last_global_signal >= GLOBAL_COOLDOWN) or is_forced:
                    signals_sent = 0
                    max_allowed = MAX_SIGNALS_PER_ROUND

                    for coin in all_results:
                        if signals_sent >= max_allowed:
                            break

                        if coin['symbol'] in last_signals and now - last_signals[coin['symbol']] < COOLDOWN:
                            continue

                        reasons_str = ", ".join(coin.get('reasons', []))
                        msg = (
                            f"🟢 *{coin['symbol']} (LONG)*\n"
                            f"Puan: {coin['score']} | Güven: %{coin['confidence']}\n"
                            f"Giriş: {coin['price']} | %{coin['change']}\n"
                            f"🎯 TP: {coin['tp']:.4f} | 🛑 SL: {coin['sl']:.4f}\n"
                            f"OI: %{coin['oi']:.2f} | RelVol: {coin['rel_vol']}x\n"
                            f"RS: {coin['rs']:.1f} | Funding: {coin['funding']*100:.4f}%\n"
                            f"Delta: {coin['delta']:.2f} | Sebep: {reasons_str}"
                        )
                        await send_telegram(session, msg)
                        print(f"✅ {coin['symbol']} LONG (Puan: {coin['score']})")
                        last_signals[coin['symbol']] = now
                        signal_history.append(coin['symbol'])
                        signals_sent += 1
                        await asyncio.sleep(random.uniform(0.5, 1.0))

                    if signals_sent > 0:
                        last_global_signal = now
                else:
                    if all_results:
                        remaining = int(GLOBAL_COOLDOWN - (now - last_global_signal))
                        print(f"⏳ Küresel Cooldown devrede ({remaining}sn kaldı). {all_results[0]['symbol']} sonraki tura saklanıyor.")

                if is_forced:
                    pending_command = None

                print(f"🔍 Eşiği geçen {len(all_results)} LONG adayı (Min Skor: {min_score_atr})")

                # Hata sayacını sıfırla
                consecutive_errors = 0

                elapsed = time.time() - start_time
                if elapsed < 35:
                    await asyncio.sleep(35 - elapsed)
                else:
                    await asyncio.sleep(1)

            except Exception as e:
                consecutive_errors += 1
                print(f"Kritik hata: {e}")
                traceback.print_exc()

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    await send_telegram(session, "❌ Çok fazla ardışık hata oluştu. Bot 2 dakika dinlenmeye geçiyor.")
                    await asyncio.sleep(120)
                    consecutive_errors = 0
                else:
                    await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
