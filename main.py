import asyncio
import aiohttp
import os
import time
import random
from statistics import mean, median, stdev

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
SPOT_GLOBAL_URL = "https://api.binance.com"

CACHE_DURATION = 180
COOLDOWN = 600
GLOBAL_COOLDOWN = 120

SEMAPHORE = asyncio.Semaphore(20)

STABLECOIN_BLACKLIST = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "USDPUSDT", "FDUSDUSDT", "USTCUSDT", "EURSUSDT"
}

cache = {"funding": {}, "oi": {}, "klines_1h": {}}
last_signals = {}
bot_running = True
pending_command = None

# =========================================================
# TELEGRAM
# =========================================================
async def send_telegram(session, text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        await session.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"Telegram gönderme hatası: {e}")

async def telegram_polling(session):
    global bot_running, pending_command
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            resp = await fetch(session, url, {"offset": offset, "timeout": 30})
            if resp and resp.get("ok") and resp["result"]:
                for update in resp["result"]:
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip().lower()
                    if text == "/status":
                        await send_telegram(session, f"🤖 Bot çalışıyor. Son sinyaller: {len(last_signals)} coin beklemede.")
                    elif text == "/stop":
                        bot_running = False
                        await send_telegram(session, "🛑 Bot durduruldu.")
                    elif text == "/start":
                        bot_running = True
                        await send_telegram(session, "✅ Bot yeniden başlatıldı.")
                    elif text == "/next":
                        pending_command = "FORCE_NEXT"
                        await send_telegram(session, "⏩ Bir sonraki sinyal zorla gönderilecek.")
        except:
            pass
        await asyncio.sleep(1)

# =========================================================
# API
# =========================================================
async def fetch(session, url, params=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with SEMAPHORE:
            async with session.get(url, params=params, headers=headers, timeout=20) as resp:
                if resp.status == 429:
                    await asyncio.sleep(5)
                    return None
                if resp.status != 200:
                    return None
                return await resp.json()
    except:
        return None

async def fetch_api(session, base, endpoint, params=None):
    return await fetch(session, f"{base}{endpoint}", params)

async def get_cached(session, cache_name, key, base, endpoint, params):
    now = time.time()
    if key in cache[cache_name] and now - cache[cache_name][key]["time"] < CACHE_DURATION:
        return cache[cache_name][key]["data"]
    data = await fetch_api(session, base, endpoint, params)
    if data:
        cache[cache_name][key] = {"time": now, "data": data}
    return data

# =========================================================
# İNDİKATÖRLER
# =========================================================
def calculate_ema(prices, period):
    if len(prices) < period: return None
    m = 2 / (period + 1)
    ema = mean(prices[:period])
    for p in prices[period:]: ema = (p - ema) * m + ema
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

def calculate_macd(prices, fast=12, slow=26, signal=9):
    ema_fast = calculate_ema(prices, fast)
    ema_slow = calculate_ema(prices, slow)
    if ema_fast is None or ema_slow is None: return None, None, None
    macd_line = ema_fast - ema_slow
    if len(prices) >= slow + signal:
        macd_vals = []
        for i in range(slow-1, len(prices)):
            e_f = calculate_ema(prices[:i+1], fast)
            e_s = calculate_ema(prices[:i+1], slow)
            if e_f and e_s: macd_vals.append(e_f - e_s)
        if len(macd_vals) >= signal:
            signal_line = calculate_ema(macd_vals, signal)
            histogram = macd_line - signal_line
            return macd_line, signal_line, histogram
    return macd_line, None, None

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
# BINANCE TR SPOT LİSTESİ (Tüm coinler buradan)
# =========================================================
async def get_spot_symbols_tr(session):
    info = await fetch_api(session, SPOT_TR_URL, "/api/v3/exchangeInfo")
    if not info:
        info = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/exchangeInfo")
    if not info:
        return set()
    symbols = set()
    for s in info.get("symbols", []):
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            sym = s["symbol"]
            if sym not in STABLECOIN_BLACKLIST:
                symbols.add(sym)
    return symbols

async def get_futures_symbols(session):
    info = await fetch_api(session, FAPI_URL, "/fapi/v1/exchangeInfo")
    if not info:
        return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
            and s["symbol"] not in STABLECOIN_BLACKLIST}

async def get_daily_change_map(session, symbols):
    change_map = {}
    data = await fetch_api(session, SPOT_TR_URL, "/api/v3/ticker/24hr")
    if not data:
        data = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/ticker/24hr")
    if data:
        for item in data:
            sym = item.get("symbol", "")
            if sym in symbols:
                try: change_map[sym] = float(item["priceChangePercent"])
                except: change_map[sym] = 0.0
    return change_map

# =========================================================
# SCAN COIN (FUTURES VARSA ORADAN, YOKSA SPOT)
# =========================================================
async def scan_coin(session, symbol, is_futures_available, market_median,
                    btc_change, min_score_atr, klines_1h_cache, daily_change):
    if symbol in last_signals and time.time() - last_signals[symbol] < COOLDOWN:
        return None
    if daily_change is not None and daily_change > 10.0:
        return None

    # 5m verisi: futures varsa oradan, yoksa TR spot'tan
    if is_futures_available:
        kl_5m = await fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                {"symbol": symbol, "interval": "5m", "limit": 20})
        if not kl_5m:
            kl_5m = await fetch_api(session, SPOT_TR_URL, "/api/v3/klines",
                                    {"symbol": symbol, "interval": "5m", "limit": 20})
    else:
        kl_5m = await fetch_api(session, SPOT_TR_URL, "/api/v3/klines",
                                {"symbol": symbol, "interval": "5m", "limit": 20})
        if not kl_5m:
            kl_5m = await fetch_api(session, SPOT_GLOBAL_URL, "/api/v3/klines",
                                    {"symbol": symbol, "interval": "5m", "limit": 20})

    if not kl_5m or len(kl_5m) < 20:
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

    taker_ratio = taker_buy / volume if volume > 0 else 0

    if quote_volume < 3_000_000: return None
    if abs(change_pct) > 8.0: return None

    if market_median > 0 and volume > 0:
        rel_vol = round(volume / market_median, 2)
    else:
        rel_vol = 0.0

    prev_vols = [float(k[5]) for k in kl_5m[-7:-2]]
    avg_vol = mean(prev_vols) if prev_vols else volume
    speed_ratio = volume / avg_vol if avg_vol > 0 else 0
    heavy_check = speed_ratio > 1.2 or rel_vol > 1.2

    delta = taker_buy - (volume - taker_buy)
    delta_ratio = delta / volume if volume > 0 else 0
    body_ratio = abs(close_price - open_price) / (high - low) if (high - low) > 0 else 0
    wick_ratio = 1 - body_ratio

    highs = [float(k[2]) for k in kl_5m[-16:-1]]
    lows = [float(k[3]) for k in kl_5m[-16:-1]]
    closes = [float(k[4]) for k in kl_5m[-16:-1]]
    atr_val = calculate_atr(highs, lows, closes)
    if atr_val is None:
        return None

    rsi = calculate_rsi(closes, 14)
    macd_line, signal_line, histogram = calculate_macd(closes)
    bb_mid, bb_upper, bb_lower = calculate_bollinger(closes, 20, 2)

    # OI / Funding (sadece futures)
    oi_change = None
    funding_rate = 0.0
    if is_futures_available:
        oi_data = await get_cached(session, "oi", symbol, FAPI_URL,
                                   "/fapi/v1/openInterestHist",
                                   {"symbol": symbol, "period": "5m", "limit": 2})
        if oi_data and len(oi_data) >= 2:
            prev_oi = float(oi_data[-2]["sumOpenInterestValue"])
            curr_oi = float(oi_data[-1]["sumOpenInterestValue"])
            if prev_oi > 0: oi_change = round(((curr_oi - prev_oi) / prev_oi) * 100, 2)
        else:
            oi_change = 0.0

        funding = await get_cached(session, "funding", symbol, FAPI_URL,
                                   "/fapi/v1/premiumIndex", {"symbol": symbol})
        if funding: funding_rate = float(funding.get("lastFundingRate", 0))

    # Trend
    ema20_1h = ema50_4h = None
    bullish_structure = False
    if symbol in klines_1h_cache:
        kl_1h = klines_1h_cache[symbol]
        closes_1h = [float(k[4]) for k in kl_1h]
        ema20_1h = calculate_ema(closes_1h, 20)

    if heavy_check and is_futures_available:
        kl_4h = await fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                {"symbol": symbol, "interval": "4h", "limit": 60})
        kl_15m = await fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                 {"symbol": symbol, "interval": "15m", "limit": 6})
        if kl_4h:
            closes_4h = [float(k[4]) for k in kl_4h]
            ema50_4h = calculate_ema(closes_4h, 50)
        if kl_15m and len(kl_15m) >= 4:
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
    if oi_change is not None and oi_change > 1 and delta_ratio > 0.15 and close_price > open_price:
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

    if btc_change <= 0 and oi_change is not None and oi_change > 2 and delta_ratio > 0.15:
        long_score += 3; reasons.append("BTC'ye rağmen güçlü")

    if ema20_1h and close_price > ema20_1h: long_score += 1; reasons.append("1h EMA20 üstü")
    if ema50_4h and close_price > ema50_4h: long_score += 2; reasons.append("4h EMA50 üstü")
    if bullish_structure: long_score += 2; reasons.append("15m yükselen yapı")

    if rel_vol > 1.8: long_score += 2; reasons.append("Yüksek rel vol")
    elif rel_vol > 1.3: long_score += 1

    if wick_ratio > 0.5: long_score -= 1

    if btc_change <= -0.8: long_score -= 4; reasons.append("BTC düşüş baskısı")
    if btc_change > 1.5 and symbol != "BTCUSDT": long_score -= 2

    if ema50_4h and close_price > ema50_4h and ema20_1h and close_price > ema20_1h and bullish_structure:
        long_score += 3; reasons.append("Multi-TF uyumu")

    if rsi and 30 < rsi < 70 and close_price > open_price:
        long_score += 1; reasons.append(f"RSI {rsi:.0f}")
    if macd_line and signal_line and macd_line > signal_line and histogram > 0:
        long_score += 2; reasons.append("MACD bullish")
    if bb_lower and close_price <= bb_lower * 1.01 and change_pct > 0:
        long_score += 2; reasons.append("Bollinger alt dönüş")

    if long_score < min_score_atr:
        return None

    confidence = min(95, 45 + int(long_score * 3))
    sl_price = round(close_price - atr_val * 1.5, 4)
    tp_price = round(close_price + atr_val * 2.5, 4)

    # Mesaj için OI gösterimi
    oi_str = f"%{oi_change:.2f}" if oi_change is not None else "N/A"

    return {
        "symbol": symbol,
        "direction": "LONG",
        "score": long_score,
        "confidence": confidence,
        "price": round(close_price, 4),
        "change": round(change_pct, 2),
        "oi": oi_str,
        "funding": funding_rate,
        "delta": delta_ratio,
        "rel_vol": rel_vol,
        "trend": "Bullish",
        "squeeze": squeeze,
        "rs": round(rs, 1),
        "sl": sl_price,
        "tp": tp_price,
        "reasons": reasons
    }

# =========================================================
# MAIN
# =========================================================
async def main():
    global bot_running, pending_command
    print("🚀 BINANCE TR TÜM COINLER (FUTURES ÖNCELİKLİ)")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        asyncio.create_task(telegram_polling(session))
        await send_telegram(session, "✅ Binance TR tüm coinler taranıyor. /status /stop /start /next")

        spot_symbols = await get_spot_symbols_tr(session)
        if not spot_symbols:
            await send_telegram(session, "❌ Spot sembol listesi alınamadı!")
            return
        futures_set = await get_futures_symbols(session)

        # Tarama listesi: Binance TR spot listesi
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
                btc_change = 0.0
                btc_atr_percent = 0.0
                if btc_klines:
                    btc_open, btc_close = float(btc_klines[-2][1]), float(btc_klines[-2][4])
                    btc_change = ((btc_close - btc_open) / btc_open) * 100
                    btc_highs = [float(k[2]) for k in btc_klines[-5:]]
                    btc_lows = [float(k[3]) for k in btc_klines[-5:]]
                    btc_atr_percent = ((max(btc_highs) - min(btc_lows)) / min(btc_lows)) * 100

                min_score_atr = 7 if btc_atr_percent < 1.0 else (9 if btc_atr_percent > 2.5 else 8)

                # 1h cache (futures)
                futures_list = [s for s in COIN_LIST if s in futures_set]
                tasks_1h = [fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                      {"symbol": sym, "interval": "1h", "limit": 20}) for sym in futures_list]
                responses_1h = await asyncio.gather(*tasks_1h)
                klines_1h_cache = {}
                for sym, kl in zip(futures_list, responses_1h):
                    if kl and len(kl) >= 20:
                        klines_1h_cache[sym] = kl

                # 5m verileri çekilmeden market median hesaplaması için önceden hacim topla
                # (scan_coin içinde tekrar çekiliyor, ama median için ön çekim yapalım)
                # Daha verimli olması için önce tüm 5m verileri çekip hacimleri alalım
                tasks_5m = []
                for sym in COIN_LIST:
                    if sym in futures_set:
                        tasks_5m.append(fetch_api(session, FAPI_URL, "/fapi/v1/klines",
                                                  {"symbol": sym, "interval": "5m", "limit": 2}))
                    else:
                        tasks_5m.append(fetch_api(session, SPOT_TR_URL, "/api/v3/klines",
                                                  {"symbol": sym, "interval": "5m", "limit": 2}))
                pre_responses = await asyncio.gather(*tasks_5m)
                vols = []
                for r in pre_responses:
                    if r and len(r) >= 2:
                        try: vols.append(float(r[-2][5]))
                        except: pass
                filtered_vols = [v for v in vols if v > 100000]
                market_median = median(sorted(filtered_vols)[2:-2]) if len(filtered_vols) > 4 else (median(filtered_vols) if filtered_vols else 1)

                # Şimdi asıl tarama (scan_coin içinde tekrar 5m çekiyor)
                scan_tasks = []
                for sym in COIN_LIST:
                    is_fut = sym in futures_set
                    daily_change = daily_map.get(sym)
                    task = scan_coin(session, sym, is_fut, market_median,
                                    btc_change, min_score_atr, klines_1h_cache, daily_change)
                    scan_tasks.append(task)

                results = [r for r in await asyncio.gather(*scan_tasks) if r]
                for coin in results:
                    coin['final_rank'] = (coin['score'] * 0.4) + (coin['rel_vol'] * 0.2) + (abs(coin['delta']) * 0.2) + (
                        abs(float(coin['oi'].replace('%',''))) * 0.2 if coin['oi'] != 'N/A' else 0)
                results.sort(key=lambda x: x['final_rank'], reverse=True)

                now = time.time()
                force = (pending_command == "FORCE_NEXT")
                if force:
                    pending_command = None

                if now - last_global_signal >= GLOBAL_COOLDOWN or force:
                    for coin in results:
                        if coin['symbol'] in last_signals and now - last_signals[coin['symbol']] < COOLDOWN:
                            continue
                        reasons_str = ", ".join(coin.get('reasons', []))
                        msg = (
                            f"🟢 *{coin['symbol']} (LONG)*\n"
                            f"Puan: {coin['score']} | Güven: %{coin['confidence']}\n"
                            f"Giriş: {coin['price']} | %{coin['change']}\n"
                            f"🎯 TP: {coin['tp']:.4f} | 🛑 SL: {coin['sl']:.4f}\n"
                            f"OI: {coin['oi']} | RelVol: {coin['rel_vol']}x\n"
                            f"RS: {coin['rs']:.1f} | Funding: {coin['funding']*100:.4f}%\n"
                            f"Delta: {coin['delta']:.2f} | Sebep: {reasons_str}"
                        )
                        await send_telegram(session, msg)
                        print(f"✅ {coin['symbol']} LONG (Puan: {coin['score']})")
                        last_global_signal = now
                        last_signals[coin['symbol']] = now
                        break
                else:
                    if results:
                        print(f"⏳ Cooldown devrede. {results[0]['symbol']} atlandı.")

                print(f"🔍 Eşiği geçen {len(results)} LONG adayı (Min Skor: {min_score_atr})")

                elapsed = time.time() - start_time
                if elapsed < 35:
                    await asyncio.sleep(35 - elapsed)
                else:
                    await asyncio.sleep(1)

            except Exception as e:
                print(f"Kritik hata: {e}")
                import traceback; traceback.print_exc()
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
