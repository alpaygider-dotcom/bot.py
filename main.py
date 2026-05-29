import asyncio
import aiohttp
import os
import time
import random
from statistics import mean, median

# =========================================================
# AYARLAR
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ BOT_TOKEN veya CHAT_ID ortam değişkeni eksik!")
    exit(1)

FAPI_URL = "https://fapi.binance.com"
SPOT_TR_URL = "https://api.trbinance.com"          # Binance TR
SPOT_GLOBAL_URL = "https://api.binance.com"        # yedek global spot

CACHE_DURATION = 180
COOLDOWN = 600                # 10 dakika
GLOBAL_COOLDOWN = 120         # 2 dakika

SEMAPHORE = asyncio.Semaphore(20)

# =========================================================
# CACHE & HAFIZA
# =========================================================
cache = {"funding": {}, "oi": {}, "klines_1h": {}}
last_signals = {}

# =========================================================
# TELEGRAM
# =========================================================
async def send_telegram(session, coin):
    try:
        tp = f"{coin['tp']:.4f}" if coin.get('tp') is not None else "N/A"
        sl = f"{coin['sl']:.4f}" if coin.get('sl') is not None else "N/A"
        squeeze_tag = "🔥 SQUEEZE " if coin.get("squeeze") else ""

        msg = (
            f"🟢 *{squeeze_tag}{coin['symbol']} (LONG)*\n"
            f"Puan: {coin['score']} | Güven: %{coin['confidence']}\n"
            f"Giriş: {coin['price']} | %{coin['change']}\n"
            f"🎯 TP: {tp} | 🛑 SL: {sl}\n"
            f"OI: %{coin['oi']} | RelVol: {coin['rel_vol']}x\n"
            f"RS: {coin.get('rs', 0):.1f} | Funding: {coin['funding']*100:.4f}%\n"
            f"Delta: {coin['delta']:.2f} | Trend: 📈"
        )

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        await session.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"Telegram hatası: {e}")

# =========================================================
# API İSTEKLERİ
# =========================================================
async def fetch(session, base_url, endpoint, params=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))
        async with SEMAPHORE:
            async with session.get(f"{base_url}{endpoint}", params=params, headers=headers, timeout=20) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    await asyncio.sleep(retry_after)
                    return None
                if resp.status != 200:
                    return None
                return await resp.json()
    except:
        return None

async def get_cached(session, cache_name, key, base_url, endpoint, params):
    now = time.time()
    if key in cache[cache_name] and now - cache[cache_name][key]["time"] < CACHE_DURATION:
        return cache[cache_name][key]["data"]
    data = await fetch(session, base_url, endpoint, params)
    if data:
        cache[cache_name][key] = {"time": now, "data": data}
    return data

def calculate_ema(prices, period):
    if len(prices) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]: ema = (p - ema) * multiplier + ema
    return ema

def calculate_atr(highs, lows, closes, period=14):
    if len(highs) < period + 1: return None
    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(highs))]
    return mean(tr[-period:]) if tr else None

# =========================================================
# BINANCE TR SPOT SEMBOLLERİ
# =========================================================
async def get_spot_symbols_tr(session):
    """Binance TR spot USDT çiftlerini döndürür (başarısız olursa global spot)."""
    info = await fetch(session, SPOT_TR_URL, "/api/v3/exchangeInfo")
    if not info:
        info = await fetch(session, SPOT_GLOBAL_URL, "/api/v3/exchangeInfo")
    if not info:
        return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}

async def get_futures_symbols(session):
    """Futures USDT perpetual sözleşmelerini döndürür."""
    info = await fetch(session, FAPI_URL, "/fapi/v1/exchangeInfo")
    if not info:
        return set()
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}

# =========================================================
# GÜNLÜK DEĞİŞİM HARİTASI
# =========================================================
async def get_daily_change_map(session, symbols):
    """Verilen semboller için günlük değişim haritası (önce TR spot, sonra global spot, en son futures)."""
    change_map = {}
    data = await fetch(session, SPOT_TR_URL, "/api/v3/ticker/24hr")
    if not data:
        data = await fetch(session, SPOT_GLOBAL_URL, "/api/v3/ticker/24hr")
    if not data:
        data = await fetch(session, FAPI_URL, "/fapi/v1/ticker/24hr")
    if data:
        for item in data:
            sym = item.get("symbol", "")
            if sym in symbols:
                try:
                    change_map[sym] = float(item["priceChangePercent"])
                except:
                    change_map[sym] = 0.0
    return change_map

# =========================================================
# SCAN COIN (COOLDOWN SIKI KONTROL, GÜNLÜK %10 FİLTRESİ)
# =========================================================
async def scan_coin(session, symbol, is_futures_available, pre_fetched_5m, market_median,
                    btc_change, min_score_atr, klines_1h_cache, daily_change):
    # ---------- COOLDOWN KONTROLÜ (EN BAŞTA) ----------
    if symbol in last_signals and time.time() - last_signals[symbol] < COOLDOWN:
        return None

    # Günlük %10 artmış coini ele
    if daily_change is not None and daily_change > 10.0:
        return None

    kl_5m = pre_fetched_5m
    if not kl_5m or len(kl_5m) < 20:
        return None

    closed = kl_5m[:-1]
    last_closed = closed[-1]
    open_price, close_price, high, low, volume, quote_volume, taker_buy = (
        float(last_closed[1]), float(last_closed[4]), float(last_closed[2]), float(last_closed[3]),
        float(last_closed[5]), float(last_closed[7]), float(last_closed[9])
    )
    change_pct = ((close_price - open_price) / open_price) * 100

    # 1 saatlik %8 filtresi
    if len(closed) >= 13:
        price_1h_ago = float(closed[-13][4])
        hour_change = (close_price - price_1h_ago) / price_1h_ago * 100
        if hour_change > 8.0:
            return None

    taker_ratio = taker_buy / volume if volume > 0 else 0

    if quote_volume < 3_000_000: return None
    if abs(change_pct) > 8.0: return None

    prev_vols = [float(k[5]) for k in kl_5m[-7:-2]]
    avg_vol = mean(prev_vols) if prev_vols else volume
    speed_ratio = volume / avg_vol if avg_vol > 0 else 0
    rel_vol = volume / market_median if market_median > 0 else 0
    heavy_check = speed_ratio > 1.2 or rel_vol > 1.2

    delta = taker_buy - (volume - taker_buy)
    delta_ratio = delta / volume if volume > 0 else 0
    body_ratio = abs(close_price - open_price) / (high - low) if (high - low) > 0 else 0
    wick_ratio = 1 - body_ratio

    # ATR
    highs = [float(k[2]) for k in kl_5m[-16:-1]]
    lows = [float(k[3]) for k in kl_5m[-16:-1]]
    closes = [float(k[4]) for k in kl_5m[-16:-1]]
    atr_val = calculate_atr(highs, lows, closes)
    if atr_val is None:
        return None

    # OI / Funding (sadece futures'ta varsa)
    oi_change = 0
    funding_rate = 0
    if is_futures_available:
        oi_data = await get_cached(session, "oi", symbol, FAPI_URL,
                                   "/fapi/v1/openInterestHist",
                                   {"symbol": symbol, "period": "5m", "limit": 2})
        if oi_data and len(oi_data) >= 2:
            prev_oi = float(oi_data[-2]["sumOpenInterestValue"])
            curr_oi = float(oi_data[-1]["sumOpenInterestValue"])
            if prev_oi > 0: oi_change = ((curr_oi - prev_oi) / prev_oi) * 100

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
        kl_4h = await fetch(session, FAPI_URL, "/fapi/v1/klines",
                            {"symbol": symbol, "interval": "4h", "limit": 60})
        kl_15m = await fetch(session, FAPI_URL, "/fapi/v1/klines",
                             {"symbol": symbol, "interval": "15m", "limit": 6})
        if kl_4h:
            closes_4h = [float(k[4]) for k in kl_4h]
            ema50_4h = calculate_ema(closes_4h, 50)
        if kl_15m and len(kl_15m) >= 4:
            h_list = [float(k[2]) for k in kl_15m[-4:]]
            l_list = [float(k[3]) for k in kl_15m[-4:]]
            if h_list[-1] > h_list[-2] and l_list[-1] > l_list[-2]:
                bullish_structure = True

    # ==== LONG SKORLAMA ====
    long_score = 0
    squeeze = False

    if speed_ratio > 1.8 and change_pct > 0:
        long_score += 2

    recent_range = high - low
    if recent_range > 0:
        normalized_change = change_pct / (recent_range / close_price * 100) if (recent_range / close_price * 100) > 0 else 0
        if 0.1 < normalized_change < 5 and normalized_change > 0:
            long_score += 2

    if taker_ratio > 0.55: long_score += 2
    if delta_ratio > 0.15: long_score += 2

    if oi_change > 1 and delta_ratio > 0.15 and close_price > open_price:
        long_score += 2

    if funding_rate < -0.005 and change_pct > 0:
        long_score += 2
        squeeze = True

    if btc_change > 0:
        rs = change_pct - btc_change
    else:
        rs = change_pct + abs(btc_change)
    if rs > 1.0: long_score += 2

    if heavy_check and len(kl_5m) > 6:
        recent_high = max(float(k[2]) for k in kl_5m[-7:-2])
        recent_low = min(float(k[3]) for k in kl_5m[-7:-2])
        compression_range = recent_high - recent_low
        compression = (compression_range / close_price) * 100 if close_price > 0 else 0
        breakout_strength = (speed_ratio > 1.5 and delta_ratio > 0.12 and taker_ratio > 0.54)
        if compression < 1.2 and breakout_strength and delta_ratio > 0.12:
            long_score += 3

    if btc_change <= 0 and oi_change > 2 and delta_ratio > 0.15:
        long_score += 3

    if ema20_1h and close_price > ema20_1h: long_score += 1
    if ema50_4h and close_price > ema50_4h: long_score += 2
    if bullish_structure: long_score += 2

    if rel_vol > 1.8: long_score += 2
    elif rel_vol > 1.3: long_score += 1

    if wick_ratio > 0.5: long_score -= 1

    if btc_change <= -0.8:
        long_score -= 4

    if btc_change > 1.5 and symbol != "BTCUSDT":
        long_score -= 2

    if ema50_4h and close_price > ema50_4h and ema20_1h and close_price > ema20_1h and bullish_structure:
        long_score += 3

    if long_score < min_score_atr:
        return None

    confidence = min(95, 45 + int(long_score * 3))
    sl_price = round(close_price - atr_val * 1.5, 4)
    tp_price = round(close_price + atr_val * 2.5, 4)

    return {
        "symbol": symbol,
        "direction": "LONG",
        "score": long_score,
        "confidence": confidence,
        "price": round(close_price, 4),
        "change": round(change_pct, 2),
        "oi": round(oi_change, 2),
        "funding": funding_rate,
        "delta": delta_ratio,
        "rel_vol": round(rel_vol, 2),
        "trend": "Bullish",
        "squeeze": squeeze,
        "rs": round(rs, 1),
        "sl": sl_price,
        "tp": tp_price
    }

# =========================================================
# MAIN
# =========================================================
async def main():
    print("🚀 SADECE BINANCE TR SPOT (COOLDOWN DÜZGÜN)")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        spot_symbols = await get_spot_symbols_tr(session)
        if not spot_symbols:
            print("❌ Spot sembol listesi alınamadı.")
            return

        futures_set = await get_futures_symbols(session)
        print(f"✅ {len(spot_symbols)} spot coin taranacak. ({len(futures_set)} futures verisi var)")

        COIN_LIST = sorted(spot_symbols)
        last_global_signal = 0

        while True:
            try:
                start_time = time.time()

                # Günlük değişim haritası
                daily_map = await get_daily_change_map(session, spot_symbols)

                # BTC
                btc_klines = await fetch(session, FAPI_URL, "/fapi/v1/klines",
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

                # 5m verileri (spot)
                tasks_5m = [fetch(session, SPOT_TR_URL, "/api/v3/klines",
                                  {"symbol": sym, "interval": "5m", "limit": 20}) for sym in COIN_LIST]
                responses_5m = await asyncio.gather(*tasks_5m)

                # Başarısız olanları global spot ile dene
                for i, (sym, resp) in enumerate(zip(COIN_LIST, responses_5m)):
                    if resp is None or len(resp) < 20:
                        responses_5m[i] = await fetch(session, SPOT_GLOBAL_URL, "/api/v3/klines",
                                                      {"symbol": sym, "interval": "5m", "limit": 20})

                # 1h cache (futures'tan)
                futures_list = [s for s in COIN_LIST if s in futures_set]
                tasks_1h = [fetch(session, FAPI_URL, "/fapi/v1/klines",
                                  {"symbol": sym, "interval": "1h", "limit": 20}) for sym in futures_list]
                responses_1h = await asyncio.gather(*tasks_1h)
                klines_1h_cache = {}
                for sym, kl in zip(futures_list, responses_1h):
                    if kl and len(kl) >= 20:
                        klines_1h_cache[sym] = kl

                # Market median
                vols = []
                for r in responses_5m:
                    if r and len(r) >= 2:
                        try:
                            vols.append(float(r[-2][5]))
                        except:
                            pass
                filtered_vols = [v for v in vols if v > 100000]
                market_median = median(sorted(filtered_vols)[2:-2]) if len(filtered_vols) > 4 else (
                    median(filtered_vols) if filtered_vols else 1)

                # Tarama
                scan_tasks = []
                for sym, kl_5m in zip(COIN_LIST, responses_5m):
                    if not kl_5m or len(kl_5m) < 20: continue
                    is_fut = sym in futures_set
                    daily_change = daily_map.get(sym)
                    task = scan_coin(session, sym, is_fut, kl_5m, market_median,
                                    btc_change, min_score_atr, klines_1h_cache, daily_change)
                    scan_tasks.append(task)

                results = [r for r in await asyncio.gather(*scan_tasks) if r]

                # Sıralama
                for coin in results:
                    coin['final_rank'] = (coin['score'] * 0.4) + (coin['rel_vol'] * 0.2) + (abs(coin['delta']) * 0.2) + (abs(coin['oi']) * 0.2)
                results.sort(key=lambda x: x['final_rank'], reverse=True)

                # Telegram gönderimi (cooldown'a uy)
                now = time.time()
                if now - last_global_signal >= GLOBAL_COOLDOWN:
                    for coin in results:
                        # Tekrar cooldown kontrolü (garanti olsun)
                        if coin['symbol'] in last_signals and now - last_signals[coin['symbol']] < COOLDOWN:
                            continue
                        await send_telegram(session, coin)
                        print(f"✅ Sinyal: {coin['symbol']} LONG (Puan: {coin['score']})")
                        last_global_signal = now
                        last_signals[coin['symbol']] = now
                        break  # Sadece bir sinyal gönder
                else:
                    if results:
                        print(f"⏳ Global cooldown devrede. {results[0]['symbol']} atlandı.")

                print(f"🔍 Eşiği geçen {len(results)} LONG adayı (Min Skor: {min_score_atr})")

                elapsed = time.time() - start_time
                if elapsed < 35:
                    await asyncio.sleep(35 - elapsed)
                else:
                    await asyncio.sleep(1)

            except Exception as e:
                print(f"Kritik hata: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
