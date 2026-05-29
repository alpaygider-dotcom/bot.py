import asyncio
import aiohttp
import os
import time
import random
from datetime import datetime
from statistics import mean, median

# =========================================================
# AYARLAR
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

FAPI_URL = "https://fapi.binance.com"
SPOT_URL = "https://api.binance.com"

CACHE_DURATION = 180
COOLDOWN = 600
GLOBAL_COOLDOWN = 120  # Saniye cinsinden

SEMAPHORE = asyncio.Semaphore(20)

# =========================================================
# CACHE & HAFIZA
# =========================================================
cache = {"funding": {}, "oi": {}, "klines_1h": {}}  # Toplu 1h cache eklendi
last_signals = {}

# =========================================================
# TELEGRAM (Detaylı mesaj)
# =========================================================
async def send_telegram(session, coin):
    try:
        emoji = "🟢" if coin['direction'] == "LONG" else "🔴"
        squeeze_tag = "🔥 SQUEEZE " if coin.get("squeeze") else ""

        msg = (
            f"{emoji} *{squeeze_tag}{coin['symbol']} ({coin['direction']})*\n"
            f"Puan: {coin['score']} | Güven: %{coin['confidence']}\n"
            f"F: {coin['price']} | %{coin['change']}\n"
            f"OI: %{coin['oi']} | RelVol: {coin['rel_vol']}x\n"
            f"RS: {coin.get('rs', 0):.1f} | Funding: {coin['funding']*100:.4f}%\n"
            f"Delta: {coin['delta']:.2f} | Trend: {'📈' if coin['trend'] == 'Bullish' else '📉'}"
        )

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        await session.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"Telegram hatası: {e}")

# =========================================================
# API İSTEKLERİ (429 RETRY-AFTER DESTEKLİ)
# =========================================================
async def fetch(session, url_type, endpoint, params=None):
    base = FAPI_URL if url_type == "fapi" else SPOT_URL
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))
        async with SEMAPHORE:
            async with session.get(f"{base}{endpoint}", params=params, headers=headers, timeout=20) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    print(f"429 Rate Limit. {retry_after}s bekleniyor...")
                    await asyncio.sleep(retry_after)
                    return None
                if resp.status != 200:
                    return None
                return await resp.json()
    except Exception as e:
        print(f"API Hatası ({endpoint}): {e}")
        return None

async def get_cached(session, cache_name, symbol, endpoint, params):
    now = time.time()
    if symbol in cache[cache_name] and now - cache[cache_name][symbol]["time"] < CACHE_DURATION:
        return cache[cache_name][symbol]["data"]
    data = await fetch(session, "fapi", endpoint, params)
    if data:
        cache[cache_name][symbol] = {"time": now, "data": data}
    return data

def calculate_ema(prices, period):
    if len(prices) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]: ema = (p - ema) * multiplier + ema
    return ema

# =========================================================
# PİYASA VERİLERİ
# =========================================================
async def get_orderbook_bias(session, symbol):
    depth = await fetch(session, "fapi", "/fapi/v1/depth", {"symbol": symbol, "limit": 20})
    if not depth: return 0
    try:
        bids = sum(float(x[0]) * float(x[1]) for x in depth["bids"][:10])
        asks = sum(float(x[0]) * float(x[1]) for x in depth["asks"][:10])
        return bids / asks if asks > 0 else 0
    except: return 0

async def get_top_trader_bias(session, symbol):
    data = await fetch(session, "fapi", "/futures/data/topLongShortPositionRatio", {"symbol": symbol, "period": "5m", "limit": 2})
    if not data: return 0
    try: return float(data[-1]["longShortRatio"])
    except: return 0

# =========================================================
# COIN LİSTESİ
# =========================================================
async def get_all_futures_usdt(session):
    exchange_info = await fetch(session, "fapi", "/fapi/v1/exchangeInfo")
    if not exchange_info: return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    symbols = [s["symbol"] for s in exchange_info["symbols"] if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"]
    print(f"✅ Toplam {len(symbols)} coin taramaya hazır.")
    return symbols

# =========================================================
# SCAN COIN (HATALAR DÜZELTİLDİ, TOPLU CACHE EKLENDİ)
# =========================================================
async def scan_coin(session, symbol, pre_fetched_5m, market_median, btc_change, min_score_atr, btc_atr_percent, klines_1h_cache):
    # 1. TEMEL VERİ VE FİLTRELER
    kl_5m = pre_fetched_5m
    if not kl_5m or len(kl_5m) < 6: return None

    # Son kapanmış mum (CANLI MUMA BAKMIYORUZ)
    last_closed = kl_5m[-2]
    open_price, close_price, high, low, volume, quote_volume, taker_buy = (
        float(last_closed[1]), float(last_closed[4]), float(last_closed[2]), float(last_closed[3]),
        float(last_closed[5]), float(last_closed[7]), float(last_closed[9])
    )
    change_pct = ((close_price - open_price) / open_price) * 100
    taker_ratio = taker_buy / volume if volume > 0 else 0

    # Likidite filtresi
    if quote_volume < 3_000_000:
        return None

    # Aşırı uç hareketleri ele
    if abs(change_pct) > 8.0:
        return None

    # Hacim ve relatif hacim
    prev_vols = [float(k[5]) for k in kl_5m[-7:-2]]
    avg_vol = mean(prev_vols) if prev_vols else volume
    speed_ratio = volume / avg_vol if avg_vol > 0 else 0
    rel_vol = volume / market_median if market_median > 0 else 0
    heavy_check = speed_ratio > 1.2 or rel_vol > 1.2

    # Delta ve gerçek fitil oranı (DÜZELTİLDİ)
    delta = taker_buy - (volume - taker_buy)
    delta_ratio = delta / volume if volume > 0 else 0
    body_ratio = abs(close_price - open_price) / (high - low) if (high - low) > 0 else 0
    wick_ratio = 1 - body_ratio  # Gövde dışındaki her şey fitildir

    # 2. AĞIR VERİLER (TOPLU CACHE VE SADECE heavy_check VARSA)
    oi_change = 0
    funding_rate = 0
    ema20_1h = ema50_4h = None
    bullish_structure = bearish_structure = False
    ob_ratio = top_ratio = 0

    # Toplu 1h cache'den al
    if symbol in klines_1h_cache:
        kl_1h = klines_1h_cache[symbol]
        closes_1h = [float(k[4]) for k in kl_1h]
        ema20_1h = calculate_ema(closes_1h, 20)

    if heavy_check:
        # OI (Cache kullanılıyor)
        oi_data = await get_cached(session, "oi", symbol, "/fapi/v1/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 2})
        if oi_data and len(oi_data) >= 2:
            prev_oi = float(oi_data[-2]["sumOpenInterestValue"])
            curr_oi = float(oi_data[-1]["sumOpenInterestValue"])
            if prev_oi > 0: oi_change = ((curr_oi - prev_oi) / prev_oi) * 100

        # Funding
        funding = await get_cached(session, "funding", symbol, "/fapi/v1/premiumIndex", {"symbol": symbol})
        if funding: funding_rate = float(funding.get("lastFundingRate", 0))

        # 4h ve 15m (sadece heavy varsa)
        kl_4h = await fetch(session, "fapi", "/fapi/v1/klines", {"symbol": symbol, "interval": "4h", "limit": 60})
        kl_15m = await fetch(session, "fapi", "/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": 6})

        if kl_4h:
            closes_4h = [float(k[4]) for k in kl_4h]
            ema50_4h = calculate_ema(closes_4h, 50)
        if kl_15m and len(kl_15m) >= 4:
            h_list = [float(k[2]) for k in kl_15m[-4:]]
            l_list = [float(k[3]) for k in kl_15m[-4:]]
            if h_list[-1] > h_list[-2] and l_list[-1] > l_list[-2]: bullish_structure = True
            if h_list[-1] < h_list[-2] and l_list[-1] < l_list[-2]: bearish_structure = True

        ob_ratio = await get_orderbook_bias(session, symbol)
        top_ratio = await get_top_trader_bias(session, symbol)

    # =========================================================
    # SKORLAMA (TÜM İYİLEŞTİRMELER EKLENDİ)
    # =========================================================
    long_score, short_score = 0, 0
    squeeze = False

    # --- 1. HACİM HIZI (Yönlü) ---
    if speed_ratio > 1.8:
        if change_pct > 0: long_score += 2
        else: short_score += 2

    # --- 2. NORMALİZE FİYAT DEĞİŞİMİ ---
    recent_range = high - low
    if recent_range > 0:
        normalized_change = change_pct / (recent_range / close_price * 100) if (recent_range / close_price * 100) > 0 else 0
        if 0.1 < abs(normalized_change) < 5:
            if normalized_change > 0: long_score += 2
            else: short_score += 2

    # --- 3. TAKER RATIO & DELTA ---
    if taker_ratio > 0.55: long_score += 2
    if taker_ratio < 0.45: short_score += 2
    if delta_ratio > 0.15: long_score += 2
    if delta_ratio < -0.15: short_score += 2

    # --- 4. OI (SADECE YÖNLÜ VE ANLAMLI) ---
    if oi_change > 1 and delta_ratio > 0.15 and close_price > open_price:
        long_score += 2
    if oi_change > 1 and delta_ratio < -0.15 and close_price < open_price:
        short_score += 2

    # --- 5. FUNDING SQUEEZE ---
    if funding_rate < -0.005 and change_pct > 0:
        long_score += 2
        squeeze = True
    if funding_rate > 0.005 and change_pct < 0:
        short_score += 2

    # --- 6. GERÇEK RELATIVE STRENGTH (BTC'ye göre) ---
    if btc_change > 0:
        rs = change_pct - btc_change
    else:
        rs = change_pct + abs(btc_change)
    if rs > 1.0: long_score += 2
    if rs < -1.0: short_score += 2

    # --- 7. ERKEN SIKIŞMA KIRILIMI (COMPRESSION BREAKOUT) ---
    if heavy_check and len(kl_5m) > 6:
        recent_high = max(float(k[2]) for k in kl_5m[-6:-1])
        recent_low = min(float(k[3]) for k in kl_5m[-6:-1])
        compression_range = recent_high - recent_low
        compression = (compression_range / close_price) * 100 if close_price > 0 else 0
        breakout_strength = (speed_ratio > 1.5 and delta_ratio > 0.12 and taker_ratio > 0.54)

        if compression < 1.2 and breakout_strength:
            if delta_ratio > 0.12:
                long_score += 3
            elif delta_ratio < -0.12:
                short_score += 3

    # --- 8. BTC RELATIVE FLOW (Lider coin avı) ---
    if btc_change <= 0 and oi_change > 2 and delta_ratio > 0.15:
        long_score += 3

    # --- 9. TREND VE YAPI ---
    if ema20_1h and close_price > ema20_1h: long_score += 1
    if ema20_1h and close_price < ema20_1h: short_score += 1
    if ema50_4h and close_price > ema50_4h: long_score += 2
    if ema50_4h and close_price < ema50_4h: short_score += 2
    if bullish_structure: long_score += 2
    if bearish_structure: short_score += 2

    # --- 10. RELATIVE VOLUME ---
    if rel_vol > 1.8: long_score += 2; short_score += 2
    elif rel_vol > 1.3: long_score += 1; short_score += 1

    # --- 11. ORDERBOOK & TOP TRADER ---
    if delta_ratio > 0.15 and ob_ratio > 1.3: long_score += 1
    if delta_ratio < -0.15 and ob_ratio < 0.7: short_score += 1
    if top_ratio > 1.1: long_score += 1
    if top_ratio < 0.9: short_score += 1

    # --- 12. FİTİL CEZASI (DÜZELTİLDİ) ---
    if wick_ratio > 0.5: long_score -= 1; short_score -= 1

    # --- 13. MARKET TREND FİLTRESİ ---
    market_long_allowed = btc_change > -0.8
    market_short_allowed = btc_change < 1.5
    if not market_long_allowed:
        long_score -= 4
    if not market_short_allowed:
        short_score -= 4

    # --- 14. BTC DOMINANCE ETKİSİ ---
    if btc_change > 1.5 and symbol != "BTCUSDT":
        long_score -= 2

    # --- 15. MULTI-TIMEFRAME ALIGNMENT ---
    if ema50_4h and close_price > ema50_4h and ema20_1h and close_price > ema20_1h and bullish_structure:
        long_score += 3
    if ema50_4h and close_price < ema50_4h and ema20_1h and close_price < ema20_1h and bearish_structure:
        short_score += 3

    # =========================================================
    # SONUÇ (COOLDOWN TUZAĞI KALDIRILDI)
    # =========================================================
    result = None
    if long_score >= min_score_atr:
        confidence = min(95, 45 + int(long_score * 3))
        result = {
            "symbol": symbol, "direction": "LONG", "score": long_score, "confidence": confidence,
            "price": round(close_price, 4), "change": round(change_pct, 2), "oi": round(oi_change, 2),
            "funding": funding_rate, "delta": delta_ratio, "rel_vol": round(rel_vol, 2),
            "trend": "Bullish", "squeeze": squeeze, "rs": round(rs, 1)
        }
    elif short_score >= min_score_atr:
        confidence = min(95, 45 + int(short_score * 3))
        result = {
            "symbol": symbol, "direction": "SHORT", "score": short_score, "confidence": confidence,
            "price": round(close_price, 4), "change": round(change_pct, 2), "oi": round(oi_change, 2),
            "funding": funding_rate, "delta": delta_ratio, "rel_vol": round(rel_vol, 2),
            "trend": "Bearish", "squeeze": False, "rs": round(rs, 1)
        }

    # Artık burada last_signals işaretlemesi YAPMIYORUZ.
    return result

# =========================================================
# MAIN (COOLDOWN TUZAĞI DÜZELTİLDİ, SIRALAMA GELİŞTİRİLDİ)
# =========================================================
async def main():
    print("🚀 ULTRA PROFESYONEL SCANNER (HATALAR GİDERİLDİ)")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        COIN_LIST = await get_all_futures_usdt(session)
        if not COIN_LIST: return

        last_global_signal = 0

        while True:
            try:
                start_time = time.time()
                print(f"\n--- {datetime.now().strftime('%H:%M:%S')} ---")

                # 1. BTC VERİSİ VE PİYASA REJİMİ
                btc_klines = await fetch(session, "fapi", "/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "15m", "limit": 10})
                btc_change = 0.0
                btc_atr_percent = 0.0
                if btc_klines:
                    btc_open, btc_close = float(btc_klines[-2][1]), float(btc_klines[-2][4])
                    btc_change = ((btc_close - btc_open) / btc_open) * 100
                    btc_highs = [float(k[2]) for k in btc_klines[-5:]]
                    btc_lows = [float(k[3]) for k in btc_klines[-5:]]
                    btc_atr_percent = ((max(btc_highs) - min(btc_lows)) / min(btc_lows)) * 100

                # Adaptif Eşik Ayarı
                if btc_atr_percent < 1.0:
                    min_score_atr = 7
                elif btc_atr_percent > 2.5:
                    min_score_atr = 9
                else:
                    min_score_atr = 8

                # 2. TOPLU VERİ ÇEKİMLERİ (5m ve 1h)
                tasks_5m = [fetch(session, "fapi", "/fapi/v1/klines", {"symbol": sym, "interval": "5m", "limit": 8}) for sym in COIN_LIST]
                tasks_1h = [fetch(session, "fapi", "/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": 20}) for sym in COIN_LIST]
                responses_5m, responses_1h = await asyncio.gather(asyncio.gather(*tasks_5m), asyncio.gather(*tasks_1h))

                # Toplu 1h cache'i oluştur
                klines_1h_cache = {}
                for sym, kl_1h in zip(COIN_LIST, responses_1h):
                    if kl_1h and len(kl_1h) >= 20:
                        klines_1h_cache[sym] = kl_1h

                # Güvenli market median hesaplama
                vols = [float(r[-2][5]) for r in responses_5m if r and len(r) >= 2]
                filtered_vols = [v for v in vols if v > 100000]
                market_median = median(sorted(filtered_vols)[2:-2]) if len(filtered_vols) > 4 else (median(filtered_vols) if filtered_vols else 1)

                # 3. TARAMA İŞLEMLERİNİ BAŞLAT
                scan_tasks = []
                for sym, kl_5m in zip(COIN_LIST, responses_5m):
                    if not kl_5m or len(kl_5m) < 6: continue
                    task = scan_coin(session, sym, kl_5m, market_median, btc_change, min_score_atr, btc_atr_percent, klines_1h_cache)
                    scan_tasks.append(task)

                results = [r for r in await asyncio.gather(*scan_tasks) if r]

                # Gelişmiş sinyal sıralaması (final_rank)
                for coin in results:
                    coin['final_rank'] = (coin['score'] * 0.4) + (coin['rel_vol'] * 0.2) + (abs(coin['delta']) * 0.2) + (abs(coin['oi']) * 0.2)
                results.sort(key=lambda x: x['final_rank'], reverse=True)

                # 4. TELEGRAM'A GÖNDERİM (COOLDOWN TUZAĞI DÜZELTİLDİ)
                now = time.time()
                if now - last_global_signal >= GLOBAL_COOLDOWN:
                    if results:
                        top_coin = results[0]
                        await send_telegram(session, top_coin)
                        print(f"✅ Sinyal: {top_coin['symbol']} {top_coin['direction']} (Puan: {top_coin['score']}, Güven: %{top_coin['confidence']})")
                        last_global_signal = now
                        last_signals[top_coin['symbol']] = now  # SADECE GÖNDERİLEN COINE CEZA YAZ
                else:
                    if results:
                        print(f"⏳ Global Cooldown devrede. {results[0]['symbol']} sinyali atlandı.")

                print(f"🔍 Eşiği geçen {len(results)} coin (Adaptif Min Skor: {min_score_atr})")

                # 5. DÖNGÜ KORUMASI
                elapsed = time.time() - start_time
                sleep_time = max(30, 12)
                if elapsed < sleep_time:
                    await asyncio.sleep(sleep_time - elapsed)
                else:
                    await asyncio.sleep(1)

            except Exception as e:
                print(f"Kritik hata: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
