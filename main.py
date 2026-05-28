import asyncio
import aiohttp
import os
import time
import logging
import traceback
from statistics import mean, stdev

# ==================================================
# CONFIG
# ==================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

BASE_URL = "https://fapi.binance.com"

SCAN_INTERVAL = 40
COOLDOWN = 600                 # symbol bazlı (10 dk)
GLOBAL_COOLDOWN = 120          # market‑wide (2 dk)
MAX_CONCURRENT_REQUESTS = 20

last_signal = {}
last_global_signal = 0

# ==================================================
# PORTFOLIO & RISK
# ==================================================
balance = 1000.0
equity = 1000.0
daily_pnl = 0.0
consecutive_losses = 0
trading_paused = False

weights = {
    "trend": 1.0,
    "volume": 1.0,
    "breakout": 1.0,
    "whale": 1.0,
    "regime": 1.0
}

# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(
    filename='bot_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==================================================
# TELEGRAM QUEUE
# ==================================================
telegram_queue = asyncio.Queue()

async def telegram_worker(session):
    while True:
        text = await telegram_queue.get()
        try:
            if BOT_TOKEN and CHAT_ID:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                async with session.post(url, json={"chat_id": CHAT_ID, "text": text}) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(1)
                        await telegram_queue.put(text)
        except:
            pass
        finally:
            telegram_queue.task_done()
        await asyncio.sleep(0.35)

async def send_telegram(text):
    await telegram_queue.put(text)

# ==================================================
# FETCH (429 rate‑limit korumalı)
# ==================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

async def fetch_json(session, endpoint, params=None, max_retries=3):
    url = BASE_URL + endpoint
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429:
                    retry_after = resp.headers.get("Retry-After", "5")
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = 5
                    logging.warning(f"429 Rate‑limit, {wait}s bekleniyor.")
                    await asyncio.sleep(wait)
                    continue
                elif resp.status in (403, 451):
                    logging.error(f"Blocked {resp.status} at {url}")
                    return None
                else:
                    logging.error(f"HTTP {resp.status} for {url}")
        except asyncio.TimeoutError:
            logging.error(f"Timeout {endpoint}")
        except Exception:
            logging.exception(f"Fetch error {endpoint}")

        if attempt < max_retries - 1:
            await asyncio.sleep(2 ** attempt)

    return None

# ==================================================
# INDICATORS
# ==================================================
def ema(values, period):
    if len(values) < period: return None
    m = 2 / (period + 1)
    e = mean(values[:period])
    for x in values[period:]:
        e = (x - e) * m + e
    return e

def atr(highs, lows, closes, period=14):
    if len(highs) < period + 1: return None
    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(highs))]
    return mean(tr[-period:]) if len(tr) >= period else None

def detect_sweep(highs, lows, closes):
    rh = max(highs[-21:-1]) if len(highs) >= 21 else max(highs[:-1])
    rl = min(lows[-21:-1]) if len(lows) >= 21 else min(lows[:-1])
    up = highs[-1] > rh and closes[-1] < highs[-1]
    down = lows[-1] < rl and closes[-1] > lows[-1]
    return up, down

def sideways_breakout(closes):
    recent = closes[-16:-1]  # son 15 tamamlanmış mum
    if len(recent) < 15: return False, False, False
    highest = max(recent)
    lowest = min(recent)
    range_pct = (highest - lowest) / lowest * 100
    compressed = range_pct < 2.5
    up = closes[-1] > highest and closes[-2] <= highest
    down = closes[-1] < lowest and closes[-2] >= lowest
    return compressed, up, down

def orderflow_strength(volume, taker_buy):
    if volume <= 0: return 0
    r = taker_buy / volume
    d = (taker_buy - (volume - taker_buy)) / volume
    score = 0
    if r > 0.62: score += 1
    if r < 0.38: score -= 1
    if d > 0.18: score += 1
    if d < -0.18: score -= 1
    return score

# ==================================================
# EXTERNAL DATA
# ==================================================
async def get_heavy_data(session, symbol):
    funding = 0.0; oi_change = 0.0
    f = await fetch_json(session, "/fapi/v1/premiumIndex", {"symbol": symbol})
    if f: funding = float(f.get("lastFundingRate", 0))
    oi = await fetch_json(session, "/futures/data/openInterestHist",
                          {"symbol": symbol, "period": "5m", "limit": 2})
    if oi and len(oi) >= 2:
        prev = float(oi[-2]["sumOpenInterest"])
        curr = float(oi[-1]["sumOpenInterest"])
        if prev > 0: oi_change = (curr - prev) / prev * 100
    return funding, oi_change

# ==================================================
# BTC BIAS + RELATIVE STRENGTH
# ==================================================
async def get_btc_data(session):
    kl = await fetch_json(session, "/fapi/v1/klines", {"symbol":"BTCUSDT","interval":"15m","limit":50})
    if not kl: return "NEUTRAL", 0.0
    c = [float(k[4]) for k in kl]
    e20, e50 = ema(c, 20), ema(c, 50)
    if not e20 or not e50: return "NEUTRAL", 0.0
    if c[-1] > e20 > e50: bias = "BULLISH"
    elif c[-1] < e20 < e50: bias = "BEARISH"
    else: bias = "NEUTRAL"

    btc_last = kl[-2]
    btc_open, btc_close = float(btc_last[1]), float(btc_last[4])
    btc_change = (btc_close - btc_open) / btc_open * 100
    return bias, btc_change

# ==================================================
# SYMBOLS
# ==================================================
async def get_all_symbols(session):
    info = await fetch_json(session, "/fapi/v1/exchangeInfo")
    if not info: return []
    return [s["symbol"] for s in info["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"]

# ==================================================
# CLASSIFY
# ==================================================
def classify_signal(score):
    if score >= 14: return "🛡️ ZIRHLI"
    if score >= 10: return "🔥 GÜÇLÜ"
    return None

# ==================================================
# RISK ENGINE
# ==================================================
def risk_engine(pnl):
    global equity, daily_pnl, consecutive_losses, trading_paused
    equity += pnl
    daily_pnl += pnl
    if pnl < 0: consecutive_losses += 1
    else: consecutive_losses = 0
    if daily_pnl < -80 or equity < balance * 0.85 or consecutive_losses >= 5:
        trading_paused = True

def evolve(pnl):
    global weights
    if pnl > 0:
        for k in weights: weights[k] *= 1.01
    else:
        weights["trend"] *= 0.99
        weights["whale"] *= 0.99
    for k in weights: weights[k] = max(0.4, min(weights[k], 2.5))

# ==================================================
# SCAN
# ==================================================
async def scan_coin(session, symbol, btc_bias, btc_change, sem):
    global trading_paused, last_global_signal
    async with sem:
        if trading_paused: return
        try:
            kl = await fetch_json(session, "/fapi/v1/klines",
                                  {"symbol": symbol, "interval": "5m", "limit": 80})
            if not kl: return

            closed_kl = kl[:-1]

            c = [float(k[4]) for k in closed_kl]
            h = [float(k[2]) for k in closed_kl]
            l = [float(k[3]) for k in closed_kl]
            v = [float(k[5]) for k in closed_kl]
            qv = [float(k[7]) for k in closed_kl]

            last = closed_kl[-1]
            open_p, close_p = float(last[1]), float(last[4])
            high_p, low_p = float(last[2]), float(last[3])
            vol, tbuy = float(last[5]), float(last[9])

            # ========== DÜŞÜK LİKİDİTE ==========
            quote_volume_12 = sum(qv[-12:])
            if quote_volume_12 < 15_000_000:
                return

            # ========== WICK MANIPULATION (0.45) ==========
            body = abs(close_p - open_p)
            candle_range = high_p - low_p
            body_ratio = body / candle_range if candle_range > 0 else 0
            if body_ratio < 0.45:
                return

            change = (close_p - open_p) / open_p * 100

            vol_z = (vol - mean(v)) / stdev(v) if len(v) > 1 and stdev(v) > 0 else 0

            # ========== TREND FİLTRESİ ==========
            ema20 = ema(c, 20)
            ema50 = ema(c, 50)
            if not ema20 or not ema50:
                return
            trend_long = close_p > ema20 > ema50 and (ema20 - ema50) > close_p * 0.003
            trend_short = close_p < ema20 < ema50 and (ema50 - ema20) > close_p * 0.003

            # giriş süzgeci
            if abs(change) < 0.2 and vol_z < 0.8:
                return

            sw_up, sw_down = detect_sweep(h, l, c)
            atr_val = atr(h, l, c) or close_p * 0.005
            compressed, break_up, break_down = sideways_breakout(c)
            of_score = orderflow_strength(vol, tbuy)
            funding, oi_ch = await get_heavy_data(session, symbol)

            # ========== SKORLAMA ==========
            long_score = 0
            short_score = 0
            reasons = []

            # momentum
            if change > 0.9 and close_p > c[-5]:
                long_score += 3 * weights["trend"]
                reasons.append("Güçlü yükseliş")
            if change < -0.9 and close_p < c[-5]:
                short_score += 3 * weights["trend"]
                reasons.append("Güçlü düşüş")

            # hacim
            if vol_z > 3 and vol > mean(v[-20:]) * 2.2:
                if change > 0:
                    long_score += 2 * weights["volume"]
                else:
                    short_score += 2 * weights["volume"]
                reasons.append("Hacim patlaması")

            # sweep
            if sw_down:
                long_score += 4 * weights["whale"]
                reasons.append("Dip sweep")
            if sw_up:
                short_score += 4 * weights["whale"]
                reasons.append("Tepe sweep")

            # sıkışma kırılımı
            if compressed and break_up:
                long_score += 4 * weights["breakout"]
                reasons.append("Yatay kırılım yukarı")
            if compressed and break_down:
                short_score += 4 * weights["breakout"]
                reasons.append("Yatay kırılım aşağı")

            # orderflow
            if of_score > 0: long_score += of_score
            if of_score < 0: short_score += abs(of_score)

            # OI (yönlü)
            if oi_ch > 4 and change > 0:
                long_score += 2
                reasons.append("OI artışı + yükseliş")
            if oi_ch > 4 and change < 0:
                short_score += 2
                reasons.append("OI artışı + düşüş")

            # funding squeeze
            if funding < -0.005 and change > 0:
                long_score += 3
                reasons.append("Short squeeze riski")
            if funding > 0.005 and change < 0:
                short_score += 3
                reasons.append("Long squeeze riski")

            # TREND
            if trend_long:
                long_score += 3
                reasons.append("Trend yukarı")
            if trend_short:
                short_score += 3
                reasons.append("Trend aşağı")

            # BTC relative strength
            if change > btc_change * 1.8:
                long_score += 2
                reasons.append("BTC'ye göre güçlü")
            if btc_change > 0.5 and change < -0.5:
                short_score += 2
                reasons.append("BTC'ye göre zayıf")

            # BTC bias
            if btc_bias == "BULLISH": long_score += 1
            if btc_bias == "BEARISH": short_score += 1

            best = max(long_score, short_score)
            sig = classify_signal(best)
            if not sig: return

            direction = "LONG" if long_score > short_score else "SHORT"

            now = time.time()
            # Market‑wide cooldown kontrolü
            if now - last_global_signal < GLOBAL_COOLDOWN:
                return
            if symbol in last_signal and now - last_signal[symbol] < COOLDOWN:
                return

            last_signal[symbol] = now
            last_global_signal = now

            pnl = (best - 10) * 0.4
            risk_engine(pnl)
            evolve(pnl)

            sl = close_p - atr_val * 1.5 if direction == "LONG" else close_p + atr_val * 1.5
            tp = close_p + atr_val * 2.5 if direction == "LONG" else close_p - atr_val * 2.5
            confidence = min(95, int(best * 7))

            msg = (f"{sig} {symbol} {direction}\n"
                   f"Güven: %{confidence}\n"
                   f"Giriş: {close_p:.4f}\n"
                   f"TP: {tp:.4f}  SL: {sl:.4f}\n"
                   f"Sebep: {', '.join(reasons) if reasons else 'Momentum'}")
            print(msg)
            await send_telegram(msg)

        except Exception as e:
            logging.error(f"SCAN {symbol}: {traceback.format_exc()}")

# ==================================================
# BACKTEST
# ==================================================
async def run_backtest(session):
    try:
        await send_telegram("📊 Günlük backtest başlıyor...")
        syms = await get_all_symbols(session)
        test = syms[:50]
        total = wins = 0
        pnl_net = 0.0
        comm = 0.0004; slip = 0.0002

        for sym in test:
            kl = await fetch_json(session, "/fapi/v1/klines",
                                  {"symbol": sym, "interval": "5m", "limit": 1000})
            if not kl or len(kl) < 50: continue
            for i in range(200, len(kl)-1):
                win = kl[i-49:i+1] if i >= 49 else kl[:i+1]
                if len(win) < 30: continue
                c = [float(k[4]) for k in win]; h = [float(k[2]) for k in win]
                l = [float(k[3]) for k in win]; v = [float(k[5]) for k in win]
                last = win[-1]; op, cp = float(last[1]), float(last[4])
                vol, tbuy = float(last[5]), float(last[9])
                change = (cp - op) / op * 100
                if vol <= 0 or abs(change) < 0.1: continue

                sw_up, sw_down = detect_sweep(h, l, c)
                atr_val = atr(h, l, c) or cp * 0.005
                compressed, bu, bd = sideways_breakout(c)
                of = orderflow_strength(vol, tbuy)

                long = short = 0
                if change > 0.5: long += 1
                if change < -0.5: short += 1
                if sw_down: long += 2
                if sw_up: short += 2
                if compressed and bu: long += 2
                if compressed and bd: short += 2
                if of > 0: long += of
                if of < 0: short += abs(of)

                best = max(long, short)
                if best < 4: continue

                direction = "LONG" if long > short else "SHORT"
                entry = cp
                tp = entry + atr_val*2 if direction=="LONG" else entry - atr_val*2
                sl = entry - atr_val*1.5 if direction=="LONG" else entry + atr_val*1.5
                entry_real = entry * (1+slip+comm) if direction=="LONG" else entry * (1-slip-comm)
                fut = [float(k[4]) for k in kl[i+1:i+31]]
                exit_p = None
                for p in fut:
                    if direction=="LONG":
                        if p >= tp: exit_p = tp*(1-comm-slip); wins += 1; break
                        elif p <= sl: exit_p = sl*(1-comm-slip); break
                    else:
                        if p <= tp: exit_p = tp*(1-comm-slip); wins += 1; break
                        elif p >= sl: exit_p = sl*(1-comm-slip); break
                if exit_p is None: exit_p = entry_real
                pnl_net += (exit_p - entry_real) / entry_real * 100
                total += 1
            await asyncio.sleep(0.01)

        winrate = wins/total*100 if total else 0
        avg = pnl_net/total if total else 0
        pf = (pnl_net+wins)/(abs(pnl_net)+(total-wins)) if total else 0
        msg = (f"📊 Backtest (son 3 gün)\n"
               f"Sinyal: {total} | Kazanç: %{winrate:.1f}\n"
               f"Net PnL: %{pnl_net:.2f} | PF: {pf:.2f}")
        await send_telegram(msg)
    except Exception as e:
        logging.error(f"Backtest: {traceback.format_exc()}")

# ==================================================
# MAIN
# ==================================================
async def main():
    global trading_paused
    print("🚀 PROFESYONEL BOT (GLOBAL COOLDOWN + OPTİMİZASYON)")
    async with aiohttp.ClientSession() as session:
        worker = asyncio.create_task(telegram_worker(session))
        await send_telegram("✅ BOT AKTIF (global cooldown, optimize filtreler)")

        if await fetch_json(session, "/fapi/v1/ping"):
            print("Binance bağlantısı başarılı")
        else:
            await send_telegram("⚠️ Binance bağlantısı başarısız!")

        syms = await get_all_symbols(session)
        print(f"{len(syms)} coin taranıyor")
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        asyncio.create_task(run_backtest(session))
        last_backtest = time.time()

        while True:
            if trading_paused:
                await send_telegram("🛑 Risk limitine ulaşıldı, bot tamamen durduruluyor!")
                break

            btc_bias, btc_change = await get_btc_data(session)
            tasks = [scan_coin(session, s, btc_bias, btc_change, sem) for s in syms]
            await asyncio.gather(*tasks, return_exceptions=True)

            if time.time() - last_backtest > 86400:
                asyncio.create_task(run_backtest(session))
                last_backtest = time.time()

            await asyncio.sleep(SCAN_INTERVAL)

        worker.cancel()
        await worker

if __name__ == "__main__":
    asyncio.run(main())
