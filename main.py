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
COOLDOWN = 600
MAX_CONCURRENT_REQUESTS = 20

last_signal = {}

# ==================================================
# PORTFOLIO & RISK (opsiyonel)
# ==================================================
balance = 1000.0
equity = 1000.0
daily_pnl = 0.0
consecutive_losses = 0
trading_paused = False

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
# FETCH (USER-AGENT + RETRY)
# ==================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

async def fetch_json(session, endpoint, params=None, max_retries=3):
    url = BASE_URL + endpoint
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
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
# INDICATORS (düzeltildi)
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

def detect_regime(closes, volumes):
    move = (closes[-1] - closes[0]) / closes[0]
    vm = mean(volumes)
    vs = stdev(volumes) if len(volumes) > 1 else 0
    vz = (volumes[-1] - vm) / vs if vs > 0 else 0
    # rejim sadece bilgi amaçlı, ayrıca çift taraflı puan vermeyecek
    if abs(move) > 0.012 and vz > 1: return "TREND"
    if abs(move) < 0.004: return "RANGE"
    return "MIXED"

def detect_sweep(highs, lows, closes):
    rh = max(highs[-20:-1])
    rl = min(lows[-20:-1])
    up = highs[-1] > rh and closes[-1] < highs[-1]
    down = lows[-1] < rl and closes[-1] > lows[-1]
    return up, down

def sideways_breakout(closes, atr_val=None):
    recent = closes[-15:]
    highest, lowest = max(recent), min(recent)
    range_pct = (highest - lowest) / lowest * 100
    factor = max((atr_val / highest) * 2, 0.001) if (atr_val and highest > 0) else 0.002
    up = closes[-1] > highest * (1 - factor)
    down = closes[-1] < lowest * (1 + factor)
    return range_pct < 2.5, up, down

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
# BTC BIAS (hafifletildi)
# ==================================================
async def get_btc_bias(session):
    kl = await fetch_json(session, "/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "15m", "limit": 50})
    if not kl: return "NEUTRAL"
    c = [float(k[4]) for k in kl]
    e20, e50 = ema(c, 20), ema(c, 50)
    if not e20 or not e50: return "NEUTRAL"
    if c[-1] > e20 > e50: return "BULLISH"
    if c[-1] < e20 < e50: return "BEARISH"
    return "NEUTRAL"

# ==================================================
# SYMBOLS
# ==================================================
async def get_all_symbols(session):
    info = await fetch_json(session, "/fapi/v1/exchangeInfo")
    if not info: return []
    return [s["symbol"] for s in info["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"]

# ==================================================
# CLASSIFY (sabit ve makul)
# ==================================================
def classify_signal(score):
    if score >= 12:
        return "🛡️ ZIRHLI"
    if score >= 9:
        return "🔥 GÜÇLÜ"
    return None

# ==================================================
# SCAN (HATALAR TEMİZLENDİ)
# ==================================================
async def scan_coin(session, symbol, btc_bias, sem):
    global trading_paused
    async with sem:
        if trading_paused: return
        try:
            kl = await fetch_json(session, "/fapi/v1/klines",
                                  {"symbol": symbol, "interval": "5m", "limit": 80})
            if not kl: return

            c = [float(k[4]) for k in kl]
            h = [float(k[2]) for k in kl]
            l = [float(k[3]) for k in kl]
            v = [float(k[5]) for k in kl]

            last = kl[-2]
            open_p, close_p = float(last[1]), float(last[4])
            vol, tbuy = float(last[5]), float(last[9])
            change = (close_p - open_p) / open_p * 100

            vol_z = (vol - mean(v)) / stdev(v) if len(v) > 1 and stdev(v) > 0 else 0

            # ---------- GİRİŞ SÜZGECİ (durgun coin) ----------
            if abs(change) < 0.15 and vol_z < 0.6:
                return

            regime = detect_regime(c, v)
            sw_up, sw_down = detect_sweep(h, l, c)

            atr_val = atr(h, l, c) or close_p * 0.005
            compressed, break_up, break_down = sideways_breakout(c, atr_val)

            of_score = orderflow_strength(vol, tbuy)

            # ========== YÖNLÜ SKORLAMA (çift taraflı yok) ==========
            long_score = 0
            short_score = 0

            # momentum (yönlü)
            if change > 1.2: long_score += 2
            if change < -1.2: short_score += 2

            # hacim patlaması (yön belirtmez, ikisine de düşük puan)
            if vol_z > 2.5:
                long_score += 1
                short_score += 1

            # sweep (yönlü, güçlü)
            if sw_down: long_score += 4
            if sw_up: short_score += 4

            # sıkışma kırılımı (yönlü)
            if compressed and break_up: long_score += 4
            if compressed and break_down: short_score += 4

            # orderflow (yönlü)
            if of_score > 0: long_score += of_score
            if of_score < 0: short_score += abs(of_score)

            # BTC bias (sadece +1, şişirme yok)
            if btc_bias == "BULLISH": long_score += 1
            elif btc_bias == "BEARISH": short_score += 1

            # ========== EN İYİ SKOR ==========
            best = max(long_score, short_score)
            sig = classify_signal(best)
            if not sig: return

            direction = "LONG" if long_score > short_score else "SHORT"

            # cooldown
            now = time.time()
            if symbol in last_signal and now - last_signal[symbol] < COOLDOWN:
                return
            last_signal[symbol] = now

            # TP / SL
            sl = close_p - atr_val * 1.5 if direction == "LONG" else close_p + atr_val * 1.5
            tp = close_p + atr_val * 2.5 if direction == "LONG" else close_p - atr_val * 2.5

            msg = (f"{sig} {symbol} {direction}\n"
                   f"Giriş: {close_p:.4f}\n"
                   f"TP: {tp:.4f}  SL: {sl:.4f}")
            print(msg)
            await send_telegram(msg)

        except Exception as e:
            logging.error(f"SCAN {symbol}: {traceback.format_exc()}")

# ==================================================
# MAIN
# ==================================================
async def main():
    print("🚀 TEMİZ SİNYAL BOTU (çift yönlü puanlar temizlendi)")
    async with aiohttp.ClientSession() as session:
        worker = asyncio.create_task(telegram_worker(session))
        await send_telegram("✅ TEMİZ BOT AKTİF")

        if await fetch_json(session, "/fapi/v1/ping"):
            print("Binance bağlantısı başarılı")
        else:
            await send_telegram("⚠️ Binance bağlantısı başarısız!")

        syms = await get_all_symbols(session)
        print(f"{len(syms)} coin taranıyor")
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        while True:
            bias = await get_btc_bias(session)
            tasks = [scan_coin(session, s, bias, sem) for s in syms]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
