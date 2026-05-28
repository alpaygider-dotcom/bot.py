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
# INDICATORS (değişmedi)
# ==================================================
def ema(values, period):
    if len(values) < period:
        return None
    m = 2 / (period + 1)
    e = mean(values[:period])
    for x in values[period:]:
        e = (x - e) * m + e
    return e

def atr(highs, lows, closes, period=14):
    if len(highs) < period + 1:
        return None
    tr = []
    for i in range(1, len(highs)):
        tr.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    return mean(tr[-period:])

def detect_regime(closes, volumes):
    move = (closes[-1] - closes[0]) / closes[0]
    vm = mean(volumes)
    vs = stdev(volumes) if len(volumes) > 1 else 0
    vz = (volumes[-1] - vm) / vs if vs > 0 else 0
    score = 0
    if abs(move) > 0.01: score += 1
    if abs(move) > 0.02: score += 1
    if vz > 1: score += 1
    if score >= 2: return "TREND"
    if abs(move) < 0.003: return "RANGE"
    return "MIXED"

def detect_sweep(highs, lows, closes):
    rh = max(highs[-20:-1])
    rl = min(lows[-20:-1])
    up = highs[-1] > rh and closes[-1] < highs[-1]
    down = lows[-1] < rl and closes[-1] > lows[-1]
    return up, down

def sideways_breakout(closes, atr_val=None):
    recent = closes[-15:]
    highest = max(recent)
    lowest = min(recent)
    range_pct = ((highest - lowest) / lowest) * 100
    factor = max((atr_val / highest) * 2, 0.001) if (atr_val and highest > 0) else 0.002
    up = closes[-1] > highest * (1 - factor)
    down = closes[-1] < lowest * (1 + factor)
    return range_pct < 2.5, up, down

def orderflow_strength(volume, taker_buy, hist_tb=None, hist_vol=None):
    if volume <= 0:
        return 0, 0
    ratio = taker_buy / volume
    delta = (taker_buy - (volume - taker_buy)) / volume
    score = 0
    if ratio > 0.62: score += 2
    if ratio < 0.38: score -= 2
    if delta > 0.18: score += 2
    if delta < -0.18: score -= 2

    cvd_trend = 0
    if hist_tb and hist_vol and len(hist_tb) >= 5:
        cvd_vals = []
        cvd = 0
        for tb, vol in zip(hist_tb[-10:], hist_vol[-10:]):
            cvd += tb - (vol - tb) if vol > 0 else 0
            cvd_vals.append(cvd)
        if len(cvd_vals) >= 5:
            x = list(range(5))
            y = cvd_vals[-5:]
            n = 5
            sx = sum(x); sy = sum(y)
            sxy = sum(x[i]*y[i] for i in range(5))
            sx2 = sum(i*i for i in x)
            denom = n*sx2 - sx*sx
            slope = (n*sxy - sx*sy) / denom if denom != 0 else 0
            cvd_trend = 1 if slope > 0 else -1 if slope < 0 else 0
            if cvd_trend > 0: score += 2
            elif cvd_trend < 0: score -= 2
    return score, cvd_trend

# ==================================================
# EXTERNAL DATA
# ==================================================
async def get_heavy_data(session, symbol):
    funding = 0.0; oi_change = 0.0; ls = 1.0
    f = await fetch_json(session, "/fapi/v1/premiumIndex", {"symbol": symbol})
    if f: funding = float(f.get("lastFundingRate", 0))
    oi = await fetch_json(session, "/futures/data/openInterestHist",
                          {"symbol": symbol, "period": "5m", "limit": 2})
    if oi and len(oi) >= 2:
        prev = float(oi[-2]["sumOpenInterest"])
        curr = float(oi[-1]["sumOpenInterest"])
        if prev > 0: oi_change = (curr - prev) / prev * 100
    r = await fetch_json(session, "/futures/data/topLongShortPositionRatio",
                         {"symbol": symbol, "period": "5m", "limit": 1})
    if r:
        try: ls = float(r[-1]["longShortRatio"])
        except: pass
    return funding, oi_change, ls

async def get_liquidation_data(session, symbol):
    if not COINGLASS_API_KEY: return None, None
    try:
        headers = {"coinglassSecret": COINGLASS_API_KEY}
        params = {"symbol": symbol.replace("USDT",""), "time_type": "1"}
        async with session.get(
            "https://open-api.coinglass.com/public/v2/liquidation",
            params=params, headers=headers, timeout=10
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("code") == "0":
                    items = data["data"]["liquidationList"]
                    long_liq = sum(float(it["longVolUsd"]) for it in items)
                    short_liq = sum(float(it["shortVolUsd"]) for it in items)
                    return long_liq, short_liq
    except:
        pass
    return None, None

# ==================================================
# ADAPTIVE VOLATILITY
# ==================================================
def calc_volatility_factor(closes):
    if len(closes) < 20: return 1.0
    returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
    vol = stdev(returns) if len(returns) > 1 else 0
    base = 0.005
    factor = vol / base if base > 0 else 1
    return min(max(factor, 0.7), 1.4)

# ==================================================
# CLASSIFY (ZIRHLI SİNYAL EKLENDİ)
# ==================================================
def classify_signal(score, vol_factor=1.0, use_floor=True):
    if use_floor:
        armored = max(15 * vol_factor, 12)
        strong = max(13 * vol_factor, 10)
        medium = max(8 * vol_factor, 6)
        weak = max(5 * vol_factor, 4)
    else:
        armored = 15 * vol_factor
        strong = 13 * vol_factor
        medium = 8 * vol_factor
        weak = 5 * vol_factor

    if score >= armored:
        return "🛡️ ZIRHLI SİNYAL"
    if score >= strong:
        return "🔥 GÜÇLÜ"
    if score >= medium:
        return "🟡 ORTA"
    if score >= weak:
        return "🟢 ZAYIF"
    return None

# ==================================================
# RISK ENGINE
# ==================================================
def risk_engine(pnl):
    global equity, daily_pnl, consecutive_losses, trading_paused
    equity += pnl
    daily_pnl += pnl
    if pnl < 0:
        consecutive_losses += 1
    else:
        consecutive_losses = 0
    if daily_pnl < -80 or equity < balance * 0.85 or consecutive_losses >= 5:
        trading_paused = True

def evolve(pnl):
    global weights
    if pnl > 0:
        for k in weights: weights[k] *= 1.01
    else:
        weights["trend"] *= 0.99
        weights["whale"] *= 0.99
    for k in weights:
        weights[k] = max(0.4, min(weights[k], 2.5))

# ==================================================
# BTC BIAS
# ==================================================
async def get_btc_bias(session):
    kl = await fetch_json(session, "/fapi/v1/klines", {"symbol":"BTCUSDT","interval":"15m","limit":50})
    if not kl: return "NEUTRAL"
    c = [float(k[4]) for k in kl]
    e20 = ema(c, 20); e50 = ema(c, 50)
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
# SCAN (ZIRHLI HIBRIT)
# ==================================================
async def scan_coin(session, symbol, btc_bias, sem):
    global trading_paused
    async with sem:
        if trading_paused: return
        try:
            kl = await fetch_json(session, "/fapi/v1/klines",
                                  {"symbol": symbol, "interval": "5m", "limit": 80})
            if not kl: return
            kl_1h = await fetch_json(session, "/fapi/v1/klines",
                                     {"symbol": symbol, "interval": "1h", "limit": 80})

            c = [float(k[4]) for k in kl]
            h = [float(k[2]) for k in kl]
            l = [float(k[3]) for k in kl]
            v = [float(k[5]) for k in kl]
            tb = [float(k[9]) for k in kl]

            last = kl[-2]
            open_p = float(last[1]); close_p = float(last[4])
            vol = float(last[5]); tbuy = float(last[9])
            change = (close_p - open_p) / open_p * 100

            vm = mean(v); vs = stdev(v) if len(v) > 1 else 0
            vz = (vol - vm) / vs if vs > 0 else 0

            vol_factor = calc_volatility_factor(c)
            if abs(change) < 0.25 * vol_factor and vz < 0.8 * vol_factor:
                return

            regime = detect_regime(c, v)
            sw_up, sw_down = detect_sweep(h, l, c)

            atr_val = atr(h, l, c) or close_p * 0.005
            comp, break_up, break_down = sideways_breakout(c, atr_val)

            funding, oi_ch, ls = await get_heavy_data(session, symbol)
            liq_l, liq_s = await get_liquidation_data(session, symbol)

            of_score, cvd_trend = orderflow_strength(vol, tbuy, tb, v)

            bonus_l = bonus_s = 0
            if kl_1h:
                c1 = [float(k[4]) for k in kl_1h]
                e20 = ema(c1, 20); e50 = ema(c1, 50)
                if e20 and e50:
                    if c1[-1] > e20 > e50: bonus_l += 3
                    if c1[-1] < e20 < e50: bonus_s += 3

            long_score = 0
            short_score = 0

            if change > 1: long_score += 2 * weights["trend"]
            if change < -1: short_score += 2 * weights["trend"]

            if vz > 2 * vol_factor:
                long_score += 2 * weights["volume"]
                short_score += 2 * weights["volume"]

            if regime == "TREND":
                long_score += 1 * weights["regime"]
                short_score += 1 * weights["regime"]

            if sw_down: long_score += 3 * weights["whale"]
            if sw_up: short_score += 3 * weights["whale"]

            if oi_ch > 4:
                long_score += 3
                short_score += 3
            if funding < -0.01 and change > 0: long_score += 3
            if funding > 0.01 and change < 0: short_score += 3

            if ls > 1.5: short_score += 1
            if ls < 0.7: long_score += 1

            if comp and break_up: long_score += 3 * weights["breakout"]
            if comp and break_down: short_score += 3 * weights["breakout"]

            if of_score > 0: long_score += of_score
            if of_score < 0: short_score += abs(of_score)
            if cvd_trend > 0: long_score += 2
            if cvd_trend < 0: short_score += 2

            long_score += bonus_l
            short_score += bonus_s

            if btc_bias == "BULLISH": long_score += 2; short_score -= 1
            if btc_bias == "BEARISH": short_score += 2; long_score -= 1

            if liq_l and liq_s:
                if liq_l > liq_s * 1.3: long_score += 2
                if liq_s > liq_l * 1.3: short_score += 2

            best = max(long_score, short_score)
            sig = classify_signal(best, vol_factor, use_floor=True)
            if not sig: return

            direction = "LONG" if long_score > short_score else "SHORT"

            if btc_bias == "BULLISH" and direction == "SHORT" and best < 11: return
            if btc_bias == "BEARISH" and direction == "LONG" and best < 11: return

            now = time.time()
            if symbol in last_signal and now - last_signal[symbol] < COOLDOWN: return
            last_signal[symbol] = now

            pnl = (best - 10) * 0.5
            risk_engine(pnl)
            evolve(pnl)

            sl = close_p - atr_val * 1.5 if direction == "LONG" else close_p + atr_val * 1.5
            tp = close_p + atr_val * 2.0 if direction == "LONG" else close_p - atr_val * 2.0

            msg = f"{sig} {symbol} {direction}\nTP:{tp:.4f} SL:{sl:.4f}\nScore:{best}"
            print(msg)
            await send_telegram(msg)

        except Exception as e:
            logging.error(f"SCAN {symbol}: {traceback.format_exc()}")

# ==================================================
# BACKTEST (ZIRHLI HIBRIT)
# ==================================================
async def run_backtest(session):
    try:
        await send_telegram("📊 BACKTEST BAŞLADI")
        syms = await get_all_symbols(session)
        if not syms:
            await send_telegram("❌ Sembol listesi boş!")
            return
        await send_telegram(f"📋 {len(syms)} sembol bulundu")
        test = syms[:200]
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
                tb = [float(k[9]) for k in win]
                last = win[-1]; op = float(last[1]); cp = float(last[4])
                vol = float(last[5]); tbuy = float(last[9])
                change = (cp - op) / op * 100
                if vol <= 0 or abs(change) < 0.1: continue

                regime = detect_regime(c, v)
                sw_up, sw_down = detect_sweep(h, l, c)
                atr_val = atr(h, l, c) or cp * 0.005
                comp, bu, bd = sideways_breakout(c, atr_val)
                of, cvd = orderflow_strength(vol, tbuy, tb, v)

                long = short = 0
                if change > 0.5: long += 1
                if change < -0.5: short += 1
                if sw_down: long += 2
                if sw_up: short += 2
                if comp and bu: long += 2
                if comp and bd: short += 2
                if of > 0: long += of
                if of < 0: short += abs(of)
                if cvd > 0: long += 1
                if cvd < 0: short += 1
                if regime == "TREND": long += 1; short += 1

                best = max(long, short)
                sig = classify_signal(best, 0.2, use_floor=False)
                if not sig: continue

                dir = "LONG" if long > short else "SHORT"
                entry = cp
                tp = entry + atr_val*2 if dir=="LONG" else entry - atr_val*2
                sl = entry - atr_val*1.5 if dir=="LONG" else entry + atr_val*1.5
                entry_real = entry * (1+slip+comm) if dir=="LONG" else entry * (1-slip-comm)
                fut = [float(k[4]) for k in kl[i+1:i+31]]
                exit_p = None
                for p in fut:
                    if dir=="LONG":
                        if p >= tp: exit_p = tp*(1-comm-slip); wins += 1; break
                        elif p <= sl: exit_p = sl*(1-comm-slip); break
                    else:
                        if p <= tp: exit_p = tp*(1-comm-slip); wins += 1; break
                        elif p >= sl: exit_p = sl*(1-comm-slip); break
                if exit_p is None: exit_p = entry_real
                pnl_net += (exit_p - entry_real) / entry_real * 100
                total += 1

        winrate = wins/total*100 if total else 0
        avg = pnl_net/total if total else 0
        pf = (pnl_net+wins)/(abs(pnl_net)+(total-wins)) if total else 0
        msg = (f"📊 BACKTEST\nSinyal:{total} Kazan:{wins} (%{winrate:.1f})\n"
               f"Ort.PnL:%{avg:.2f} Küm.PnL:%{pnl_net:.2f} PF:{pf:.2f}")
        await send_telegram(msg)
    except Exception as e:
        logging.error(f"Backtest: {traceback.format_exc()}")

# ==================================================
# MAIN (TEMIZ OTURUM)
# ==================================================
async def main():
    print("🚀 ZIRHLI PROFESYONEL BOT BAŞLATILDI")
    async with aiohttp.ClientSession() as session:
        worker = asyncio.create_task(telegram_worker(session))
        await send_telegram("🛡️ ZIRHLI PROFESYONEL BOT ONLINE")

        # Bağlantı testi
        if await fetch_json(session, "/fapi/v1/ping") is not None:
            await send_telegram("🌐 Binance bağlantısı başarılı")
        else:
            await send_telegram("⚠️ Bağlantı başarısız, ancak denemeye devam edilecek.")

        syms = await get_all_symbols(session)
        print(f"{len(syms)} coin")
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        asyncio.create_task(run_backtest(session))
        last_backtest = time.time()

        try:
            while True:
                bias = await get_btc_bias(session)
                tasks = [scan_coin(session, sym, bias, sem) for sym in syms]
                await asyncio.gather(*tasks, return_exceptions=True)
                if time.time() - last_backtest > 86400:
                    asyncio.create_task(run_backtest(session))
                    last_backtest = time.time()
                await asyncio.sleep(SCAN_INTERVAL)
        except asyncio.CancelledError:
            print("Bot kapatılıyor...")
        finally:
            worker.cancel()
            await worker

if __name__ == "__main__":
    asyncio.run(main())
