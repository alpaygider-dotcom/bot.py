import asyncio
import os
import time
import aiohttp
from binance import AsyncClient, BinanceSocketManager
from statistics import median

# =========================================================
# AYARLAR
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ BOT_TOKEN veya CHAT_ID eksik! Railway Environment Variables'a ekle.")
    exit(1)

STABLECOIN_BLACKLIST = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT", "USDTUSDT", "FDUSDUSDT"}
MAJOR_COINS_BLACKLIST = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}

# Sadece en popüler 100 coin (Railway'i yormamak için)
TOP_COINS = [
    "1000pepeusdt", "1000shibusdt", "1mbabydogeusdt", "aaveusdt", "adausdt", "aevousdt", "agusdt", "algousdt",
    "aliceusdt", "ankrusdt", "apeusdt", "aptusdt", "arbusdt", "arkusdt", "arpaustd", "atausdt", "atomusdt",
    "avaxusdt", "axsusdt", "bakeusdt", "balusdt", "bandusdt", "barusdt", "batusdt", "bchusdt", "belusdt",
    "blurusdt", "bnbusdt", "bntusdt", "bomeusdt", "bonkusdt", "btcusdt", "bttusdt", "cakeusdt", "catusdt",
    "celousdt", "chzusdt", "ckbusdt", "comptusdt", "crvusdt", "ctkustd", "dashusdt", "dogeusdt", "dotusdt",
    "dydxusdt", "dymusdt", "egldusdt", "enjusdt", "ensustd", "enausdt", "etcusdt", "ethusdt", "ethfiusdt",
    "filusdt", "flokiusdt", "ftmusdt", "galaustd", "galausdt", "gmtusdt", "grtusdt", "gunusdt", "hbarusdt",
    "hotusdt", "icpusdt", "idexusdt", "imxustd", "injussdt", "iostusdt", "iotausdt", "jasmyusdt", "jtojusdt",
    "jupusdt", "kavausdt", "kdaustd", "klayusdt", "kmdusdt", "krsusdt", "ktousdt", "lrcusdt", "ltcusdt",
    "lunausdt", "magicusdt", "mantaustd", "maskusdt", "mavusdt", "minausdt", "mntausdt", "movrusdt",
    "nearusdt", "neousdt", "nfpusdt", "notusdt", "nzdsusdt", "oceanusdt", "ogusdt", "oneusdt", "ontusdt",
    "opusdt", "ordiusdt", "paxgusdt", "pendleusdt", "pepeusdt", "pixelusdt", "polusdt", "polyxusdt", "portalustd"
]

MIN_VOL_SPIKE_DIP = 2.5
MIN_VOL_SPIKE_SQUEEZE = 4.0
MIN_RSI = 30
MIN_PRICE_JUMP = 3.0
LS_DROP_THRESHOLD = -2.0
LS_HIGH_THRESHOLD = 2.0

# Global veri deposu
DATA_STORE = {}

# =========================================================
# TELEGRAM GÖNDERME
# =========================================================
async def send_telegram(message):
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            await session.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"})
    except:
        pass

# =========================================================
# RSI HESAPLAMA (Wilder Smoothing)
# =========================================================
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(-diff)
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# =========================================================
# GEÇMİŞ VERİLERİ ÖN YÜKLEME
# =========================================================
async def preload_historical_data(client, symbols):
    print("⏳ Geçmiş veriler yükleniyor...")
    semaphore = asyncio.Semaphore(10)
    
    async def fetch_history(sym):
        async with semaphore:
            try:
                klines = await client.get_klines(symbol=sym.upper(), interval="1m", limit=100)
                prices = [float(k[4]) for k in klines[:-1]]
                volumes = [float(k[5]) for k in klines[:-1]]
                DATA_STORE[sym] = {"prices": prices, "volumes": volumes, "last_ls": 0}
            except Exception as e:
                print(f"⚠️ {sym} geçmişi alınamadı: {e}")

    await asyncio.gather(*[fetch_history(s) for s in symbols])
    print("✅ Tüm coinlerin geçmiş verileri hafızaya yüklendi!")

# =========================================================
# LONG/SHORT ORANI ÇEKME (Daha seyrek)
# =========================================================
async def fetch_ls_ratio(session, symbol):
    try:
        url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
        params = {"symbol": symbol.upper(), "period": "5m", "limit": 2}
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                if len(data) >= 2:
                    prev = float(data[-2]["longShortRatio"])
                    curr = float(data[-1]["longShortRatio"])
                    change = ((curr - prev) / prev) * 100
                    return {"ratio": curr, "change": change}
    except:
        pass
    return None

# =========================================================
# SİNYAL KONTROLÜ
# =========================================================
def check_signals(symbol, data, close_price, volume, kline):
    prices = data["prices"]
    volumes = data["volumes"]
    
    if len(prices) < 20 or len(volumes) < 20:
        return None
    
    median_vol = median(volumes[-20:])
    rsi = calculate_rsi(prices)
    recent_low = min(prices[-5:])
    rise_pct = ((close_price - recent_low) / recent_low) * 100 if recent_low > 0 else 0
    price_change_pct = ((close_price - prices[-6]) / prices[-6]) * 100 if len(prices) >= 6 else 0
    
    tbuy = float(kline["t"]) if "t" in kline else 0
    buy_sell_ratio = tbuy / max(volume - tbuy, 1)
    
    # DİP DÖNÜŞÜ
    dip_score = 0
    dip_reasons = []
    if rsi < MIN_RSI:
        dip_score += 3
        dip_reasons.append(f"RSI {rsi:.1f}")
    if rise_pct < 1.0 and close_price > recent_low:
        dip_score += 2
        dip_reasons.append("Fiyat dip oluşturuyor")
    if volume > median_vol * MIN_VOL_SPIKE_DIP:
        dip_score += 3
        dip_reasons.append(f"Hacim x{volume/median_vol:.1f}")
    if close_price > float(kline["o"]):
        dip_score += 2
        dip_reasons.append("Yeşil mum")
    
    # SHORT SQUEEZE
    squeeze_score = 0
    squeeze_reasons = []
    if price_change_pct > MIN_PRICE_JUMP:
        squeeze_score += 3
        squeeze_reasons.append(f"Fiyat %{price_change_pct:.1f}")
    if volume > median_vol * MIN_VOL_SPIKE_SQUEEZE:
        squeeze_score += 4
        squeeze_reasons.append(f"Hacim x{volume/median_vol:.1f}")
    if close_price > float(kline["o"]):
        squeeze_score += 1
        squeeze_reasons.append("Yeşil mum")
    
    # CEZALAR
    penalty = 0
    if len(prices) >= 20:
        ema20 = sum(prices[-20:]) / 20
        if close_price < ema20:
            penalty += 3
    if buy_sell_ratio < 0.8:
        penalty += 3
    
    dip_total = dip_score - penalty
    squeeze_total = squeeze_score - penalty
    
    if dip_total >= 10:
        return {"type": "DİP DÖNÜŞÜ", "score": dip_total, "price": close_price, "rsi": rsi, "volume": volume, "median_vol": median_vol, "reasons": dip_reasons}
    if squeeze_total >= 10:
        return {"type": "SHORT SQUEEZE", "score": squeeze_total, "price": close_price, "rsi": rsi, "volume": volume, "median_vol": median_vol, "reasons": squeeze_reasons}
    return None

# =========================================================
# SOKET VERİ İŞLEME
# =========================================================
async def handle_socket_message(msg, session):
    if "data" not in msg:
        return
    kline = msg["data"]["k"]
    symbol = msg["data"]["s"].lower()
    if symbol not in DATA_STORE:
        return
    
    close_price = float(kline["c"])
    volume = float(kline["v"])
    is_candle_closed = kline["x"]
    
    if is_candle_closed:
        DATA_STORE[symbol]["prices"].append(close_price)
        DATA_STORE[symbol]["volumes"].append(volume)
        if len(DATA_STORE[symbol]["prices"]) > 100:
            DATA_STORE[symbol]["prices"].pop(0)
            DATA_STORE[symbol]["volumes"].pop(0)
        
        signal = check_signals(symbol, DATA_STORE[symbol], close_price, volume, kline)
        if signal:
            # L/S oranını 10 dakikada bir çek
            if time.time() - DATA_STORE[symbol]["last_ls"] > 600:
                ls_data = await fetch_ls_ratio(session, symbol)
                DATA_STORE[symbol]["last_ls"] = time.time()
            
            ls_extra = ""
            if ls_data:
                if ls_data["change"] < LS_DROP_THRESHOLD:
                    ls_extra = "✅ Short'lar azalıyor"
                elif ls_data["ratio"] > LS_HIGH_THRESHOLD and ls_data["change"] < 0:
                    ls_extra = "✅ Long'lar azalıyor"
            
            message = f"""<b>{signal["type"]}</b>
{symbol.upper()}
Fiyat: {signal['price']:.6f}
Skor: {signal['score']}
RSI: {signal['rsi']:.1f}
Hacim: {signal['volume']:,.0f}
{ls_extra}
{' , '.join(signal['reasons'])}"""
            await send_telegram(message)

# =========================================================
# YARDIMCI FONKSİYONLAR
# =========================================================
async def get_all_spot_symbols(client):
    data = await client.get_exchange_info()
    symbols = []
    for s in data.get("symbols", []):
        sym = s["symbol"]
        if sym.endswith("USDT") and sym not in STABLECOIN_BLACKLIST and sym not in MAJOR_COINS_BLACKLIST:
            symbols.append(sym.lower())
    return sorted(symbols)

# =========================================================
# ANA DÖNGÜ
# =========================================================
async def run_bot():
    print("🚀 Bot başlatılıyor...")
    await send_telegram("🤖 Bot başlatılıyor, sinyaller 2-3 dakika içinde gelmeye başlayacak.")
    
    client = await AsyncClient.create()
    try:
        # Burada TOP_COINS kullanıyoruz, tüm coinler yerine
        all_coins = TOP_COINS
        print(f"📊 Toplam {len(all_coins)} coin taranıyor.")
        
        if len(DATA_STORE) == 0:
            await preload_historical_data(client, all_coins)
        
        bm = BinanceSocketManager(client)
        streams = [f"{sym}@kline_1m" for sym in all_coins]
        print(f"📡 {len(streams)} stream dinleniyor...")
        
        async with aiohttp.ClientSession() as session:
            async with bm.multiplex_socket(streams) as stream:
                print("✅ WebSocket bağlantısı kuruldu!")
                while True:
                    try:
                        res = await stream.recv()
                        await handle_socket_message(res, session)
                    except Exception as e:
                        print(f"Soket hatası: {e}")
                        break
    except Exception as e:
        print(f"Bot hatası: {e}")
        await send_telegram(f"❌ Hata: {str(e)[:100]}")
    finally:
        await client.close_connection()
        print("✅ Binance client kapatıldı.")

# =========================================================
# MAIN
# =========================================================
async def main():
    while True:
        try:
            await run_bot()
        except Exception as e:
            print(f"Kritik hata: {e}")
        await asyncio.sleep(60)  # Bağlantı koparsa 60 saniye bekle

if __name__ == "__main__":
    asyncio.run(main())
