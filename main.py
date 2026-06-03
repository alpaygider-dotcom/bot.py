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

MIN_VOL_SPIKE_DIP = 2.5
MIN_VOL_SPIKE_SQUEEZE = 4.0
MIN_RSI = 30
MIN_PRICE_JUMP = 3.0
LS_DROP_THRESHOLD = -2.0
LS_HIGH_THRESHOLD = 2.0

# Global veri deposu (Tüm coinlerin geçmişini burada tutacağız)
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
# RSI HESAPLAMA (Wilder Smoothing - DOĞRU)
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
    
    # Wilder smoothing
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# =========================================================
# GEÇMİŞ VERİLERİ ÖN YÜKLEME (Preload)
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
# LONG/SHORT ORANI ÇEKME
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
# SİNYAL KONTROLÜ (Merkezi)
# =========================================================
def check_signals(symbol, data, close_price, volume, kline):
    """Veri deposundaki verilere göre sinyal üretir"""
    prices = data["prices"]
    volumes = data["volumes"]
    
    # Yeterli veri yoksa çık
    if len(prices) < 20 or len(volumes) < 20:
        return None
    
    # Ortalama hacim (Median - Daha sağlam)
    median_vol = median(volumes[-20:])
    
    # RSI (Tüm geçmişle hesapla)
    rsi = calculate_rsi(prices)
    
    # Fiyat değişimi (Son 5 mum en düşüğüne göre)
    recent_low = min(prices[-5:])
    rise_pct = ((close_price - recent_low) / recent_low) * 100 if recent_low > 0 else 0
    
    # Fiyat değişimi (Son 6 mum öncesine göre)
    price_change_pct = ((close_price - prices[-6]) / prices[-6]) * 100 if len(prices) >= 6 else 0
    
    # Buy/sell ratio (Taker buy volume)
    tbuy = float(kline["t"]) if "t" in kline else 0
    sell_vol = max(volume - tbuy, 1)
    buy_sell_ratio = tbuy / sell_vol
    
    # ---------- DİP DÖNÜŞÜ ----------
    dip_score = 0
    dip_reasons = []
    
    # 1. RSI aşırı satım
    if rsi < MIN_RSI:
        dip_score += 3
        dip_reasons.append(f"RSI {rsi:.1f} (aşırı satım)")
    
    # 2. Fiyat dip kontrolü
    if rise_pct < 1.0 and close_price > recent_low:
        dip_score += 2
        dip_reasons.append("Fiyat dip oluşturuyor")
    
    # 3. Hacim patlaması
    if volume > median_vol * MIN_VOL_SPIKE_DIP:
        dip_score += 3
        dip_reasons.append(f"Hacim patlaması x{volume/median_vol:.1f}")
    
    # 4. Mum yeşil
    if close_price > float(kline["o"]):
        dip_score += 2
        dip_reasons.append("Yeşil mum")
    
    # 5. Düşüş hızı yavaşlaması
    if len(prices) >= 3:
        drop_last = prices[-1] - prices[-2]
        drop_prev = prices[-2] - prices[-3]
        if drop_last < 0 and drop_prev < 0 and drop_last > drop_prev:
            dip_score += 2
            dip_reasons.append("Düşüş hızı yavaşlıyor")
    
    # ---------- SHORT SQUEEZE ----------
    squeeze_score = 0
    squeeze_reasons = []
    
    # 1. Anormal fiyat hareketi
    if price_change_pct > MIN_PRICE_JUMP:
        squeeze_score += 3
        squeeze_reasons.append(f"Fiyat patlaması %{price_change_pct:.1f}")
    
    # 2. Anormal hacim
    if volume > median_vol * MIN_VOL_SPIKE_SQUEEZE:
        squeeze_score += 4
        squeeze_reasons.append(f"Anormal hacim x{volume/median_vol:.1f}")
    
    # 3. Mum yeşil
    if close_price > float(kline["o"]):
        squeeze_score += 1
        squeeze_reasons.append("Yeşil mum")
    
    # 4. RSI aşırı alım (Squeeze onayı)
    if rsi > 70:
        squeeze_score += 2
        squeeze_reasons.append(f"RSI {rsi:.1f} (aşırı alım)")
    
    # ---------- CEZALAR ----------
    penalty = 0
    penalty_reasons = []
    
    # 1. Düşüş trendi cezası
    if len(prices) >= 20:
        ema20 = sum(prices[-20:]) / 20
        if close_price < ema20:
            penalty += 3
            penalty_reasons.append("Fiyat EMA20 altında (düşüş trendi)")
    
    # 2. Buy/sell ratio cezası
    if buy_sell_ratio < 0.8:
        penalty += 3
        penalty_reasons.append("Alıcı baskısı zayıf")
    
    # ---------- SKOR HESAPLAMA ----------
    dip_total = dip_score - penalty
    squeeze_total = squeeze_score - penalty
    
    # DİP DÖNÜŞÜ SİNYALİ
    if dip_total >= 10:
        return {
            "type": "DİP DÖNÜŞÜ",
            "score": dip_total,
            "price": close_price,
            "rsi": rsi,
            "volume": volume,
            "median_vol": median_vol,
            "reasons": dip_reasons + penalty_reasons,
            "ls_extra": ""
        }
    
    # SHORT SQUEEZE SİNYALİ
    if squeeze_total >= 10:
        return {
            "type": "SHORT SQUEEZE",
            "score": squeeze_total,
            "price": close_price,
            "rsi": rsi,
            "volume": volume,
            "median_vol": median_vol,
            "reasons": squeeze_reasons + penalty_reasons,
            "ls_extra": ""
        }
    
    return None

# =========================================================
# SOKET VERİ İŞLEME (Merkezi)
# =========================================================
async def handle_socket_message(msg, session):
    """Tek bir merkezden tüm coinlerin WebSocket verilerini işler"""
    if "data" not in msg:
        return
    
    kline = msg["data"]["k"]
    symbol = msg["data"]["s"].lower()
    
    if symbol not in DATA_STORE:
        return
    
    close_price = float(kline["c"])
    volume = float(kline["v"])
    is_candle_closed = kline["x"]
    
    # Mum kapandıysa hafızayı güncelle
    if is_candle_closed:
        DATA_STORE[symbol]["prices"].append(close_price)
        DATA_STORE[symbol]["volumes"].append(volume)
        
        # 100 mumu aşma (Memory Leak önleme)
        if len(DATA_STORE[symbol]["prices"]) > 100:
            DATA_STORE[symbol]["prices"].pop(0)
            DATA_STORE[symbol]["volumes"].pop(0)
        
        # SİNYAL KONTROLÜ
        signal = check_signals(symbol, DATA_STORE[symbol], close_price, volume, kline)
        
        if signal:
            # L/S oranını her 5 dakikada bir çek
            ls_data = None
            if time.time() - DATA_STORE[symbol]["last_ls"] > 300:
                ls_data = await fetch_ls_ratio(session, symbol)
                DATA_STORE[symbol]["last_ls"] = time.time()
            
            ls_extra = ""
            if ls_data:
                if ls_data["change"] < LS_DROP_THRESHOLD:
                    ls_extra = "<b>✅ L/S onayı:</b> Short'lar azalıyor (Squeeze başladı!)"
                elif ls_data["ratio"] > LS_HIGH_THRESHOLD and ls_data["change"] < 0:
                    ls_extra = "<b>✅ L/S onayı:</b> Long'lar azalıyor (Sıkışma öncesi)"
            
            signal["ls_extra"] = ls_extra
            
            # Telegram mesajı oluştur
            emoji = "🟢" if signal["type"] == "DİP DÖNÜŞÜ" else "🚀"
            message = f"""{emoji} <b>{signal["type"]}</b>
            Sembol: {symbol.upper()}
            Fiyat: {signal["price"]:.6f}
            Skor: {signal["score"]}
            RSI: {signal["rsi"]:.1f}
            Hacim: {signal["volume"]:,.0f} (Ortalama {signal["median_vol"]:,.0f})
            {signal["ls_extra"]}
            
            💡 <b>Anlamı:</b> {' , '.join(signal['reasons'])}"""
            
            await send_telegram(message)

# =========================================================
# YARDIMCI FONKSİYONLAR
# =========================================================
async def get_all_spot_symbols(client):
    """Dinamik olarak tüm USDT çiftlerini çeker"""
    data = await client.get_exchange_info()
    symbols = []
    for s in data.get("symbols", []):
        sym = s["symbol"]
        if sym.endswith("USDT") and sym not in STABLECOIN_BLACKLIST and sym not in MAJOR_COINS_BLACKLIST:
            symbols.append(sym.lower())
    return sorted(symbols)

# =========================================================
# ANA DÖNGÜ (Auto-Reconnect + Session Kapatma)
# =========================================================
async def run_bot():
    """Botun ana döngüsü - Bağlantı koparsa yeniden başlar"""
    client = None
    try:
        print("🚀 Nihai Bot (Auto-Reconnect) Başlatılıyor...")
        await send_telegram("🤖 <b>Dip Hunter Bot Yeniden Başlıyor...</b>")
        
        client = await AsyncClient.create()
        
        # 1. Adım: Tüm coinleri çek
        all_coins = await get_all_spot_symbols(client)
        print(f"📊 Toplam {len(all_coins)} coin taranıyor.")
        
        # 2. Adım: Hafızayı doldur (Eğer boşsa)
        if len(DATA_STORE) == 0:
            await preload_historical_data(client, all_coins)
        
        # 3. Adım: Tek bir Multiplex Soket oluştur
        bm = BinanceSocketManager(client)
        streams = [f"{sym}@kline_1m" for sym in all_coins]
        
        print(f"📡 {len(streams)} stream tek bir bağlantı üzerinden dinleniyor...")
        
        # Her döngüde yeni bir session oluştur, döngü sonunda kapat
        async with aiohttp.ClientSession() as session:
            async with bm.multiplex_socket(streams) as stream:
                print("✅ WebSocket bağlantısı kuruldu!")
                await send_telegram("✅ WebSocket bağlantısı kuruldu! Sinyaller gelmeye başlayacak.")
                
                while True:
                    try:
                        res = await stream.recv()
                        await handle_socket_message(res, session)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        print(f"Soket okuma hatası: {e}")
                        # Hata alındı, döngü kırılacak ve yeniden bağlanacak
                        break
        
    except Exception as e:
        print(f"Bot döngüsü hatası: {e}")
        await send_telegram(f"❌ Bağlantı koptu, 10 saniye sonra yeniden başlatılıyor... (Hata: {str(e)[:100]})")
    finally:
        # Client'i kapat
        if client:
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
            print(f"Ana hata: {e}")
            await send_telegram("❌ Kritik hata, 30 saniye sonra yeniden başlatılıyor...")
            await asyncio.sleep(30)
        finally:
            # Her döngüde yeni bir başlangıç için bekleyelim
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
