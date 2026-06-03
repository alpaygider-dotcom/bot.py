import asyncio
import os
import aiohttp
from binance import AsyncClient, BinanceSocketManager

# =========================================================
# AYARLAR
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ BOT_TOKEN veya CHAT_ID eksik! Railway Environment Variables'a ekle.")
    exit(1)

SYMBOLS = ["portalusdt", "gunusdt", "tonusdt", "ethfiusdt"]  # İstediğin coinler

# Sinyal eşikleri
MIN_VOL_SPIKE_DIP = 2.5
MIN_VOL_SPIKE_SQUEEZE = 4.0
MIN_RSI = 30
MIN_PRICE_JUMP = 3.0

# L/S oranı eşikleri
LS_DROP_THRESHOLD = -2.0   # %2'den fazla düşüş
LS_HIGH_THRESHOLD = 2.0    # Oran 2.0'nin üstü

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
# RSI HESAPLAMA
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
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

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
# ANA İŞLEM
# =========================================================
async def process_symbol(client, session, symbol):
    bm = BinanceSocketManager(client)
    kline_stream = bm.kline_socket(symbol=symbol, interval="1m")
    
    price_history = []
    volume_history = []
    last_ls_check = 0
    
    print(f"✅ {symbol.upper()} dinleniyor...")
    
    async with kline_stream as stream:
        while True:
            try:
                kline = await stream.recv()
                if not kline:
                    continue
                
                close_price = float(kline["k"]["c"])
                volume = float(kline["k"]["v"])
                is_candle_closed = kline["k"]["x"]
                
                if is_candle_closed:
                    price_history.append(close_price)
                    volume_history.append(volume)
                    
                    if len(price_history) > 100:
                        price_history.pop(0)
                        volume_history.pop(0)
                    
                    # Sinyal kontrolü için yeterli veri var mı?
                    if len(volume_history) >= 20:
                        avg_vol = sum(volume_history[-20:-1]) / 19
                        rsi = calculate_rsi(price_history)
                        recent_low = min(price_history[-5:])
                        price_change_pct = ((close_price - price_history[-6]) / price_history[-6]) * 100 if len(price_history) >= 6 else 0
                        
                        # L/S oranını her 5 dakikada bir çek
                        ls_data = None
                        if time.time() - last_ls_check > 300:
                            ls_data = await fetch_ls_ratio(session, symbol)
                            last_ls_check = time.time()
                        
                        # 1. DİP DÖNÜŞÜ (L/S onayı ile)
                        if rsi < MIN_RSI and close_price > recent_low * 1.001 and volume > avg_vol * MIN_VOL_SPIKE_DIP and kline["k"]["c"] > kline["k"]["o"]:
                            ls_extra = ""
                            if ls_data and ls_data["ratio"] > LS_HIGH_THRESHOLD and ls_data["change"] < 0:
                                ls_extra = "<b>✅ L/S onayı:</b> Long'lar azalıyor (Sıkışma öncesi)"
                            message = f"""<b>🟢 DİP DÖNÜŞÜ</b>
                            Sembol: {symbol.upper()}
                            Fiyat: {close_price:.6f}
                            RSI: {rsi:.2f}
                            Hacim: {volume:,.0f} (Ortalama {avg_vol:,.0f})
                            {ls_extra}
                            
                            💡 <b>Anlamı:</b> Coin aşırı satıldı. Anormal hacimle alıcılar geliyor. Kısa vadeli toparlanma beklenebilir."""
                            await send_telegram(message)
                        
                        # 2. SHORT SQUEEZE (L/S onayı ile)
                        if price_change_pct > MIN_PRICE_JUMP and volume > avg_vol * MIN_VOL_SPIKE_SQUEEZE:
                            ls_extra = ""
                            if ls_data and ls_data["change"] < LS_DROP_THRESHOLD:
                                ls_extra = "<b>✅ L/S onayı:</b> Short'lar azalıyor (Squeeze başladı!)"
                            message = f"""<b>🚀 SHORT SQUEEZE</b>
                            Sembol: {symbol.upper()}
                            Fiyat: {close_price:.6f}
                            Son % Değişim: {price_change_pct:.1f}%
                            Hacim: {volume:,.0f} (Ortalama {avg_vol:,.0f})
                            {ls_extra}
                            
                            💡 <b>Anlamı:</b> Fiyat anormal yüksek hacimle fırladı. Short'lar panik yapıyor."""
                            await send_telegram(message)
                            
            except Exception as e:
                print(f"Bağlantı hatası ({symbol}): {e}")
                await asyncio.sleep(5)

# =========================================================
# MAIN
# =========================================================
async def main():
    print("🚀 Railway Bot L/S entegre başlatılıyor...")
    await send_telegram("🤖 <b>Dip Hunter Bot (L/S Entegre) Aktif!</b>")
    
    client = await AsyncClient.create()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for sym in SYMBOLS:
            tasks.append(asyncio.create_task(process_symbol(client, session, sym)))
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"Ana hata: {e}")
            await send_telegram("❌ Bot kritik hata verdi. Yeniden başlıyor...")
            await asyncio.sleep(10)
            await main()
        finally:
            await client.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
