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
CHAT_ID   = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ BOT_TOKEN veya CHAT_ID eksik!")
    exit(1)

STABLECOIN_BLACKLIST = {
    "USDCUSDT","BUSDUSDT","TUSDUSDT","DAIUSDT",
    "USDTUSDT","FDUSDUSDT","USDPUSDT","EURUSDT"
}
MAJOR_BLACKLIST = {"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT"}

# Sinyal parametreleri
VOL_SPIKE_DIP      = 3.0   # Dip sinyali için hacim çarpanı
VOL_SPIKE_SQUEEZE  = 4.5   # Squeeze için hacim çarpanı
RSI_DIP_MAX        = 35    # Dip RSI eşiği
RSI_SQUEEZE_MIN    = 45    # Squeeze RSI eşiği (aşırı satışta squeeze olmaz)
PRICE_JUMP_MIN     = 2.5   # Squeeze için min fiyat artışı %
DIP_LOOKBACK       = 50    # Kaç mumda dip ara
SIGNAL_COOLDOWN    = 1800  # Aynı coin için sinyal arası min saniye (30 dk)
MIN_CANDLES        = 60    # Analiz için minimum mum sayısı

DATA_STORE = {}

# =========================================================
# TELEGRAM
# =========================================================
async def send_telegram(text: str):
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10)
            )
    except Exception as e:
        print(f"Telegram hatası: {e}")

# =========================================================
# RSI (Wilder)
# =========================================================
def calculate_rsi(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(max(d, 0.0))
        losses.append(abs(min(d, 0.0)))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    return 100.0 if al == 0 else 100.0 - (100.0 / (1 + ag / al))

# =========================================================
# EMA
# =========================================================
def calculate_ema(prices: list, period: int) -> float:
    if len(prices) < period:
        return prices[-1]
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema

# =========================================================
# DİP TESPİTİ - Gerçek dip: son N mumun en düşüğüne yakın mı?
# =========================================================
def is_near_bottom(prices: list, lookback: int = 50, threshold_pct: float = 3.0) -> bool:
    """Fiyat, son lookback mumun en düşüğünden threshold_pct% içinde mi?"""
    if len(prices) < lookback:
        return False
    recent_low  = min(prices[-lookback:])
    current     = prices[-1]
    distance_pct = ((current - recent_low) / recent_low) * 100
    return distance_pct <= threshold_pct

# =========================================================
# TREND TESPİTİ
# =========================================================
def get_trend(prices: list) -> str:
    """EMA9 > EMA21 → yükseliş, tersi düşüş"""
    if len(prices) < 21:
        return "belirsiz"
    ema9  = calculate_ema(prices, 9)
    ema21 = calculate_ema(prices, 21)
    if ema9 > ema21 * 1.002:
        return "yukselis"
    if ema9 < ema21 * 0.998:
        return "dusus"
    return "yatay"

# =========================================================
# BINANCE TR — TÜM SPOT USDT ÇİFTLERİ
# =========================================================
async def get_all_spot_symbols(client: AsyncClient) -> list:
    data = await client.get_exchange_info()
    symbols = []
    for s in data.get("symbols", []):
        sym = s["symbol"]
        if (sym.endswith("USDT")
                and s.get("status") == "TRADING"
                and sym not in STABLECOIN_BLACKLIST
                and sym not in MAJOR_BLACKLIST):
            symbols.append(sym.lower())
    return sorted(symbols)

# =========================================================
# GEÇMİŞ VERİ ÖN YÜKLEME
# =========================================================
async def preload_historical_data(client: AsyncClient, symbols: list):
    print("⏳ Geçmiş veriler yükleniyor...")
    sem = asyncio.Semaphore(10)

    async def fetch(sym: str):
        async with sem:
            try:
                klines = await client.get_klines(
                    symbol=sym.upper(), interval="1m", limit=120
                )
                DATA_STORE[sym] = {
                    "prices":      [float(k[4]) for k in klines[:-1]],
                    "volumes":     [float(k[5]) for k in klines[:-1]],
                    "taker_buy":   [float(k[9]) for k in klines[:-1]],  # ✅ Gerçek taker buy
                    "last_signal": 0,
                    "last_ls":     0,
                    "ls_data":     None,
                }
            except Exception as e:
                print(f"Veri yükleme hatası {sym}: {e}")

    await asyncio.gather(*[fetch(s) for s in symbols])
    print(f"✅ {len(DATA_STORE)} coin için veri yüklendi.")

# =========================================================
# L/S ORANI — Düzeltilmiş Mantık
# =========================================================
async def fetch_ls_ratio(session: aiohttp.ClientSession, symbol: str) -> dict | None:
    """
    Long/Short oranı:
    - ratio > 1  → piyasada daha çok long pozisyon var
    - change < 0 → long oranı azalıyor (uzun'lar kapanıyor)
    - Squeeze için: ratio < 1 (short ağırlıklı) VE change artıyor → short'lar sıkışıyor
    """
    try:
        url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
        params = {"symbol": symbol.upper(), "period": "5m", "limit": 3}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if len(data) < 2:
                return None
            prev  = float(data[-2]["longShortRatio"])
            curr  = float(data[-1]["longShortRatio"])
            long_pct  = float(data[-1]["longAccount"])   # 0-1 arası
            short_pct = float(data[-1]["shortAccount"])
            change_pct = ((curr - prev) / prev) * 100
            return {
                "ratio":     curr,
                "long_pct":  long_pct,
                "short_pct": short_pct,
                "change":    change_pct,
            }
    except:
        return None

def interpret_ls(ls: dict | None, signal_type: str) -> str:
    """L/S verisini yorumla, onay veya uyarı mesajı döndür."""
    if ls is None:
        return ""
    ratio   = ls["ratio"]
    change  = ls["change"]
    s_pct   = ls["short_pct"] * 100  # yüzde
    l_pct   = ls["long_pct"] * 100

    if signal_type == "DİP":
        # Dip sinyali: long'lar çok azalmışsa (panik satış) ve dönmeye başlıyorsa güçlü
        if s_pct > 55 and change > 1.0:
            return f"🟢 Short ağırlıklı (%{s_pct:.0f}) ve short'lar artıyor → squeeze riski yüksek"
        if l_pct > 60 and change < -2.0:
            return f"🔴 Long'lar hızla kapanıyor (%{l_pct:.0f}) → dip henüz gelmemiş olabilir"
        if s_pct > 50:
            return f"🟡 Short ağırlıklı piyasa (%{s_pct:.0f}) → dip dönüşüne destek"
        return ""

    elif signal_type == "SQUEEZE":
        # Squeeze: short oranı yüksek VE azalıyorsa = short'lar kapanıyor = gerçek squeeze
        if s_pct > 55 and change < -1.5:
            return f"🟢 Short'lar kapanıyor (%{s_pct:.0f} → azalıyor) → GÜÇLÜ SQUEEZE SİNYALİ"
        if s_pct > 50:
            return f"🟡 Short ağırlıklı (%{s_pct:.0f}) → squeeze devam edebilir"
        if l_pct > 65:
            return f"🔴 Long ağırlıklı (%{l_pct:.0f}) → gerçek squeeze değil olabilir"
        return ""

    return ""

# =========================================================
# SİNYAL KONTROLÜ — Yeniden Yazıldı
# =========================================================
def check_signals(symbol: str, data: dict, close: float, volume: float, taker_buy: float):
    prices    = data["prices"]
    volumes   = data["volumes"]
    taker_buys = data["taker_buy"]

    if len(prices) < MIN_CANDLES:
        return None

    # --- Temel metrikler ---
    vol_base    = median(volumes[-50:])          # 50 mum medyanı, daha güvenilir baz
    rsi         = calculate_rsi(prices)
    trend       = get_trend(prices)
    ema21       = calculate_ema(prices, 21)
    ema50       = calculate_ema(prices, 50)

    # Taker buy oranı: hacmin ne kadarı alış emirleriyle gerçekleşti?
    taker_ratio = taker_buy / volume if volume > 0 else 0.5  # ✅ Artık doğru alan

    # Fiyat değişimleri
    price_5m    = ((close - prices[-5])  / prices[-5])  * 100 if len(prices) >= 5  else 0
    price_15m   = ((close - prices[-15]) / prices[-15]) * 100 if len(prices) >= 15 else 0

    # Hacim çarpanı
    vol_mult = volume / vol_base if vol_base > 0 else 1

    signals = []

    # ==========================================================
    # SİNYAL 1: DİP + HACİM
    # Mantık: Fiyat gerçek dip bölgesinde, RSI aşırı satış,
    #         hacim patlaması var, alıcılar devreye girmiş
    # ==========================================================
    if (
        rsi < RSI_DIP_MAX                          # RSI aşırı satış bölgesinde
        and is_near_bottom(prices, DIP_LOOKBACK, 4.0)  # Son 50 mumun dibine yakın
        and vol_mult >= VOL_SPIKE_DIP              # Hacim patlaması
        and taker_ratio >= 0.52                    # Alıcılar baskın (taker buy > %52)
        and close > prices[-3]                     # Son 3 mumda fiyat toparlanıyor
        and price_5m > -1.0                        # Serbest düşüş değil
    ):
        score = 0
        reasons = []

        # RSI skoru
        if rsi < 25:
            score += 4; reasons.append(f"RSI {rsi:.1f} (çok aşırı satış)")
        elif rsi < 30:
            score += 3; reasons.append(f"RSI {rsi:.1f}")
        else:
            score += 1; reasons.append(f"RSI {rsi:.1f}")

        # Hacim skoru
        if vol_mult >= 6:
            score += 4; reasons.append(f"Hacim x{vol_mult:.1f} 🔥")
        elif vol_mult >= 4:
            score += 3; reasons.append(f"Hacim x{vol_mult:.1f}")
        else:
            score += 2; reasons.append(f"Hacim x{vol_mult:.1f}")

        # Taker buy
        if taker_ratio >= 0.65:
            score += 3; reasons.append(f"Alış baskısı %{taker_ratio*100:.0f}")
        elif taker_ratio >= 0.55:
            score += 2; reasons.append(f"Alış baskısı %{taker_ratio*100:.0f}")

        # Trend: yatay veya dönüş
        if trend == "yatay":
            score += 1; reasons.append("Yatay trend (dip oluşumu)")
        elif trend == "yukselis":
            score += 2; reasons.append("Trend dönüşü başlıyor")

        # EMA50 üstünde mi? (daha güçlü)
        if close > ema50:
            score += 1; reasons.append("EMA50 üstü")

        if score >= 8:
            signals.append({
                "type": "DİP + HACİM",
                "score": score,
                "rsi": rsi,
                "vol_mult": vol_mult,
                "taker_ratio": taker_ratio,
                "price": close,
                "price_5m": price_5m,
                "reasons": reasons,
            })

    # ==========================================================
    # SİNYAL 2: SHORT SQUEEZE
    # Mantık: Fiyat hızla yukarı kırıyor, devasa hacim,
    #         alıcılar ezici çoğunlukta, RSI henüz aşırı alış değil
    # ==========================================================
    if (
        price_5m  >= PRICE_JUMP_MIN                # 5 dakikada %2.5+ artış
        and vol_mult >= VOL_SPIKE_SQUEEZE          # Çok güçlü hacim
        and taker_ratio >= 0.60                    # Alıcılar ezici çoğunlukta
        and rsi >= RSI_SQUEEZE_MIN                 # RSI çok düşük değil (aşırı satışta squeeze olmaz)
        and rsi < 75                               # RSI henüz aşırı alışta değil
        and close > ema21                          # EMA21 üstünde
    ):
        score = 0
        reasons = []

        if price_5m >= 5:
            score += 4; reasons.append(f"%{price_5m:.1f} 5dk artış 🚀")
        elif price_5m >= 3.5:
            score += 3; reasons.append(f"%{price_5m:.1f} 5dk artış")
        else:
            score += 2; reasons.append(f"%{price_5m:.1f} 5dk artış")

        if vol_mult >= 8:
            score += 5; reasons.append(f"Hacim x{vol_mult:.1f} 💥")
        elif vol_mult >= 6:
            score += 4; reasons.append(f"Hacim x{vol_mult:.1f}")
        else:
            score += 3; reasons.append(f"Hacim x{vol_mult:.1f}")

        if taker_ratio >= 0.75:
            score += 3; reasons.append(f"Alış baskısı %{taker_ratio*100:.0f} 🔥")
        elif taker_ratio >= 0.65:
            score += 2; reasons.append(f"Alış baskısı %{taker_ratio*100:.0f}")

        if price_15m >= 4:
            score += 2; reasons.append(f"%{price_15m:.1f} 15dk ivme")

        if score >= 9:
            signals.append({
                "type": "SHORT SQUEEZE",
                "score": score,
                "rsi": rsi,
                "vol_mult": vol_mult,
                "taker_ratio": taker_ratio,
                "price": close,
                "price_5m": price_5m,
                "reasons": reasons,
            })

    return signals if signals else None

# =========================================================
# SOKET VERİ İŞLEME
# =========================================================
async def handle_socket_message(msg: dict, session: aiohttp.ClientSession):
    if "data" not in msg:
        return
    kline  = msg["data"]["k"]
    symbol = msg["data"]["s"].lower()

    if symbol not in DATA_STORE or not kline["x"]:  # Sadece kapanan mumlar
        return

    close      = float(kline["c"])
    volume     = float(kline["v"])
    taker_buy  = float(kline["V"])  # ✅ Gerçek taker buy volume (kline["t"] değil!)

    # Veriyi güncelle
    d = DATA_STORE[symbol]
    d["prices"].append(close)
    d["volumes"].append(volume)
    d["taker_buy"].append(taker_buy)
    for key in ("prices", "volumes", "taker_buy"):
        if len(d[key]) > 150:
            d[key].pop(0)

    # Cooldown kontrolü
    if time.time() - d["last_signal"] < SIGNAL_COOLDOWN:
        return

    signals = check_signals(symbol, d, close, volume, taker_buy)
    if not signals:
        return

    # L/S oranı (10 dk cache)
    if time.time() - d["last_ls"] > 600:
        d["ls_data"] = await fetch_ls_ratio(session, symbol)
        d["last_ls"] = time.time()

    for sig in signals:
        ls_comment = interpret_ls(d["ls_data"], 
                                  "DİP" if "DİP" in sig["type"] else "SQUEEZE")

        text = (
            f"<b>{'🟢' if 'DİP' in sig['type'] else '🚀'} {sig['type']}</b>\n"
            f"<b>{symbol.upper()}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 Fiyat: <b>{sig['price']:.6f}</b>\n"
            f"📊 Skor: {sig['score']}\n"
            f"📈 RSI: {sig['rsi']:.1f}\n"
            f"📦 Hacim: x{sig['vol_mult']:.1f} (medyandan)\n"
            f"🛒 Alış Baskısı: %{sig['taker_ratio']*100:.0f}\n"
            f"⚡ 5dk Değişim: %{sig['price_5m']:+.1f}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"{'  '.join(sig['reasons'])}\n"
        )
        if ls_comment:
            text += f"\n{ls_comment}\n"

        text += f"\n⏰ {time.strftime('%H:%M:%S')}"

        await send_telegram(text)
        d["last_signal"] = time.time()
        print(f"✅ Sinyal gönderildi: {symbol.upper()} - {sig['type']}")

# =========================================================
# ANA BOT DÖNGÜSÜ
# =========================================================
async def run_bot():
    print("🚀 Bot başlatılıyor...")
    await send_telegram("🤖 Bot başlatılıyor, hazırlanıyor...")

    client = await AsyncClient.create()
    try:
        all_coins = await get_all_spot_symbols(client)
        print(f"📊 {len(all_coins)} coin bulundu.")
        await send_telegram(f"📊 <b>{len(all_coins)}</b> coin taranıyor.")

        await preload_historical_data(client, all_coins)

        bm      = BinanceSocketManager(client)
        streams = [f"{sym}@kline_1m" for sym in all_coins]
        print(f"📡 {len(streams)} stream bağlanıyor...")

        async with aiohttp.ClientSession() as session:
            async with bm.multiplex_socket(streams) as stream:
                await send_telegram("✅ WebSocket bağlantısı kuruldu, sinyaller bekleniyor...")
                print("✅ WebSocket hazır.")
                while True:
                    try:
                        res = await asyncio.wait_for(stream.recv(), timeout=30)
                        await handle_socket_message(res, session)
                    except asyncio.TimeoutError:
                        print("⏱ Timeout, bekleniyor...")
                    except Exception as e:
                        print(f"Soket hatası: {e}")
                        break
    except Exception as e:
        print(f"Bot hatası: {e}")
        await send_telegram(f"❌ Hata: {str(e)[:200]}")
    finally:
        await client.close_connection()

async def main():
    while True:
        try:
            await run_bot()
        except Exception as e:
            print(f"Kritik hata: {e}")
        print("♻️ 60 saniye sonra yeniden başlatılıyor...")
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
