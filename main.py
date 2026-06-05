"""
╔══════════════════════════════════════════════════════════════╗
║   BİNANCE TR — AKILLI KRİPTO SİNYAL BOTU v3.0             ║
║   3 Katmanlı Filtre | Yükselişten ÖNCE coin bul            ║
╚══════════════════════════════════════════════════════════════╝

MİMARİ:
  Her 5 dk  → Tüm coinler 15dk REST verisiyle taranır
  En iyi 40 → 1dk WebSocket ile gerçek zamanlı izlenir
  3 katman onayı geçerse → Telegram sinyali gönderilir

KATMANLAR:
  K1 (15dk): RSI < 38 + OBV yükseliyor + BB alt %30 bölgesi
  K2 (1dk):  Hacim x3+ + Taker buy %60+ + VWAP filtresi
  K3:        Pump koruması + Spam filtresi + L/S yorumu
"""

import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
import aiohttp
from binance import AsyncClient, BinanceSocketManager
from statistics import median, stdev

# ══════════════════════════════════════════════════════════════
# ORTAM DEĞİŞKENLERİ
# ══════════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ BOT_TOKEN veya CHAT_ID eksik!")
    exit(1)

def tr_saat() -> str:
    """Türkiye saati (UTC+3)"""
    return datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M:%S")

# ══════════════════════════════════════════════════════════════
# KARA LİSTELER
# ══════════════════════════════════════════════════════════════

# Stablecoin ve sabit değerli tokenlar
STABLECOIN_LISTESI = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT", "USDTUSDT",
    "FDUSDUSDT", "USDPUSDT", "EURUSDT", "USDPAXUSDT", "SUSDUSDT",
    "USTUSDT", "FRAXUSDT", "LUSDUSDT", "AEURUSDT", "GBPUSDT",
}

# Ana coinler — farklı dinamikler, bu bot için uygun değil
MAJOR_LISTESI = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "BCHUSDT", "LTCUSDT", "LINKUSDT", "DOTUSDT", "ADAUSDT",
    "AVAXUSDT", "MATICUSDT", "TRXUSDT", "ATOMUSDT", "NEARUSDT",
}

# Ölü / güvenilmez / manipülasyona açık coinler
OLU_LISTESI = {
    "LUNCUSDT", "USTCUSDT", "MOONUSDT", "SAFEMOONUSDT",
    "SHIBUSDT",  # Çok ince piyasa, tek işlem hacmi patlatıyor
    "PEPEUSDT",  # Meme coin, manipülasyon riski
    "1MBABYDOGEUSDT", "BABYDOGEUSDT",
}

# ══════════════════════════════════════════════════════════════
# PARAMETRELER — Dengeli ayar
# ══════════════════════════════════════════════════════════════

# ── Katman 1 (15dk) ─────────────────────────────────────────
K1_RSI_MIN      = 15    # Altı = serbest düşüş, atla
K1_RSI_MAX      = 38    # Üstü = gerçek dip değil (39-40 filtresi)
K1_OBV_PERIYOT  = 10    # OBV trend hesabı için kaç mum
K1_BB_PERIYOT   = 20    # Bollinger bant periyodu
K1_BB_STD       = 2.0   # Bollinger standart sapma çarpanı
K1_BB_SIKISMA   = 0.06  # Bant dar ise volatilite bekleniyor
K1_BB_MAX       = 0.30  # BB pozisyonu bu üstü = dip değil, HARD REDDET
K1_MIN_SKOR     = 6     # K1 geçmek için minimum skor

# ── Katman 2 (1dk) ──────────────────────────────────────────
K2_HACIM_MIN    = 3.0   # Medyan hacmin minimum kaç katı
K2_HACIM_MAX    = 50.0  # Üstü = ince piyasa gürültüsü
K2_TAKER_MIN    = 0.60  # Minimum taker buy oranı (%60)
K2_TAKER_MAX    = 0.92  # Üstü = tek işlem spike'ı, güvenilmez
K2_USD_MIN      = 5000  # Minimum USD hacim (o mumda)
K2_5DK_MIN      = 0.05  # 5dk minimum fiyat hareketi %
K2_VWAP_MIN     = -0.80 # VWAP'ın bu kadar altında olanı reddet
K2_MIN_SKOR     = 15    # Toplam minimum skor (K1 + K2)

# ── Koruma ──────────────────────────────────────────────────
PUMP_SAAT       = 6     # Pump koruması: son kaç saat
PUMP_PCT        = 15.0  # Bu kadar çıkmışsa sinyal verme

# ── Sistem ──────────────────────────────────────────────────
SINYAL_BEKLEME  = 1800  # Aynı coin için min sinyal arası (30dk)
MIN_15DK_MUM    = 50    # Analiz için min 15dk mum sayısı
MIN_1DK_MUM     = 30    # Analiz için min 1dk mum sayısı
TARAMA_SURE     = 300   # 15dk tarama aralığı (5dk)
MAX_IZLEME      = 40    # Aynı anda max izlenen coin

# ── Spam Koruması ───────────────────────────────────────────
SPAM_PENCERE    = 300   # 5 dakika
SPAM_ESIK       = 5     # 5dk içinde max sinyal sayısı

# ══════════════════════════════════════════════════════════════
# GLOBAL DEPOLAR
# ══════════════════════════════════════════════════════════════
STORE_15DK    = {}      # 15dk mum verileri
STORE_1DK     = {}      # 1dk mum verileri (sadece izlenenler)
IZLENEN       = set()   # Şu an izlenen coinler
SINYAL_ZAMANI = {}      # Son sinyal zamanları {sembol: timestamp}
SON_SINYALLER = []      # Spam koruması için [(timestamp, sembol)]
WS_YENILE     = False   # WebSocket yenileme bayrağı

# ══════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════
async def telegram(mesaj: str):
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": mesaj, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10),
            )
    except Exception as e:
        print(f"[Telegram Hata] {e}")

# ══════════════════════════════════════════════════════════════
# TEKNİK ANALİZ
# ══════════════════════════════════════════════════════════════

def rsi(fiyatlar: list, n: int = 14) -> float:
    """Wilder RSI hesapla"""
    if len(fiyatlar) < n + 1:
        return 50.0
    g, k = [], []
    for i in range(1, len(fiyatlar)):
        d = fiyatlar[i] - fiyatlar[i-1]
        g.append(max(d, 0.0))
        k.append(abs(min(d, 0.0)))
    ag = sum(g[:n]) / n
    ak = sum(k[:n]) / n
    for i in range(n, len(g)):
        ag = (ag * (n-1) + g[i]) / n
        ak = (ak * (n-1) + k[i]) / n
    return round(100.0 - (100.0 / (1 + ag / ak)), 2) if ak > 0 else 100.0


def ema(fiyatlar: list, n: int) -> float:
    """EMA hesapla"""
    if len(fiyatlar) < n:
        return fiyatlar[-1] if fiyatlar else 0.0
    k = 2 / (n + 1)
    e = sum(fiyatlar[:n]) / n
    for f in fiyatlar[n:]:
        e = f * k + e * (1 - k)
    return e


def bollinger(fiyatlar: list, n: int = 20, std_k: float = 2.0) -> dict:
    """
    Bollinger Bantları
    yuzde_b: 0 = alt bant, 1 = üst bant, 0.5 = orta
    genislik: bantların ne kadar açık olduğu
    """
    if len(fiyatlar) < n:
        f = fiyatlar[-1] if fiyatlar else 0
        return {"ust": f, "orta": f, "alt": f, "genislik": 0.0, "yuzde_b": 0.5}
    son  = fiyatlar[-n:]
    orta = sum(son) / n
    std  = stdev(son)
    ust  = orta + std_k * std
    alt  = orta - std_k * std
    gen  = (ust - alt) / orta if orta > 0 else 0
    yb   = (fiyatlar[-1] - alt) / (ust - alt) if (ust - alt) > 0 else 0.5
    return {"ust": ust, "orta": orta, "alt": alt, "genislik": gen, "yuzde_b": yb}


def obv_analiz(fiyatlar: list, hacimler: list, n: int = 10) -> tuple:
    """
    OBV trendi + Bullish Divergence
    Divergence: Fiyat düşüyor ama OBV yükseliyor = gizli birikim
    Döner: (yukseliyor, divergence, aciklama)
    """
    if len(fiyatlar) < n + 1 or len(fiyatlar) != len(hacimler):
        return False, False, ""
    obv = [0.0]
    for i in range(1, len(fiyatlar)):
        if fiyatlar[i] > fiyatlar[i-1]:
            obv.append(obv[-1] + hacimler[i])
        elif fiyatlar[i] < fiyatlar[i-1]:
            obv.append(obv[-1] - hacimler[i])
        else:
            obv.append(obv[-1])
    obv_trend   = (obv[-1] - obv[-n]) / max(abs(obv[-n]), 1) * 100
    fiyat_trend = (fiyatlar[-1] - fiyatlar[-n]) / fiyatlar[-n] * 100
    yukseliyor  = obv_trend > 2.0
    divergence  = fiyat_trend < -1.0 and obv_trend > 1.0
    if divergence:
        return True, True,  f"🔍 Gizli Birikim (OBV {obv_trend:+.1f}%)"
    elif yukseliyor:
        return True, False, f"📈 OBV Yükseliyor ({obv_trend:+.1f}%)"
    else:
        return False, False, f"📉 OBV Düşüyor ({obv_trend:+.1f}%)"


def vwap(yuksekler, dusukler, kapanis, hacimler) -> float:
    """
    VWAP — kurumsal alıcıların referans fiyatı.
    Fiyat VWAP'ı kırarsa güçlü sinyal.
    """
    if not hacimler:
        return kapanis[-1] if kapanis else 0.0
    pv = sum(((y+d+k)/3) * h for y, d, k, h in zip(yuksekler, dusukler, kapanis, hacimler))
    v  = sum(hacimler)
    return pv / v if v > 0 else (kapanis[-1] if kapanis else 0.0)


def pump_var_mi(fiyatlar: list) -> tuple:
    """
    Son PUMP_SAAT saatte aşırı yükseliş var mı?
    15dk mumlar: 1 saat = 4 mum
    """
    n = PUMP_SAAT * 4
    if len(fiyatlar) < n:
        return False, 0.0
    en_dusuk = min(fiyatlar[-n:])
    pct      = ((fiyatlar[-1] - en_dusuk) / en_dusuk * 100) if en_dusuk > 0 else 0
    return pct >= PUMP_PCT, round(pct, 1)

# ══════════════════════════════════════════════════════════════
# LONG/SHORT ORANI
# ══════════════════════════════════════════════════════════════
async def ls_cek(session: aiohttp.ClientSession, sembol: str) -> dict | None:
    """
    Futures L/S oranı çek.
    Spot coinin futures'ı yoksa None döner — sinyal yine gelir.
    """
    try:
        url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
        p   = {"symbol": sembol.upper(), "period": "5m", "limit": 3}
        async with session.get(url, params=p, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return None
            d = await r.json()
            if len(d) < 2:
                return None
            once   = float(d[-2]["longShortRatio"])
            simdi  = float(d[-1]["longShortRatio"])
            l_pct  = float(d[-1]["longAccount"])
            s_pct  = float(d[-1]["shortAccount"])
            degisim = ((simdi - once) / once * 100) if once > 0 else 0
            return {"l": l_pct, "s": s_pct, "degisim": degisim}
    except:
        return None


def ls_yorum(ls: dict | None) -> str:
    """L/S yorumu — boş string döner yoksa"""
    if not ls:
        return ""
    s = ls["s"] * 100
    l = ls["l"] * 100
    d = ls["degisim"]
    if s > 55 and d < -1.5:
        return f"🟢 Short'lar kapanıyor! (%{s:.0f} short, azalıyor) → Squeeze yakın"
    if s > 55:
        return f"🟡 Short ağırlıklı (%{s:.0f}) → Dönüş ihtimali var"
    if l > 60 and d < -3.0:
        return f"🟢 Long panik satışı (%{l:.0f}) → Dip tükenme sinyali"
    if 45 < s < 55:
        return f"⚪ Dengeli L/S (%{l:.0f}L / %{s:.0f}S)"
    return ""

# ══════════════════════════════════════════════════════════════
# SEMBOL LİSTESİ
# ══════════════════════════════════════════════════════════════
async def sembolleri_cek(client: AsyncClient) -> list:
    """Binance TR'deki aktif USDT çiftlerini çek, kara listeleri uygula"""
    try:
        veri = await client.get_exchange_info()
        return sorted([
            s["symbol"].lower()
            for s in veri.get("symbols", [])
            if s["symbol"].endswith("USDT")
            and s.get("status") == "TRADING"
            and s["symbol"] not in STABLECOIN_LISTESI
            and s["symbol"] not in MAJOR_LISTESI
            and s["symbol"] not in OLU_LISTESI
        ])
    except Exception as e:
        print(f"[Sembol Hata] {e}")
        return []

# ══════════════════════════════════════════════════════════════
# VERİ ÇEKME
# ══════════════════════════════════════════════════════════════
async def cek_15dk(client: AsyncClient, sembol: str) -> bool:
    try:
        klines = await client.get_klines(symbol=sembol.upper(), interval="15m", limit=100)
        if len(klines) < MIN_15DK_MUM:
            return False
        STORE_15DK[sembol] = {
            "f": [float(k[4]) for k in klines[:-1]],   # kapanis
            "y": [float(k[2]) for k in klines[:-1]],   # yuksek
            "d": [float(k[3]) for k in klines[:-1]],   # dusuk
            "h": [float(k[5]) for k in klines[:-1]],   # hacim
        }
        return True
    except:
        return False


async def cek_1dk(client: AsyncClient, sembol: str):
    try:
        klines = await client.get_klines(symbol=sembol.upper(), interval="1m", limit=80)
        STORE_1DK[sembol] = {
            "f":  [float(k[4]) for k in klines[:-1]],  # kapanis
            "y":  [float(k[2]) for k in klines[:-1]],  # yuksek
            "d":  [float(k[3]) for k in klines[:-1]],  # dusuk
            "h":  [float(k[5]) for k in klines[:-1]],  # hacim
            "tb": [float(k[9]) for k in klines[:-1]],  # taker buy volume ✅
        }
    except:
        pass

# ══════════════════════════════════════════════════════════════
# KATMAN 1 — 15 DAKİKALIK ANALİZ
# ══════════════════════════════════════════════════════════════
def katman1(sembol: str) -> dict | None:
    """
    15dk verileriyle ön tarama.
    Geçmesi için: RSI < 38, OBV yükseliyor, BB alt %30 bölgesi
    """
    if sembol not in STORE_15DK:
        return None
    d = STORE_15DK[sembol]
    f, h = d["f"], d["h"]

    if len(f) < MIN_15DK_MUM:
        return None

    # ── Pump koruması ────────────────────────────────────────
    pump, pump_pct = pump_var_mi(f)
    if pump:
        return None

    # ── Temel hesaplamalar ───────────────────────────────────
    r  = rsi(f)
    bb = bollinger(f, K1_BB_PERIYOT, K1_BB_STD)
    obv_yukseliyor, obv_div, obv_acik = obv_analiz(f, h, K1_OBV_PERIYOT)

    # ── HARD FİLTRELER (skor hesabı yok, direkt reddet) ─────

    # RSI aralık dışı
    if not (K1_RSI_MIN <= r <= K1_RSI_MAX):
        return None

    # BB pozisyonu çok yukarıda = dip değil
    # Divergence istisnası: fiyat düşerken OBV yükseliyorsa BB biraz yukarıda olabilir
    if bb["yuzde_b"] > K1_BB_MAX and not obv_div:
        return None

    # OBV şartı — yükselmeli ya da divergence olmalı
    if not obv_yukseliyor and not obv_div:
        return None

    # ── SKOR HESABI ─────────────────────────────────────────
    skor = 0
    sebepler = []

    # RSI skoru
    if   r < 25: skor += 4; sebepler.append(f"RSI {r:.1f} 🔴")
    elif r < 30: skor += 3; sebepler.append(f"RSI {r:.1f}")
    elif r < 35: skor += 2; sebepler.append(f"RSI {r:.1f}")
    else:        skor += 1; sebepler.append(f"RSI {r:.1f}")

    # OBV skoru
    if   obv_div:          skor += 4; sebepler.append(obv_acik)
    elif obv_yukseliyor:   skor += 2; sebepler.append(obv_acik)

    # Bollinger skoru
    if   bb["yuzde_b"] <= 0.05: skor += 4; sebepler.append("BB alt bandına değdi 🎯")
    elif bb["yuzde_b"] <= 0.15: skor += 3; sebepler.append("BB alt bandına yakın")
    elif bb["yuzde_b"] <= 0.25: skor += 2; sebepler.append("BB alt çeyreği")
    elif bb["yuzde_b"] <= 0.30: skor += 1; sebepler.append("BB alt bölgesi")

    # BB sıkışması — tek başına yetmez, destek puan
    if bb["genislik"] < K1_BB_SIKISMA:
        skor += 1; sebepler.append("BB Sıkışması ⚡")

    if skor < K1_MIN_SKOR:
        return None

    return {
        "skor":    skor,
        "rsi":     r,
        "bb":      bb,
        "obv_div": obv_div,
        "sebepler":sebepler,
        "ema21":   ema(f, 21),
        "ema50":   ema(f, 50) if len(f) >= 50 else None,
    }

# ══════════════════════════════════════════════════════════════
# KATMAN 2 — 1 DAKİKALIK ANALİZ
# ══════════════════════════════════════════════════════════════
def katman2(sembol: str, k1: dict) -> dict | None:
    """
    1dk verisiyle giriş zamanlaması.
    Geçmesi için: Hacim x3+, Taker %60+, VWAP filtresi, fiyat kıpırdıyor
    """
    if sembol not in STORE_1DK:
        return None
    d = STORE_1DK[sembol]
    f, y, dd, h, tb = d["f"], d["y"], d["d"], d["h"], d["tb"]

    if len(f) < MIN_1DK_MUM:
        return None

    son_f  = f[-1]
    son_h  = h[-1]
    son_tb = tb[-1]

    baz_h    = median(h[-50:]) if len(h) >= 50 else median(h)
    hacim_k  = son_h / baz_h if baz_h > 0 else 1.0
    taker    = son_tb / son_h if son_h > 0 else 0.5
    usd_hacim = son_h * son_f

    # VWAP — son 30 mum
    vw        = vwap(y[-30:], dd[-30:], f[-30:], h[-30:])
    vwap_fark = ((son_f - vw) / vw * 100) if vw > 0 else 0

    # Fiyat değişimleri
    d3  = ((son_f - f[-3])  / f[-3]  * 100) if len(f) >= 3  else 0
    d5  = ((son_f - f[-5])  / f[-5]  * 100) if len(f) >= 5  else 0
    d15 = ((son_f - f[-15]) / f[-15] * 100) if len(f) >= 15 else 0

    # ── HARD FİLTRELER ───────────────────────────────────────
    if hacim_k  < K2_HACIM_MIN:  return None  # Yeterli hacim yok
    if hacim_k  > K2_HACIM_MAX:  return None  # Gürültü spike
    if taker    < K2_TAKER_MIN:  return None  # Alıcı baskısı yok
    if taker    > K2_TAKER_MAX:  return None  # Tek işlem spike
    if usd_hacim < K2_USD_MIN:   return None  # USD hacim çok düşük
    if d5       < K2_5DK_MIN:    return None  # Fiyat kıpırdamıyor
    if d3       < -1.5:          return None  # Hâlâ düşüyor
    if vwap_fark < K2_VWAP_MIN:  return None  # VWAP'tan çok uzak

    # ── SKOR HESABI (K1 skorundan devam) ────────────────────
    skor     = k1["skor"]
    sebepler = list(k1["sebepler"])

    # Hacim skoru
    if   hacim_k >= 7: skor += 5; sebepler.append(f"Hacim x{hacim_k:.1f} 💥💥")
    elif hacim_k >= 5: skor += 4; sebepler.append(f"Hacim x{hacim_k:.1f} 💥")
    elif hacim_k >= 4: skor += 3; sebepler.append(f"Hacim x{hacim_k:.1f}")
    else:              skor += 2; sebepler.append(f"Hacim x{hacim_k:.1f}")

    # Taker buy skoru
    if   taker >= 0.75: skor += 4; sebepler.append(f"Alış %{taker*100:.0f} 🔥🔥")
    elif taker >= 0.68: skor += 3; sebepler.append(f"Alış %{taker*100:.0f} 🔥")
    elif taker >= 0.60: skor += 2; sebepler.append(f"Alış %{taker*100:.0f}")

    # VWAP skoru
    if   vwap_fark >= 0.5:  skor += 3; sebepler.append(f"VWAP kırıldı +%{vwap_fark:.2f} ✅")
    elif vwap_fark >= -0.2: skor += 2; sebepler.append(f"VWAP yakın %{vwap_fark:.2f}")
    elif vwap_fark >= -0.8: skor += 1

    # 5dk ivme skoru
    if   d5 >= 2.0: skor += 3; sebepler.append(f"5dk ivme %{d5:+.1f} ⚡")
    elif d5 >= 1.0: skor += 2; sebepler.append(f"5dk ivme %{d5:+.1f}")
    elif d5 >= 0.1: skor += 1

    # EMA pozisyonu
    if k1["ema50"] and son_f > k1["ema50"]:
        skor += 2; sebepler.append("EMA50 üstü 💪")
    elif son_f > k1["ema21"]:
        skor += 1; sebepler.append("EMA21 üstü")

    # OBV Divergence bonus
    if k1["obv_div"]:
        skor += 2

    if skor < K2_MIN_SKOR:
        return None

    # Güç seviyesi
    if   skor >= 22: guc = "⭐⭐⭐ ÇOK GÜÇLÜ"; emoji = "🔥🔥🔥"
    elif skor >= 18: guc = "⭐⭐ GÜÇLÜ";       emoji = "🔥🔥"
    else:            guc = "⭐ ORTA";           emoji = "🔥"

    return {
        "skor":     skor,
        "guc":      guc,
        "emoji":    emoji,
        "fiyat":    son_f,
        "vwap_f":   vwap_fark,
        "hacim_k":  hacim_k,
        "usd":      usd_hacim,
        "taker":    taker,
        "d5":       d5,
        "d15":      d15,
        "rsi":      k1["rsi"],
        "bb":       k1["bb"],
        "obv_div":  k1["obv_div"],
        "sebepler": sebepler,
    }

# ══════════════════════════════════════════════════════════════
# SİNYAL MESAJI
# ══════════════════════════════════════════════════════════════
async def sinyal_gonder(sembol: str, k2: dict, ls: dict | None):
    bb   = k2["bb"]
    ls_m = ls_yorum(ls)

    # BB durum metni
    if   bb["yuzde_b"] <= 0.05: bb_m = "Alt banda değdi 🎯"
    elif bb["yuzde_b"] <= 0.15: bb_m = f"Alt banda yakın (%{bb['yuzde_b']*100:.0f})"
    elif bb["yuzde_b"] <= 0.25: bb_m = f"Alt çeyrek (%{bb['yuzde_b']*100:.0f})"
    else:                        bb_m = f"Alt bölge (%{bb['yuzde_b']*100:.0f})"

    obv_m = "🔍 Gizli Birikim!" if k2["obv_div"] else "📈 OBV Yükseliyor"

    msg = (
        f"<b>🎯 BİRİKİM + KIRILIM</b>  {k2['emoji']}\n"
        f"<b>{sembol.upper()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Fiyat: <b>{k2['fiyat']:.6f}</b>\n"
        f"📊 Güç: <b>{k2['skor']}</b> puan — {k2['guc']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📉 15dk Analiz:</b>\n"
        f"  RSI: {k2['rsi']:.1f}\n"
        f"  Bollinger: {bb_m}\n"
        f"  OBV: {obv_m}\n"
        f"\n"
        f"<b>⚡ 1dk Analiz:</b>\n"
        f"  Hacim: x{k2['hacim_k']:.1f}  (${k2['usd']:,.0f})\n"
        f"  Alış Baskısı: %{k2['taker']*100:.0f}\n"
        f"  VWAP: {'+' if k2['vwap_f'] >= 0 else ''}{k2['vwap_f']:.2f}%\n"
        f"  5dk: {k2['d5']:+.1f}%  |  15dk: {k2['d15']:+.1f}%\n"
    )

    if ls_m:
        msg += f"\n<b>📊 L/S:</b> {ls_m}\n"

    msg += (
        f"\n<b>Sebepler:</b>\n"
        + "  |  ".join(k2["sebepler"][:5])
        + f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {tr_saat()} (TR)"
    )

    await telegram(msg)
    print(f"✅ SİNYAL → {sembol.upper()} | {k2['skor']} puan | {k2['guc']}")

# ══════════════════════════════════════════════════════════════
# TARAMA GÖREVİ — Her 5 dakikada (REST API)
# ══════════════════════════════════════════════════════════════
async def tarama(client: AsyncClient):
    global IZLENEN, WS_YENILE

    tum = await sembolleri_cek(client)
    if not tum:
        return

    print(f"[Tarama] {len(tum)} coin taranıyor...")

    # Paralel veri çekme (10'ar, rate limit koruması)
    sem = asyncio.Semaphore(10)
    async def cek(sym):
        async with sem:
            await cek_15dk(client, sym)
            await asyncio.sleep(0.05)

    await asyncio.gather(*[cek(s) for s in tum])

    # Katman 1 analiz — adayları bul
    adaylar = []
    for sym in tum:
        sonuc = katman1(sym)
        if sonuc:
            adaylar.append((sym, sonuc))

    # En yüksek skorlulardan MAX_IZLEME kadar al
    adaylar.sort(key=lambda x: x[1]["skor"], reverse=True)
    yeni = {s for s, _ in adaylar[:MAX_IZLEME]}

    # Yeni coinler için 1dk veri yükle
    yeni_eklenen = yeni - IZLENEN
    if yeni_eklenen:
        sem2 = asyncio.Semaphore(5)
        async def yukle(sym):
            async with sem2:
                await cek_1dk(client, sym)
        await asyncio.gather(*[yukle(s) for s in yeni_eklenen])
        print(f"[Tarama] {len(yeni)} aday | +{len(yeni_eklenen)} yeni")

    # Listeden çıkanları temizle
    for sym in (IZLENEN - yeni):
        STORE_1DK.pop(sym, None)

    # Yenileme gerekiyor mu?
    if yeni_eklenen or (IZLENEN - yeni):
        WS_YENILE = True

    IZLENEN = yeni

# ══════════════════════════════════════════════════════════════
# WEBSOCKET MESAJ İŞLEME
# ══════════════════════════════════════════════════════════════
async def isle(msg: dict, session: aiohttp.ClientSession):
    if "data" not in msg:
        return
    k      = msg["data"]["k"]
    sembol = msg["data"]["s"].lower()

    # Sadece izlenen coinlerin KAPANAN mumları
    if sembol not in IZLENEN or not k["x"]:
        return

    kapanis   = float(k["c"])
    yuksek    = float(k["h"])
    dusuk     = float(k["l"])
    hacim     = float(k["v"])
    taker_buy = float(k["V"])  # ✅ Doğru alan (k["t"] timestamp'tir!)

    if sembol not in STORE_1DK:
        return

    # 1dk veriyi güncelle
    d = STORE_1DK[sembol]
    for anahtar, deger in [("f", kapanis), ("y", yuksek), ("d", dusuk),
                            ("h", hacim), ("tb", taker_buy)]:
        d[anahtar].append(deger)
        if len(d[anahtar]) > 150:
            d[anahtar].pop(0)

    # ── Cooldown: aynı coin 30dk içinde tekrar sinyal vermez ─
    if time.time() - SINYAL_ZAMANI.get(sembol, 0) < SINYAL_BEKLEME:
        return

    # ── Katman 1 analizi ─────────────────────────────────────
    k1 = katman1(sembol)
    if not k1:
        return

    # ── Katman 2 analizi ─────────────────────────────────────
    k2 = katman2(sembol, k1)
    if not k2:
        return

    # ── Sinyal onaylandı ─────────────────────────────────────
    SINYAL_ZAMANI[sembol] = time.time()

    # ── Spam koruması: 5dk içinde max 5 sinyal ───────────────
    # Çok fazla sinyal = Bitcoin hareketi yansıması, gürültü
    simdi = time.time()
    SON_SINYALLER.append((simdi, sembol))
    SON_SINYALLER[:] = [(t, s) for t, s in SON_SINYALLER if simdi - t < SPAM_PENCERE]

    if len(SON_SINYALLER) > SPAM_ESIK:
        print(f"[Spam] {sembol.upper()} tutuldu ({len(SON_SINYALLER)} sinyal/5dk)")
        return

    # ── L/S oranı çek ────────────────────────────────────────
    ls = await ls_cek(session, sembol)

    # ── Gönder ───────────────────────────────────────────────
    await sinyal_gonder(sembol, k2, ls)

# ══════════════════════════════════════════════════════════════
# ANA BOT DÖNGÜSÜ
# ══════════════════════════════════════════════════════════════
async def bot():
    global WS_YENILE

    print("🚀 Bot başlatılıyor...")
    await telegram(
        "🤖 <b>Kripto Sinyal Botu Başlatıldı</b>\n\n"
        "3 katmanlı analiz sistemi yükleniyor...\n"
        "İlk sinyaller 5-6 dakika içinde gelecek."
    )

    client = await AsyncClient.create()

    try:
        # ── İlk tarama ───────────────────────────────────────
        await tarama(client)

        # ── Arka planda periyodik tarama ─────────────────────
        async def arka_plan():
            while True:
                await asyncio.sleep(TARAMA_SURE)
                try:
                    await tarama(client)
                except Exception as e:
                    print(f"[Tarama Hata] {e}")

        asyncio.create_task(arka_plan())

        bm = BinanceSocketManager(client)

        async with aiohttp.ClientSession() as session:
            while True:
                if not IZLENEN:
                    print("İzlenecek coin yok, 30sn bekleniyor...")
                    await asyncio.sleep(30)
                    continue

                streams = [f"{s}@kline_1m" for s in list(IZLENEN)]
                print(f"📡 WebSocket: {len(streams)} coin izleniyor")
                WS_YENILE = False

                try:
                    async with bm.multiplex_socket(streams) as stream:
                        while True:
                            try:
                                res = await asyncio.wait_for(
                                    stream.recv(), timeout=45
                                )
                                await isle(res, session)

                                # İzleme listesi değiştiyse sessizce yenile
                                if WS_YENILE:
                                    print("📡 İzleme listesi güncellendi, yenileniyor...")
                                    break

                            except asyncio.TimeoutError:
                                pass  # Normal, sessizce devam
                            except Exception as e:
                                print(f"[WS Mesaj Hata] {e}")
                                break

                except Exception as e:
                    print(f"[WS Bağlantı Hata] {e}")
                    await asyncio.sleep(15)

    except Exception as e:
        print(f"[Bot Hata] {e}")
        await telegram(f"❌ Bot hatası: {str(e)[:200]}")
    finally:
        await client.close_connection()
        print("Bağlantı kapatıldı.")

# ══════════════════════════════════════════════════════════════
# BAŞLAT
# ══════════════════════════════════════════════════════════════
async def main():
    while True:
        try:
            await bot()
        except Exception as e:
            print(f"[Kritik Hata] {e}")
        print("♻️ 60sn sonra yeniden başlatılıyor...")
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
