"""
╔══════════════════════════════════════════════════════════════╗
║   BİNANCE TR — AKILLI KRİPTO SİNYAL BOTU v3.1             ║
║   3 Katmanlı Filtre | Yükselişten ÖNCE coin bul            ║
╚══════════════════════════════════════════════════════════════╝

DEĞİŞİKLİKLER (v3.0 → v3.1):
  + Tüm Binance coinleri taranır (hacim filtresi kaldırıldı)
  + BTC 24h değişim takibi — RS filtresi ve bonus puanı
  + RSI yapışması filtresi — 15 mumdur düşüyorsa falling knife
  + 5 mumluk taker buy ortalaması — tek mum spike koruması
  + BTC çöküşünde dinamik skor eşiği
  + MAX_IZLEME 40 → 55
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
    return datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M:%S")

# ══════════════════════════════════════════════════════════════
# KARA LİSTELER
# ══════════════════════════════════════════════════════════════
STABLECOIN_LISTESI = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT", "USDTUSDT",
    "FDUSDUSDT", "USDPUSDT", "EURUSDT", "USDPAXUSDT", "SUSDUSDT",
    "USTUSDT", "FRAXUSDT", "LUSDUSDT", "AEURUSDT", "GBPUSDT",
}
MAJOR_LISTESI = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "BCHUSDT", "LTCUSDT", "LINKUSDT", "DOTUSDT", "ADAUSDT",
    "AVAXUSDT", "MATICUSDT", "TRXUSDT", "ATOMUSDT", "NEARUSDT",
    "XLMUSDT", "ARBUSDT", "FILUSDT", "INJUSDT", "DASHUSDT",
}
OLU_LISTESI = {
    "LUNCUSDT", "USTCUSDT", "MOONUSDT", "SAFEMOONUSDT",
    "SHIBUSDT", "PEPEUSDT", "1MBABYDOGEUSDT", "BABYDOGEUSDT",
    "MEMEUSDT", "PUMPUSDT",
    "HMSTRUSDT", "PIXELUSDT", "PLAYDIPUSDT",
    "HEMIUSDT",          # Düşük hacimli, manipülasyon riski
    "BANANAS31USDT",     # Sürekli negatif RS, çok zayıf coin
    # 币安人生 — olası semboller (hangisi olduğu bilinmiyor, ikisi de engelle)
    "BINANCELIFEUSDT", "BIANRENSHENGUSDT", "RENSUSDT", "BNLIFEUSDT",
}
EMTIA_LISTESI = {
    "PAXGUSDT", "XAUTUSDT",
}

# ══════════════════════════════════════════════════════════════
# PARAMETRELER
# ══════════════════════════════════════════════════════════════

# Katman 1 — izleme listesi filtresi (gevşek)
K1_RSI_MIN      = 15
K1_RSI_MAX      = 45
K1_OBV_PERIYOT  = 10
K1_BB_PERIYOT   = 20
K1_BB_STD       = 2.0
K1_BB_SIKISMA   = 0.06
K1_BB_MAX       = 0.45
K1_MIN_SKOR     = 4

# Katman 2 — gerçek sinyal filtresi (sıkı)
K2_RSI_MAX      = 38
K2_BB_MAX       = 0.35
K2_HACIM_MIN    = 2.5
K2_HACIM_MAX    = 50.0
K2_TAKER_MIN    = 0.60    # Son mum taker buy
K2_TAKER5_MIN   = 0.55    # 5 mum ortalama taker buy
K2_TAKER_MAX    = 0.92
K2_USD_MIN      = 50000  # $15K'dan $50K'ya — AVNT/SYRUP/CHZ tipi ince coinler elenir
K2_5DK_MIN      = 0.08
K2_5DK_MAX      = 3.0
K2_15DK_MAX     = 2.0
K2_VWAP_MIN     = -0.50
K2_VWAP_MAX     = 1.50
K2_MIN_SKOR     = 15

# Koruma
PUMP_SAAT       = 6
PUMP_PCT        = 12.0
PUMP_24H_PCT    = 20.0

# Sistem
SINYAL_BEKLEME  = 10800   # 3 saat
MIN_15DK_MUM    = 50
MIN_1DK_MUM     = 30
TARAMA_SURE     = 300
MAX_IZLEME      = 55
BTC_RSI_MIN     = 45
RS_MIN          = -10.0   # BTC göreceli güç minimum eşiği

# Spam
SPAM_PENCERE    = 300
SPAM_ESIK       = 5

# ── Long Sinyali ────────────────────────────────────────────
LS_TARAMA_SURE  = 600    # 10 dakikada bir tara
LS_DUSUS_MIN    = 2.0    # Minimum long düşüşü (yüzde puan)
LS_DUSUS_GEREK  = 3      # Kaç periyotta düşüş görülmeli
LS_PUMP_LIMIT   = 5.0    # Son timeframe'lerde max artış % (pump koruması)
LS_COOLDOWN     = 14400  # Aynı coin için 4 saat cooldown
LS_MIN_LONG_PCT = 50.0   # Long oranı en az %50 olmalı (long ağırlıklı piyasa)

# ══════════════════════════════════════════════════════════════
# GLOBAL DEPOLAR
# ══════════════════════════════════════════════════════════════
STORE_15DK    = {}
STORE_1DK     = {}
IZLENEN       = set()
SINYAL_ZAMANI = {}
SON_SINYALLER = []
WS_YENILE     = False
BTC_RSI       = 50.0
BTC_24H       = 0.0       # BTC 24 saatlik değişim %
LS_SINYAL_ZAMANI = {}     # Long sinyali cooldown {sembol: timestamp}

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
    if len(fiyatlar) < n:
        return fiyatlar[-1] if fiyatlar else 0.0
    k = 2 / (n + 1)
    e = sum(fiyatlar[:n]) / n
    for f in fiyatlar[n:]:
        e = f * k + e * (1 - k)
    return e


def bollinger(fiyatlar: list, n: int = 20, std_k: float = 2.0) -> dict:
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
    """OBV EMA trend + Bullish Divergence (sıfıra bölme hatası düzeltildi)"""
    if len(fiyatlar) < 25 or len(fiyatlar) != len(hacimler):
        return False, False, ""
    obv = [0.0]
    for i in range(1, len(fiyatlar)):
        if fiyatlar[i] > fiyatlar[i-1]:
            obv.append(obv[-1] + hacimler[i])
        elif fiyatlar[i] < fiyatlar[i-1]:
            obv.append(obv[-1] - hacimler[i])
        else:
            obv.append(obv[-1])
    obv_ema5  = ema(obv, 5)
    obv_ema20 = ema(obv, 20)
    yukseliyor = obv_ema5 > obv_ema20 * 1.001
    fiyat_ll = fiyatlar[-1] < min(fiyatlar[-n:-1])
    obv_hl   = obv[-1] > obv[-n]
    divergence = fiyat_ll and obv_hl
    if divergence:
        return True, True,  "🔍 Gizli Birikim (Fiyat↓ OBV↑)"
    elif yukseliyor:
        return True, False, "📈 OBV EMA Yükseliyor"
    else:
        return False, False, "📉 OBV EMA Düşüyor"


def vwap(yuksekler, dusukler, kapanis, hacimler) -> float:
    if not hacimler:
        return kapanis[-1] if kapanis else 0.0
    pv = sum(((y+d+k)/3) * h for y, d, k, h in zip(yuksekler, dusukler, kapanis, hacimler))
    v  = sum(hacimler)
    return pv / v if v > 0 else (kapanis[-1] if kapanis else 0.0)


def pump_var_mi(fiyatlar: list) -> tuple:
    n6 = PUMP_SAAT * 4
    if len(fiyatlar) >= n6:
        en_dusuk_6h = min(fiyatlar[-n6:])
        pct_6h = ((fiyatlar[-1] - en_dusuk_6h) / en_dusuk_6h * 100) if en_dusuk_6h > 0 else 0
        if pct_6h >= PUMP_PCT:
            return True, round(pct_6h, 1)
    n24 = 24 * 4
    if len(fiyatlar) >= n24:
        en_dusuk_24h = min(fiyatlar[-n24:])
        pct_24h = ((fiyatlar[-1] - en_dusuk_24h) / en_dusuk_24h * 100) if en_dusuk_24h > 0 else 0
        if pct_24h >= PUMP_24H_PCT:
            return True, round(pct_24h, 1)
    return False, 0.0

# ══════════════════════════════════════════════════════════════
# LONG/SHORT ORANI
# ══════════════════════════════════════════════════════════════
async def ls_cek(session: aiohttp.ClientSession, sembol: str) -> dict | None:
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
    except Exception:
        return None


def ls_yorum(ls: dict | None) -> str:
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
# LONG SİNYALİ — FUTURES L/S TAKİBİ
# ══════════════════════════════════════════════════════════════

async def futures_sembolleri_getir(spot_semboller: set) -> list:
    """
    Binance Futures'ta PERPETUAL olan VE spot listesinde bulunan sembolleri döner.
    Yani Binance TR'deki coinlerin futures versiyonları.
    """
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://fapi.binance.com/fapi/v1/exchangeInfo",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    return []
                veri = await r.json()
        futures_set = {
            sym["symbol"]
            for sym in veri.get("symbols", [])
            if sym.get("status") == "TRADING"
            and sym.get("contractType") == "PERPETUAL"
            and sym["symbol"].endswith("USDT")
        }
        # Spot listesiyle kesişim (spot lowercase, futures uppercase)
        kesisim = futures_set & {s.upper() for s in spot_semboller}
        return sorted(kesisim)
    except Exception as e:
        print(f"[Futures Sembol Hata] {e}")
        return []


def pump_kontrol_spot(sembol: str) -> tuple:
    """
    Mevcut 15dk verisini kullanarak pump kontrolü (ekstra API çağrısı yok).
    15dk mumlar: 1 ≈ 15dk, 2 ≈ 30dk, 4 = 1s, 8 = 2s, 24 = 6s, 48 = 12s
    Döner: (pump_var, max_artis_pct)
    """
    anahtar = sembol.lower()
    if anahtar not in STORE_15DK:
        return False, 0.0
    f = STORE_15DK[anahtar]["f"]
    if len(f) < 5:
        return False, 0.0
    su_an = f[-1]
    en_yuksek = 0.0
    for n in [1, 2, 4, 8, 24, 48]:      # ~15dk, 30dk, 1s, 2s, 6s, 12s
        if len(f) > n and f[-n-1] > 0:
            degisim = (su_an - f[-n-1]) / f[-n-1] * 100
            en_yuksek = max(en_yuksek, degisim)
    return en_yuksek >= LS_PUMP_LIMIT, round(en_yuksek, 1)


async def ls_cok_periyot_analiz(session: aiohttp.ClientSession, sembol: str) -> dict | None:
    """
    5m ve 1h periyotlarda L/S oranını çek ve analiz et.
    5m (limit=7): 5dk, 10dk, 30dk kontrolü
    1h (limit=13): 1s, 2s, 6s, 12s kontrolü
    """
    url  = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    veri = {}
    for periyot, limit in [("5m", 7), ("1h", 13)]:
        try:
            p = {"symbol": sembol, "period": periyot, "limit": limit}
            async with session.get(url, params=p, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    d = await r.json()
                    if len(d) >= 2:
                        veri[periyot] = d
        except Exception:
            continue

    if not veri:
        return None

    # Güncel L/S durumu
    son = (veri.get("5m") or veri.get("1h"))[-1]
    long_pct  = float(son.get("longAccount", 0.5)) * 100
    short_pct = float(son.get("shortAccount", 0.5)) * 100

    # Long oranı yeterliyse devam et
    if long_pct < LS_MIN_LONG_PCT:
        return None

    dusus_sayisi = 0
    dusus_detay  = {}

    # 5m verisiyle kısa vade (5dk, 10dk, 30dk)
    if "5m" in veri:
        d = veri["5m"]
        curr = float(d[-1]["longAccount"]) * 100
        for etiket, n, esik in [
            ("5dk",  1, LS_DUSUS_MIN * 0.5),
            ("10dk", 2, LS_DUSUS_MIN * 0.75),
            ("30dk", 6, LS_DUSUS_MIN * 1.25),
        ]:
            if len(d) > n:
                prev = float(d[-n-1]["longAccount"]) * 100
                degisim = curr - prev            # Negatif = long düşüyor
                dusus_detay[etiket] = round(degisim, 2)
                if degisim < -esik:
                    dusus_sayisi += 1

    # 1h verisiyle uzun vade (1s, 2s, 6s, 12s)
    if "1h" in veri:
        d = veri["1h"]
        curr = float(d[-1]["longAccount"]) * 100
        for etiket, n, esik in [
            ("1s",  1,  LS_DUSUS_MIN * 1.5),
            ("2s",  2,  LS_DUSUS_MIN * 2.0),
            ("6s",  6,  LS_DUSUS_MIN * 2.5),
            ("12s", 12, LS_DUSUS_MIN * 3.5),
        ]:
            if len(d) > n:
                prev = float(d[-n-1]["longAccount"]) * 100
                degisim = curr - prev
                dusus_detay[etiket] = round(degisim, 2)
                if degisim < -esik:
                    dusus_sayisi += 1

    return {
        "sembol":       sembol,
        "long_pct":     round(long_pct, 1),
        "short_pct":    round(short_pct, 1),
        "dusus_sayisi": dusus_sayisi,
        "dusus_detay":  dusus_detay,
    }


async def long_sinyali_gonder(analiz: dict, pump_pct: float):
    """Long düşüşü sinyal mesajı — ayrı sinyal tipi"""
    sembol   = analiz["sembol"]
    l_pct    = analiz["long_pct"]
    s_pct    = analiz["short_pct"]
    detay    = analiz["dusus_detay"]
    sayi     = analiz["dusus_sayisi"]

    # Düşüş gösteren periyotları formatla
    satirlar = ""
    for etiket in ["5dk", "10dk", "30dk", "1s", "2s", "6s", "12s"]:
        if etiket in detay:
            val = detay[etiket]
            if val < -0.5:
                emoji = "🔴" if val < -3 else "🟡"
                satirlar += f"  {emoji} {etiket}: {val:+.1f} pp\n"

    msg = (
        f"<b>📉 LONG DÜŞÜŞÜ → YÜKSELİŞ SİNYALİ</b>\n"
        f"<b>{sembol}</b>  ({sayi} periyot onayladı)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 Futures L/S Oranı:</b>\n"
        f"  🟢 Long: <b>%{l_pct:.1f}</b>  |  🔴 Short: %{s_pct:.1f}\n"
        f"\n"
        f"<b>📉 Long Düşüşü (periyotlar):</b>\n"
        f"{satirlar}"
        f"\n"
        f"<b>📌 Mantık:</b>\n"
        f"Long'lar kapanıyor → Overleveraged pozisyonlar temizleniyor\n"
        f"Temizlenme sonrası yükseliş ihtimali yüksek ↑\n"
        f"\n"
        f"Son max pump: +%{pump_pct:.1f} (limit: +%{LS_PUMP_LIMIT:.0f})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {tr_saat()} (TR)"
    )

    await telegram(msg)
    print(f"📉 LONG SİNYALİ → {sembol} | Long: %{l_pct:.1f} | {sayi} periyot")


async def long_sinyali_tarama(client: AsyncClient):
    """
    Her 10 dakikada çalışır.
    Binance Futures'ta long oranı düşen coinleri tespit eder.
    Sadece Binance TR'de olan coinler kontrol edilir.
    Pump yapmamış coinlerde sinyal gönderilir.
    """
    global LS_SINYAL_ZAMANI

    if not STORE_15DK:
        return  # Spot veri henüz yüklenmemiş

    # Futures ile spot kesişimini bul
    spot_set = set(STORE_15DK.keys())   # lowercase: "avntusdt"
    futures_semboller = await futures_sembolleri_getir(spot_set)

    if not futures_semboller:
        print("[Long Tarama] Futures sembol listesi boş")
        return

    print(f"[Long Tarama] {len(futures_semboller)} futures coini taranıyor | {tr_saat()}")

    sinyaller = []
    sem = asyncio.Semaphore(5)

    async def tara(sembol: str):
        async with sem:
            await asyncio.sleep(0.1)  # Rate limit koruması

            # Cooldown kontrolü
            if time.time() - LS_SINYAL_ZAMANI.get(sembol, 0) < LS_COOLDOWN:
                return

            # Pump filtresi (mevcut 15dk veri kullanılır, ekstra API çağrısı yok)
            pump_var, pump_pct = pump_kontrol_spot(sembol)
            if pump_var:
                return

            # L/S oranı analizi
            try:
                async with aiohttp.ClientSession() as session:
                    analiz = await ls_cok_periyot_analiz(session, sembol)
            except Exception:
                return

            if not analiz:
                return

            if analiz["dusus_sayisi"] >= LS_DUSUS_GEREK:
                sinyaller.append((sembol, analiz, pump_pct))

    await asyncio.gather(*[tara(s) for s in futures_semboller])

    # En çok periyotta düşüş gösterenleri önce gönder, max 3 sinyal
    sinyaller.sort(key=lambda x: x[1]["dusus_sayisi"], reverse=True)

    for sembol, analiz, pump_pct in sinyaller[:3]:
        LS_SINYAL_ZAMANI[sembol] = time.time()
        await long_sinyali_gonder(analiz, pump_pct)
        await asyncio.sleep(2)  # Telegram flood koruması

    if sinyaller:
        print(f"[Long Tarama] {len(sinyaller)} sinyal bulundu, "
              f"{min(len(sinyaller), 3)} gönderildi")


# ══════════════════════════════════════════════════════════════
# SEMBOL LİSTESİ — Tüm Binance coinleri
# ══════════════════════════════════════════════════════════════
async def sembolleri_cek(client: AsyncClient) -> list:
    try:
        veri = await client.get_exchange_info()
        semboller = [
            s["symbol"].lower()
            for s in veri.get("symbols", [])
            if s["symbol"].endswith("USDT")
            and s.get("status") == "TRADING"
            and s["symbol"] not in STABLECOIN_LISTESI
            and s["symbol"] not in MAJOR_LISTESI
            and s["symbol"] not in OLU_LISTESI
            and s["symbol"] not in EMTIA_LISTESI
        ]
        print(f"[Sembol] Toplam {len(semboller)} coin taranacak")
        return sorted(semboller)
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
            "f": [float(k[4]) for k in klines[:-1]],
            "y": [float(k[2]) for k in klines[:-1]],
            "d": [float(k[3]) for k in klines[:-1]],
            "h": [float(k[5]) for k in klines[:-1]],
        }
        return True
    except Exception:
        return False


async def cek_1dk(client: AsyncClient, sembol: str):
    try:
        klines = await client.get_klines(symbol=sembol.upper(), interval="1m", limit=80)
        STORE_1DK[sembol] = {
            "f":  [float(k[4]) for k in klines[:-1]],
            "y":  [float(k[2]) for k in klines[:-1]],
            "d":  [float(k[3]) for k in klines[:-1]],
            "h":  [float(k[5]) for k in klines[:-1]],
            "tb": [float(k[9]) for k in klines[:-1]],
        }
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════
# KATMAN 1 — 15 DAKİKALIK ANALİZ
# ══════════════════════════════════════════════════════════════
def katman1(sembol: str) -> dict | None:
    if sembol not in STORE_15DK:
        return None
    d = STORE_15DK[sembol]
    f, h = d["f"], d["h"]
    if len(f) < MIN_15DK_MUM:
        return None

    pump, _ = pump_var_mi(f)
    if pump:
        return None

    r  = rsi(f)
    bb = bollinger(f, K1_BB_PERIYOT, K1_BB_STD)
    obv_yukseliyor, obv_div, obv_acik = obv_analiz(f, h, K1_OBV_PERIYOT)

    # Hard filtreler
    if not (K1_RSI_MIN <= r <= K1_RSI_MAX):
        return None
    if bb["yuzde_b"] > K1_BB_MAX and not obv_yukseliyor and not obv_div:
        return None

    # RSI yapışması — 15 mumdur düşüyorsa falling knife
    if len(f) >= 20 and not obv_div:
        if rsi(f[:-15]) < K1_RSI_MAX and f[-1] < f[-15]:
            return None

    # Göreceli güç filtresi — BTC'den çok zayıf coin reddet
    rs_degeri = None
    if len(f) >= 97:
        coin_24h = ((f[-1] - f[-97]) / f[-97]) * 100
        rs_degeri = coin_24h - BTC_24H
        # BTC düşüşteyken: RS < -10% reddet
        if BTC_24H < -3.0 and rs_degeri < RS_MIN:
            return None
        # Her zaman: RS < -12% reddet (币安人生 -12.7%, CHZ -10.3% tipi coinler)
        if rs_degeri < -12.0:
            return None

    # Skor
    skor = 0
    sebepler = []

    if   r < 25: skor += 4; sebepler.append(f"RSI {r:.1f} 🔴")
    elif r < 30: skor += 3; sebepler.append(f"RSI {r:.1f}")
    elif r < 35: skor += 2; sebepler.append(f"RSI {r:.1f}")
    else:        skor += 1; sebepler.append(f"RSI {r:.1f}")

    if   obv_div:        skor += 4; sebepler.append(obv_acik)
    elif obv_yukseliyor: skor += 2; sebepler.append(obv_acik)

    if   bb["yuzde_b"] <= 0.05: skor += 4; sebepler.append("BB alt bandına değdi 🎯")
    elif bb["yuzde_b"] <= 0.15: skor += 3; sebepler.append("BB alt bandına yakın")
    elif bb["yuzde_b"] <= 0.25: skor += 2; sebepler.append("BB alt çeyreği")
    elif bb["yuzde_b"] <= 0.30: skor += 1; sebepler.append("BB alt bölgesi")

    if bb["genislik"] < K1_BB_SIKISMA:
        skor += 1; sebepler.append("BB Sıkışması ⚡")

    # Göreceli güç bonus puanı
    if rs_degeri is not None:
        if   rs_degeri >= 5: skor += 3; sebepler.append(f"BTC'den güçlü (RS +{rs_degeri:.1f}%) 💪💪")
        elif rs_degeri >= 2: skor += 2; sebepler.append(f"BTC'den güçlü (RS +{rs_degeri:.1f}%) 💪")
        elif rs_degeri >= 0: skor += 1; sebepler.append(f"BTC ile paralel (RS {rs_degeri:+.1f}%)")

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
        "rs":      rs_degeri,
    }

# ══════════════════════════════════════════════════════════════
# KATMAN 2 — 1 DAKİKALIK ANALİZ
# ══════════════════════════════════════════════════════════════
def katman2(sembol: str, k1: dict, min_skor: int = None) -> dict | None:
    if min_skor is None:
        min_skor = K2_MIN_SKOR
    if sembol not in STORE_1DK:
        return None
    d = STORE_1DK[sembol]
    f, y, dd, h, tb = d["f"], d["y"], d["d"], d["h"], d["tb"]
    if len(f) < MIN_1DK_MUM:
        return None

    son_f  = f[-1]
    son_h  = h[-1]
    son_tb = tb[-1]

    baz_h     = median(h[-50:]) if len(h) >= 50 else median(h)
    hacim_k   = son_h / baz_h if baz_h > 0 else 1.0
    taker     = son_tb / son_h if son_h > 0 else 0.5
    usd_hacim = son_h * son_f

    # 5 mumluk taker ortalaması — tek mum balina spike koruması
    if len(tb) >= 5 and len(h) >= 5:
        taker5 = sum(tb[-5:]) / max(sum(h[-5:]), 1)
    else:
        taker5 = taker

    vw        = vwap(y[-30:], dd[-30:], f[-30:], h[-30:])
    vwap_fark = ((son_f - vw) / vw * 100) if vw > 0 else 0

    d3  = ((son_f - f[-3])  / f[-3]  * 100) if len(f) >= 3  else 0
    d5  = ((son_f - f[-5])  / f[-5]  * 100) if len(f) >= 5  else 0
    d15 = ((son_f - f[-15]) / f[-15] * 100) if len(f) >= 15 else 0

    # Hard filtreler
    if hacim_k   < K2_HACIM_MIN:  return None
    if hacim_k   > K2_HACIM_MAX:  return None
    if taker     < K2_TAKER_MIN:  return None
    if taker     > K2_TAKER_MAX:  return None
    if taker5    < K2_TAKER5_MIN: return None
    if usd_hacim < K2_USD_MIN:    return None
    if d5        < K2_5DK_MIN:    return None
    if d5        > K2_5DK_MAX:    return None
    if d15       > K2_15DK_MAX:   return None
    if d3        < -1.5:          return None
    if vwap_fark < K2_VWAP_MIN:   return None
    if vwap_fark > K2_VWAP_MAX:   return None
    if k1["rsi"] > K2_RSI_MAX:    return None
    if k1["bb"]["yuzde_b"] > K2_BB_MAX and not k1["obv_div"]:
        return None

    # Skor
    skor     = k1["skor"]
    sebepler = list(k1["sebepler"])

    if   hacim_k >= 7: skor += 5; sebepler.append(f"Hacim x{hacim_k:.1f} 💥💥")
    elif hacim_k >= 5: skor += 4; sebepler.append(f"Hacim x{hacim_k:.1f} 💥")
    elif hacim_k >= 4: skor += 3; sebepler.append(f"Hacim x{hacim_k:.1f}")
    else:              skor += 2; sebepler.append(f"Hacim x{hacim_k:.1f}")

    if   taker >= 0.75: skor += 4; sebepler.append(f"Alış %{taker*100:.0f} 🔥🔥")
    elif taker >= 0.68: skor += 3; sebepler.append(f"Alış %{taker*100:.0f} 🔥")
    elif taker >= 0.60: skor += 2; sebepler.append(f"Alış %{taker*100:.0f}")

    if   vwap_fark >= 0.5:  skor += 3; sebepler.append(f"VWAP kırıldı +%{vwap_fark:.2f} ✅")
    elif vwap_fark >= -0.2: skor += 2; sebepler.append(f"VWAP yakın %{vwap_fark:.2f}")
    elif vwap_fark >= -0.5: skor += 1

    if   d5 >= 2.0: skor += 3; sebepler.append(f"5dk ivme %{d5:+.1f} ⚡")
    elif d5 >= 1.0: skor += 2; sebepler.append(f"5dk ivme %{d5:+.1f}")
    elif d5 >= 0.1: skor += 1

    if k1["ema50"] and son_f > k1["ema50"]:
        skor += 2; sebepler.append("EMA50 üstü 💪")
    elif son_f > k1["ema21"]:
        skor += 1; sebepler.append("EMA21 üstü")

    if k1["obv_div"]:
        skor += 2

    if skor < min_skor:
        return None

    if   skor >= 22: guc = "⭐⭐⭐ ÇOK GÜÇLÜ"; emoji = "🔥🔥🔥"
    elif skor >= 18: guc = "⭐⭐ GÜÇLÜ";       emoji = "🔥🔥"
    else:            guc = "⭐ ORTA";           emoji = "🔥"

    return {
        "skor": skor, "guc": guc, "emoji": emoji,
        "fiyat": son_f, "vwap_f": vwap_fark,
        "hacim_k": hacim_k, "usd": usd_hacim,
        "taker": taker, "taker5": taker5,
        "d5": d5, "d15": d15,
        "rsi": k1["rsi"], "bb": k1["bb"],
        "obv_div": k1["obv_div"], "sebepler": sebepler,
        "rs": k1.get("rs"),
    }

# ══════════════════════════════════════════════════════════════
# SİNYAL MESAJI
# ══════════════════════════════════════════════════════════════
async def sinyal_gonder(sembol: str, k2: dict, ls: dict | None):
    bb   = k2["bb"]
    ls_m = ls_yorum(ls)

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
        f"  Alış Baskısı: %{k2['taker']*100:.0f} (5dk ort: %{k2['taker5']*100:.0f})\n"
        f"  VWAP: {'+' if k2['vwap_f'] >= 0 else ''}{k2['vwap_f']:.2f}%\n"
        f"  5dk: {k2['d5']:+.1f}%  |  15dk: {k2['d15']:+.1f}%\n"
    )

    if ls_m:
        msg += f"\n<b>📊 L/S:</b> {ls_m}\n"

    if k2.get("rs") is not None:
        rs = k2["rs"]
        rs_emoji = "💪" if rs >= 2 else ("➡️" if rs >= 0 else "⚠️")
        msg += f"\n<b>📈 BTC'ye RS:</b> {rs:+.1f}% {rs_emoji}\n"

    msg += (
        f"\n<b>Sebepler:</b>\n"
        + "  |  ".join(k2["sebepler"][:5])
        + f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {tr_saat()} (TR)"
    )

    await telegram(msg)
    print(f"✅ SİNYAL → {sembol.upper()} | {k2['skor']} puan | {k2['guc']}")

# ══════════════════════════════════════════════════════════════
# TARAMA GÖREVİ
# ══════════════════════════════════════════════════════════════
async def tarama(client: AsyncClient):
    global IZLENEN, WS_YENILE, BTC_RSI, BTC_24H

    tum = await sembolleri_cek(client)
    if not tum:
        return

    # BTC trend
    try:
        btc_klines = await client.get_klines(symbol="BTCUSDT", interval="15m", limit=100)
        btc_fiyat  = [float(k[4]) for k in btc_klines]
        BTC_RSI    = rsi(btc_fiyat)
        BTC_24H    = ((btc_fiyat[-1] - btc_fiyat[-97]) / btc_fiyat[-97] * 100) if len(btc_fiyat) >= 97 else 0.0
        print(f"[Tarama] BTC RSI: {BTC_RSI:.1f} | 24h: {BTC_24H:+.1f}% | "
              f"{'✅ Uygun' if BTC_RSI >= BTC_RSI_MIN else '⚠️ Bearish'}")
    except Exception as e:
        print(f"[BTC Hata] {e}")

    print(f"[Tarama] {len(tum)} coin taranıyor...")

    sem = asyncio.Semaphore(10)
    async def cek(sym):
        async with sem:
            await cek_15dk(client, sym)
            await asyncio.sleep(0.05)
    await asyncio.gather(*[cek(s) for s in tum])

    adaylar = []
    for sym in tum:
        sonuc = katman1(sym)
        if sonuc:
            adaylar.append((sym, sonuc))

    adaylar.sort(key=lambda x: x[1]["skor"], reverse=True)
    yeni = {s for s, _ in adaylar[:MAX_IZLEME]}

    print(f"[Tarama] K1 geçen: {len(adaylar)} coin → İzlemeye alınan: {len(yeni)} | {tr_saat()}")

    yeni_eklenen = yeni - IZLENEN
    if yeni_eklenen:
        sem2 = asyncio.Semaphore(5)
        async def yukle(sym):
            async with sem2:
                await cek_1dk(client, sym)
        await asyncio.gather(*[yukle(s) for s in yeni_eklenen])

    for sym in (IZLENEN - yeni):
        STORE_1DK.pop(sym, None)

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

    if sembol not in IZLENEN or not k["x"]:
        return

    kapanis   = float(k["c"])
    yuksek    = float(k["h"])
    dusuk     = float(k["l"])
    hacim     = float(k["v"])
    taker_buy = float(k["V"])

    if sembol not in STORE_1DK:
        return

    d = STORE_1DK[sembol]
    for anahtar, deger in [("f", kapanis), ("y", yuksek), ("d", dusuk),
                            ("h", hacim), ("tb", taker_buy)]:
        d[anahtar].append(deger)
        if len(d[anahtar]) > 150:
            d[anahtar].pop(0)

    if time.time() - SINYAL_ZAMANI.get(sembol, 0) < SINYAL_BEKLEME:
        return

    # BTC durumuna göre dinamik skor eşiği
    if BTC_24H < -8.0:
        min_skor_dinamik = K2_MIN_SKOR + 5   # Çöküş: skor 20
    elif BTC_RSI < BTC_RSI_MIN:
        min_skor_dinamik = K2_MIN_SKOR + 3   # Bearish: skor 18
    else:
        min_skor_dinamik = K2_MIN_SKOR        # Normal: skor 15

    k1 = katman1(sembol)
    if not k1:
        return

    k2 = katman2(sembol, k1, min_skor_dinamik)
    if not k2:
        return

    SINYAL_ZAMANI[sembol] = time.time()

    simdi = time.time()
    SON_SINYALLER.append((simdi, sembol))
    SON_SINYALLER[:] = [(t, s) for t, s in SON_SINYALLER if simdi - t < SPAM_PENCERE]
    if len(SON_SINYALLER) > SPAM_ESIK:
        print(f"[Spam] {sembol.upper()} tutuldu ({len(SON_SINYALLER)} sinyal/5dk)")
        return

    ls = await ls_cek(session, sembol)
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
        await tarama(client)

        async def arka_plan():
            while True:
                await asyncio.sleep(TARAMA_SURE)
                try:
                    await tarama(client)
                except Exception as e:
                    print(f"[Tarama Hata] {e}")
        asyncio.create_task(arka_plan())

        # ── Long sinyali arka plan görevi ────────────────────
        async def arka_plan_long():
            await asyncio.sleep(120)  # Spot veri yüklensin diye 2 dk bekle
            while True:
                try:
                    await long_sinyali_tarama(client)
                except Exception as e:
                    print(f"[Long Tarama Hata] {e}")
                await asyncio.sleep(LS_TARAMA_SURE)

        asyncio.create_task(arka_plan_long())

        bm = BinanceSocketManager(client)

        async with aiohttp.ClientSession() as session:
            while True:
                if not IZLENEN:
                    print("İzlenecek coin yok, 30sn bekleniyor...")
                    await asyncio.sleep(30)
                    continue

                streams = [f"{s}@kline_1m" for s in list(IZLENEN)]
                baglanti_zamani = time.time()
                WS_YENILE       = False
                mesaj_sayisi    = 0
                timeout_sayisi  = 0
                son_heartbeat   = time.time()
                MAX_TIMEOUT     = 4

                print(f"[WS] Bağlanıyor: {len(streams)} stream | {tr_saat()}")

                try:
                    async with bm.multiplex_socket(streams) as stream:
                        print(f"[WS] ✅ Bağlantı kuruldu: {len(streams)} coin izleniyor")
                        while True:
                            try:
                                res = await asyncio.wait_for(stream.recv(), timeout=45)
                                mesaj_sayisi  += 1
                                timeout_sayisi = 0
                                await isle(res, session)

                                if time.time() - son_heartbeat > 300:
                                    sure = int((time.time() - baglanti_zamani) / 60)
                                    print(f"[WS] 💓 Sağlıklı | {mesaj_sayisi} mesaj | "
                                          f"{sure} dk uptime | {len(streams)} coin | {tr_saat()}")
                                    son_heartbeat = time.time()

                                if WS_YENILE:
                                    sure = int((time.time() - baglanti_zamani) / 60)
                                    print(f"[WS] İzleme güncellendi, yenileniyor "
                                          f"({sure} dk, {mesaj_sayisi} mesaj)")
                                    break

                            except asyncio.TimeoutError:
                                timeout_sayisi += 1
                                gecen = int(time.time() - baglanti_zamani)
                                print(f"[WS] ⚠️ Timeout #{timeout_sayisi} | "
                                      f"Uptime: {gecen}sn | {tr_saat()}")
                                if timeout_sayisi >= MAX_TIMEOUT:
                                    print(f"[WS] ❌ {MAX_TIMEOUT} timeout! Yenileniyor...")
                                    break

                            except Exception as e:
                                gecen = int(time.time() - baglanti_zamani)
                                print(f"[WS] ❌ {type(e).__name__}: {e} | "
                                      f"Uptime: {gecen}sn | {mesaj_sayisi} mesaj | {tr_saat()}")
                                break

                except Exception as e:
                    print(f"[WS] ❌ Bağlantı kurulamadı: {type(e).__name__}: {e} | {tr_saat()}")
                    await asyncio.sleep(15)
                else:
                    await asyncio.sleep(3)

    except Exception as e:
        print(f"[Bot Hata] {e}")
        await telegram(f"❌ Bot hatası: {str(e)[:200]}")
    finally:
        await client.close_connection()
        print("Bağlantı kapatıldı.")

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
