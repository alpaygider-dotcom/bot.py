import aiohttp

SPOT_YENILEME_SURESI = 1800  # 30 dakika

SPOT_SEMBOLLERI = set()
FUTURES_SEMBOLLERI = set()
SON_SEMBOL_YENILEME = 0


async def sembolleri_yenile(client, session):
    global SPOT_SEMBOLLERI
    global FUTURES_SEMBOLLERI
    global SON_SEMBOL_YENILEME

    # 1. Binance Global Spot: ana tarama evreni
    spot_info = await client.get_exchange_info()

    yeni_spot = {
        s["symbol"]
        for s in spot_info["symbols"]
        if s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
        and s.get("isSpotTradingAllowed", True)
        and s.get("baseAsset") != "USDT"
        and s["symbol"] not in STABLECOIN_LISTESI
        and s["symbol"] not in OLU_LISTESI
        and s["symbol"] not in EMTIA_LISTESI
    }

    # 2. Futures: sadece ek veri desteği
    yeni_futures = set()

    try:
        async with session.get(
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        ) as response:
            response.raise_for_status()
            futures_info = await response.json()

        yeni_futures = {
            s["symbol"]
            for s in futures_info["symbols"]
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        }

    except (aiohttp.ClientError, ValueError) as e:
        # Futures API başarısızsa Spot taraması devam eder.
        print(f"Futures sembol yenileme hatası: {e}")

    eski_spot = SPOT_SEMBOLLERI.copy()

    SPOT_SEMBOLLERI = yeni_spot
    FUTURES_SEMBOLLERI = yeni_futures
    SON_SEMBOL_YENILEME = time.time()

    eklenen = yeni_spot - eski_spot
    cikan = eski_spot - yeni_spot

    print(
        f"Spot: {len(yeni_spot)} | "
        f"Futures destekli: {len(yeni_spot & yeni_futures)} | "
        f"Yeni: {len(eklenen)} | "
        f"Çıkan: {len(cikan)}"
    )

    if eklenen:
        print("Yeni Spot çiftleri:", sorted(eklenen))

    if cikan:
        print("Artık aktif olmayan Spot çiftleri:", sorted(cikan))

    return SPOT_SEMBOLLERI
