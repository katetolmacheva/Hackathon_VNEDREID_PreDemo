from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import requests

from tinkoff.invest import Client, CandleInterval


def _q_to_float(q) -> float:
    return q.units + q.nano / 1e9


def _fetch_history(token: str, ticker: str, days: int) -> List[Dict]:
    with Client(token=token, app_name="tinvest_history") as cli:
        instruments = cli.instruments.find_instrument(query=ticker).instruments
        if not instruments:
            return []
        figi = instruments[0].figi
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        resp = cli.market_data.get_candles(
            figi=figi,
            from_=start,
            to=end,
            interval=CandleInterval.CANDLE_INTERVAL_DAY,
        )
        data = [
            {
                "date": c.time.date(),
                "open": _q_to_float(c.open),
                "high": _q_to_float(c.high),
                "low": _q_to_float(c.low),
                "close": _q_to_float(c.close),
            }
            for c in resp.candles
        ]
        return data


async def get_ticker_history(token: str, ticker: str, days: int = 30) -> List[Dict]:
    return await asyncio.to_thread(_fetch_history, token, ticker, days)


def is_valid_moex_ticker(ticker: str) -> bool:
    """
    Проверяет существование тикера на Московской бирже.
    
    Args:
        ticker (str): Тикер для проверки
        
    Returns:
        bool: True если тикер существует, False в противном случае
    """
    ticker = ticker.upper()
    end = datetime.today()
    start = end - timedelta(days=1)

    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
    params = {
        "from": start.strftime("%Y-%m-%d"),
        "till": end.strftime("%Y-%m-%d"),
        "interval": 24,
        "iss.meta": "off"
    }

    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code != 200:
            return False

        data = response.json()
        return bool(data.get("candles", {}).get("data"))

    except Exception:
        return False
