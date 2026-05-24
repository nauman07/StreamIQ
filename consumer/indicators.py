import pandas as pd
import numpy as np


def compute_sma(series: pd.Series, window: int) -> float:
    """Simple moving average over last N periods."""
    if len(series) < window:
        return None
    return round(float(series.tail(window).mean()), 4)


def compute_rsi(series: pd.Series, period: int = 14) -> float:
    """
    RSI — Relative Strength Index.
    Returns value 0-100. >70 overbought, <30 oversold.
    """
    if len(series) < period + 1:
        return None

    delta  = series.diff()
    gain   = delta.where(delta > 0, 0.0)
    loss   = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean().iloc[-1]
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean().iloc[-1]

    if avg_loss == 0:
        return 100.0

    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi), 4)


def compute_volume_spike(volume_series: pd.Series, window: int = 20, threshold: float = 2.0) -> bool:
    """True if latest volume > threshold * rolling average."""
    if len(volume_series) < window:
        return False
    avg = volume_series.tail(window).mean()
    current = volume_series.iloc[-1]
    return bool(current > avg * threshold)


def compute_signal(sma_20: float, sma_50: float, rsi: float) -> str:
    """
    Simple signal based on SMA crossover + RSI.
    BUY:  SMA20 > SMA50 (uptrend) AND RSI < 70 (not overbought)
    SELL: SMA20 < SMA50 (downtrend) AND RSI > 30 (not oversold)
    NEUTRAL: everything else
    """
    if sma_20 is None or sma_50 is None or rsi is None:
        return "NEUTRAL"
    if sma_20 > sma_50 and rsi < 70:
        return "BUY"
    if sma_20 < sma_50 and rsi > 30:
        return "SELL"
    return "NEUTRAL"


def process_tick(symbol: str, history: pd.DataFrame) -> dict:
    """
    Given a symbol and its historical closes + volumes,
    compute all indicators for the latest tick.
    """
    closes  = history["close"]
    volumes = history["volume"]

    sma_20       = compute_sma(closes, 20)
    sma_50       = compute_sma(closes, 50)
    rsi_14       = compute_rsi(closes, 14)
    volume_spike = compute_volume_spike(volumes, 20)
    signal       = compute_signal(sma_20, sma_50, rsi_14)

    latest = history.iloc[-1]
    return {
        "symbol":       symbol,
        "ts":           str(latest["ts"]),
        "close":        float(latest["close"]),
        "volume":       int(latest["volume"]),
        "sma_20":       sma_20,
        "sma_50":       sma_50,
        "rsi_14":       rsi_14,
        "volume_spike": volume_spike,
        "signal":       signal,
    }
