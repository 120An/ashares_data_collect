import numpy as np
import pandas as pd
import pytest

from tick_analysis.liquidity import (
    effective_spread, kyle_lambda, amihud_illiquidity, roll_spread,
)


def test_effective_spread_sign(simple_frame):
    es = effective_spread(simple_frame, in_bps=True)
    assert (es.dropna() >= 0).all()


def test_amihud_positive(simple_frame):
    a = amihud_illiquidity(simple_frame, freq="3s")
    assert a >= 0


def test_kyle_lambda_runs(simple_frame):
    lam = kyle_lambda(simple_frame, freq="3s", method="lee_ready")
    assert isinstance(lam, float)


def test_roll_spread_known_series():
    # 随机 bid-ask bounce：中价恒 10.01、真实价差 s=0.02，成交在买/卖侧 iid 跳动。
    # Roll 估计 = 2*sqrt(-cov(Δp_t,Δp_{t-1})) 应 ≈ 0.02。
    rng = np.random.default_rng(42)
    n = 5000
    mid, half = 10.01, 0.01
    q = rng.choice([-1, 1], size=n)          # iid 买卖指示
    prices = mid + half * q
    df = pd.DataFrame({
        "datetime": pd.date_range("2026-06-01 09:30:00", periods=n, freq="3s"),
        "last_price": prices,
        "volume": np.arange(1, n + 1) * 100,
        "amount": np.arange(1, n + 1) * 1000.0,
        "transaction_num": np.arange(1, n + 1),
        "bid_price_1": mid - half, "ask_price_1": mid + half,
        "bid_vol_1": 100, "ask_vol_1": 100,
    })
    roll = roll_spread(df)
    assert roll == pytest.approx(0.02, abs=4e-3)
