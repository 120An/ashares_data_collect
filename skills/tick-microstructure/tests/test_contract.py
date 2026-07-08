import pandas as pd
import pytest

from tick_analysis.contract import (
    REQUIRED, validate_tick_frame, from_dataframe, from_xtquant, from_packed_parquet,
)


def test_validate_passes(simple_frame):
    out = validate_tick_frame(simple_frame)
    assert list(out["datetime"]) == sorted(out["datetime"])


def test_validate_missing_required(simple_frame):
    bad = simple_frame.drop(columns=["amount"])
    with pytest.raises(ValueError, match="缺必需列"):
        validate_tick_frame(bad)


def test_validate_missing_orderbook(simple_frame):
    bad = simple_frame.drop(columns=["bid_price_1"])
    with pytest.raises(ValueError, match="盘口"):
        validate_tick_frame(bad, require_orderbook=True)


def test_from_dataframe_mapping():
    raw = pd.DataFrame({
        "dt": pd.to_datetime(["2026-06-01 09:30:03"]),
        "price": [10.0], "vol": [100], "amt": [1000.0], "cnt": [5],
        "b1": [9.99], "a1": [10.01], "bv1": [10], "av1": [20],
    })
    mapping = {"dt": "datetime", "price": "last_price", "vol": "volume",
               "amt": "amount", "cnt": "transaction_num",
               "b1": "bid_price_1", "a1": "ask_price_1",
               "bv1": "bid_vol_1", "av1": "ask_vol_1"}
    out = from_dataframe(raw, mapping)
    assert out["last_price"].iloc[0] == 10.0
    assert {"datetime", "last_price", "amount"}.issubset(out.columns)


def test_from_xtquant_expands_and_converts():
    raw = pd.DataFrame({
        "time": [1748742603000],
        "lastPrice": [10.0], "volume": [100], "amount": [1000.0],
        "transactionNum": [5],
        "askPrice": [[10.01, 10.02, 10.03, 10.04, 10.05]],
        "bidPrice": [[9.99, 9.98, 9.97, 9.96, 9.95]],
        "askVol": [[20, 21, 22, 23, 24]],
        "bidVol": [[10, 11, 12, 13, 14]],
    })
    out = from_xtquant(raw)
    assert out["ask_price_1"].iloc[0] == 10.01
    assert out["bid_vol_5"].iloc[0] == 14
    assert out["last_price"].iloc[0] == 10.0
    assert out["datetime"].notna().all()


def test_from_packed_parquet_lots_to_shares():
    # volume 原生 100 手；amount=130000 元；价 13 元 → 转股后 amount/volume=价格
    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2026-06-01 09:30:00"]),
        "last_price": [13.0], "volume": [100], "amount": [130000.0],
        "transaction_num": [1],
        "bid_price_1": [12.99], "ask_price_1": [13.01],
        "bid_vol_1": [10], "ask_vol_1": [10],
    })
    out = from_packed_parquet(df)
    assert out["volume"].iloc[0] == 10000                       # 100手 → 1万股
    assert out["amount"].iloc[0] / out["volume"].iloc[0] == pytest.approx(13.0)


def test_from_xtquant_accepts_dict():
    raw = {
        "time": [1748742603000],
        "lastPrice": [10.0], "volume": [100], "amount": [1000.0],
        "transactionNum": [5],
        "askPrice": [[10.01, 10.02, 10.03, 10.04, 10.05]],
        "bidPrice": [[9.99, 9.98, 9.97, 9.96, 9.95]],
        "askVol": [[20, 21, 22, 23, 24]], "bidVol": [[10, 11, 12, 13, 14]],
    }
    out = from_xtquant(raw)
    assert out["ask_price_1"].iloc[0] == 10.01
    assert out["volume"].iloc[0] == 10000   # 100手 → 1万股
