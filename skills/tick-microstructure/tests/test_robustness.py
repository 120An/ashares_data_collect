import numpy as np
import pandas as pd
import pytest

import tick_analysis as ta
from tick_analysis.moneyflow import classify_trades, money_flow
from tick_analysis.orderbook import microprice, order_book_imbalance
from tick_analysis.auction import auction_features


def test_analyze_empty_returns_dict(simple_frame):
    empty = simple_frame.iloc[0:0]
    assert ta.analyze(empty) == {}


def test_classify_single_row_no_keyerror(simple_frame):
    one = simple_frame.iloc[[0]]            # row0 has dVol=0 → no qualifying trade
    out = classify_trades(one, method="lee_ready")
    assert len(out) == 0                    # empty, must NOT raise KeyError('dir')


def test_nan_price_not_counted_as_sell(simple_frame):
    f = simple_frame.copy()
    f["last_price"] = np.nan                # all prices NaN
    mf = money_flow(f, method="lee_ready")
    assert mf["sell"] == 0 and mf["buy"] == 0   # NaN price → neutral, not fabricated sell


def test_microprice_one_sided_book_is_nan(simple_frame):
    f = simple_frame.copy()
    f.loc[f.index[1], "ask_price_1"] = 0.0   # 一字涨停: 卖盘空
    f.loc[f.index[1], "ask_vol_1"] = 0
    mp = microprice(f)
    assert pd.isna(mp.iloc[1])               # must be NaN, not 0.0


def test_tick_method_classification(simple_frame):
    d = classify_trades(simple_frame, method="tick")
    assert list(d) == [1, -1, 1]


def test_obi_multilevel_differs(simple_frame):
    f = simple_frame.copy()
    # 给第2..5档非零量，使多档 OBI ≠ 一档 OBI
    for i in range(2, 6):
        f[f"bid_vol_{i}"] = 500
        f[f"ask_vol_{i}"] = 0
    o1 = order_book_imbalance(f, levels=1)
    o5 = order_book_imbalance(f, levels=5)
    assert o5.iloc[1] != pytest.approx(o1.iloc[1])
    assert o5.iloc[1] > o1.iloc[1]           # 加了买量 → 更偏多


def test_auction_distinct_snapshot_keys(auction_frame):
    f = auction_features(auction_frame)
    assert f["open_n_snapshots"] == 2
    assert f["close_n_snapshots"] == 2       # 不再相互覆盖
