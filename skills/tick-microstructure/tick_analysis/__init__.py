"""tick_analysis：A 股 L1 快照 tick 微观结构分析（纯 pandas/numpy）。

快速上手：
    import tick_analysis as ta
    df = ta.from_packed_parquet("Z:/.../01.parquet", stock_code="002602.SZ")
    result = ta.analyze(df)
"""
from __future__ import annotations

__version__ = "0.1.0"

from .contract import (
    validate_tick_frame, from_dataframe, from_xtquant, from_packed_parquet,
)
from .preprocess import (
    to_increments, mid_price, prevailing_quote, split_sessions, continuous,
)
from .moneyflow import (
    classify_trades, money_flow, avg_trade_amount, trade_size_buckets,
    main_force_flow,
)
from .orderbook import (
    order_book_imbalance, microprice, quoted_spread, weiba_weicha, book_depth,
)
from .liquidity import (
    effective_spread, kyle_lambda, amihud_illiquidity, roll_spread,
)
from .auction import opening_auction, closing_auction, auction_features
from .execution import (
    vwap, twap, slippage_vs_vwap, participation_rate, implementation_shortfall,
)


def analyze(df, big_threshold: float = 50e4, method: str = "bvc") -> dict:
    """一次性跑标准电池，返回结构化 dict。"""
    df = validate_tick_frame(df)
    if df.empty:
        return {}
    return {
        "money_flow": money_flow(df, method),
        "main_force": main_force_flow(df, big_threshold, method),
        "orderbook": {
            "obi_mean": float(order_book_imbalance(df, 5).mean()),
            "spread_bp_mean": float(quoted_spread(df, in_bps=True).mean()),
        },
        "liquidity": {
            "amihud": amihud_illiquidity(df),
            "kyle_lambda": kyle_lambda(df, method=method),
            "roll_spread": roll_spread(df),
        },
        "auction": auction_features(df),
        "vwap": vwap(df),
    }


__all__ = [
    "validate_tick_frame", "from_dataframe", "from_xtquant", "from_packed_parquet",
    "to_increments", "mid_price", "prevailing_quote", "split_sessions", "continuous",
    "classify_trades", "money_flow", "avg_trade_amount", "trade_size_buckets",
    "main_force_flow", "order_book_imbalance", "microprice", "quoted_spread",
    "weiba_weicha", "book_depth", "effective_spread", "kyle_lambda",
    "amihud_illiquidity", "roll_spread", "opening_auction", "closing_auction",
    "auction_features", "vwap", "twap", "slippage_vs_vwap", "participation_rate",
    "implementation_shortfall", "analyze",
]
