"""列契约：规范列名、校验、各 tick 源 → 契约的适配器。"""
from __future__ import annotations

import pandas as pd

# 必需列（单只股票、按 datetime 升序）。volume 单位规范为 **股(shares)**；
# QMT 源(from_xtquant/from_packed_parquet) volume 原生为 手，适配器按 lot_size 转股。
# 通用 from_dataframe 不缩放：调用方须自行保证 volume 为股。
REQUIRED = ["datetime", "last_price", "volume", "amount", "transaction_num"]
# 至少一档盘口
ORDERBOOK_L1 = ["bid_price_1", "ask_price_1", "bid_vol_1", "ask_vol_1"]

# xtquant 原始 → 契约 的标量重命名
_XT_SCALAR = {
    "lastPrice": "last_price",
    "lastClose": "last_close",
    "transactionNum": "transaction_num",
    "stockStatus": "stock_status",
}
# xtquant 原始 list 盘口列 → 契约前缀
_XT_LIST = {
    "askPrice": "ask_price", "bidPrice": "bid_price",
    "askVol": "ask_vol", "bidVol": "bid_vol",
}


def validate_tick_frame(df: pd.DataFrame, levels: int = 5,
                        require_orderbook: bool = True) -> pd.DataFrame:
    """校验契约并按 datetime 升序返回拷贝。缺必需列抛 ValueError。"""
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"tick frame 缺必需列: {missing}")
    if require_orderbook:
        miss = [c for c in ORDERBOOK_L1 if c not in df.columns]
        if miss:
            raise ValueError(f"tick frame 缺盘口列(至少一档): {miss}")
    return df.sort_values("datetime").reset_index(drop=True)


def from_dataframe(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """通用列名映射：把任意源的列重命名到契约后校验。"""
    out = df.rename(columns=mapping).copy()
    return validate_tick_frame(out, require_orderbook=True)


def from_xtquant(raw: pd.DataFrame, stock_code: str | None = None, lot_size: int = 100) -> pd.DataFrame:
    """xtdata.get_market_data_ex(period='tick') 单股 DataFrame → 契约。

    展开 askPrice/bidPrice/askVol/bidVol 列表列为 _1.._5；
    time(UTC 毫秒)→datetime(+8h 北京时间)；标量列重命名。
    """
    df = pd.DataFrame(raw) if isinstance(raw, dict) else raw.copy()
    for raw_col, prefix in _XT_LIST.items():
        if raw_col not in df.columns:
            continue
        first = df[raw_col].iloc[0] if len(df) else None
        if not isinstance(first, (list, tuple)):
            continue
        n = len(first)
        for i in range(n):
            df[f"{prefix}_{i + 1}"] = df[raw_col].apply(
                lambda x, idx=i: x[idx] if isinstance(x, (list, tuple)) and len(x) > idx else None
            )
        df = df.drop(columns=[raw_col])
    if "time" in df.columns and pd.api.types.is_numeric_dtype(df["time"]):
        df["datetime"] = pd.to_datetime(df["time"], unit="ms") + pd.Timedelta(hours=8)
        df = df.drop(columns=["time"])
    df = df.rename(columns={k: v for k, v in _XT_SCALAR.items() if k in df.columns})
    if stock_code is not None:
        df["stock_code"] = stock_code
    if "volume" in df.columns:
        df["volume"] = df["volume"] * lot_size   # QMT volume 手→股，保证 amount/volume=价格
    return validate_tick_frame(df, require_orderbook=True)


def from_packed_parquet(path_or_df, stock_code: str | None = None, lot_size: int = 100) -> pd.DataFrame:
    """读本项目按日打包 parquet（列已天然契约对齐）。

    path_or_df 为 parquet 路径(str/Path) 或已读入的 DataFrame。
    需要 pyarrow（仅此函数）；缺失给清晰报错。
    """
    if isinstance(path_or_df, pd.DataFrame):
        df = path_or_df
    else:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("from_packed_parquet 需要 pyarrow：pip install pyarrow") from exc
        filters = [("stock_code", "=", stock_code)] if stock_code else None
        df = pq.read_table(str(path_or_df), filters=filters).to_pandas()
    df = df.copy()
    if "volume" in df.columns:
        df["volume"] = df["volume"] * lot_size   # QMT volume 手→股，保证 amount/volume=价格
    return validate_tick_frame(df, require_orderbook=True)
