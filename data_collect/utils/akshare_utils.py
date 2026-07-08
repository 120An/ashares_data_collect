"""akshare 懒加载 + ETF 净值/信息取数封装（含纯映射函数，便于单测）。"""
from __future__ import annotations

import logging
import time

import pandas as pd

from data_collect.utils.etf_utils import is_etf_code

logger = logging.getLogger(__name__)

_ak = None


def require_akshare():
    """懒加载 akshare 单例。"""
    global _ak
    if _ak is None:
        import akshare
        _ak = akshare
    return _ak


def _retry(fn, *args, tries: int = 3, wait: float = 2.0, **kwargs):
    """akshare 网络调用简单重试。"""
    last = None
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last = exc
            logger.warning(f"akshare {getattr(fn, '__name__', fn)} 第{i+1}/{tries}次失败: {exc}")
            time.sleep(wait)
    raise last


# ---- 取数 ----

def get_etf_spot() -> pd.DataFrame:
    """全市场 ETF 实时快照（fund_etf_spot_em）。"""
    ak = require_akshare()
    return _retry(ak.fund_etf_spot_em)


def get_etf_nav_hist(code: str, start: str, end: str) -> pd.DataFrame:
    """单只 ETF 官方净值历史（fund_etf_fund_info_em）。start/end 为 YYYYMMDD。"""
    ak = require_akshare()
    return _retry(ak.fund_etf_fund_info_em, fund=code, start_date=start, end_date=end)


def get_fund_name_map() -> dict:
    """基金代码 -> (基金简称, 基金类型)。"""
    ak = require_akshare()
    df = _retry(ak.fund_name_em)
    return {str(c): (s, t) for c, s, t in zip(df["基金代码"], df["基金简称"], df["基金类型"])}


# ---- 纯映射函数（单测覆盖）----

def build_nav_spot_df(spot: pd.DataFrame, trade_date) -> pd.DataFrame:
    """fund_etf_spot_em -> etf_nav 的 spot 部分（code/trade_date/close/iopv/discount_rate）。"""
    if spot is None or spot.empty:
        return pd.DataFrame(columns=["code", "trade_date", "close", "iopv", "discount_rate"])
    out = pd.DataFrame({
        "code": spot["代码"].astype(str).str.zfill(6),
        "trade_date": pd.to_datetime(trade_date).date(),
        "close": pd.to_numeric(spot["最新价"], errors="coerce"),
        "iopv": pd.to_numeric(spot["IOPV实时估值"], errors="coerce"),
        "discount_rate": pd.to_numeric(spot["基金折价率"], errors="coerce"),
    })
    return out[out["code"].map(is_etf_code)].reset_index(drop=True)


def build_nav_official_df(hist: pd.DataFrame, code: str) -> pd.DataFrame:
    """fund_etf_fund_info_em -> etf_nav 的官方净值部分。"""
    if hist is None or hist.empty:
        return pd.DataFrame(columns=["code", "trade_date", "nav_date", "unit_nav", "accum_nav", "daily_growth"])
    d = pd.to_datetime(hist["净值日期"], errors="coerce").dt.date
    out = pd.DataFrame({
        "code": str(code),
        "trade_date": d,
        "nav_date": d,
        "unit_nav": pd.to_numeric(hist["单位净值"], errors="coerce"),
        "accum_nav": pd.to_numeric(hist.get("累计净值"), errors="coerce"),
        "daily_growth": pd.to_numeric(hist.get("日增长率"), errors="coerce"),
    })
    return out.dropna(subset=["trade_date"]).reset_index(drop=True)
