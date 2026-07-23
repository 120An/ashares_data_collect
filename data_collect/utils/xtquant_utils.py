"""xtquant 公共工具：懒加载 xtdata、获取A股股票列表、批量下载等。"""

from __future__ import annotations

import importlib
import logging
from typing import List

logger = logging.getLogger(__name__)


_xtdata_loaded = False


def require_xtdata():
    """懒加载 xtquant.xtdata 模块，首次加载后禁用 hello 打印。"""
    global _xtdata_loaded
    try:
        xtdata_module = importlib.import_module("xtquant.xtdata")
    except Exception:
        try:
            xtquant_module = importlib.import_module("xtquant")
            xtdata_module = getattr(xtquant_module, "xtdata")
        except Exception as inner_exc:
            raise ImportError("缺少 xtquant，请确认QMT环境可用") from inner_exc

    if not _xtdata_loaded:
        try:
            xtdata_module.enable_hello = False
        except Exception:
            pass
        _xtdata_loaded = True

    return xtdata_module


def get_a_share_codes() -> List[str]:
    """获取A股股票代码列表。"""
    xtdata = require_xtdata()
    try:
        codes = xtdata.get_stock_list_in_sector("沪深A股") or []
        if codes:
            return sorted(set(codes))
    except Exception as exc:
        logger.warning(f"读取 沪深A股 失败: {exc}")

    merged = set()
    for sector in ["上证A股", "深证A股", "北证A股"]:
        try:
            part = xtdata.get_stock_list_in_sector(sector) or []
            merged.update(part)
        except Exception as exc:
            logger.warning(f"读取板块 {sector} 失败: {exc}")

    return sorted(merged)


def download_history_with_retry(
    stock_list: List[str],
    period: str,
    start_time: str,
    end_time: str,
    max_retries: int = 3,
    **kwargs,
) -> None:
    """下载历史数据到本地，直接调用 xtquant 下载（内部自动处理缓存）。"""
    xtdata = require_xtdata()

    for attempt in range(1, max_retries + 1):
        try:
            xtdata.download_history_data2(
                stock_list=stock_list, period=period,
                start_time=start_time, end_time=end_time,
            )
            return
        except Exception as exc:
            action = "重试" if attempt < max_retries else "跳过"
            logger.warning(
                f"下载异常 (第{attempt}/{max_retries}次, "
                f"{len(stock_list)}只{period}): {exc}, {action}..."
            )


def get_etf_codes(sectors: List[str] | None = None, exclude_money: bool = False) -> List[str]:
    """获取场内 ETF 代码列表（带市场后缀，如 510300.SH）。

    依赖本地板块缓存（同 get_a_share_codes）：需先 download_sector_data()——由 pipeline 的
    a_share_sector 任务或首次手动执行；**不在此内联下载**（download_sector_data 无超时、曾挂死）。
    sectors: ETF 板块名列表（默认沪深/沪市/深市ETF，实测板块名）。
    号段兜底过滤（is_etf_code）防止非 ETF 混入。exclude_money: 剔除货币 ETF（511xxx / 159001 等）。
    """
    from data_collect.utils.etf_utils import to_bare_code, with_suffix, is_etf_code

    xtdata = require_xtdata()
    if sectors is None:
        sectors = ["沪深ETF", "沪市ETF", "深市ETF"]

    merged = set()
    for sec in sectors:
        try:
            merged.update(xtdata.get_stock_list_in_sector(sec) or [])
        except Exception as exc:
            logger.warning(f"读取ETF板块 {sec} 失败: {exc}")

    bare = {to_bare_code(c) for c in merged}
    bare = {b for b in bare if is_etf_code(b)}     # 号段兜底
    if exclude_money:
        bare = {b for b in bare if b[:3] not in ("511",) and b not in ("159001", "159003", "159005")}

    return sorted(with_suffix(b) for b in bare)


_INDEX_SECTORS = ["沪深指数"]          # 实测含沪 221 + 深 388（609 个）
_INDEX_EXTRA = ["899050.BJ"]           # 北证50 不在任何指数板块，手工补
_INDEX_EXCLUDE_PREFIX = ("395",)       # sz395* 段 QMT 无数据（实测 14 个全空）；QMT 补数据后删此行即放开


def get_index_codes() -> List[str]:
    """获取全市场指数代码列表（带点，如 000300.SH）。

    依赖本地板块缓存（同 get_etf_codes）：需先 download_sector_data()——由 pipeline 的
    a_share_sector 任务或首次手动执行；**不在此内联下载**（无超时曾挂死）。
    并入北证50、剔除 395* 段（裸码前缀判定）、去重排序。
    """
    xtdata = require_xtdata()
    merged = set()
    for sec in _INDEX_SECTORS:
        try:
            merged.update(xtdata.get_stock_list_in_sector(sec) or [])
        except Exception as exc:
            logger.warning(f"读取指数板块 {sec} 失败: {exc}")
    merged.update(_INDEX_EXTRA)
    result = {c for c in merged if not c.split(".")[0].startswith(_INDEX_EXCLUDE_PREFIX)}
    return sorted(result)
