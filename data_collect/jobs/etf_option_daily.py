"""ETF 期权日快照采集（新浪源：T型报价 + 希腊字母 + IV，写 etf_option_daily）。

50/300/科创50/500 四标的全链：合约清单（getStockName 月份 + OP_UP_/OP_DOWN_ 列表）
→ 批量 T 型报价（CON_OP_，50码/请求）→ 批量希腊字母（CON_SO_）→ 合并入库。
交易所/新浪预先算好 greeks/IV，无需本地 BSM。

实时源**无历史** → run_backfill 直接 raise（前向积累，同 index_minute 教义）；
无 run_verify（快照错过即不可得，靠 pipeline retries 当日兜底）。
run 为**按日替换**语义（盘中误跑被 17:30 正式跑修正）。

新浪坑（配方实测）：GBK 编码；去 `var hq_str_X="..."` 壳逗号分割；必带 Referer 否则 403；
希腊字母 raw[1:4] 是 3 个空串必须跳过（否则 Delta/IV 全错位）；iv 为小数（0.17=17%）。
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List

import pandas as pd
import requests

from data_collect.utils.date_utils import is_market_day
from data_collect.utils.db import replace_day_then_insert
from data_collect.utils.df_utils import normalize_trade_date

logger = logging.getLogger(__name__)

TABLE_NAME = "etf_option_daily"
UNDERLYINGS = {"510050": "50ETF", "510300": "300ETF",
               "588000": "科创50ETF", "510500": "500ETF"}
_HDR = {"Referer": "https://stock.finance.sina.com.cn/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
_BATCH = 50
_LINE_RE = re.compile(r'hq_str_(?:CON_OP_|CON_SO_)(\w+)="([^"]*)"')


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_tquote(v: List[str]) -> Dict:
    """T型报价 51 字段按位取值（<43 字段视为坏行返回空）。"""
    if len(v) < 43:
        return {}
    return {"bid_vol": _f(v[0]), "bid": _f(v[1]), "last": _f(v[2]),
            "ask": _f(v[3]), "ask_vol": _f(v[4]), "open_interest": _f(v[5]),
            "pct": _f(v[6]), "strike": _f(v[7]), "prev_close": _f(v[8]),
            "open": _f(v[9]), "limit_up": _f(v[10]), "limit_down": _f(v[11]),
            "name": v[37], "high": _f(v[39]), "low": _f(v[40]),
            "volume": _f(v[41]), "amount": _f(v[42])}


def parse_greeks(raw: List[str]) -> Dict:
    """希腊字母：raw[1:4] 为 3 个空串必须跳过（否则字段全错位）。"""
    if len(raw) < 16:
        return {}
    v = [raw[0]] + raw[4:]
    return {"delta": _f(v[2]), "gamma": _f(v[3]), "theta": _f(v[4]),
            "vega": _f(v[5]), "iv": _f(v[6]), "theory": _f(v[12])}


def build_option_row(trade_date: str, option_code: str, underlying: str,
                     call_put: str, expiry_month: str, tq: Dict, greeks: Dict) -> Dict:
    row = {"trade_date": pd.to_datetime(trade_date).date(),
           "option_code": option_code, "underlying": underlying,
           "call_put": call_put, "expiry_month": expiry_month}
    row.update({k: v for k, v in tq.items()})
    row.update({k: v for k, v in greeks.items()})
    return row


def _sina_get(list_param: str) -> str:
    r = requests.get(f"https://hq.sinajs.cn/list={list_param}",
                     headers=_HDR, timeout=10)
    r.encoding = "gbk"
    return r.text


def _parse_batch(text: str) -> Dict[str, List[str]]:
    """批量响应 → {code: fields}（每行 var hq_str_CON_OP_xxx="..."）。"""
    return {m.group(1): m.group(2).split(",") for m in _LINE_RE.finditer(text)}


def _fetch_months(underlying: str) -> List[str]:
    cate = UNDERLYINGS[underlying]
    url = ("https://stock.finance.sina.com.cn/futures/api/openapi.php/"
           f"StockOptionService.getStockName?exchange=null&cate={cate}")
    months = requests.get(url, headers=_HDR, timeout=10).json()["result"]["data"]["contractMonth"]
    # 丢首个（重复项）、转 YYMM、保序去重
    seen, out = set(), []
    for m in months[1:]:
        mm = m.replace("-", "")[2:]
        if mm not in seen:
            seen.add(mm); out.append(mm)
    return out


def _fetch_contract_codes(underlying: str, month: str, call: bool) -> List[str]:
    flag = "OP_UP_" if call else "OP_DOWN_"
    text = _sina_get(f"{flag}{underlying}{month}")
    if '"' not in text:
        return []
    return [c.replace("CON_OP_", "") for c in text.split('"')[1].split(",")
            if c.startswith("CON_OP_")]


def _collect_day(trade_date: str) -> pd.DataFrame:
    rows, bad = [], 0
    for underlying in UNDERLYINGS:
        try:
            months = _fetch_months(underlying)
        except Exception as exc:
            logger.warning(f"期权月份获取失败 {underlying}: {exc}")
            continue
        for month in months:
            for call in (True, False):
                codes = _fetch_contract_codes(underlying, month, call)
                if not codes:
                    continue
                cp = "C" if call else "P"
                for i in range(0, len(codes), _BATCH):
                    batch = codes[i:i + _BATCH]
                    tq_map = _parse_batch(_sina_get(",".join(f"CON_OP_{c}" for c in batch)))
                    gk_map = _parse_batch(_sina_get(",".join(f"CON_SO_{c}" for c in batch)))
                    for c in batch:
                        tq = parse_tquote(tq_map.get(c, []))
                        if not tq:
                            bad += 1
                            continue
                        greeks = parse_greeks(gk_map.get(c, []))
                        rows.append(build_option_row(trade_date, c, underlying,
                                                     cp, month, tq, greeks))
    if bad:
        logger.warning(f"期权坏行丢弃 {bad} 条（字段数不足）")
    return pd.DataFrame(rows)


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，ETF期权跳过。"
    df = _collect_day(trade_date)
    tried, inserted = replace_day_then_insert(TABLE_NAME, trade_date, df)
    return f"{trade_date} ETF期权完成，{len(df)} 合约，写入 {tried}/{inserted} 条。"


def run_backfill(*args, **kwargs) -> str:
    raise NotImplementedError(
        "ETF期权为实时快照源（新浪 hq.sinajs），无历史数据不可回补；"
        "仅支持前向积累（每交易日盘后采当日收盘快照）。")
