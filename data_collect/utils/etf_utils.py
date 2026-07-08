"""ETF 代码工具：后缀码↔裸6位、市场推导、ETF 号段判定。"""
from __future__ import annotations

# 沪市 ETF/场内基金号段前缀；深市 ETF/LOF 号段前缀
_SH_ETF_PREFIXES = ("50", "51", "52", "56", "58")   # 510/511/512/513/515/516/518/560-563/588/589 等
_SZ_ETF_PREFIXES = ("15", "16")                       # 159 ETF / 16x LOF


def to_bare_code(code: str) -> str:
    """'510300.SH' / 'sh510300' / '510300' -> '510300'。"""
    text = str(code).strip()
    if "." in text:
        return text.split(".", 1)[0]
    if len(text) == 8 and text[:2].lower() in ("sh", "sz"):
        return text[2:]
    return text


def infer_market(bare_code: str) -> str:
    """裸6位 -> 'SH'/'SZ'（按号段）。"""
    c = str(bare_code)
    if c[:2] in _SZ_ETF_PREFIXES:
        return "SZ"
    return "SH"


def with_suffix(bare_code: str) -> str:
    """裸6位 -> '510300.SH'。"""
    return f"{bare_code}.{infer_market(bare_code)}"


def is_etf_code(bare_code: str) -> bool:
    """按号段判定是否 ETF/场内基金（防止股票混入）。"""
    c = str(bare_code)
    if len(c) != 6 or not c.isdigit():
        return False
    return c[:2] in _SH_ETF_PREFIXES or c[:2] in _SZ_ETF_PREFIXES
