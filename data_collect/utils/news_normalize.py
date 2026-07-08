"""新闻规范化工具：三级 _id、文本清洗、时间规范化、股票打标（架构 §5.2/§5.3）。

纯函数为主，唯一连库入口 load_name_dict（可 monkeypatch）。各函数职责：
- make_id：三级幂等 _id（native id > sha1(url) > sha1(source|pub_time|title)）；
- clean_text：去 HTML、全半角、繁→简、空白规整（保留段落换行）；
- normalize_time：多格式 → 北京时间串，失败返回 None（回退 fetch_time 由调用方管）；
- tag_stocks：正则代码 + 简称词典打标；
- load_name_dict：从 instrument_info 快照表加载 {简称: 代码}。
"""

from __future__ import annotations

import hashlib
import html
import logging
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

import pandas as pd

from data_collect.utils.db import get_connection

logger = logging.getLogger(__name__)

# 北京时区（xtdata 同款 +8h 语义，无夏令时）
_TZ_BEIJING = timezone(timedelta(hours=8))

# 数值 epoch 换算后的年份合理带（与 opensearch_utils._YEAR_MIN/MAX 一致，不 import
# 以免拖入 opensearchpy 依赖）：新闻时间戳合理域极窄，界外即上游事故——如 14 位
# YYYYMMDDHHMMSS 整数被误当 epoch 毫秒会静默产出 2612 年
_YEAR_MIN, _YEAR_MAX = 2000, 2100


# ---------- make_id ----------

def make_id(doc: dict) -> str:
    """生成幂等 _id，三级策略，同输入必同输出（sha1 于 utf-8 编码串）。

    1. `native_id` 非空 → ``f"{source}-{native_id}"``。对 spec"直接用"的安全强化：
       加 source 前缀防跨源数字 id 撞车——不同源的自增 id 可能相同，裸用会在
       create-only 写入下静默吞掉另一篇文章；
    2. 否则 `url` 非空 → ``sha1(url)``。不加前缀：同 URL 跨源天然去重是特性；
    3. 否则 → ``sha1(f"{source}|{pub_time}|{title}")``。title 空时用
       ``content[:128]`` 替位（快讯可能无标题）；四要素全空抛 ValueError。

    消费方契约：走三级分支时 pub_time 必须以稳定形态传入（统一传 normalize_time
    之后的串）——否则同一文档两次采集因格式漂移得到不同 _id，create-only 去重失效。
    """
    source = str(doc.get("source") or "")

    native_id = doc.get("native_id")
    if native_id not in (None, ""):
        return f"{source}-{native_id}"

    url = doc.get("url")
    if url:
        return hashlib.sha1(str(url).encode("utf-8")).hexdigest()

    pub_time = str(doc.get("pub_time") or "")
    title = str(doc.get("title") or "")
    if not title:
        title = str(doc.get("content") or "")[:128]
    if not (source or pub_time or title):
        raise ValueError("make_id: source/pub_time/title/content 全空，无法生成 _id")
    return hashlib.sha1(f"{source}|{pub_time}|{title}".encode("utf-8")).hexdigest()


# ---------- clean_text ----------

# 剥 HTML：先把块级分隔（<br>/</p>）转换行，避免剥标签后两句无分隔粘连
_BLOCK_TAG_RE = re.compile(r"<\s*(?:br\s*/?|/p)\s*>", re.IGNORECASE)
# 标签必以字母开头（可带闭合斜杠）：财经正文常见 "PE<20"、"涨幅>5%" 比较符，
# 裸 <[^>]+> 会把 "<20的股票中，涨幅>" 整段当标签吞掉（含源头正确转义的 &lt;
# 经先 unescape 还原后误吞）；{0,200} 长度上限防极端吞噬
_TAG_RE = re.compile(r"</?[a-zA-Z][^>]{0,200}>")
# 行内空白（含全角空格；NFKC 后全角空格已转半角，保留 　 属双保险）
_INLINE_WS_RE = re.compile(r"[ \t　]+")

# opencc 繁→简转换器：模块级 lazy 单例（构造有成本，避免每次调用重建）
_opencc = None


def _get_opencc():
    global _opencc
    if _opencc is None:
        from opencc import OpenCC  # opencc-python-reimplemented
        _opencc = OpenCC("t2s")
    return _opencc


def clean_text(s) -> str:
    """清洗文本：去 HTML → 全半角（NFKC）→ 繁→简（opencc）→ 空白规整。

    去 HTML 用 html.unescape 后剥字母开头的标签（不引 bs4，接口源正文本就近纯
    文本，正则够用；"PE<20" 等比较符不吞）。空白规整保留段落换行（联播长文的
    段落结构对检索/阅读有意义）：
    行内空白折叠为单空格、各行 strip、>=3 连续换行折叠为空行分段、整体 strip。
    None/空返回 ""。
    """
    if not s:
        return ""
    s = html.unescape(str(s))
    s = _BLOCK_TAG_RE.sub("\n", s)
    s = _TAG_RE.sub("", s)
    s = unicodedata.normalize("NFKC", s)
    s = _get_opencc().convert(s)
    # 空白规整（统一换行符后按行处理，保留段落结构）
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = "\n".join(_INLINE_WS_RE.sub(" ", line).strip() for line in s.split("\n"))
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


# ---------- normalize_time ----------

# 字符串格式逐个尝试（ISO 尾部时区先截断再配 T 格式）
_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%Y%m%d",
)

# 源特有格式钉死处：发现某源固定输出特殊格式时在此登记 {source: 解析逻辑}，
# 不预先虚构未观测格式（当前各源格式均被 _TIME_FORMATS 覆盖）
_SOURCE_HINTS: Dict[str, object] = {}


def normalize_time(raw, source: str = "") -> str | None:
    """规范化时间到北京时间串 ``"YYYY-MM-DD HH:MM:SS"``，失败/缺失返回 None。

    调用方（job）负责对 None 回退 fetch_time 并在信封加 ``time_estimated: true``
    标记——本函数不管信封。`source` 参数当前用于日志上下文与将来按源固化特例
    （发现源特有格式时在 `_SOURCE_HINTS` 处钉死）。

    支持输入：
    - datetime/pd.Timestamp：naive 视为北京时间，tz-aware 换算到北京；NaT → None；
    - date：补 " 00:00:00"；
    - int/float：>1e12 视为 epoch 毫秒、>1e9 视为 epoch 秒（UTC→北京 +8h）；
      换算结果限 2000~2100 年合理带，界外（如 14 位 YYYYMMDDHHMMSS 被误当
      epoch）与换算溢出均 warning+None；NaN → None；其余量级视为无法解析；
    - str：依次试 _TIME_FORMATS；ISO 带尾部时区（+08:00/Z）截断前 19 字符再配
      （国内源 tz 恒为 +08:00，截断即北京时间）；空串 → None。
    """
    if raw is None:
        return None

    # datetime 分支（pd.Timestamp 为 datetime 子类，NaT 亦落此支——先 isna 拦截）
    if isinstance(raw, datetime):
        if pd.isna(raw):
            return None
        if raw.tzinfo is not None:
            raw = raw.astimezone(_TZ_BEIJING)
        return raw.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(raw, date):
        return f"{raw:%Y-%m-%d} 00:00:00"

    # 数值分支：epoch 秒/毫秒（bool 是 int 子类，True→1 量级不足自然落到告警）
    if isinstance(raw, (int, float)):
        if pd.isna(raw):
            return None
        value = float(raw)
        if value > 1e12:  # epoch 毫秒
            value /= 1000.0
        if value > 1e9:   # epoch 秒
            try:
                dt = datetime.fromtimestamp(value, tz=_TZ_BEIJING)
            except (OSError, OverflowError, ValueError):
                # 量级像 epoch 但换算溢出（如恰为 1e12 的秒值 → 33658 年，
                # Windows fromtimestamp 直接 OSError）——按无法解析处理
                dt = None
            if dt is not None and _YEAR_MIN <= dt.year <= _YEAR_MAX:
                return dt.strftime("%Y-%m-%d %H:%M:%S")
        logger.warning(
            f"normalize_time: 数值不在合理 epoch 域（source={source}）: {raw!r}"
        )
        return None

    # 字符串分支
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        candidates = [text]
        if len(text) > 19 and text[10:11] == "T":
            candidates.append(text[:19])  # ISO 尾部时区/毫秒截断
        for cand in candidates:
            for fmt in _TIME_FORMATS:
                try:
                    return datetime.strptime(cand, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

    logger.warning(
        f"normalize_time: 无法解析（source={source}）: {str(raw)[:50]!r}"
    )
    return None


# ---------- tag_stocks ----------

# 6 位代码，可带 .SH/.SZ/.BJ 后缀；前后无邻接数字（避免 8 位日期串误配）
_STOCK_CODE_RE = re.compile(r"(?<!\d)(\d{6})(?:\.(SH|SZ|BJ))?(?!\d)")


def _infer_market(code: str) -> str | None:
    """裸 6 位代码按前缀推市场后缀；非 A 股代码空间返回 None（宁缺勿滥）。"""
    # 北交所优先判（920xx 段须先于"9 开头→SH"规则）
    if code.startswith(("43", "83", "87", "92")):
        return ".BJ"
    if code.startswith(("60", "68", "9")):   # 沪主板/科创板/沪 B
        return ".SH"
    if code.startswith(("00", "30", "2")):   # 深主板/创业板/深 B
        return ".SZ"
    return None


def tag_stocks(title, name_dict: Dict[str, str]) -> List[str]:
    """从标题打标股票代码，返回去重排序的规范代码列表（如 ["600519.SH"]）。

    - 正则命中：带后缀直接规范化；裸 6 位按前缀推市场，推不出则丢弃；
    - 简称词典命中：``name_dict`` 为 {简称: 代码}，对 title 做子串匹配；
    - title 空/None → []。
    """
    if not title:
        return []
    title = str(title)

    codes = set()
    for code, suffix in _STOCK_CODE_RE.findall(title):
        if suffix:
            codes.add(f"{code}.{suffix}")
        else:
            market = _infer_market(code)
            if market:
                codes.add(code + market)

    for name, code in name_dict.items():
        if name and name in title:
            codes.add(code)

    return sorted(codes)


# ---------- normalize_stock_code ----------

# 严格 6 位代码 + 可选 .SH/.SZ/.BJ 后缀（用于 secCode 等权威单码规范化，非文本打标）
_SECCODE_RE = re.compile(r"(\d{6})(?:\.(SH|SZ|BJ))?$")


def normalize_stock_code(code) -> str | None:
    """单个股票代码 → 规范 ``"NNNNNN.SH/SZ/BJ"``；非 A 股代码空间返回 None。

    公告 ``secCode`` 是权威标的代码（区别于 tag_stocks 从标题正则打标——标题常含
    保荐机构/律所等他方公司名，正则打标会误标）。已带后缀直接规范化（大小写归一），
    裸 6 位按号段推市场（复用 _infer_market，北交所 920/83/87/43 优先），推不出返回 None。
    """
    if not code:
        return None
    text = str(code).strip().upper()
    m = _SECCODE_RE.fullmatch(text)
    if not m:
        return None
    digits, suffix = m.group(1), m.group(2)
    if suffix:
        return f"{digits}.{suffix}"
    market = _infer_market(digits)
    return digits + market if market else None


# ---------- load_name_dict ----------

# instrument_info 快照表（jobs/a_share_instrument.py 自动建表）：
# stock_code VARCHAR(20) PK（形如 000001.SZ），"InstrumentName" TEXT 证券名称。
# 列名建表时带引号保留大小写，查询必须同样引号；正则过滤只取 A 股代码空间。
_NAME_DICT_SQL = r"""
    SELECT "stock_code", "InstrumentName"
    FROM "instrument_info"
    WHERE "stock_code" ~ '^\d{6}\.(SH|SZ|BJ)$'
"""


def load_name_dict() -> Dict[str, str]:
    """从 instrument_info 加载 {简称: 代码}（tag_stocks 词典用）。

    名称做与 clean_text 同构的规整（NFKC + t2s + 去空白），保证与清洗后的标题
    对齐：全角 "Ａ" 须转半角、"乾照光电" 等含繁体字形的简体专名会被 clean_text
    的 t2s 改写（乾→干），词典键不同步改写则这些票永远打不中；空名称跳过；
    含 "ST" 保留（ST 股也是股）。
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_NAME_DICT_SQL)
            rows = cur.fetchall()

    name_dict: Dict[str, str] = {}
    for code, name in rows:
        if not name:
            continue
        name = _get_opencc().convert(unicodedata.normalize("NFKC", str(name)))
        name = re.sub(r"\s+", "", name)
        if not name:
            continue
        name_dict[name] = code
    return name_dict


# instrument_info 的 A 股代码集（个股新闻全市场扫描用），同 _NAME_DICT_SQL 表/过滤
_A_SHARE_CODES_SQL = r"""
    SELECT "stock_code"
    FROM "instrument_info"
    WHERE "stock_code" ~ '^\d{6}\.(SH|SZ|BJ)$'
"""


def load_a_share_codes() -> List[str]:
    """从 instrument_info 取在市 A 股**裸 6 位**码（stock_news_em 的 symbol 形态）。

    与 load_name_dict 同源（同表/同 A 股正则过滤）。去重保序。**跨平台**（读 PG，
    不依赖 xtquant/Windows）。注意与 xtquant_utils.get_a_share_codes（走 QMT、仅
    Windows）区分——本函数命名 load_a_share_codes 避免混淆。
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_A_SHARE_CODES_SQL)
            rows = cur.fetchall()

    codes: List[str] = []
    seen: set = set()
    for (stock_code,) in rows:
        bare = str(stock_code).split(".")[0]
        if len(bare) == 6 and bare.isdigit() and bare not in seen:
            seen.add(bare)
            codes.append(bare)
    return codes
