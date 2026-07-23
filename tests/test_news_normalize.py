"""news_normalize 单元测试：纯函数为主，不连库、不连 OpenSearch。

覆盖（计划 T5 + 设计决策）：
- make_id 三级策略各分支 + 决定论（同输入同 id）+ native 带 source 前缀防跨源撞车
  + url 分支跨源同 url 同 id + 三级 title 空用 content 替位 + 全空 ValueError；
- clean_text HTML 标签/实体、全半角（NFKC）、繁→简（opencc）、空白折叠、
  段落换行保留、None/空；
- normalize_time 字符串多格式、epoch 秒/毫秒（+8h 语义：已知 UTC epoch 断言北京
  时间串）、date-only、datetime/date/pd.Timestamp 对象、无法解析→None、None/NaN→None；
- tag_stocks 带后缀、裸 6 位按前缀推市场、8 位日期串不误配、简称词典命中、
  混合去重排序、空 title；
- load_name_dict mock 连接（不连真库）：dict 正确、空名称跳过、名称去空格。
"""

import hashlib
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from data_collect.utils import news_normalize as nn


# ---------- make_id ----------

def test_make_id_native_id_wins_with_source_prefix():
    """一级：native_id 非空 → source-native_id（前缀防跨源数字 id 撞车），优先于 url。"""
    doc = {"native_id": "12345", "source": "cls", "url": "https://example.com/a"}
    assert nn.make_id(doc) == "cls-12345"


def test_make_id_native_id_cross_source_no_collision():
    """不同源相同自增 id 不得同 id（create-only 下撞车会静默吞文章）。"""
    id_a = nn.make_id({"native_id": "1", "source": "cls"})
    id_b = nn.make_id({"native_id": "1", "source": "eastmoney"})
    assert id_a != id_b


def test_make_id_url_branch_sha1():
    """二级：无 native_id → sha1(url) hexdigest（40 位十六进制）。"""
    url = "https://example.com/news/42"
    expected = hashlib.sha1(url.encode("utf-8")).hexdigest()
    assert nn.make_id({"url": url, "source": "cls"}) == expected


def test_make_id_url_cross_source_same_id():
    """url 分支不加 source 前缀：同 URL 跨源天然去重是特性。"""
    url = "https://example.com/news/42"
    id_a = nn.make_id({"url": url, "source": "cls"})
    id_b = nn.make_id({"url": url, "source": "eastmoney"})
    assert id_a == id_b


def test_make_id_fallback_branch():
    """三级：source|pub_time|title 拼串 sha1。"""
    doc = {"source": "cctv", "pub_time": "2026-07-03 19:00:00", "title": "新闻联播"}
    expected = hashlib.sha1("cctv|2026-07-03 19:00:00|新闻联播".encode("utf-8")).hexdigest()
    assert nn.make_id(doc) == expected


def test_make_id_fallback_empty_title_uses_content():
    """三级 title 空 → content[:128] 替位（快讯可能无标题）。"""
    content = "盘面快讯" * 100  # 超过 128 字符，验证截断参与
    doc = {"source": "cls", "pub_time": "2026-07-03 09:30:00", "title": "", "content": content}
    expected = hashlib.sha1(
        f"cls|2026-07-03 09:30:00|{content[:128]}".encode("utf-8")
    ).hexdigest()
    assert nn.make_id(doc) == expected


def test_make_id_deterministic():
    """决定论：同输入两次调用必同 id（三分支各验一次）。"""
    docs = [
        {"native_id": 99, "source": "cls"},
        {"url": "https://example.com/x", "source": "cls"},
        {"source": "cctv", "pub_time": "2026-07-03", "title": "标题"},
    ]
    for doc in docs:
        assert nn.make_id(dict(doc)) == nn.make_id(dict(doc))


def test_make_id_all_empty_raises():
    """source/pub_time/title/content 全空 → ValueError（拒绝生成无意义 id）。"""
    with pytest.raises(ValueError):
        nn.make_id({})
    with pytest.raises(ValueError):
        nn.make_id({"native_id": "", "url": "", "source": "", "pub_time": "",
                    "title": "", "content": ""})


# ---------- clean_text ----------

def test_clean_text_strips_html_tags_and_entities():
    assert nn.clean_text("<p>你好&amp;世界</p>") == "你好&世界"


def test_clean_text_unescape_then_strip():
    """设计定序：先 unescape 再剥标签（&lt;b&gt; 还原成标签后一并剥除）。"""
    assert nn.clean_text("&lt;b&gt;加粗&lt;/b&gt;文本") == "加粗文本"


def test_clean_text_block_tags_become_newline():
    """<br>/</p> 转换行：避免剥标签后两句无分隔粘连。"""
    assert nn.clean_text("句子一<br>句子二") == "句子一\n句子二"
    assert nn.clean_text("<p>段一</p><p>段二</p>") == "段一\n段二"


def test_clean_text_fullwidth_to_halfwidth():
    assert nn.clean_text("ＡＢＣ１２３％") == "ABC123%"


def test_clean_text_traditional_to_simplified():
    assert nn.clean_text("臺灣經濟數據") == "台湾经济数据"
    assert nn.clean_text("滬深兩市成交額創新高") == "沪深两市成交额创新高"


def test_clean_text_collapses_inline_whitespace():
    """行内多空格/制表/全角空格折叠为单空格，行首尾修剪。"""
    assert nn.clean_text("你好　　世界   末尾  ") == "你好 世界 末尾"
    assert nn.clean_text("  a\t\tb  ") == "a b"


def test_clean_text_preserves_paragraph_newlines():
    """保留段落结构：单换行保留，>=3 连续换行折叠为空行分段。"""
    assert nn.clean_text("行一\n行二") == "行一\n行二"
    assert nn.clean_text("第一段\n\n\n\n第二段") == "第一段\n\n第二段"


def test_clean_text_none_and_empty():
    assert nn.clean_text(None) == ""
    assert nn.clean_text("") == ""
    assert nn.clean_text("   \n  ") == ""


def test_clean_text_comparison_operators_preserved():
    """裸比较符不当标签吞：财经正文 "PE<20"、"涨幅>5%" 须完整保留。

    标签正则以字母锚定（</?[a-zA-Z]...），"<20的股票中，涨幅>" 不再整段被吞。
    全角逗号经 NFKC 转半角属预期。
    """
    assert nn.clean_text("PE<20的股票中，涨幅>5%的有三只") == "PE<20的股票中,涨幅>5%的有三只"
    assert nn.clean_text("利率<3%且涨幅>5%") == "利率<3%且涨幅>5%"


def test_clean_text_escaped_lt_roundtrip():
    """源头正确转义的 &lt;20 经 unescape 还原后不得被当标签吞掉。"""
    assert nn.clean_text("动态PE&lt;20") == "动态PE<20"


# ---------- normalize_time ----------

def test_normalize_time_full_string():
    assert nn.normalize_time("2026-07-03 09:30:00") == "2026-07-03 09:30:00"


def test_normalize_time_minute_string():
    assert nn.normalize_time("2026-07-03 09:30") == "2026-07-03 09:30:00"


def test_normalize_time_iso_with_tz_suffix():
    """ISO 格式尾部时区截断（国内源恒 +08:00，截断即北京时间）。"""
    assert nn.normalize_time("2026-07-03T09:30:00+08:00") == "2026-07-03 09:30:00"
    assert nn.normalize_time("2026-07-03T09:30:00") == "2026-07-03 09:30:00"


def test_normalize_time_date_only():
    assert nn.normalize_time("2026-07-03") == "2026-07-03 00:00:00"
    assert nn.normalize_time("20260703") == "2026-07-03 00:00:00"


def test_normalize_time_epoch_seconds_utc_to_beijing():
    """+8h 语义：1767225600 = 2026-01-01 00:00:00 UTC → 北京 08:00:00。"""
    assert nn.normalize_time(1767225600) == "2026-01-01 08:00:00"


def test_normalize_time_epoch_millis():
    assert nn.normalize_time(1767225600123) == "2026-01-01 08:00:00"


def test_normalize_time_datetime_naive_as_beijing():
    """naive datetime 视为北京时间，原样格式化。"""
    assert nn.normalize_time(datetime(2026, 7, 3, 9, 30, 5)) == "2026-07-03 09:30:05"


def test_normalize_time_datetime_aware_converted():
    """tz-aware datetime 换算到北京时间（UTC 01:30 → 北京 09:30）。"""
    raw = datetime(2026, 7, 3, 1, 30, 0, tzinfo=timezone.utc)
    assert nn.normalize_time(raw) == "2026-07-03 09:30:00"


def test_normalize_time_pd_timestamp():
    assert nn.normalize_time(pd.Timestamp("2026-07-03 09:30:00")) == "2026-07-03 09:30:00"


def test_normalize_time_date_object():
    assert nn.normalize_time(date(2026, 7, 3)) == "2026-07-03 00:00:00"


def test_normalize_time_14digit_datetime_int_rejected():
    """14 位 YYYYMMDDHHMMSS 整数（财经接口常见）不得被当 epoch 毫秒静默产出 2612 年。"""
    assert nn.normalize_time(20260703143000) is None


def test_normalize_time_epoch_boundary_and_overflow_none():
    """恰落界/超界数值不得裸抛（Windows fromtimestamp OSError），按契约返回 None。"""
    assert nn.normalize_time(1e12) is None   # 界值：按秒解读 → 33658 年 → 换算溢出
    assert nn.normalize_time(4e15) is None   # 超界毫秒 → 换算溢出


def test_normalize_time_missing_returns_none():
    assert nn.normalize_time(None) is None
    assert nn.normalize_time(float("nan")) is None
    assert nn.normalize_time(pd.NaT) is None
    assert nn.normalize_time("") is None
    assert nn.normalize_time("   ") is None


def test_normalize_time_unparseable_warns_and_returns_none(caplog):
    """无法解析 → 记 warning（含 source 上下文）并返回 None。"""
    import logging
    with caplog.at_level(logging.WARNING, logger="data_collect.utils.news_normalize"):
        assert nn.normalize_time("不是时间", source="cls") is None
    assert any("cls" in rec.message for rec in caplog.records)


def test_normalize_time_weird_type_returns_none():
    assert nn.normalize_time(["2026-07-03"]) is None


# ---------- tag_stocks ----------

def test_tag_stocks_with_suffix():
    assert nn.tag_stocks("贵州茅台(600519.SH)放量大涨", {}) == ["600519.SH"]


def test_tag_stocks_bare_code_infers_market():
    assert nn.tag_stocks("600519 午后拉升", {}) == ["600519.SH"]     # 沪主板
    assert nn.tag_stocks("000001 平盘", {}) == ["000001.SZ"]         # 深主板
    assert nn.tag_stocks("300750 涨停", {}) == ["300750.SZ"]         # 创业板
    assert nn.tag_stocks("688981 上涨", {}) == ["688981.SH"]         # 科创板
    assert nn.tag_stocks("430047 异动", {}) == ["430047.BJ"]         # 北交所
    assert nn.tag_stocks("920001 上市", {}) == ["920001.BJ"]         # 北交所 920 段（先于 9 开头判 SH）


def test_tag_stocks_unknown_prefix_dropped():
    """非 A 股代码空间（如 51 开头 ETF）宁缺勿滥，不打标。"""
    assert nn.tag_stocks("512880 证券ETF 放量", {}) == []


def test_tag_stocks_date_string_no_false_positive():
    """8 位日期串不得误配出 6 位代码。"""
    assert nn.tag_stocks("20260703 市场综述", {}) == []


def test_tag_stocks_name_dict_hit():
    name_dict = {"宁德时代": "300750.SZ", "贵州茅台": "600519.SH"}
    assert nn.tag_stocks("宁德时代发布麒麟电池新品", name_dict) == ["300750.SZ"]


def test_tag_stocks_mixed_dedup_sorted():
    """正则命中 + 简称命中重复只留一份，输出去重排序。"""
    name_dict = {"贵州茅台": "600519.SH"}
    result = nn.tag_stocks("贵州茅台600519.SH领涨，000001跟涨", name_dict)
    assert result == ["000001.SZ", "600519.SH"]


def test_tag_stocks_empty_title():
    assert nn.tag_stocks("", {"贵州茅台": "600519.SH"}) == []
    assert nn.tag_stocks(None, {"贵州茅台": "600519.SH"}) == []


# ---------- load_name_dict ----------

class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed_sql = None

    def execute(self, sql):
        self.executed_sql = sql

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeConn:
    def __init__(self, rows):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_load_name_dict(monkeypatch):
    """mock 连接：{名称: 代码}；空名称跳过；名称去空格并 NFKC 对齐 clean_text。"""
    rows = [
        ("600519.SH", "贵州茅台"),
        ("000002.SZ", "万 科Ａ"),   # 含空格 + 全角字母 → "万科A"
        ("300750.SZ", ""),          # 空名称跳过
        ("430047.BJ", None),        # NULL 名称跳过
        ("601398.SH", "  "),        # 纯空白跳过
    ]
    fake_conn = _FakeConn(rows)
    monkeypatch.setattr(nn, "get_connection", lambda: fake_conn)

    result = nn.load_name_dict()
    assert result == {"贵州茅台": "600519.SH", "万科A": "000002.SZ"}
    # 查询须过滤 A 股代码空间并使用带引号的 InstrumentName 列
    assert "instrument_info" in fake_conn.cursor_obj.executed_sql
    assert '"InstrumentName"' in fake_conn.cursor_obj.executed_sql


def test_load_name_dict_empty_table(monkeypatch):
    monkeypatch.setattr(nn, "get_connection", lambda: _FakeConn([]))
    assert nn.load_name_dict() == {}


def test_load_name_dict_t2s_aligned_with_clean_text(monkeypatch):
    """词典键须与 clean_text 同构做 t2s：简体专名"乾照光电"被 t2s 改写（乾→干），
    词典不同步改写则该票的标题命中永远失败（系统性漏标）。"""
    monkeypatch.setattr(
        nn, "get_connection", lambda: _FakeConn([("300102.SZ", "乾照光电")])
    )
    name_dict = nn.load_name_dict()
    title = nn.clean_text("乾照光电发布业绩预告")
    assert nn.tag_stocks(title, name_dict) == ["300102.SZ"]


# ---------- normalize_stock_code（公告 secCode 权威规范化）----------

def test_normalize_stock_code_shenzhen():
    assert nn.normalize_stock_code("301191") == "301191.SZ"
    assert nn.normalize_stock_code("000001") == "000001.SZ"


def test_normalize_stock_code_shanghai():
    assert nn.normalize_stock_code("600519") == "600519.SH"
    assert nn.normalize_stock_code("688001") == "688001.SH"


def test_normalize_stock_code_beijing():
    # 北交所 920/83/87/43 号段（920 须先于 9→SH 规则命中）
    assert nn.normalize_stock_code("920001") == "920001.BJ"
    assert nn.normalize_stock_code("830799") == "830799.BJ"


def test_normalize_stock_code_keeps_explicit_suffix():
    assert nn.normalize_stock_code("000001.SZ") == "000001.SZ"
    assert nn.normalize_stock_code("600519.sh") == "600519.SH"   # 大小写归一


def test_normalize_stock_code_invalid_returns_none():
    assert nn.normalize_stock_code("") is None
    assert nn.normalize_stock_code(None) is None
    assert nn.normalize_stock_code("12345") is None        # 非 6 位
    assert nn.normalize_stock_code("abcdef") is None
    assert nn.normalize_stock_code("199999") is None       # 6 位但号段推不出市场


# ---------- load_a_share_codes（个股新闻全市场裸码）----------

def test_load_a_share_codes(monkeypatch):
    """instrument_info 的 A 股 stock_code → 裸 6 位、去重保序。"""
    rows = [
        ("600519.SH",), ("000001.SZ",), ("920019.BJ",),
        ("600519.SH",),          # 重复 → 去重
    ]
    fake_conn = _FakeConn(rows)
    monkeypatch.setattr(nn, "get_connection", lambda: fake_conn)
    codes = nn.load_a_share_codes()
    assert codes == ["600519", "000001", "920019"]         # 裸 6 位、保序、去重
    assert "instrument_info" in fake_conn.cursor_obj.executed_sql
