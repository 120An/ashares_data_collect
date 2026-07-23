"""get_index_codes 测试（不依赖 xtquant 或数据库）。"""
from data_collect.utils.xtquant_utils import get_index_codes


def test_get_index_codes_merges_and_filters(monkeypatch):
    class FakeXtdata:
        def get_stock_list_in_sector(self, name):
            return ["000300.SH", "000905.SH", "399006.SZ", "395001.SZ", "395010.SZ"]

    monkeypatch.setattr(
        "data_collect.utils.xtquant_utils.require_xtdata", lambda: FakeXtdata()
    )
    result = get_index_codes()
    assert "899050.BJ" in result          # 手工补入北证50
    assert "000300.SH" in result
    assert "399006.SZ" in result
    assert "395001.SZ" not in result      # 395* 段剔除
    assert "395010.SZ" not in result
    assert result == sorted(result)       # 去重排序
