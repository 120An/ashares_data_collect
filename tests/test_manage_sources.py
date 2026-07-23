"""manage_sources CLI 单测：validate 退出码 / list 过滤输出。test 子命令走真网络，
仅测 adapter 分派的不支持分支。"""

import manage_sources as ms
from data_collect.utils.source_registry import Source


def _src(**kw):
    base = dict(id="s1", adapter="rss", channel="policy", job="news_policy",
                url="https://x/f.xml")
    base.update(kw)
    return Source(**base)


def test_validate_ok(monkeypatch, capsys):
    monkeypatch.setattr(ms.sr, "validate_registry", lambda: [])
    monkeypatch.setattr(ms.sr, "load_all", lambda: [_src()])
    assert ms.main(["validate"]) == 0
    assert "校验通过" in capsys.readouterr().out


def test_validate_fail(monkeypatch, capsys):
    monkeypatch.setattr(ms.sr, "validate_registry", lambda: ["id 重复: x"])
    assert ms.main(["validate"]) == 1
    assert "id 重复" in capsys.readouterr().out


def test_list_filters(monkeypatch, capsys):
    monkeypatch.setattr(ms.sr, "load_all", lambda: [
        _src(id="a1"),
        _src(id="b1", job="news_us", channel="us_news"),
        _src(id="c1", enabled=False),
    ])
    assert ms.main(["list", "--job", "news_policy"]) == 0
    out = capsys.readouterr().out
    assert "a1" in out and "c1" in out and "b1" not in out

    assert ms.main(["list", "--disabled"]) == 0
    out = capsys.readouterr().out
    assert "c1" in out and "a1" not in out


def test_test_unsupported_source(monkeypatch, capsys):
    """无冒烟实现的 akshare 源 → 退出码 2 + 清晰提示（NotImplementedError 兜住）。"""
    monkeypatch.setattr(ms.sr, "get_source", lambda i: _src(adapter="akshare"))
    assert ms.main(["test", "s1"]) == 2
    assert "冒烟失败" in capsys.readouterr().out
