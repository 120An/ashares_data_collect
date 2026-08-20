"""news_policy 单测：全 mock。③ 政策/宏观（注册 RSS/API + 财经早餐）。"""

import datetime
import json
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from data_collect.jobs import news_policy as npo
from data_collect.utils.source_registry import Source

_FETCH_TIME = "2026-07-08 15:00:00"


# 测试源表：替代注册表，patch 到 npo._policy_sources（与真实 config 解耦）
def _src(sid, url, channel="policy", *, adapter="rss", proxy_url=""):
    return Source(id=sid, adapter=adapter, channel=channel, job="news_policy",
                  url=url, proxy_url=proxy_url,
                  headers={"User-Agent": "test"}, timeout=17)


_TEST_POLICY = {
    "govcn_policy": _src("govcn_policy", "https://gov.cn/policy.xml", "policy"),
    "govcn_gwy": _src("govcn_gwy", "https://sousuo.www.gov.cn/search-gov/data",
                       adapter="api"),
    "stats": _src("stats", "https://stats.gov.cn/rss.xml", "policy"),
}


def _entry(title="国务院关于印发《提振消费方案》的通知",
           link="https://www.gov.cn/zhengce/1.htm", guid=None,
           summary="方案提出支持贵州茅台等消费龙头……",
           parsed=time.struct_time((2026, 7, 8, 2, 30, 0, 0, 0, 0))):
    """合成 RSS entry（dict 即可——实现只用 .get；parsed 为 UTC struct_time）。"""
    entry = {"title": title, "link": link, "summary": summary}
    if guid is not None:
        entry["id"] = guid
    if parsed is not None:
        entry["published_parsed"] = parsed
    return entry


def _cjzc_df(rows):
    return pd.DataFrame(rows, columns=["标题", "摘要", "发布时间", "链接"])


def _cjzc_row(title="财经早餐0708", url="https://em.com/cjzc/1.html"):
    return {"标题": title, "摘要": f"{title} 摘要内容",
            "发布时间": "2026-07-08 07:30:00", "链接": url}


def _govcn_item(summary="修改并完善住房公积金制度。"):
    return {
        "id": "7078478",
        "title": "国务院关于修改《住房公积金管理条例》的决定",
        "summary": summary,
        "pubtimeStr": "2026.08.18",
        "pubtime": 1786982400000,
        "puborg": "国务院",
        "url": "https://www.gov.cn/zhengce/zhengceku/202608/content_7078478.htm",
        "pcode": "001",
        "index": 1,
    }


# ---------- 信封契约 ----------

def test_entry_envelope_contract():
    env = npo._entry_to_envelope("govcn_policy", "policy",
                                 _entry(guid="gov-123"), _FETCH_TIME,
                                 {"贵州茅台": "600519.SH"})
    assert env["_id"] == "govcn_policy-gov-123"          # 一级：source-native_id
    assert env["channel"] == "policy"
    assert env["source"] == "govcn_policy"
    assert env["pub_time"] == "2026-07-08 10:30:00"      # UTC 02:30 → 北京 10:30
    assert env["url"] == "https://www.gov.cn/zhengce/1.htm"
    assert "600519.SH" in env["stocks"]                  # 摘要提及 → 打标
    assert env["vec_status"] == "pending"
    assert "time_estimated" not in env
    assert env["raw_title"].startswith("国务院")          # 原文仅归档


def test_entry_without_guid_uses_url_id():
    env = npo._entry_to_envelope("stats", "policy", _entry(), _FETCH_TIME, {})
    import hashlib
    assert env["_id"] == hashlib.sha1(
        b"https://www.gov.cn/zhengce/1.htm").hexdigest()  # 二级：sha1(url)


def test_entry_without_time_falls_back_and_marks():
    e1 = npo._entry_to_envelope("stats", "policy", _entry(parsed=None),
                                _FETCH_TIME, {})
    e2 = npo._entry_to_envelope("stats", "policy", _entry(parsed=None),
                                "2026-07-08 16:00:00", {})
    assert e1["pub_time"] == _FETCH_TIME and e1["time_estimated"] is True
    assert e1["_id"] == e2["_id"]                        # 决定论：fetch_time 不参与 _id


def test_entry_html_summary_cleaned():
    env = npo._entry_to_envelope(
        "govcn_policy", "policy",
        _entry(summary="<p>第一段Ａ</p><p>第二段</p>"), _FETCH_TIME, {})
    assert "<p>" not in env["content"] and "第一段A" in env["content"]


def test_govcn_gwy_api_envelope_contract_and_raw_evidence():
    item = _govcn_item()
    before = dict(item)
    env = npo._govcn_gwy_to_envelope(item, _FETCH_TIME, {})

    assert env["_id"] == "govcn_gwy-7078478"
    assert env["title"] == item["title"]
    assert env["content"] == item["summary"]
    assert env["url"] == item["url"]
    assert env["source"] == "govcn_gwy" and env["channel"] == "policy"
    assert env["pub_time"] == "2026-08-18 00:00:00"
    assert env["time_estimated"] is True
    assert env["vec_status"] == "pending"
    raw = json.loads(env["raw_content"])
    assert {key: raw[key] for key in (
        "id", "title", "summary", "pubtimeStr", "puborg", "url"
    )} == {key: item[key] for key in (
        "id", "title", "summary", "pubtimeStr", "puborg", "url"
    )}
    assert item == before


def test_govcn_gwy_api_empty_summary_falls_back_to_title():
    env = npo._govcn_gwy_to_envelope(_govcn_item(summary=""), _FETCH_TIME, {})
    assert env["content"] == env["title"]


def test_govcn_gwy_date_precision_is_not_presented_as_exact_time():
    env = npo._govcn_gwy_to_envelope(_govcn_item(), _FETCH_TIME, {})
    assert env["pub_time"].endswith("00:00:00")
    assert env["time_estimated"] is True
    assert "publish_time_precision" not in env  # legacy envelope; dual projection owns it


class _ApiResponse:
    def __init__(self, payload=None, *, status_code=200, json_error=None):
        self.payload = payload
        self.status_code = status_code
        self.json_error = json_error

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class _ApiSession:
    def __init__(self, response):
        self.response = response
        self.trust_env = True
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs, self.trust_env))
        return self.response

    def close(self):
        self.closed = True


def _patch_api_session(monkeypatch, response):
    import requests

    session = _ApiSession(response)
    monkeypatch.setattr(requests, "Session", lambda: session)
    return session


def test_fetch_govcn_gwy_api_uses_registry_facts_and_ignores_ambient_proxy(
        monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    response = _ApiResponse({
        "code": 200,
        "msg": "操作成功",
        "searchVO": {"listVO": [_govcn_item()]},
    })
    session = _patch_api_session(monkeypatch, response)

    rows = npo._fetch_govcn_gwy_api(_TEST_POLICY["govcn_gwy"])

    assert rows == [_govcn_item()]
    assert session.closed is True
    url, kwargs, trust_env = session.calls[0]
    assert url == _TEST_POLICY["govcn_gwy"].url
    assert trust_env is False
    assert "proxies" not in kwargs
    assert "verify" not in kwargs
    assert kwargs["headers"] == {"User-Agent": "test"}
    assert kwargs["timeout"] == 17
    assert kwargs["params"] == npo._GOVCN_GWY_API_PARAMS


def test_fetch_govcn_gwy_api_respects_explicit_source_proxy(monkeypatch):
    response = _ApiResponse({"code": "200", "searchVO": {"listVO": []}})
    session = _patch_api_session(monkeypatch, response)
    source = _src("govcn_gwy", _TEST_POLICY["govcn_gwy"].url,
                  adapter="api", proxy_url="http://127.0.0.1:8899")

    assert npo._fetch_govcn_gwy_api(source) == []
    _, kwargs, trust_env = session.calls[0]
    assert trust_env is False
    assert kwargs["proxies"] == {
        "http": "http://127.0.0.1:8899",
        "https": "http://127.0.0.1:8899",
    }


def test_fetch_govcn_gwy_api_rejects_non_success_business_code(monkeypatch):
    response = _ApiResponse({
        "code": 500,
        "msg": "操作失败",
        "searchVO": {"listVO": []},
    })
    _patch_api_session(monkeypatch, response)

    with pytest.raises(RuntimeError, match=r"业务状态失败: code=500"):
        npo._fetch_govcn_gwy_api(_TEST_POLICY["govcn_gwy"])


@pytest.mark.parametrize(
    "response, message",
    [
        (_ApiResponse(status_code=503), "HTTP 503"),
        (_ApiResponse(json_error=ValueError("bad json")), "JSON 解析失败"),
        (_ApiResponse({}), "searchVO"),
        (_ApiResponse({"searchVO": {}}), "listVO"),
        (_ApiResponse({"searchVO": {"listVO": {}}}), "应为 list"),
    ],
)
def test_fetch_govcn_gwy_api_rejects_http_and_malformed_payload(
        monkeypatch, response, message):
    _patch_api_session(monkeypatch, response)
    with pytest.raises(RuntimeError, match=message):
        npo._fetch_govcn_gwy_api(_TEST_POLICY["govcn_gwy"])


def test_cjzc_envelope_contract():
    env = npo._cjzc_to_envelope(pd.Series(_cjzc_row()), _FETCH_TIME, {})
    assert env["channel"] == "media" and env["source"] == "em_cjzc"
    assert env["pub_time"] == "2026-07-08 07:30:00"
    assert env["url"] == "https://em.com/cjzc/1.html"


# ---------- run：四源合并/隔离/归档 ----------

@pytest.fixture
def env(monkeypatch):
    calls = SimpleNamespace(bulk=[], archived=[], alerts=[])
    health_sink = npo.source_health_shadow.InMemoryShadowSink()
    e = SimpleNamespace(calls=calls,
                        health_sink=health_sink,
                        rss={"govcn_policy": [_entry(guid="a1")],
                             "stats": [_entry(guid="a3",
                                              link="https://stats.gov.cn/3.htm")]},
                        api=[_govcn_item()],
                        cjzc=_cjzc_df([_cjzc_row()]))

    def fake_rss(url, **kwargs):
        for source, src in _TEST_POLICY.items():
            if src.adapter == "rss" and src.url == url:
                value = e.rss[source]
                if isinstance(value, Exception):
                    raise value
                return value
        raise AssertionError(f"未知 RSS url: {url}")

    def fake_api(source):
        assert source.id == "govcn_gwy" and source.adapter == "api"
        if isinstance(e.api, Exception):
            raise e.api
        return e.api

    def fake_cjzc():
        if isinstance(e.cjzc, Exception):
            raise e.cjzc
        return e.cjzc

    monkeypatch.setattr(npo, "_policy_sources", lambda: dict(_TEST_POLICY))
    monkeypatch.setattr(npo, "_fetch_rss_entries", fake_rss)
    monkeypatch.setattr(npo, "_fetch_govcn_gwy_api", fake_api)
    monkeypatch.setattr(npo, "_fetch_cjzc", fake_cjzc)
    monkeypatch.setattr(npo, "_now",
                        lambda: datetime.datetime(2026, 7, 8, 15, 0, 0))
    monkeypatch.setattr(npo, "_flush_spool_safe", lambda: None)
    monkeypatch.setattr(npo.news_archive, "load_ids", lambda s, d: set())
    monkeypatch.setattr(npo.news_archive, "append",
                        lambda s, d, envs: (calls.archived.append(
                            (s, d, list(envs))), len(list(envs)))[1])
    monkeypatch.setattr(npo.osu, "get_client", lambda: object())
    monkeypatch.setattr(npo.osu, "bulk_create",
                        lambda c, docs: (calls.bulk.append(list(docs)),
                                         (len(list(docs)), 0))[1])
    monkeypatch.setattr(npo, "_alert", lambda msg: calls.alerts.append(msg))
    monkeypatch.setattr(npo.nn, "load_name_dict", lambda: {})
    monkeypatch.setattr(npo, "_SOURCE_HEALTH_SINK", health_sink)
    return e


def test_run_merges_four_sources(env):
    summary = npo.run()
    assert len(env.calls.bulk) == 1
    docs = env.calls.bulk[0]
    assert len(docs) == 4
    assert {d["channel"] for d in docs} == {"policy", "media"}
    assert all("raw_title" not in d for d in docs)       # 入库剥 raw_*
    archived_sources = {a[0] for a in env.calls.archived}
    assert archived_sources == {"govcn_policy", "govcn_gwy", "stats", "em_cjzc"}
    assert "政策:" in summary and "入库新增 4" in summary
    observations = [
        item for item in env.health_sink.records
        if item.observation_type is npo.source_health_shadow.ObservationType.COLLECT
    ]
    assert len(observations) == 4
    assert all(
        item.outcome is npo.source_health_shadow.ObservationOutcome.SUCCESS
        for item in observations
    )
    gov = next(item for item in observations if item.source_id == "govcn_policy")
    assert gov.collected_item_count == 1 and gov.new_item_count is None
    assert gov.completeness_info["archive_new_item_count"] == 1
    assert gov.parse_failure_count == 0
    assert gov.last_item_publish_time.isoformat() == "2026-07-08T10:30:00+08:00"
    assert gov.job_run_id.startswith("jrun_")
    assert gov.attempt_no == 1
    gwy = next(item for item in observations if item.source_id == "govcn_gwy")
    assert gwy.last_item_publish_time.isoformat() == "2026-08-18T00:00:00+08:00"
    assert {item.job_run_id for item in observations} == {gov.job_run_id}


def test_run_isolates_failed_source_and_alerts(env):
    env.rss["stats"] = RuntimeError("RSS HTTP 503")
    summary = npo.run()
    assert "stats 失败" in summary and "失败源[stats]" in summary
    assert len(env.calls.bulk[0]) == 3                   # 其余三源照常
    assert len(env.calls.alerts) == 1                    # 单源失败告警一次
    failed = [
        item for item in env.health_sink.records
        if item.source_id == "stats"
    ]
    assert len(failed) == 1
    assert failed[0].outcome is npo.source_health_shadow.ObservationOutcome.FAILURE
    assert failed[0].collected_item_count is None
    assert failed[0].error_code == "unknown_error"


def test_run_isolates_failed_govcn_gwy_api_and_keeps_rss_and_cjzc(env):
    env.api = RuntimeError("govcn_gwy API HTTP 503")
    summary = npo.run()
    assert "govcn_gwy 失败" in summary and "失败源[govcn_gwy]" in summary
    assert len(env.calls.bulk[0]) == 3
    assert {doc["source"] for doc in env.calls.bulk[0]} == {
        "govcn_policy", "stats", "em_cjzc"
    }
    assert len(env.calls.alerts) == 1


def test_run_records_empty_source_as_success(env):
    env.rss["stats"] = []

    summary = npo.run()

    stats = [
        item for item in env.health_sink.records
        if item.source_id == "stats"
    ]
    assert len(stats) == 1
    assert stats[0].outcome is npo.source_health_shadow.ObservationOutcome.SUCCESS
    assert stats[0].collected_item_count == 0
    assert stats[0].new_item_count is None
    assert stats[0].completeness_info["archive_new_item_count"] == 0
    assert stats[0].empty_success is True
    assert "stats 0条(新0)" in summary


def test_run_isolates_hanging_source(env, monkeypatch):
    import time as _time
    monkeypatch.setattr(npo, "_FETCH_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(npo, "_fetch_cjzc", lambda: _time.sleep(3))
    summary = npo.run()
    assert "em_cjzc 失败" in summary
    assert len(env.calls.bulk[0]) == 3


def test_run_all_sources_failed_raises(env):
    for s in ("govcn_policy", "stats"):
        env.rss[s] = RuntimeError("boom")
    env.api = RuntimeError("boom")
    env.cjzc = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="全部采集失败"):
        npo.run()


def test_run_dedups_within_batch(env):
    env.rss["govcn_policy"] = [_entry(guid="dup"), _entry(guid="dup")]
    npo.run()
    ids = [d["_id"] for d in env.calls.bulk[0]]
    assert len(ids) == len(set(ids))


def test_archive_groups_by_pub_date(env):
    env.rss["govcn_policy"] = [
        _entry(guid="d1", parsed=time.struct_time((2026, 7, 7, 2, 0, 0, 0, 0, 0))),
        _entry(guid="d2", parsed=time.struct_time((2026, 7, 8, 2, 0, 0, 0, 0, 0))),
    ]
    npo.run()
    dates = {a[1] for a in env.calls.archived if a[0] == "govcn_policy"}
    assert dates == {"20260707", "20260708"}


# ---------- verify：归档重放补库 ----------

def test_verify_replays_archive(env, monkeypatch):
    monkeypatch.setattr(npo, "get_news_config", lambda: {"verify_days_back": 1})
    monkeypatch.setattr(npo, "_today", lambda: datetime.date(2026, 7, 8))
    replayed = {"govcn_policy": [npo._entry_to_envelope(
        "govcn_policy", "policy", _entry(guid="old"), _FETCH_TIME, {})]}
    monkeypatch.setattr(npo.news_archive, "replay",
                        lambda s, dr: iter(replayed.get(s, [])))
    summary = npo.run_verify("20990101", "20990102")     # 框架窗口被忽略
    assert len(env.calls.bulk) == 1
    assert env.calls.bulk[0][0]["_id"] == "govcn_policy-old"
    assert "raw_title" not in env.calls.bulk[0][0]
    assert "重放" in summary
    verify_items = [
        item for item in env.health_sink.records
        if item.observation_type is npo.source_health_shadow.ObservationType.VERIFY
    ]
    assert len(verify_items) == 4
    gov = next(item for item in verify_items if item.source_id == "govcn_policy")
    assert gov.outcome is npo.source_health_shadow.ObservationOutcome.SUCCESS
    assert gov.completeness_info["archive_replay_succeeded"] is True
    assert gov.collected_item_count == 1


def test_verify_uses_pipeline_health_context_when_provided(env, monkeypatch):
    monkeypatch.setattr(npo, "get_news_config", lambda: {"verify_days_back": 1})
    monkeypatch.setattr(npo, "_today", lambda: datetime.date(2026, 7, 8))
    monkeypatch.setattr(npo.news_archive, "replay", lambda source, window: iter([]))
    run_id = npo.source_health_shadow.make_job_run_id(
        "news_policy_verify", datetime.datetime(2026, 7, 8, tzinfo=datetime.timezone.utc)
    )

    npo.run_verify(
        "20990101",
        "20990102",
        **{
            npo.source_health_shadow.JOB_RUN_ID_CONTEXT_KEY: run_id,
            npo.source_health_shadow.ATTEMPT_NO_CONTEXT_KEY: 2,
        },
    )

    verify_items = [
        item for item in env.health_sink.records
        if item.observation_type is npo.source_health_shadow.ObservationType.VERIFY
    ]
    assert verify_items
    assert {item.job_run_id for item in verify_items} == {run_id}
    assert {item.attempt_no for item in verify_items} == {2}


def test_pipeline_and_source_observations_share_context_across_retry(env, monkeypatch):
    from data_collect import pipeline as pipeline_module

    original_rss = dict(env.rss)
    original_api = list(env.api)
    original_cjzc = env.cjzc
    alerts = []

    def in_process(job_path, fn_name, timeout=None, **kwargs):
        attempt = kwargs[npo.source_health_shadow.ATTEMPT_NO_CONTEXT_KEY]
        if attempt == 1:
            env.rss.update({source: RuntimeError("first attempt") for source in env.rss})
            env.api = RuntimeError("first attempt")
            env.cjzc = RuntimeError("first attempt")
        else:
            env.rss.clear()
            env.rss.update(original_rss)
            env.api = original_api
            env.cjzc = original_cjzc
        return npo.run(**kwargs)

    monkeypatch.setattr(pipeline_module, "_SOURCE_HEALTH_SINK", env.health_sink)
    monkeypatch.setattr(pipeline_module, "execute_in_subprocess", in_process)
    monkeypatch.setattr(
        pipeline_module, "send_dingtalk", lambda message: alerts.append(message)
    )

    result = pipeline_module._run_one_task(
        {
            "name": "news_policy",
            "job": "data_collect.jobs.news_policy",
            "retries": 1,
        },
        None,
        {},
    )

    assert result.success is True
    task_items = [
        item for item in env.health_sink.records
        if item.observation_type is npo.source_health_shadow.ObservationType.TASK
    ]
    source_items = [
        item for item in env.health_sink.records
        if item.observation_type is npo.source_health_shadow.ObservationType.COLLECT
    ]
    run_ids = {item.job_run_id for item in task_items + source_items}
    assert len(run_ids) == 1
    assert {
        item.attempt_no for item in task_items
        if item.outcome is npo.source_health_shadow.ObservationOutcome.STARTED
    } == {1, 2}
    assert {item.attempt_no for item in source_items} == {1, 2}
    assert len(alerts) == 1


def test_shadow_sink_failure_does_not_change_result_or_existing_alerts(env, monkeypatch):
    class BrokenSink:
        def emit(self, record):
            raise OSError("shadow unavailable")

    monkeypatch.setattr(npo, "_SOURCE_HEALTH_SINK", BrokenSink())
    env.rss["stats"] = RuntimeError("RSS HTTP 503")

    summary = npo.run()

    assert "stats 失败" in summary
    assert len(env.calls.bulk[0]) == 3
    assert len(env.calls.alerts) == 1
