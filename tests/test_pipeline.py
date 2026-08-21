"""pipeline.py 测试（不依赖 xtquant 或数据库）。"""

import datetime
import json

import pytest

from data_collect import pipeline as pipeline_module
from data_collect.news_model import source_health as source_health_shadow
from data_collect.pipeline import (
    _topological_sort,
    _get_current_platform,
    TaskResult,
    execute_in_subprocess,
)


def test_topological_sort_simple():
    tasks = [
        {"name": "a", "job": "mod_a"},
        {"name": "b", "job": "mod_b", "depends_on": ["a"]},
    ]
    result = _topological_sort(tasks)
    names = [t["name"] for t in result]
    assert names.index("a") < names.index("b")


def test_topological_sort_no_deps():
    tasks = [
        {"name": "x", "job": "mod_x"},
        {"name": "y", "job": "mod_y"},
    ]
    result = _topological_sort(tasks)
    assert len(result) == 2


def test_topological_sort_diamond():
    tasks = [
        {"name": "a", "job": "m"},
        {"name": "b", "job": "m", "depends_on": ["a"]},
        {"name": "c", "job": "m", "depends_on": ["a"]},
        {"name": "d", "job": "m", "depends_on": ["b", "c"]},
    ]
    result = _topological_sort(tasks)
    names = [t["name"] for t in result]
    assert names.index("a") < names.index("b")
    assert names.index("a") < names.index("c")
    assert names.index("b") < names.index("d")
    assert names.index("c") < names.index("d")


def test_topological_sort_cycle_detection():
    tasks = [
        {"name": "a", "job": "m", "depends_on": ["b"]},
        {"name": "b", "job": "m", "depends_on": ["a"]},
    ]
    with pytest.raises(ValueError, match="循环依赖"):
        _topological_sort(tasks)


def test_topological_sort_missing_dep():
    tasks = [
        {"name": "a", "job": "m", "depends_on": ["nonexistent"]},
    ]
    with pytest.raises(ValueError, match="不存在"):
        _topological_sort(tasks)


def test_get_current_platform():
    plat = _get_current_platform()
    assert plat in ("windows", "linux")


def test_task_result_dataclass():
    r = TaskResult(name="test", success=True, message="ok", duration=1.5)
    assert r.name == "test"
    assert r.success
    assert not r.skipped


# ===== execute_in_subprocess 超时/成功 =====

def test_execute_in_subprocess_success():
    """正常返回时透传 job 函数的返回值。"""
    result = execute_in_subprocess(
        "tests._sleep_job", "run", timeout=5, seconds=0.1,
    )
    assert result == "slept 0.1s"


def test_execute_in_subprocess_timeout_raises():
    """超时时抛 TimeoutError 并强杀子进程。"""
    with pytest.raises(TimeoutError, match="任务执行超时"):
        execute_in_subprocess(
            "tests._sleep_job", "run", timeout=1, seconds=10,
        )


def test_execute_in_subprocess_no_timeout():
    """timeout=None 时不限时（兼容旧调用）。"""
    result = execute_in_subprocess(
        "tests._sleep_job", "run", timeout=None, seconds=0.05,
    )
    assert result == "slept 0.05s"


def test_execute_in_subprocess_propagates_exception(tmp_path):
    """job 内部抛错时原样向上抛。"""
    marker = tmp_path / "counter.txt"
    with pytest.raises(RuntimeError, match="flaky failure"):
        execute_in_subprocess(
            "tests._sleep_job", "run_flaky",
            timeout=5, fail_times=99, marker_path=str(marker),
        )


# ===== Step 8 task-level shadow observations =====

def test_task_failure_never_invents_per_source_failures(monkeypatch):
    sink = source_health_shadow.InMemoryShadowSink()
    monkeypatch.setattr(pipeline_module, "_SOURCE_HEALTH_SINK", sink)
    monkeypatch.setattr(
        pipeline_module,
        "execute_in_subprocess",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("task boom")),
    )

    result = pipeline_module._run_one_task(
        {"name": "news_policy", "job": "fake.job", "retries": 0},
        None,
        {},
    )

    assert result.success is False
    assert sink.records
    assert all(item.source_id is None for item in sink.records)
    terminal = [
        item for item in sink.records
        if item.outcome is source_health_shadow.ObservationOutcome.FAILURE
    ]
    assert len(terminal) == 1 and terminal[0].error_code == "task_error"


def test_pipeline_retry_count_and_notify_count_are_unchanged(monkeypatch):
    sink = source_health_shadow.InMemoryShadowSink()
    calls = []
    alerts = []

    def flaky(*args, **kwargs):
        calls.append(dict(kwargs))
        if len(calls) < 3:
            raise RuntimeError("retry me")
        return "ok"

    monkeypatch.setattr(pipeline_module, "_SOURCE_HEALTH_SINK", sink)
    monkeypatch.setattr(pipeline_module, "execute_in_subprocess", flaky)
    monkeypatch.setattr(
        pipeline_module, "send_dingtalk", lambda message: alerts.append(message)
    )

    result = pipeline_module._run_one_task(
        {"name": "retry_task", "job": "fake.job", "retries": 2},
        None,
        {
            source_health_shadow.SHADOW_JSONL_PATH_CONTEXT_KEY:
                "must-not-reach-legacy-job.jsonl",
        },
    )

    assert result.success is True and result.message == "ok"
    assert len(calls) == 3                         # 1 initial + exactly 2 retries
    assert all(
        source_health_shadow.JOB_RUN_ID_CONTEXT_KEY not in kwargs
        and source_health_shadow.ATTEMPT_NO_CONTEXT_KEY not in kwargs
        and source_health_shadow.SHADOW_JSONL_PATH_CONTEXT_KEY not in kwargs
        for kwargs in calls
    )                                             # 非 news_policy kwargs 不变
    assert len(alerts) == 2                       # original pre-retry alerts only
    retry_events = [
        item for item in sink.records
        if item.outcome is source_health_shadow.ObservationOutcome.RETRY
    ]
    started_events = [
        item for item in sink.records
        if item.outcome is source_health_shadow.ObservationOutcome.STARTED
    ]
    assert len(retry_events) == 2
    assert len(started_events) == 3


def test_news_policy_retry_context_keeps_job_id_and_increments_attempt(monkeypatch):
    sink = source_health_shadow.InMemoryShadowSink()
    calls = []

    def flaky(*args, **kwargs):
        calls.append(dict(kwargs))
        if len(calls) == 1:
            raise RuntimeError("retry once")
        return "ok"

    monkeypatch.setattr(pipeline_module, "_SOURCE_HEALTH_SINK", sink)
    monkeypatch.setattr(pipeline_module, "execute_in_subprocess", flaky)
    monkeypatch.setattr(pipeline_module, "send_dingtalk", lambda message: None)

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
    run_ids = [
        kwargs[source_health_shadow.JOB_RUN_ID_CONTEXT_KEY]
        for kwargs in calls
    ]
    attempt_numbers = [
        kwargs[source_health_shadow.ATTEMPT_NO_CONTEXT_KEY]
        for kwargs in calls
    ]
    assert len(set(run_ids)) == 1
    assert attempt_numbers == [1, 2]
    task_items = [
        item for item in sink.records
        if item.observation_type is source_health_shadow.ObservationType.TASK
    ]
    assert {item.job_run_id for item in task_items} == {run_ids[0]}
    assert {
        item.attempt_no for item in task_items
        if item.outcome is source_health_shadow.ObservationOutcome.STARTED
    } == {1, 2}


def test_news_policy_verify_receives_same_internal_context(monkeypatch):
    captured = []
    monkeypatch.setattr(
        pipeline_module,
        "execute_in_subprocess",
        lambda *args, **kwargs: captured.append(dict(kwargs)) or "ok",
    )

    result = pipeline_module._run_one_task(
        {
            "name": "news_policy_verify",
            "job": "data_collect.jobs.news_policy",
            "fn": "run_verify",
            "days_back": 1,
        },
        None,
        {},
    )

    assert result.success is True
    assert captured[0][source_health_shadow.JOB_RUN_ID_CONTEXT_KEY].startswith(
        "jrun_"
    )
    assert captured[0][source_health_shadow.ATTEMPT_NO_CONTEXT_KEY] == 1
    assert source_health_shadow.SHADOW_JSONL_PATH_CONTEXT_KEY not in captured[0]


def test_default_null_sink_does_not_propagate_jsonl_or_create_file(
        tmp_path, monkeypatch):
    captured = []
    shadow_path = tmp_path / "must-not-exist.jsonl"
    monkeypatch.setattr(
        pipeline_module, "_SOURCE_HEALTH_SINK",
        source_health_shadow.NullShadowSink(),
    )
    monkeypatch.setattr(
        pipeline_module,
        "execute_in_subprocess",
        lambda *args, **kwargs: captured.append(dict(kwargs)) or "ok",
    )

    result = pipeline_module._run_one_task(
        {"name": "news_policy", "job": "data_collect.jobs.news_policy"},
        None,
        {},
    )

    assert result.success is True
    assert source_health_shadow.SHADOW_JSONL_PATH_CONTEXT_KEY not in captured[0]
    assert not shadow_path.exists()


def test_news_policy_jsonl_path_is_stable_across_retry(monkeypatch, tmp_path):
    path = tmp_path / "retry-shadow.jsonl"
    calls = []

    def flaky(*args, **kwargs):
        calls.append(dict(kwargs))
        if len(calls) == 1:
            raise RuntimeError("retry once")
        return "ok"

    monkeypatch.setattr(
        pipeline_module, "_SOURCE_HEALTH_SINK",
        source_health_shadow.JsonlShadowSink(path),
    )
    monkeypatch.setattr(pipeline_module, "execute_in_subprocess", flaky)
    monkeypatch.setattr(pipeline_module, "send_dingtalk", lambda message: None)

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
    assert [
        item[source_health_shadow.SHADOW_JSONL_PATH_CONTEXT_KEY]
        for item in calls
    ] == [str(path.resolve()), str(path.resolve())]
    assert len({
        item[source_health_shadow.JOB_RUN_ID_CONTEXT_KEY] for item in calls
    }) == 1
    assert [
        item[source_health_shadow.ATTEMPT_NO_CONTEXT_KEY] for item in calls
    ] == [1, 2]


def test_jsonl_path_crosses_real_subprocess_for_news_policy_verify(
        tmp_path, monkeypatch):
    """真实 ProcessPool/spawn：父 task 与子 source 写入同一个 JSONL。"""
    archive = tmp_path / "archive"
    spool = tmp_path / "spool"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "news:\n"
        f"  archive_base: '{archive.as_posix()}'\n"
        f"  archive_base_posix: '{archive.as_posix()}'\n"
        f"  spool_dir: '{spool.as_posix()}'\n"
        "  verify_days_back: 1\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DATA_COLLECT_CONFIG", str(config_path))
    shadow_path = tmp_path / "gray" / "observations.jsonl"
    monkeypatch.setattr(
        pipeline_module, "_SOURCE_HEALTH_SINK",
        source_health_shadow.JsonlShadowSink(shadow_path),
    )

    result = pipeline_module._run_one_task(
        {
            "name": "news_policy_verify",
            "job": "data_collect.jobs.news_policy",
            "fn": "run_verify",
            "days_back": 1,
            "timeout": 20,
        },
        None,
        {},
    )

    assert result.success is True
    records = [
        json.loads(line) for line in shadow_path.read_text(encoding="utf-8").splitlines()
    ]
    task_records = [item for item in records if item["observation_type"] == "task"]
    source_records = [
        item for item in records if item["observation_type"] == "verify"
    ]
    assert [item["outcome"] for item in task_records] == ["started", "success"]
    assert {item["source_id"] for item in source_records} == {
        "govcn_policy", "govcn_gwy", "em_cjzc"
    }
    assert len(source_records) == 3
    assert len({item["job_run_id"] for item in records}) == 1
    assert {item["attempt_no"] for item in records} == {1}


def test_news_policy_restored_jsonl_write_failure_is_fail_open(
        tmp_path, monkeypatch):
    from data_collect.jobs import news_policy

    monkeypatch.setattr(news_policy, "get_news_config",
                        lambda: {"verify_days_back": 1})
    monkeypatch.setattr(news_policy, "_today",
                        lambda: datetime.date(2026, 8, 21))
    monkeypatch.setattr(news_policy, "_policy_sources", lambda: {})
    monkeypatch.setattr(news_policy.news_archive, "replay",
                        lambda source, window: iter(()))
    monkeypatch.setattr(
        news_policy, "_SOURCE_HEALTH_SINK",
        source_health_shadow.NullShadowSink(),
    )
    run_id = source_health_shadow.make_job_run_id(
        "news_policy_verify", source_health_shadow.utc_now()
    )

    result = pipeline_module._call_job_fn(
        "data_collect.jobs.news_policy",
        "run_verify",
        start_date="20260820",
        end_date="20260821",
        **{
            source_health_shadow.JOB_RUN_ID_CONTEXT_KEY: run_id,
            source_health_shadow.ATTEMPT_NO_CONTEXT_KEY: 1,
            # Opening a directory as a JSONL file fails; collection must not.
            source_health_shadow.SHADOW_JSONL_PATH_CONTEXT_KEY: str(tmp_path),
        },
    )

    assert result.startswith("政策 verify(")


def test_pipeline_timeout_is_task_level_unknown_and_incomplete(monkeypatch):
    sink = source_health_shadow.InMemoryShadowSink()
    monkeypatch.setattr(pipeline_module, "_SOURCE_HEALTH_SINK", sink)
    monkeypatch.setattr(
        pipeline_module,
        "execute_in_subprocess",
        lambda *a, **k: (_ for _ in ()).throw(TimeoutError("hard kill")),
    )

    result = pipeline_module._run_one_task(
        {"name": "timeout_task", "job": "fake.job", "retries": 0},
        None,
        {},
    )

    assert result.success is False
    timeout_event, = [
        item for item in sink.records
        if item.outcome is source_health_shadow.ObservationOutcome.TIMEOUT
    ]
    assert timeout_event.source_id is None
    assert timeout_event.incomplete is True
    assert timeout_event.completeness_status is source_health_shadow.CompletenessStatus.UNKNOWN


def test_shadow_sink_failure_does_not_retry_or_change_task_result(monkeypatch):
    class BrokenSink:
        def emit(self, record):
            raise OSError("shadow disk down")

    calls = []
    alerts = []

    def succeeds(*args, **kwargs):
        calls.append(1)
        return "original result"

    monkeypatch.setattr(pipeline_module, "_SOURCE_HEALTH_SINK", BrokenSink())
    monkeypatch.setattr(pipeline_module, "execute_in_subprocess", succeeds)
    monkeypatch.setattr(
        pipeline_module, "send_dingtalk", lambda message: alerts.append(message)
    )

    result = pipeline_module._run_one_task(
        {"name": "healthy_task", "job": "fake.job", "retries": 3},
        None,
        {},
    )

    assert result.success is True
    assert result.message == "original result"
    assert len(calls) == 1
    assert alerts == []
