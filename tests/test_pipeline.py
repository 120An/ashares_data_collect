"""pipeline.py 测试（不依赖 xtquant 或数据库）。"""

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
        calls.append(1)
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
        {},
    )

    assert result.success is True and result.message == "ok"
    assert len(calls) == 3                         # 1 initial + exactly 2 retries
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
