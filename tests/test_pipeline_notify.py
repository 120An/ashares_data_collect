"""pipeline 级 notify:on_failure 通知策略（高频 pipeline 成功静默，防钉钉刷屏）。"""

from data_collect.pipeline import _should_notify, TaskResult


def _r(name, success=True, skipped=False):
    return TaskResult(name=name, success=success, message="", duration=0.1, skipped=skipped)


def test_on_failure_all_success_silent():
    # notify:on_failure + 全成功 → 不发（静默）
    assert _should_notify({"notify": "on_failure"}, [_r("a"), _r("b")]) is False


def test_on_failure_with_failure_notifies():
    # notify:on_failure + 有失败 → 发
    assert _should_notify({"notify": "on_failure"}, [_r("a"), _r("b", success=False)]) is True


def test_on_failure_upstream_skip_notifies():
    # 上游失败导致的跳过 success=False → 视为失败，发
    assert _should_notify({"notify": "on_failure"}, [_r("a", success=False, skipped=True)]) is True


def test_platform_skip_is_not_failure_silent():
    # 平台不匹配跳过 success=True → on_failure 下仍静默
    assert _should_notify({"notify": "on_failure"}, [_r("a", success=True, skipped=True)]) is False


def test_no_notify_field_always_sends():
    # 缺省（无 notify 字段）→ 向后兼容，总发
    assert _should_notify({}, [_r("a")]) is True


def test_other_notify_value_always_sends():
    # 非 on_failure 的取值 → 总发
    assert _should_notify({"notify": "always"}, [_r("a")]) is True
