from compose_runner import ecs_task


def test_resolve_n_cores_prefers_env():
    assert ecs_task._resolve_n_cores("3") == 3


def test_resolve_n_cores_detects_cpu(monkeypatch):
    monkeypatch.setattr(ecs_task.os, "cpu_count", lambda: 4)
    assert ecs_task._resolve_n_cores(None) == 4


def test_resolve_n_cores_handles_unknown_cpu(monkeypatch):
    monkeypatch.setattr(ecs_task.os, "cpu_count", lambda: None)
    assert ecs_task._resolve_n_cores(None) is None
