from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from tools import start_judge_stack as sjs


pytestmark = [pytest.mark.unit, pytest.mark.layer("unit"), pytest.mark.category("startup")]


@pytest.mark.test_id("START-01")
@pytest.mark.priority("P1")
@pytest.mark.requirement("可执行路径规范化去除外层引号")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="normalize_executable_path")
def test_normalize_executable_path() -> None:
    assert sjs.normalize_executable_path('"C:\\Program Files\\Python\\python.exe"') == r"C:\Program Files\Python\python.exe"


@pytest.mark.test_id("START-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("进程命令拼接保留含空格路径")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="build_process_command")
def test_build_process_command_quotes_spaces() -> None:
    command = sjs.build_process_command([r"C:\Program Files\Python\python.exe", "-m", "JudgeWeb.main"])
    assert '"C:\\Program Files\\Python\\python.exe"' in command
    assert "-m JudgeWeb.main" in command


@pytest.mark.test_id("START-03")
@pytest.mark.priority("P0")
@pytest.mark.requirement("PROCESSOR component id 列表按排序读取")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="load_processor_component_id_list")
def test_load_processor_component_id_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "CentralControllerConfig.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "components": {
                    "processor_b": {"component_type": "PROCESSOR"},
                    "collector_a": {"component_type": "RECEIVER"},
                    "processor_a": {"component_type": "PROCESSOR"},
                }
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sjs, "CENTRAL_CONTROLLER_CONFIG_PATH", config_path)
    assert sjs.load_processor_component_id_list() == ["processor_a", "processor_b"]


@pytest.mark.test_id("START-04")
@pytest.mark.priority("P0")
@pytest.mark.requirement("launcher manifest 包含 recovery 和 processor 列表")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="write_launcher_manifest")
def test_write_launcher_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sjs, "CONTROL_ROOT", tmp_path / "results" / "control")
    monkeypatch.setattr(sjs, "load_processor_component_id_list", lambda: ["processor_a", "processor_b"])
    monkeypatch.setattr(sjs, "_resolve_process_manifest_path", lambda: tmp_path / "results" / "control" / "judge_process_manifest.json")
    sjs.write_launcher_manifest(
        match_start_mode="resume",
        python_executable="python.exe",
        java_executable="java.exe",
        npm_executable="npm.cmd",
        applied_recovery={"recovery_mode": "resume"},
    )
    manifest_path = tmp_path / "results" / "control" / "launcher_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["match_start_mode"] == "resume"
    assert payload["processor_component_id_list"] == ["processor_a", "processor_b"]
    assert payload["applied_recovery"] == {"recovery_mode": "resume"}
    assert payload["run_provenance"]["run_kind"] == "recovery_run"


@pytest.mark.test_id("START-04B")
@pytest.mark.priority("P0")
@pytest.mark.requirement("clean 全量启动必须记录 Git 版本和关键配置指纹")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="build_run_provenance")
def test_build_run_provenance_fingerprints_clean_full_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text("value: 1\n", encoding="utf-8")
    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(sjs, "RUN_PROVENANCE_CONFIG_PATH_LIST", (config_path,))

    def fake_git_value(arguments: list[str], *, allow_empty: bool = False):
        return "" if allow_empty else "revision-1"

    monkeypatch.setattr(sjs, "_read_git_value", fake_git_value)

    provenance = sjs.build_run_provenance(
        "clear",
        {"recovery_mode": "clear_start"},
    )

    assert provenance["run_kind"] == "clean_full_run"
    assert provenance["git_revision"] == "revision-1"
    assert provenance["git_tracked_dirty"] is False
    assert len(provenance["config_sha256_by_path"]["config.yml"]) == 64


@pytest.mark.test_id("START-04C")
@pytest.mark.priority("P0")
@pytest.mark.requirement("BCI_JAVA_EXE may point to a JDK directory and must resolve to bin/java.exe")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="resolve_java_executable")
def test_resolve_java_executable_expands_configured_jdk_directory(tmp_path: Path) -> None:
    java_executable = tmp_path / "jdk" / "bin" / "java.exe"
    java_executable.parent.mkdir(parents=True)
    java_executable.write_bytes(b"")

    resolved = sjs.resolve_java_executable(str(java_executable.parents[1]), None)

    assert Path(resolved) == java_executable


@pytest.mark.test_id("START-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("clear 模式结果目录准备会创建 live/control/history")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="prepare_results_root")
def test_prepare_results_root_clear_creates_required_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(sjs, "RESULTS_ROOT", tmp_path / "results")
    monkeypatch.setattr(sjs, "archive_and_clear_active_results", lambda project_root, reason: {"archive_reason": reason})
    payload = sjs.prepare_results_root("clear")
    assert payload["recovery_mode"] == "clear_start"
    assert (tmp_path / "results" / "live").exists()
    assert (tmp_path / "results" / "control").exists()
    assert (tmp_path / "results" / "history").exists()


@pytest.mark.test_id("START-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("resume 模式调用恢复预处理")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="prepare_results_root")
def test_prepare_results_root_resume_delegates_to_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(sjs, "RESULTS_ROOT", tmp_path / "results")
    monkeypatch.setattr(sjs, "prepare_resume_recovery", lambda project_root: {"recovery_mode": "resume_ready"})
    payload = sjs.prepare_results_root("resume")
    assert payload == {"recovery_mode": "resume_ready"}


@pytest.mark.test_id("START-07")
@pytest.mark.priority("P1")
@pytest.mark.requirement("运行目录初始化创建日志和结果目录")
@pytest.mark.tested(file="tools/start_judge_stack.py", function="ensure_runtime_directories")
def test_ensure_runtime_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sjs, "PROJECT_ROOT", tmp_path)
    sjs.ensure_runtime_directories()
    assert (tmp_path / "app" / "Algorithm" / "Algorithm" / "log").exists()
    assert (tmp_path / "results" / "history").exists()
