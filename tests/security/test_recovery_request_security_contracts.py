from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


pytestmark = [pytest.mark.security, pytest.mark.layer("security"), pytest.mark.category("recovery_request_security")]


def _read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("SEC-REC-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("JudgeWeb 恢复请求入口必须对 stage 使用 normalize_stage_request_payload，并基于 checkpoint 列表校验合法性")
@pytest.mark.tested(
    file="app/JudgeWeb/JudgeWeb/main.py",
    function="post_recovery_request/post_recovery_restart_stage",
)
def test_judge_web_recovery_endpoints_normalize_stage_and_validate_checkpoint_membership() -> None:
    content = _read_text("app/JudgeWeb/JudgeWeb/main.py")

    assert "stage_payload = normalize_stage_request_payload((request_payload or {}).get('stage'))" in content
    assert "stage_payload = normalize_stage_request_payload(request_payload)" in content
    assert "checkpoint_id = build_checkpoint_id(stage_payload)" in content
    assert "checkpoint_id_set = {" in content
    assert "指定阶段当前不可作为重跑起点" in content
    assert "缺少合法的 subject_id / exp_name / exp_task" in content


@pytest.mark.test_id("SEC-REC-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("恢复请求写入必须固定到 results/control/recovery_request.json，不能接受任意文件路径输入")
@pytest.mark.tested(
    file="app/JudgeWeb/JudgeWeb/main.py",
    function="write_recovery_request",
)
def test_write_recovery_request_uses_fixed_control_path_without_user_path_passthrough() -> None:
    content = _read_text("app/JudgeWeb/JudgeWeb/main.py")
    function_body = content.split("def write_recovery_request", 1)[1].split("def resolve_recommended_recovery_stage", 1)[0]

    assert "CONTROL_ROOT / RECOVERY_REQUEST_FILE_NAME" in content
    assert "safe_write_json_file(" in content
    assert "request_payload =" in content
    assert "request_payload.get('path')" not in function_body
    assert 'request_payload.get("path")' not in function_body
    assert "stage_payload.get('path')" not in function_body
    assert 'stage_payload.get("path")' not in function_body


@pytest.mark.test_id("SEC-REC-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("恢复运行时只能用 normalize_stage_payload 构造 checkpoint，防止 ../ 和换行类 stage 注入直接落到文件系统路径")
@pytest.mark.tested(
    file="tools/recovery_runtime.py",
    function="normalize_stage_payload/build_checkpoint_id/apply_restart_from_stage",
)
def test_recovery_runtime_uses_normalized_stage_payload_instead_of_raw_path_joining() -> None:
    content = _read_text("tools/recovery_runtime.py")

    assert "normalized_stage_payload = normalize_stage_payload(stage_payload)" in content
    assert "checkpoint_id = build_checkpoint_id(normalized_stage_payload)" in content
    assert "return {" in content
    assert "subject_id': subject_id" in content or '"subject_id": subject_id' in content
    assert "exp_name': exp_name" in content or '"exp_name": exp_name' in content
    assert "exp_task': exp_task" in content or '"exp_task": exp_task' in content
    assert "../" not in content
