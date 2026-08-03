from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEAR_BAT = PROJECT_ROOT / "startup_judge_clear.bat"
RESUME_BAT = PROJECT_ROOT / "startup_judge_resume.bat"


pytestmark = [pytest.mark.condition, pytest.mark.layer("condition"), pytest.mark.category("judge_startup_control")]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("COND-JUDGE-BAT-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("clear/resume 启动脚本必须分别固定 BCI_MATCH_START_MODE=clear/resume，并统一透传 PYTHONHASHSEED=0")
@pytest.mark.tested(file="startup_judge_clear.bat;startup_judge_resume.bat", function="match_start_mode_contract")
def test_judge_startup_scripts_pin_clear_and_resume_modes_and_pythonhashseed() -> None:
    clear_content = _read(CLEAR_BAT)
    resume_content = _read(RESUME_BAT)

    assert 'set "BCI_MATCH_START_MODE=clear"' in clear_content
    assert 'set "BCI_MATCH_START_MODE=resume"' in resume_content
    assert 'set "PYTHONHASHSEED=0"' in clear_content
    assert 'set "PYTHONHASHSEED=0"' in resume_content


@pytest.mark.test_id("COND-JUDGE-BAT-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("裁判机启动脚本必须在 Python/Java 缺失时 fail closed，并对 npm 缺失给出可观测降级提示")
@pytest.mark.tested(file="startup_judge_clear.bat;startup_judge_resume.bat", function="resolve_python/resolve_java/resolve_npm")
def test_judge_startup_scripts_fail_closed_for_python_java_and_warn_for_npm() -> None:
    clear_content = _read(CLEAR_BAT)
    resume_content = _read(RESUME_BAT)

    assert "[startup_judge_clear] Python not found." in clear_content
    assert "[startup_judge_clear] Java not found." in clear_content
    assert "[startup_judge_clear] npm not found, judge-dashboard will not be auto-started." in clear_content
    assert "[startup_judge_resume] Python not found." in resume_content
    assert "[startup_judge_resume] Java not found." in resume_content
    assert "[startup_judge_resume] npm not found, judge-dashboard will not be auto-started." in resume_content


@pytest.mark.test_id("COND-JUDGE-BAT-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("裁判机启动脚本失败时必须 pause 并返回非零退出码，避免现场误以为成功启动")
@pytest.mark.tested(file="startup_judge_clear.bat;startup_judge_resume.bat", function="startup_failure_contract")
def test_judge_startup_scripts_pause_and_return_nonzero_on_failure() -> None:
    clear_content = _read(CLEAR_BAT)
    resume_content = _read(RESUME_BAT)

    assert "startup failed with exit code %errorlevel%." in clear_content
    assert "startup failed with exit code %errorlevel%." in resume_content
    assert "pause" in clear_content
    assert "pause" in resume_content
    assert "exit /b %errorlevel%" in clear_content
    assert "exit /b %errorlevel%" in resume_content
