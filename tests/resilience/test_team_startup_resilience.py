from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = [pytest.mark.resilience, pytest.mark.layer("resilience"), pytest.mark.category("team_startup")]


def _load_bat_text(project_root: Path) -> str:
    return (project_root / "startup_team.bat").read_text(encoding="utf-8", errors="ignore")


@pytest.mark.test_id("RES-TEAM-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("选手机启动脚本必须在启动前回收 9981 端口，并在自动清理失败时明确中止，避免多算法实例相互污染")
@pytest.mark.tested(
    file="startup_team.bat",
    function=":cleanup_listening_port/:ensure_port_available",
)
def test_startup_team_bat_reclaims_port_9981_and_fails_closed_when_reclaim_does_not_succeed(
    project_root_path: Path,
) -> None:
    content = _load_bat_text(project_root_path)

    assert ':cleanup_listening_port' in content
    assert ':kill_pid' in content
    assert 'taskkill /F /PID %BCI_KILL_PID%' in content
    assert 'ERROR: TCP 9981 is already occupied by PID %BCI_PORT_IN_USE_PID%' in content
    assert 'automatic cleanup did not succeed' in content
    assert 'exit /b 1' in content


@pytest.mark.test_id("RES-TEAM-02")
@pytest.mark.priority("P1")
@pytest.mark.requirement("选手机启动脚本在无法添加防火墙规则时只能告警，不能阻断算法进程启动")
@pytest.mark.tested(
    file="startup_team.bat",
    function=":ensure_firewall_rule",
)
def test_startup_team_bat_warns_but_does_not_fail_when_firewall_rule_addition_fails(
    project_root_path: Path,
) -> None:
    content = _load_bat_text(project_root_path)

    assert 'WARNING: failed to add inbound firewall rule for TCP 9981.' in content
    assert 'please run this script as Administrator once' in content
    assert ':ensure_firewall_rule' in content
    assert 'exit /b 0' in content


@pytest.mark.test_id("RES-TEAM-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("选手机启动脚本必须先终止旧的队伍窗口再启动新算法实例，避免上下文重启后残留旧进程")
@pytest.mark.tested(
    file="startup_team.bat",
    function=":terminate_existing_team_windows",
)
def test_startup_team_bat_terminates_existing_team_windows_before_launch(project_root_path: Path) -> None:
    content = _load_bat_text(project_root_path)

    assert 'call :terminate_existing_team_windows' in content
    assert 'taskkill /FI "WINDOWTITLE eq [BCI Team]*" /T /F' in content
    assert 'start "[BCI Team] Algorithm Python"' in content
