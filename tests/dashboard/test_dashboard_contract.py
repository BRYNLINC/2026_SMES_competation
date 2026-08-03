from __future__ import annotations

import json
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_ROOT = PROJECT_ROOT / "judge-dashboard"
PACKAGE_JSON_FILE = DASHBOARD_ROOT / "package.json"
REST_API_FILE = DASHBOARD_ROOT / "src" / "api" / "rest.ts"
TYPES_FILE = DASHBOARD_ROOT / "src" / "api" / "types.ts"
RECOVERY_MODAL_FILE = DASHBOARD_ROOT / "src" / "components" / "RecoveryModal.tsx"
CURRENT_TRIAL_FILE = DASHBOARD_ROOT / "src" / "components" / "CurrentTrial.tsx"
TEAM_CARD_FILE = DASHBOARD_ROOT / "src" / "components" / "TeamCard.tsx"
LIVE_SYNC_FILE = DASHBOARD_ROOT / "src" / "hooks" / "useSyncLive.ts"
JUDGE_STORE_FILE = DASHBOARD_ROOT / "src" / "store" / "useJudgeStore.ts"
OVERVIEW_FILE = DASHBOARD_ROOT / "src" / "components" / "Overview.tsx"


pytestmark = [pytest.mark.dashboard, pytest.mark.layer("dashboard"), pytest.mark.category("dashboard_contract")]


def _package_json() -> dict:
    return json.loads(PACKAGE_JSON_FILE.read_text(encoding="utf-8"))


@pytest.mark.test_id("DASH-01")
@pytest.mark.priority("P0")
@pytest.mark.requirement("dashboard 测试层必须落地，且 package.json 保留 dev/build/lint/preview 脚本供发布前契约检查")
@pytest.mark.tested(file="judge-dashboard/package.json", function="script_contract")
def test_dashboard_contract_keeps_required_scripts() -> None:
    scripts = _package_json()["scripts"]

    assert DASHBOARD_ROOT.exists()
    assert scripts["dev"] == "vite"
    assert scripts["build"] == "tsc -b && vite build"
    assert scripts["lint"] == "eslint ."
    assert scripts["preview"] == "vite preview"


@pytest.mark.requirement("REST 降级成功时页面必须保留最后有效数据，且仅在全部传输失败时显示连接中断")
def test_live_transport_contract_distinguishes_rest_fallback_from_offline() -> None:
    live_sync_source = LIVE_SYNC_FILE.read_text(encoding="utf-8")
    store_source = JUDGE_STORE_FILE.read_text(encoding="utf-8")
    overview_source = OVERVIEW_FILE.read_text(encoding="utf-8")

    assert "Promise.allSettled" in live_sync_source
    assert "setLiveTransportStatus('rest_fallback'" in live_sync_source
    assert "setLiveTransportStatus('offline')" in live_sync_source
    assert "liveTransportStatus: 'connecting'" in store_source
    assert "liveTransportStatus === 'offline'" in overview_source
    assert "离线 - 降级轮询中" not in overview_source


@pytest.mark.test_id("DASH-02")
@pytest.mark.priority("P0")
@pytest.mark.requirement("dashboard API 适配层必须显式覆盖 overview/current/teams/scoreboard/recovery 等裁判接口")
@pytest.mark.tested(file="judge-dashboard/src/api/rest.ts", function="getOverview/getCurrentTrial/getTeams/getScoreboard/getRecoveryStatus")
def test_dashboard_rest_contract_covers_required_match_endpoints() -> None:
    content = REST_API_FILE.read_text(encoding="utf-8")

    for expected_line in (
        "export async function getOverview()",
        "export async function getCurrentTrial()",
        "export async function getTeams()",
        "export async function getScoreboard()",
        "export async function getRecoveryStatus()",
        "export async function getLiveSnapshot()",
        "export async function getMatchSummary()",
    ):
        assert expected_line in content
    for endpoint in (
        "/match/overview",
        "/match/current",
        "/match/teams",
        "/match/scoreboard",
        "/recovery/status",
        "/match/summary",
    ):
        assert endpoint in content


@pytest.mark.test_id("DASH-03")
@pytest.mark.priority("P1")
@pytest.mark.requirement("dashboard 类型定义必须保留 LivePayload、MatchOverview、RecoveryStatus、ScoreboardItem，保证组件层联调时字段基线稳定")
@pytest.mark.tested(file="judge-dashboard/src/api/types.ts", function="type_contract")
def test_dashboard_type_contract_contains_core_runtime_models() -> None:
    content = TYPES_FILE.read_text(encoding="utf-8")

    for type_name in (
        "LivePayload",
        "MatchOverview",
        "RecoveryStatus",
        "ScoreboardItem",
        "TeamInfo",
        "CurrentTrial",
    ):
        assert f"export interface {type_name}" in content or f"export type {type_name}" in content


@pytest.mark.test_id("DASH-04")
@pytest.mark.priority("P1")
@pytest.mark.requirement("dashboard 关键组件文件必须存在，覆盖队伍卡片、排行榜、当前 trial、恢复弹窗和总结页")
@pytest.mark.tested(file="judge-dashboard/src/components", function="component_presence_contract")
def test_dashboard_component_contract_covers_required_views() -> None:
    component_root = DASHBOARD_ROOT / "src" / "components"

    for component_name in (
        "TeamCard.tsx",
        "Leaderboard.tsx",
        "CurrentTrial.tsx",
        "RecoveryModal.tsx",
        "SummaryPage.tsx",
        "Overview.tsx",
    ):
        assert (component_root / component_name).exists()


@pytest.mark.test_id("DASH-05")
@pytest.mark.priority("P0")
@pytest.mark.requirement("指定阶段重跑的类型、REST 请求和弹窗必须全链路携带 session_id")
@pytest.mark.tested(
    file="judge-dashboard/src/api/types.ts;judge-dashboard/src/api/rest.ts;judge-dashboard/src/components/RecoveryModal.tsx",
    function="RecoveryStageDescriptor/resumeStage/RecoveryModal",
)
def test_dashboard_recovery_contract_includes_session_id() -> None:
    types_content = TYPES_FILE.read_text(encoding="utf-8")
    rest_content = REST_API_FILE.read_text(encoding="utf-8")
    modal_content = RECOVERY_MODAL_FILE.read_text(encoding="utf-8")

    assert "export interface RecoveryStageDescriptor" in types_content
    assert "session_id: string;" in types_content
    assert "resumeStage(payload: RecoveryStageDescriptor)" in rest_content
    assert "const [sessionId, setSessionId]" in modal_content
    assert "session_id: sessionId" in modal_content
    assert "Session ID / 会话编号" in modal_content


@pytest.mark.test_id("DASH-06")
@pytest.mark.priority("P0")
@pytest.mark.requirement("Collector 阶段分发失败时 dashboard 必须保留错误字段并在当前 Trial 区域明确显示")
@pytest.mark.tested(
    file="judge-dashboard/src/api/types.ts;judge-dashboard/src/api/rest.ts;judge-dashboard/src/components/CurrentTrial.tsx",
    function="CurrentTrial/normalizeCurrentTrial/CurrentTrial",
)
def test_dashboard_current_trial_contract_displays_collector_distribution_error() -> None:
    types_content = TYPES_FILE.read_text(encoding="utf-8")
    rest_content = REST_API_FILE.read_text(encoding="utf-8")
    component_content = CURRENT_TRIAL_FILE.read_text(encoding="utf-8")

    assert "error_type?: string | null;" in types_content
    assert "error_message?: string | null;" in types_content
    assert "error_message: payload.error_message ?? null" in rest_content
    assert "裁判端数据分发失败" in component_content
    assert "trial.error_message" in component_content


@pytest.mark.test_id("DASH-07")
@pytest.mark.priority("P0")
@pytest.mark.requirement("算法掉线或异常时 dashboard 必须描述为当前 task 不计分，不得描述为选手主动放弃")
@pytest.mark.tested(file="judge-dashboard/src/components/TeamCard.tsx", function="resolvedEnvironmentStatusLabel")
def test_dashboard_describes_invalid_task_without_withdrawal_wording() -> None:
    component_content = TEAM_CARD_FILE.read_text(encoding="utf-8")

    assert "当前task不计分" in component_content
    assert "\u5f03\u6743" not in component_content
