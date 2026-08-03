import type {
  ConnectionStatus,
  CurrentTrial,
  LivePayload,
  MatchControlStatus,
  MatchOverview,
  RecoveryStatus,
  MatchSummary,
  MatchSummaryTaskItem,
  MatchSummaryTeamItem,
  MatchSummaryTeamTaskItem,
  MatchSummarySubjectTaskItem,
  RecoveryCheckpoint,
  RecoveryStageDescriptor,
  ScoreboardItem,
  SystemStatusData,
  TeamInfo,
  RuntimeStageStatus,
  RuntimeStageGroupStatus,
  RuntimeStageStageStatus,
  RuntimeStageContext,
} from './types';

const DEFAULT_API_BASE = '/api/v1';

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, '');
}

function resolveApiBase(): string {
  const envApiBase = import.meta.env.VITE_API_BASE_URL;
  if (typeof envApiBase === 'string' && envApiBase.trim() !== '') {
    return trimTrailingSlash(envApiBase.trim());
  }
  return DEFAULT_API_BASE;
}

export const API_BASE = resolveApiBase();

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(`${API_BASE}${url}`);
  if (!response.ok) {
    throw new Error(await buildHttpErrorMessage(response));
  }
  return response.json();
}

async function postJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${url}`, {
    method: 'POST',
    ...init,
  });
  if (!response.ok) {
    throw new Error(await buildHttpErrorMessage(response));
  }
  return response.json();
}

async function buildHttpErrorMessage(response: Response): Promise<string> {
  const fallbackMessage = `HTTP error! status: ${response.status}`;
  try {
    const responseText = await response.text();
    if (!responseText) {
      return fallbackMessage;
    }
    try {
      const payload = JSON.parse(responseText) as { detail?: unknown; message?: unknown };
      if (typeof payload.detail === 'string' && payload.detail.trim() !== '') {
        return payload.detail;
      }
      if (typeof payload.message === 'string' && payload.message.trim() !== '') {
        return payload.message;
      }
    } catch {
      return `${fallbackMessage} ${responseText}`;
    }
    return fallbackMessage;
  } catch {
    return fallbackMessage;
  }
}

type RawStartReadiness = {
  ready?: boolean;
  reason_list?: unknown[];
  configured_team_id_list?: unknown[];
  connected_team_id_list?: unknown[];
  pending_team_id_list?: unknown[];
  pending_group_id_list?: unknown[];
  group_readiness_list?: Array<{
    group_id?: string;
    configured_team_id_list?: unknown[];
    stage_observed?: boolean;
    collector_ready?: boolean;
  }>;
  updated_at?: number | null;
};

type RawOverviewResponse = {
  match_name?: string;
  current_match_status?: string;
  team_count?: number;
  connected_team_count?: number;
  calibration_ready_team_count?: number;
  start_readiness?: RawStartReadiness | null;
};

type RawControlStatusResponse = {
  match_control_status?: Partial<MatchControlStatus> | null;
  start_readiness?: RawStartReadiness | null;
};

type RawCurrentResponse = {
  current_trial?: Partial<CurrentTrial> | null;
};

type RawTeamsResponse = {
  team_list?: Array<Partial<TeamInfo>>;
};

type RawScoreboardResponse = {
  scoreboard?: Array<Partial<ScoreboardItem>>;
};

type RawSystemResponse = {
  judge_web?: { status?: string };
  match_control_status?: Partial<MatchControlStatus> | null;
  runtime_stage_status?: Record<string, unknown> | null;
  team_component_status_list?: Array<{
    team_id?: string;
    team_display_name?: string;
    processor_component_id?: string;
    collector_component_id?: string;
    connection_status?: string;
    run_status?: string;
    calibration_status?: string;
    updated_at?: number;
  }>;
};

type RawRecoveryStatusResponse = Partial<RecoveryStatus>;

type LiveSnapshot = {
  overview: MatchOverview | null;
  trial: CurrentTrial | null;
  teams: TeamInfo[];
  scoreboard: ScoreboardItem[];
  systemStatus: SystemStatusData | null;
  controlStatus: MatchControlStatus | null;
};

type RawMatchSummaryTaskItem = Partial<MatchSummaryTaskItem>;
type RawMatchSummaryTeamTaskItem = Partial<MatchSummaryTeamTaskItem>;
type RawMatchSummarySubjectTaskItem = Partial<MatchSummarySubjectTaskItem>;
type RawMatchSummaryTeamItem = Partial<MatchSummaryTeamItem> & {
  task_summary_list?: RawMatchSummaryTeamTaskItem[];
  subject_task_summary_list?: RawMatchSummarySubjectTaskItem[];
};
type RawMatchSummaryResponse = Partial<MatchSummary> & {
  scoreboard?: Array<Partial<ScoreboardItem>>;
  task_summary_list?: RawMatchSummaryTaskItem[];
};

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeStartReadiness(payload: RawStartReadiness | null | undefined) {
  if (!payload) {
    return null;
  }
  return {
    ready: Boolean(payload.ready),
    reason_list: Array.isArray(payload.reason_list) ? payload.reason_list.map((item) => String(item)) : [],
    configured_team_id_list: Array.isArray(payload.configured_team_id_list) ? payload.configured_team_id_list.map((item) => String(item)) : [],
    connected_team_id_list: Array.isArray(payload.connected_team_id_list) ? payload.connected_team_id_list.map((item) => String(item)) : [],
    pending_team_id_list: Array.isArray(payload.pending_team_id_list) ? payload.pending_team_id_list.map((item) => String(item)) : [],
    pending_group_id_list: Array.isArray(payload.pending_group_id_list) ? payload.pending_group_id_list.map((item) => String(item)) : [],
    group_readiness_list: (payload.group_readiness_list ?? []).map((item) => ({
      group_id: String(item.group_id ?? ''),
      configured_team_id_list: Array.isArray(item.configured_team_id_list) ? item.configured_team_id_list.map((value) => String(value)) : [],
      stage_observed: Boolean(item.stage_observed),
      collector_ready: Boolean(item.collector_ready),
    })),
    updated_at: payload.updated_at ?? null,
  };
}
function normalizeConnectionStatus(value: unknown): ConnectionStatus {
  switch (value) {
    case 'connected':
    case 'connecting':
    case 'error':
    case 'closed':
    case 'stopped':
    case 'disconnected':
    case 'reconnecting':
      return value;
    default:
      return 'closed';
  }
}

function normalizeOverview(payload: RawOverviewResponse | null | undefined): MatchOverview {
  return {
    match_name: payload?.match_name ?? 'BCI Competition 2026 Final',
    match_status: payload?.current_match_status ?? 'waiting_start',
    team_count: toNumber(payload?.team_count),
    connected_team_count: toNumber(payload?.connected_team_count),
    calibrated_team_count: toNumber(payload?.calibration_ready_team_count),
    start_readiness: normalizeStartReadiness(payload?.start_readiness),
  };
}

function normalizeMatchControlStatus(payload: Partial<MatchControlStatus> | null | undefined): MatchControlStatus {
  return {
    waiting_start: payload?.waiting_start ?? true,
    match_started: payload?.match_started ?? false,
    match_finished: payload?.match_finished ?? false,
    finished_at: payload?.finished_at ?? null,
    finished_team_id_list: Array.isArray(payload?.finished_team_id_list)
      ? payload.finished_team_id_list.map((item) => String(item))
      : [],
    pause_requested: payload?.pause_requested ?? false,
    paused: payload?.paused ?? false,
    started_at: payload?.started_at ?? null,
    paused_at: payload?.paused_at ?? null,
    resumed_at: payload?.resumed_at ?? null,
    last_seen_start_request_at: payload?.last_seen_start_request_at ?? null,
    last_seen_pause_request_at: payload?.last_seen_pause_request_at ?? null,
    last_seen_resume_request_at: payload?.last_seen_resume_request_at ?? null,
    pending_restart_stage_request: payload?.pending_restart_stage_request ?? null,
    pending_restart_stage_request_at: payload?.pending_restart_stage_request_at ?? null,
    pending_restart_stage_valid: payload?.pending_restart_stage_valid ?? false,
    coordinator_started_at: payload?.coordinator_started_at ?? null,
    start_readiness: normalizeStartReadiness((payload as Partial<MatchControlStatus> & { start_readiness?: RawStartReadiness | null })?.start_readiness),
    updated_at: payload?.updated_at ?? null,
  };
}

function normalizeCurrentTrial(payload: Partial<CurrentTrial> | null | undefined): CurrentTrial | null {
  if (!payload) {
    return null;
  }
  return {
    subject_id: String(payload.subject_id ?? '-'),
    exp_name: String(payload.exp_name ?? '-'),
    exp_task: String(payload.exp_task ?? '-'),
    session_id: payload.session_id ?? '-',
    block_id: payload.block_id ?? '-',
    trial_id: payload.trial_id ?? '-',
    true_label: payload.true_label ?? null,
    status: payload.status ?? 'idle',
    error_type: payload.error_type ?? null,
    error_message: payload.error_message ?? null,
    recovery_advice: payload.recovery_advice ?? null,
    release_wallclock: payload.release_wallclock,
    dispatch_wallclock: payload.dispatch_wallclock,
    prediction_deadline_wallclock: payload.prediction_deadline_wallclock,
    cycle_end_wallclock: payload.cycle_end_wallclock,
    trial_sent_wallclock: payload.trial_sent_wallclock,
    next_release_target_wallclock: payload.next_release_target_wallclock,
    current_subject_index: payload.current_subject_index == null ? null : toNumber(payload.current_subject_index),
    total_subject_count: payload.total_subject_count == null ? null : toNumber(payload.total_subject_count),
  };
}

function normalizeTeam(payload: Partial<TeamInfo>): TeamInfo {
  return {
    team_id: String(payload.team_id ?? ''),
    team_display_name: String(payload.team_display_name ?? payload.team_id ?? 'Unknown Team'),
    connection_status: normalizeConnectionStatus(payload.connection_status),
    run_status: String(payload.run_status ?? 'idle'),
    calibration_status: payload.calibration_status === 'ready' ? 'ready' : 'pending',
    predict_label: payload.predict_label ?? null,
    true_label: payload.true_label ?? null,
    predict_time_ms: payload.predict_time_ms == null ? null : toNumber(payload.predict_time_ms),
    is_timeout: Boolean(payload.is_timeout),
    is_invalid_output: Boolean(payload.is_invalid_output),
    judge_message: payload.judge_message ?? null,
    current_trial_score: toNumber(payload.current_trial_score),
    current_total_score: toNumber(payload.current_total_score),
    current_task_score: toNumber(payload.current_task_score),
    current_task_accuracy_percent: toNumber(payload.current_task_accuracy_percent),
    mean_accuracy_percent: toNumber(payload.mean_accuracy_percent),
    avg_reaction_time_ms: toNumber(payload.avg_reaction_time_ms),
    last_disconnect_at: payload.last_disconnect_at ?? null,
    last_disconnect_reason: payload.last_disconnect_reason ?? null,
    recovery_advice: payload.recovery_advice ?? null,
    forfeit_current_task: Boolean(payload.forfeit_current_task),
    forfeit_task_signature: payload.forfeit_task_signature ?? null,
    reconnected_at: payload.reconnected_at ?? null,
  };
}

function normalizeScoreboardItem(payload: Partial<ScoreboardItem>, rankFallback: number): ScoreboardItem {
  return {
    rank: toNumber(payload.rank, rankFallback),
    team_id: String(payload.team_id ?? ''),
    total_score: toNumber(payload.total_score),
    average_score: toNumber(payload.average_score ?? payload.total_score),
    observed_trial_count: toNumber(payload.observed_trial_count),
    mean_accuracy_percent: toNumber(payload.mean_accuracy_percent),
    avg_reaction_time_ms: toNumber(payload.avg_reaction_time_ms),
    run_status: String(payload.run_status ?? 'idle'),
  };
}

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => String(item)).filter((item) => item.trim() !== '');
}

function normalizeNumberRecord(value: unknown): Record<string, number> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  const normalizedRecord: Record<string, number> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    const keyText = String(key).trim();
    if (keyText === '') {
      continue;
    }
    normalizedRecord[keyText] = toNumber(item);
  }
  return normalizedRecord;
}

function normalizeStringListRecord(value: unknown): Record<string, string[]> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  const normalizedRecord: Record<string, string[]> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    const keyText = String(key).trim();
    if (keyText === '') {
      continue;
    }
    normalizedRecord[keyText] = normalizeStringArray(item);
  }
  return normalizedRecord;
}

function normalizeNumberArray(value: unknown): number[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => toNumber(item));
}

function normalizeRuntimeStageContext(value: unknown): RuntimeStageContext {
  const payload = (value && typeof value === 'object' && !Array.isArray(value))
    ? value as Record<string, unknown>
    : {};
  return {
    subject_id: String(payload.subject_id ?? ''),
    exp_name: String(payload.exp_name ?? ''),
    exp_task: String(payload.exp_task ?? ''),
    session_id: String(payload.session_id ?? ''),
  };
}

function normalizeRuntimeStageStageStatus(value: unknown): RuntimeStageStageStatus {
  const payload = (value && typeof value === 'object' && !Array.isArray(value))
    ? value as Record<string, unknown>
    : {};
  const trialTerminalTeamIdListByTrial = normalizeStringListRecord(payload.trial_terminal_team_id_list_by_trial);
  const trialObservedTerminalTeamIdListByTrial = normalizeStringListRecord(
    payload.trial_observed_terminal_team_id_list_by_trial
  );
  const trialForcedTerminalTeamIdListByTrial = normalizeStringListRecord(
    payload.trial_forced_terminal_team_id_list_by_trial
  );
  return {
    stage_key: String(payload.stage_key ?? ''),
    stage_context: normalizeRuntimeStageContext(payload.stage_context),
    collector_prepared: Boolean(payload.collector_prepared),
    ready_team_id_list: normalizeStringArray(payload.ready_team_id_list),
    forfeited_team_id_list: normalizeStringArray(payload.forfeited_team_id_list),
    calibration_forfeit_detail_by_team: (
      payload.calibration_forfeit_detail_by_team
      && typeof payload.calibration_forfeit_detail_by_team === 'object'
      && !Array.isArray(payload.calibration_forfeit_detail_by_team)
    )
      ? payload.calibration_forfeit_detail_by_team as Record<string, Record<string, unknown>>
      : {},
    pending_ready_team_id_list: normalizeStringArray(payload.pending_ready_team_id_list),
    online_stage_released: Boolean(payload.online_stage_released),
    pending_release: payload.pending_release && typeof payload.pending_release === 'object' && !Array.isArray(payload.pending_release)
      ? payload.pending_release as Record<string, unknown>
      : null,
    online_trial_count: toNumber(payload.online_trial_count),
    released_trial_id: toNumber(payload.released_trial_id),
    completed_trial_id_list: normalizeNumberArray(payload.completed_trial_id_list),
    completed_trial_count: toNumber(payload.completed_trial_count),
    max_completed_trial_id: toNumber(payload.max_completed_trial_id),
    trial_sent_wallclock_by_trial: normalizeNumberRecord(payload.trial_sent_wallclock_by_trial),
    next_release_target_wallclock_by_trial: normalizeNumberRecord(payload.next_release_target_wallclock_by_trial),
    trial_terminal_team_id_list_by_trial: trialTerminalTeamIdListByTrial,
    trial_observed_terminal_team_id_list_by_trial: Object.keys(trialObservedTerminalTeamIdListByTrial).length > 0
      ? trialObservedTerminalTeamIdListByTrial
      : trialTerminalTeamIdListByTrial,
    trial_forced_terminal_team_id_list_by_trial: trialForcedTerminalTeamIdListByTrial,
    trial_terminal_watchdog_deadline_wallclock_by_trial: normalizeNumberRecord(
      payload.trial_terminal_watchdog_deadline_wallclock_by_trial
    ),
    trial_terminal_watchdog_base_timeout_seconds_by_trial: normalizeNumberRecord(
      payload.trial_terminal_watchdog_base_timeout_seconds_by_trial
    ),
  };
}

function normalizeRuntimeStageGroupStatus(value: unknown): RuntimeStageGroupStatus {
  const payload = (value && typeof value === 'object' && !Array.isArray(value))
    ? value as Record<string, unknown>
    : {};
  const stageStatusList = Array.isArray(payload.stage_status_list)
    ? payload.stage_status_list.map((item) => normalizeRuntimeStageStageStatus(item))
    : [];
  return {
    group_id: String(payload.group_id ?? ''),
    configured_team_id_list: normalizeStringArray(payload.configured_team_id_list),
    stage_status_list: stageStatusList,
  };
}

function normalizeRuntimeStageStatus(value: unknown): RuntimeStageStatus {
  const payload = (value && typeof value === 'object' && !Array.isArray(value))
    ? value as Record<string, unknown>
    : {};
  const groupStatusList = Array.isArray(payload.group_status_list)
    ? payload.group_status_list.map((item) => normalizeRuntimeStageGroupStatus(item))
    : [];
  return {
    release_policy: payload.release_policy == null ? null : String(payload.release_policy),
    trial_release_interval_seconds: payload.trial_release_interval_seconds == null ? null : toNumber(payload.trial_release_interval_seconds),
    trial_terminal_watchdog_base_timeout_seconds: payload.trial_terminal_watchdog_base_timeout_seconds == null
      ? null
      : toNumber(payload.trial_terminal_watchdog_base_timeout_seconds),
    trial_terminal_watchdog_grace_seconds: payload.trial_terminal_watchdog_grace_seconds == null
      ? null
      : toNumber(payload.trial_terminal_watchdog_grace_seconds),
    match_control_status: normalizeMatchControlStatus(
      payload.match_control_status as Partial<MatchControlStatus> | null | undefined
    ),
    updated_at: payload.updated_at == null ? null : toNumber(payload.updated_at),
    group_status_list: groupStatusList,
  };
}

function normalizeSystemComponents(payload: RawSystemResponse | null | undefined): SystemStatusData {
  return {
    judge_web: {
      status: payload?.judge_web?.status ?? 'unknown',
    },
    match_control_status: normalizeMatchControlStatus(payload?.match_control_status),
    runtime_stage_status: normalizeRuntimeStageStatus(payload?.runtime_stage_status),
    team_component_status_list: (payload?.team_component_status_list ?? []).map((item) => ({
      name: String(item.team_display_name ?? item.team_id ?? item.processor_component_id ?? 'unknown'),
      status: String(item.run_status ?? item.connection_status ?? 'unknown'),
      details: [
        item.processor_component_id ? `processor=${item.processor_component_id}` : null,
        item.collector_component_id ? `collector=${item.collector_component_id}` : null,
        item.calibration_status ? `calibration=${item.calibration_status}` : null,
      ]
        .filter(Boolean)
        .join(' | '),
    })),
  };
}

function normalizeRecoveryStatus(payload: RawRecoveryStatusResponse | null | undefined): RecoveryStatus {
  return {
    results_root_exists: Boolean(payload?.results_root_exists),
    resume_available: Boolean(payload?.resume_available),
    result_team_dir_list: Array.isArray(payload?.result_team_dir_list)
      ? payload.result_team_dir_list.map((item) => String(item))
      : [],
    checkpoint_count: toNumber(payload?.checkpoint_count),
    inplace_stage_restart_supported: Boolean(payload?.inplace_stage_restart_supported),
    judge_restart_required_for_stage_restart: Boolean(payload?.judge_restart_required_for_stage_restart),
    pending_restart_stage_request: payload?.pending_restart_stage_request ?? null,
    pending_restart_stage_request_at: payload?.pending_restart_stage_request_at ?? null,
    pending_restart_stage_valid: Boolean(payload?.pending_restart_stage_valid),
    pending_restart_stage_message: payload?.pending_restart_stage_message ?? null,
    live_state_files: payload?.live_state_files,
    updated_at: payload?.updated_at ?? null,
  };
}

function normalizeMatchSummaryTaskItem(payload: RawMatchSummaryTaskItem): MatchSummaryTaskItem {
  return {
    task_id: String(payload.task_id ?? ''),
    exp_name: payload.exp_name ?? null,
    exp_task: payload.exp_task ?? null,
    team_count: toNumber(payload.team_count),
    finished_team_count: toNumber(payload.finished_team_count),
    mean_accuracy_percent: toNumber(payload.mean_accuracy_percent),
    mean_task_score: toNumber(payload.mean_task_score),
    total_observed_trial_count: toNumber(payload.total_observed_trial_count),
    total_timeout_count: toNumber(payload.total_timeout_count),
    timeout_rate_percent: toNumber(payload.timeout_rate_percent),
  };
}

function normalizeMatchSummaryTeamTaskItem(payload: RawMatchSummaryTeamTaskItem): MatchSummaryTeamTaskItem {
  return {
    task_id: String(payload.task_id ?? ''),
    exp_name: payload.exp_name ?? null,
    exp_task: payload.exp_task ?? null,
    task_status: String(payload.task_status ?? 'unknown'),
    subject_count: toNumber(payload.subject_count),
    observed_trial_count: toNumber(payload.observed_trial_count),
    accuracy_percent: toNumber(payload.accuracy_percent),
    avg_reaction_time_ms: toNumber(payload.avg_reaction_time_ms),
    task_score: toNumber(payload.task_score),
    timeout_count: toNumber(payload.timeout_count),
    timeout_rate_percent: toNumber(payload.timeout_rate_percent),
    updated_at: payload.updated_at ?? null,
  };
}

function normalizeMatchSummarySubjectTaskItem(payload: RawMatchSummarySubjectTaskItem): MatchSummarySubjectTaskItem {
  return {
    subject_id: payload.subject_id == null ? null : String(payload.subject_id),
    task_id: String(payload.task_id ?? ''),
    exp_name: payload.exp_name ?? null,
    exp_task: payload.exp_task ?? null,
    task_status: String(payload.task_status ?? 'unknown'),
    observed_trial_count: toNumber(payload.observed_trial_count),
    accuracy_percent: toNumber(payload.accuracy_percent),
    updated_at: payload.updated_at ?? null,
  };
}

function normalizeMatchSummaryTeamItem(payload: RawMatchSummaryTeamItem): MatchSummaryTeamItem {
  return {
    team_id: String(payload.team_id ?? ''),
    team_display_name: String(payload.team_display_name ?? payload.team_id ?? 'Unknown Team'),
    rank: toNumber(payload.rank),
    run_status: String(payload.run_status ?? 'unknown'),
    total_score: toNumber(payload.total_score),
    observed_trial_count: toNumber(payload.observed_trial_count),
    mean_accuracy_percent: toNumber(payload.mean_accuracy_percent),
    avg_reaction_time_ms: toNumber(payload.avg_reaction_time_ms),
    configured_task_count: toNumber(payload.configured_task_count),
    started_task_count: toNumber(payload.started_task_count),
    started_task_names: Array.isArray(payload.started_task_names)
      ? payload.started_task_names.map((item) => String(item))
      : [],
    timeout_count: toNumber(payload.timeout_count),
    timeout_rate_percent: toNumber(payload.timeout_rate_percent),
    task_summary_list: (payload.task_summary_list ?? []).map(normalizeMatchSummaryTeamTaskItem),
    subject_task_summary_list: (payload.subject_task_summary_list ?? []).map(normalizeMatchSummarySubjectTaskItem),
    final_score_result: payload.final_score_result ?? null,
    updated_at: payload.updated_at ?? null,
  };
}

function normalizeMatchSummary(payload: RawMatchSummaryResponse | null | undefined): MatchSummary {
  return {
    match_name: String(payload?.match_name ?? 'BCI Competition 2026 Final'),
    match_status: String(payload?.match_status ?? 'waiting_start'),
    team_count: toNumber(payload?.team_count),
    finished_team_count: toNumber(payload?.finished_team_count),
    match_finished: Boolean(payload?.match_finished),
    finished_at: payload?.finished_at == null ? null : toNumber(payload?.finished_at),
    total_observed_trial_count: toNumber(payload?.total_observed_trial_count),
    total_timeout_count: toNumber(payload?.total_timeout_count),
    timeout_rate_percent: toNumber(payload?.timeout_rate_percent),
    scoreboard: (payload?.scoreboard ?? []).map((item, index) => normalizeScoreboardItem(item, index + 1)),
    task_summary_list: (payload?.task_summary_list ?? []).map(normalizeMatchSummaryTaskItem),
    updated_at: payload?.updated_at ?? null,
  };
}

export async function getOverview(): Promise<MatchOverview> {
  const payload = await fetchJson<RawOverviewResponse>('/match/overview');
  return normalizeOverview(payload);
}

export async function getCurrentTrial(): Promise<CurrentTrial | null> {
  const payload = await fetchJson<RawCurrentResponse>('/match/current');
  return normalizeCurrentTrial(payload.current_trial);
}

export async function getTeams(): Promise<TeamInfo[]> {
  const payload = await fetchJson<RawTeamsResponse>('/match/teams');
  return (payload.team_list ?? []).map(normalizeTeam);
}

export async function getScoreboard(): Promise<ScoreboardItem[]> {
  const payload = await fetchJson<RawScoreboardResponse>('/match/scoreboard');
  return (payload.scoreboard ?? []).map((item, index) => normalizeScoreboardItem(item, index + 1));
}

export async function getSystemComponents(): Promise<SystemStatusData> {
  const payload = await fetchJson<RawSystemResponse>('/system/components');
  return normalizeSystemComponents(payload);
}

export async function getControlStatus(): Promise<MatchControlStatus> {
  const payload = await fetchJson<RawControlStatusResponse>('/control/status');
  return normalizeMatchControlStatus({
    ...(payload.match_control_status ?? {}),
    start_readiness: normalizeStartReadiness(payload.start_readiness),
  });
}

export async function getRecoveryStatus(): Promise<RecoveryStatus> {
  const payload = await fetchJson<RawRecoveryStatusResponse>('/recovery/status');
  return normalizeRecoveryStatus(payload);
}

export async function getLiveSnapshot(): Promise<LiveSnapshot> {
  const [overview, current, teams, scoreboard, system, controlStatus] = await Promise.all([
    getOverview().catch(() => null),
    getCurrentTrial().catch(() => null),
    getTeams().catch(() => []),
    getScoreboard().catch(() => []),
    getSystemComponents().catch(() => null),
    getControlStatus().catch(() => null),
  ]);

  return {
    overview,
    trial: current,
    teams,
    scoreboard,
    systemStatus: system,
    controlStatus,
  };
}

export async function getMatchSummary(): Promise<MatchSummary> {
  const payload = await fetchJson<RawMatchSummaryResponse>('/match/summary');
  return normalizeMatchSummary(payload);
}

export async function getMatchSummaryTeams(): Promise<MatchSummaryTeamItem[]> {
  const payload = await fetchJson<{ team_summary_list?: RawMatchSummaryTeamItem[] }>('/match/summary/teams');
  return (payload.team_summary_list ?? []).map(normalizeMatchSummaryTeamItem);
}

export function normalizeLivePayload(payload: unknown): LivePayload {
  const rawPayload = (payload ?? {}) as {
    overview?: RawOverviewResponse | null;
    current?: RawCurrentResponse | null;
    teams?: RawTeamsResponse | null;
    scoreboard?: RawScoreboardResponse | null;
    system?: RawSystemResponse | null;
    control?: RawControlStatusResponse | null;
    recovery?: unknown;
  };

  return {
    overview: rawPayload.overview === undefined ? undefined : normalizeOverview(rawPayload.overview),
    current: rawPayload.current === undefined ? undefined : normalizeCurrentTrial(rawPayload.current?.current_trial),
    teams: rawPayload.teams === undefined ? undefined : (rawPayload.teams?.team_list ?? []).map(normalizeTeam),
    scoreboard: rawPayload.scoreboard
      ? (rawPayload.scoreboard.scoreboard ?? []).map((item, index) => normalizeScoreboardItem(item, index + 1))
      : undefined,
    system: rawPayload.system === undefined ? undefined : normalizeSystemComponents(rawPayload.system),
    control: rawPayload.control === undefined
      ? undefined
      : { match_control_status: normalizeMatchControlStatus(rawPayload.control?.match_control_status) },
    recovery: rawPayload.recovery === undefined
      ? undefined
      : normalizeRecoveryStatus(rawPayload.recovery as RawRecoveryStatusResponse),
  };
}

export async function startMatch(): Promise<void> {
  await postJson('/control/start-match');
}

export async function pauseMatch(): Promise<void> {
  await postJson('/control/pause');
}

export async function resumeMatch(): Promise<void> {
  await postJson('/control/resume');
}

export async function getRecoveryCheckpoints(): Promise<RecoveryCheckpoint[]> {
  const payload = await fetchJson<{ checkpoint_list?: RecoveryCheckpoint[] }>('/recovery/checkpoints');
  return Array.isArray(payload.checkpoint_list)
    ? payload.checkpoint_list.map((item) => ({
      checkpoint_id: String(item.checkpoint_id ?? ''),
      subject_id: String(item.subject_id ?? ''),
      exp_name: String(item.exp_name ?? ''),
      exp_task: String(item.exp_task ?? ''),
      session_id: String(item.session_id ?? ''),
      created_at: item.created_at ?? null,
      description: item.description ? String(item.description) : undefined,
    }))
    : [];
}

export async function resumeGlobal(): Promise<void> {
  await postJson('/recovery/resume');
}

export async function resumeStage(payload: RecoveryStageDescriptor): Promise<void> {
  await postJson('/recovery/restart-stage', {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

