export interface StartReadiness {
  ready: boolean;
  reason_list: string[];
  configured_team_id_list?: string[];
  connected_team_id_list?: string[];
  pending_team_id_list?: string[];
  pending_group_id_list?: string[];
  group_readiness_list?: Array<{
    group_id: string;
    configured_team_id_list?: string[];
    stage_observed?: boolean;
    collector_ready?: boolean;
  }>;
  updated_at?: number | null;
}

export interface StageDescriptor {
  subject_id: string;
  exp_name: string;
  exp_task: string;
}

export interface RecoveryStageDescriptor extends StageDescriptor {
  session_id: string;
}

export type RecoveryMode = 'continue_from_checkpoint' | 'restart_from_stage';

export interface MatchOverview {
  match_name: string;
  match_status: string;
  team_count: number;
  connected_team_count: number;
  calibrated_team_count: number;
  start_readiness?: StartReadiness | null;
}

export interface RecoveryStatus {
  results_root_exists: boolean;
  resume_available: boolean;
  result_team_dir_list: string[];
  checkpoint_count?: number;
  inplace_stage_restart_supported?: boolean;
  judge_restart_required_for_stage_restart?: boolean;
  pending_recovery_mode?: RecoveryMode | null;
  pending_recovery_request?: RecoveryStageDescriptor | null;
  pending_recovery_request_at?: number | null;
  pending_recovery_valid?: boolean;
  pending_recovery_message?: string | null;
  recommended_recovery_stage?: RecoveryStageDescriptor | null;
  pending_restart_stage_request?: RecoveryStageDescriptor | null;
  pending_restart_stage_request_at?: number | null;
  pending_restart_stage_valid?: boolean;
  pending_restart_stage_message?: string | null;
  live_state_files?: {
    current_trial?: boolean;
    runtime_stage_status?: boolean;
    match_control_status?: boolean;
    team_live_count?: number;
  };
  updated_at?: number | null;
}

export interface MatchControlStatus {
  waiting_start: boolean;
  match_started: boolean;
  match_finished?: boolean;
  finished_at?: number | null;
  finished_team_id_list?: string[];
  pause_requested?: boolean;
  paused?: boolean;
  started_at?: number | null;
  paused_at?: number | null;
  resumed_at?: number | null;
  last_seen_start_request_at?: number | null;
  last_seen_pause_request_at?: number | null;
  last_seen_resume_request_at?: number | null;
  pending_recovery_mode?: RecoveryMode | null;
  pending_recovery_request?: RecoveryStageDescriptor | null;
  pending_recovery_request_at?: number | null;
  pending_recovery_valid?: boolean;
  pending_restart_stage_request?: RecoveryStageDescriptor | null;
  pending_restart_stage_request_at?: number | null;
  pending_restart_stage_valid?: boolean;
  coordinator_started_at?: number | null;
  start_readiness?: StartReadiness | null;
  updated_at?: number | null;
}

export interface RuntimeStageContext {
  subject_id: string;
  exp_name: string;
  exp_task: string;
  session_id: string;
}

export interface RuntimeStageStageStatus {
  stage_key: string;
  stage_context: RuntimeStageContext;
  collector_prepared: boolean;
  ready_team_id_list: string[];
  forfeited_team_id_list: string[];
  calibration_forfeit_detail_by_team: Record<string, Record<string, unknown>>;
  pending_ready_team_id_list: string[];
  online_stage_released: boolean;
  pending_release?: Record<string, unknown> | null;
  online_trial_count: number;
  released_trial_id: number;
  completed_trial_id_list: number[];
  completed_trial_count: number;
  max_completed_trial_id: number;
  trial_sent_wallclock_by_trial: Record<string, number>;
  next_release_target_wallclock_by_trial: Record<string, number>;
  trial_terminal_team_id_list_by_trial: Record<string, string[]>;
  trial_observed_terminal_team_id_list_by_trial: Record<string, string[]>;
  trial_forced_terminal_team_id_list_by_trial: Record<string, string[]>;
  trial_terminal_watchdog_deadline_wallclock_by_trial: Record<string, number>;
  trial_terminal_watchdog_base_timeout_seconds_by_trial: Record<string, number>;
}

export interface RuntimeStageGroupStatus {
  group_id: string;
  configured_team_id_list: string[];
  stage_status_list: RuntimeStageStageStatus[];
}

export interface RuntimeStageStatus {
  release_policy?: string | null;
  trial_release_interval_seconds?: number | null;
  trial_terminal_watchdog_base_timeout_seconds?: number | null;
  trial_terminal_watchdog_grace_seconds?: number | null;
  match_control_status?: MatchControlStatus | null;
  updated_at?: number | null;
  group_status_list: RuntimeStageGroupStatus[];
}

export interface CurrentTrial {
  subject_id: string;
  exp_name: string;
  exp_task: string;
  session_id: number | string;
  block_id: number | string;
  trial_id: number | string;
  true_label: number | string | null;
  status?: string;
  error_type?: string | null;
  error_message?: string | null;
  recovery_advice?: string | null;
  release_wallclock?: number;
  dispatch_wallclock?: number;
  prediction_deadline_wallclock?: number;
  cycle_end_wallclock?: number;
  trial_sent_wallclock?: number;
  next_release_target_wallclock?: number;
  current_subject_index?: number | null;
  total_subject_count?: number | null;
}

export type ConnectionStatus = 'connected' | 'connecting' | 'error' | 'closed' | 'stopped' | 'disconnected' | 'reconnecting';
export type CalibrationStatus = 'ready' | 'pending' | 'none';

export interface TeamInfo {
  team_id: string;
  team_display_name: string;
  connection_status: ConnectionStatus;
  run_status: string;
  calibration_status: CalibrationStatus;
  predict_label: number | string | null;
  true_label: number | string | null;
  predict_time_ms: number | null;
  is_timeout: boolean;
  is_invalid_output?: boolean;
  judge_message?: string | null;
  current_trial_score?: number;
  current_total_score: number;
  current_task_score: number;
  current_task_accuracy_percent: number;
  mean_accuracy_percent: number;
  avg_reaction_time_ms: number;
  last_disconnect_at?: number | string | null;
  last_disconnect_reason?: string | null;
  recovery_advice?: string | null;
  forfeit_current_task?: boolean;
  forfeit_task_signature?: StageDescriptor | null;
  reconnected_at?: number | string | null;
}

export interface RecoveryCheckpoint extends RecoveryStageDescriptor {
  checkpoint_id: string;
  created_at: number | string;
  description?: string;
}

export interface ScoreboardItem {
  rank: number;
  team_id: string;
  total_score: number;
  average_score?: number;
  observed_trial_count: number;
  mean_accuracy_percent: number;
  avg_reaction_time_ms: number;
  run_status: string;
}

export interface SystemComponentItem {
  name: string;
  status: string;
  details?: string;
}

export interface SystemStatusData {
  judge_web?: { status: string };
  match_control_status?: MatchControlStatus;
  runtime_stage_status?: RuntimeStageStatus;
  team_component_status_list?: SystemComponentItem[];
}

export interface MatchSummaryTaskItem {
  task_id: string;
  exp_name?: string | null;
  exp_task?: string | null;
  team_count?: number;
  finished_team_count?: number;
  mean_accuracy_percent?: number;
  mean_task_score?: number;
  total_observed_trial_count?: number;
  total_timeout_count?: number;
  timeout_rate_percent?: number;
}

export interface MatchSummaryTeamTaskItem {
  task_id: string;
  exp_name?: string | null;
  exp_task?: string | null;
  task_status: string;
  subject_count: number;
  observed_trial_count: number;
  accuracy_percent: number;
  avg_reaction_time_ms: number;
  task_score: number;
  timeout_count: number;
  timeout_rate_percent: number;
  updated_at?: string | number | null;
}

export interface MatchSummarySubjectTaskItem {
  subject_id?: string | null;
  task_id: string;
  exp_name?: string | null;
  exp_task?: string | null;
  task_status: string;
  observed_trial_count: number;
  accuracy_percent: number;
  updated_at?: string | number | null;
}

export interface MatchSummaryTeamItem {
  team_id: string;
  team_display_name: string;
  rank: number;
  run_status: string;
  total_score: number;
  observed_trial_count: number;
  mean_accuracy_percent: number;
  avg_reaction_time_ms: number;
  configured_task_count: number;
  started_task_count: number;
  started_task_names: string[];
  timeout_count: number;
  timeout_rate_percent: number;
  task_summary_list: MatchSummaryTeamTaskItem[];
  subject_task_summary_list: MatchSummarySubjectTaskItem[];
  final_score_result?: Record<string, unknown> | null;
  updated_at?: string | number | null;
}

export interface MatchSummary {
  match_name: string;
  match_status: string;
  team_count: number;
  finished_team_count: number;
  match_finished?: boolean;
  finished_at?: number | null;
  total_observed_trial_count: number;
  total_timeout_count: number;
  timeout_rate_percent: number;
  scoreboard: ScoreboardItem[];
  task_summary_list: MatchSummaryTaskItem[];
  updated_at?: number | null;
}

export interface LivePayload {
  overview?: MatchOverview | null;
  current?: CurrentTrial | null;
  teams?: TeamInfo[];
  scoreboard?: ScoreboardItem[];
  system?: SystemStatusData | null;
  control?: { match_control_status: MatchControlStatus } | null;
  recovery?: RecoveryStatus | null;
}
