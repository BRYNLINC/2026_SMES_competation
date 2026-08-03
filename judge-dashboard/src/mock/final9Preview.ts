import type {
  CurrentTrial,
  MatchControlStatus,
  MatchOverview,
  ScoreboardItem,
  SystemStatusData,
  TeamInfo,
} from '../api/types';

export type Final9PreviewData = {
  overview: MatchOverview;
  trial: CurrentTrial;
  teams: TeamInfo[];
  scoreboard: ScoreboardItem[];
  systemStatus: SystemStatusData;
  controlStatus: MatchControlStatus;
};

export function isFinal9PreviewEnabled(): boolean {
  if (typeof window === 'undefined') {
    return false;
  }
  const params = new URLSearchParams(window.location.search);
  const previewMode = params.get('preview');
  return previewMode === 'final9' || previewMode === 'final17';
}

export function buildFinal9PreviewData(): Final9PreviewData {
  const teamNameList = [
    '赛队1',
    '这是一个非常长的名字这是一个非常长的名字这是一个非常长的名字这是一个非常长的名字这是一个非常长的名字',
    '！！！@#@#@￥！@%%（%Y（@%@&%（@&%8',
    '-13216520654',
    'にほんご team',
    '韩语팀명입니다',
    '1 select * from database',
    '脑连科技队',
    'naoliankejidui',
    '智能交互战队',
    'Gamma BCILab',
    '北航脑机接口队',
    '闭环控制先锋队',
    'NeuroSpark',
    '清华感知队',
    'AlphaMotor',
    '终场逆袭队',
  ];

  const statusPattern: Array<Pick<TeamInfo, 'connection_status' | 'run_status' | 'calibration_status' | 'predict_label' | 'predict_time_ms' | 'is_timeout' | 'judge_message'>> = [
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 118.23, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 0, predict_time_ms: 126.54, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 132.82, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'starting', calibration_status: 'pending', predict_label: 0, predict_time_ms: 156.13, is_timeout: false, judge_message: '模型正在热启动，首轮预测耗时略高。' },
    { connection_status: 'reconnecting', run_status: 'starting', calibration_status: 'pending', predict_label: null, predict_time_ms: null, is_timeout: false, judge_message: '链路短暂抖动，等待恢复。' },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 109.61, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 0, predict_time_ms: 141.07, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 173.45, is_timeout: true, judge_message: '当前 trial 接近超时阈值，请关注算法延迟。' },
    { connection_status: 'disconnected', run_status: 'stopped', calibration_status: 'pending', predict_label: null, predict_time_ms: null, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 104.18, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 0, predict_time_ms: 111.46, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'starting', calibration_status: 'pending', predict_label: 1, predict_time_ms: 149.37, is_timeout: false, judge_message: '校准完成，等待进入 online 阶段。' },
    { connection_status: 'reconnecting', run_status: 'starting', calibration_status: 'pending', predict_label: null, predict_time_ms: null, is_timeout: false, judge_message: '网络重连中，预计很快恢复。' },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 97.82, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 0, predict_time_ms: 122.04, is_timeout: false, judge_message: null },
    { connection_status: 'connected', run_status: 'running', calibration_status: 'ready', predict_label: 1, predict_time_ms: 183.62, is_timeout: true, judge_message: '本轮推理耗时偏高，接近 watchdog。' },
    { connection_status: 'connected', run_status: 'idle', calibration_status: 'pending', predict_label: null, predict_time_ms: null, is_timeout: false, judge_message: '当前阶段尚未放行，等待裁判端控制。' },
  ];

  const teams: TeamInfo[] = teamNameList.map((teamName, index) => {
    const teamId = `team:${index + 1}`;
    const pattern = statusPattern[index];
    return {
      team_id: teamId,
      team_display_name: teamName,
      connection_status: pattern.connection_status,
      run_status: pattern.run_status,
      calibration_status: pattern.calibration_status,
      predict_label: pattern.predict_label,
      true_label: null,
      predict_time_ms: pattern.predict_time_ms,
      is_timeout: pattern.is_timeout,
      is_invalid_output: false,
      judge_message: pattern.judge_message,
      current_trial_score: pattern.is_timeout ? 0 : Math.max(0, 92 - index * 4.1),
      current_total_score: 520 - index * 21,
      current_task_score: Math.max(0, 91.5 - index * 3.35),
      current_task_accuracy_percent: Math.max(0, 96.2 - index * 1.7),
      mean_accuracy_percent: Math.max(0, 95.8 - index * 1.5),
      avg_reaction_time_ms: pattern.predict_time_ms ?? 0,
      last_disconnect_at: pattern.connection_status === 'disconnected' ? 1713772800 : null,
      last_disconnect_reason: pattern.connection_status === 'disconnected' ? '模拟预览: 算法端与裁判端连接中断。' : null,
      recovery_advice: pattern.connection_status === 'disconnected' ? '检查选手端程序与交换机链路，恢复后重新进入比赛。' : null,
      forfeit_current_task: false,
      forfeit_task_signature: null,
      reconnected_at: null,
    };
  });

  const scoreboard: ScoreboardItem[] = teams.map((team, index) => ({
    rank: index + 1,
    team_id: team.team_id,
    total_score: Number((560 - index * 18.7).toFixed(2)),
    average_score: Number((93.6 - index * 2.3).toFixed(2)),
    observed_trial_count: 48 - index,
    mean_accuracy_percent: Number(team.mean_accuracy_percent.toFixed(2)),
    avg_reaction_time_ms: Number((team.predict_time_ms ?? 158.0 + index * 4.2).toFixed(2)),
    run_status: team.run_status,
  }));

  return {
    overview: {
      match_name: 'BCI Competition 2026 Final',
      match_status: 'running',
      team_count: teams.length,
      connected_team_count: teams.filter((team) => team.connection_status === 'connected').length,
      calibrated_team_count: teams.filter((team) => team.calibration_status === 'ready').length,
      start_readiness: {
        ready: true,
        reason_list: [],
        configured_team_id_list: teams.map((team) => team.team_id),
        connected_team_id_list: teams.filter((team) => team.connection_status === 'connected').map((team) => team.team_id),
        pending_team_id_list: teams.filter((team) => team.connection_status !== 'connected').map((team) => team.team_id),
        pending_group_id_list: [],
        group_readiness_list: [],
        updated_at: Date.now() / 1000,
      },
    },
    trial: {
      subject_id: 'S03',
      exp_name: 'VMI',
      exp_task: 'RIGHT_VS_REST',
      session_id: 'session1',
      block_id: 1,
      trial_id: 1,
      true_label: 1,
      status: 'running',
      current_subject_index: 3,
      total_subject_count: 12,
    },
    teams,
    scoreboard,
    systemStatus: {
      judge_web: { status: 'running' },
      match_control_status: {
        waiting_start: false,
        match_started: true,
        pause_requested: false,
        paused: false,
        updated_at: Date.now() / 1000,
      },
      runtime_stage_status: {
        release_policy: 'fixed_interval',
        trial_release_interval_seconds: 1.3,
        trial_terminal_watchdog_base_timeout_seconds: 1.0,
        trial_terminal_watchdog_grace_seconds: 0.3,
        match_control_status: {
          waiting_start: false,
          match_started: true,
          pause_requested: false,
          paused: false,
          updated_at: Date.now() / 1000,
        },
        updated_at: Date.now() / 1000,
        group_status_list: [],
      },
      team_component_status_list: teams.map((team) => ({
        name: team.team_display_name,
        status: team.run_status,
        details: `connection=${team.connection_status} | calibration=${team.calibration_status}`,
      })),
    },
    controlStatus: {
      waiting_start: false,
      match_started: true,
      pause_requested: false,
      paused: false,
      started_at: Date.now() / 1000 - 180,
      updated_at: Date.now() / 1000,
    },
  };
}
