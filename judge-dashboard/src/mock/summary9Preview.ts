import type {
  MatchSummary,
  MatchSummaryTaskItem,
  MatchSummaryTeamItem,
  MatchSummaryTeamTaskItem,
  MatchSummarySubjectTaskItem,
  ScoreboardItem,
} from '../api/types';

export type Summary9PreviewData = {
  summary: MatchSummary;
  teamsSummary: MatchSummaryTeamItem[];
};

const TEAM_NAME_LIST = [
  '赛队1',
  '这是一个非常长的名字这是一个非常长的名字这是一个非常长的名字这是一个非常长的名字这是一个非常长的名字',
  '！！！@#@#@￥！@%%（%Y（@%@&%（@&%8',
  '-13216520654',
  'にほんご team',
  '韩语팀명입니다',
  'select * from database',
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

const TASK_TEMPLATE_LIST: Array<{
  task_id: string;
  exp_name: string;
  exp_task: string;
}> = [
  { task_id: 'task:1', exp_name: 'VMI', exp_task: 'LEFT_VS_REST' },
  { task_id: 'task:2', exp_name: 'VMI', exp_task: 'RIGHT_VS_REST' },
  { task_id: 'task:3', exp_name: 'VME', exp_task: 'LEFT_VS_REST' },
  { task_id: 'task:4', exp_name: 'VME', exp_task: 'RIGHT_VS_REST' },
];

export function isFinal9SummaryPreviewEnabled(): boolean {
  if (typeof window === 'undefined') {
    return false;
  }
  const params = new URLSearchParams(window.location.search);
  const previewMode = params.get('preview');
  return previewMode === 'summary9' || previewMode === 'summary17';
}

function buildTaskSummary(teamIndex: number): MatchSummaryTeamTaskItem[] {
  return TASK_TEMPLATE_LIST.map((task, taskIndex) => {
    const observedTrialCount = 48 - taskIndex;
    const timeoutCount = (teamIndex + taskIndex) % 3 === 0 ? 1 : 0;
    return {
      task_id: task.task_id,
      exp_name: task.exp_name,
      exp_task: task.exp_task,
      task_status: 'finished',
      subject_count: 12,
      observed_trial_count: observedTrialCount,
      accuracy_percent: Number((95.4 - teamIndex * 1.25 - taskIndex * 0.35).toFixed(2)),
      avg_reaction_time_ms: Number((114.8 + teamIndex * 5.4 + taskIndex * 4.1).toFixed(2)),
      task_score: Number((142.6 - teamIndex * 4.8 - taskIndex * 1.9).toFixed(2)),
      timeout_count: timeoutCount,
      timeout_rate_percent: Number(((timeoutCount / observedTrialCount) * 100).toFixed(2)),
      updated_at: Date.now() / 1000,
    };
  });
}

function buildSubjectTaskSummary(teamIndex: number): MatchSummarySubjectTaskItem[] {
  return TASK_TEMPLATE_LIST.flatMap((task, taskIndex) => (
    ['S01', 'S02'].map((subjectId, subjectIndex) => ({
      subject_id: `${subjectId}-${teamIndex + 1}`,
      task_id: task.task_id,
      exp_name: task.exp_name,
      exp_task: task.exp_task,
      task_status: 'finished',
      observed_trial_count: 24 - taskIndex,
      accuracy_percent: Number((94.9 - teamIndex * 1.1 - taskIndex * 0.45 - subjectIndex * 0.2).toFixed(2)),
      updated_at: Date.now() / 1000,
    }))
  ));
}

function buildTeamSummaryList(): MatchSummaryTeamItem[] {
  return TEAM_NAME_LIST.map((teamName, index) => {
    const taskSummaryList = buildTaskSummary(index);
    const observedTrialCount = taskSummaryList.reduce((sum, item) => sum + item.observed_trial_count, 0);
    const timeoutCount = taskSummaryList.reduce((sum, item) => sum + item.timeout_count, 0);
    return {
      team_id: `team:${index + 1}`,
      team_display_name: teamName,
      rank: index + 1,
      run_status: 'finished',
      total_score: Number((568.3 - index * 18.45).toFixed(2)),
      observed_trial_count: observedTrialCount,
      mean_accuracy_percent: Number((95.8 - index * 1.35).toFixed(2)),
      avg_reaction_time_ms: Number((118.4 + index * 6.15).toFixed(2)),
      configured_task_count: TASK_TEMPLATE_LIST.length,
      started_task_count: TASK_TEMPLATE_LIST.length,
      started_task_names: TASK_TEMPLATE_LIST.map((task) => task.exp_task),
      timeout_count: timeoutCount,
      timeout_rate_percent: Number(((timeoutCount / observedTrialCount) * 100).toFixed(2)),
      task_summary_list: taskSummaryList,
      subject_task_summary_list: buildSubjectTaskSummary(index),
      final_score_result: null,
      updated_at: Date.now() / 1000,
    };
  });
}

function buildScoreboard(teamsSummary: MatchSummaryTeamItem[]): ScoreboardItem[] {
  return teamsSummary.map((team, index) => ({
    rank: index + 1,
    team_id: team.team_id,
    total_score: team.total_score,
    average_score: Number((team.total_score / TASK_TEMPLATE_LIST.length).toFixed(2)),
    observed_trial_count: team.observed_trial_count,
    mean_accuracy_percent: team.mean_accuracy_percent,
    avg_reaction_time_ms: team.avg_reaction_time_ms,
    run_status: team.run_status,
  }));
}

function buildTaskOverviewList(): MatchSummaryTaskItem[] {
  return TASK_TEMPLATE_LIST.map((task, index) => ({
    task_id: task.task_id,
    exp_name: task.exp_name,
    exp_task: task.exp_task,
    team_count: TEAM_NAME_LIST.length,
    finished_team_count: TEAM_NAME_LIST.length,
    mean_accuracy_percent: Number((92.6 - index * 0.8).toFixed(2)),
    mean_task_score: Number((132.4 - index * 3.25).toFixed(2)),
    total_observed_trial_count: TEAM_NAME_LIST.length * (48 - index),
    total_timeout_count: index + 2,
    timeout_rate_percent: Number((((index + 2) / (TEAM_NAME_LIST.length * (48 - index))) * 100).toFixed(2)),
  }));
}

export function buildFinal9SummaryPreviewData(): Summary9PreviewData {
  const teamsSummary = buildTeamSummaryList();
  const scoreboard = buildScoreboard(teamsSummary);
  const taskSummaryList = buildTaskOverviewList();

  return {
    summary: {
      match_name: 'BCI Competition 2026 Final Summary Preview',
      match_status: 'finished',
      team_count: TEAM_NAME_LIST.length,
      finished_team_count: TEAM_NAME_LIST.length,
      match_finished: true,
      finished_at: Date.now() / 1000 - 60,
      total_observed_trial_count: teamsSummary.reduce((sum, team) => sum + team.observed_trial_count, 0),
      total_timeout_count: teamsSummary.reduce((sum, team) => sum + team.timeout_count, 0),
      timeout_rate_percent: Number((
        (teamsSummary.reduce((sum, team) => sum + team.timeout_count, 0)
          / teamsSummary.reduce((sum, team) => sum + team.observed_trial_count, 0)) * 100
      ).toFixed(2)),
      scoreboard,
      task_summary_list: taskSummaryList,
      updated_at: Date.now() / 1000,
    },
    teamsSummary,
  };
}
