import { useEffect, useState } from 'react';
import { getMatchSummary, getMatchSummaryTeams } from '../api/rest';
import type { MatchSummary, MatchSummaryTeamItem } from '../api/types';
import { Trophy, Activity, Target, AlertTriangle, FileBarChart, ChevronRight, ChevronDown, ArrowLeft, SquareChartGantt } from 'lucide-react';
import { buildFinal9SummaryPreviewData, isFinal9SummaryPreviewEnabled } from '../mock/summary9Preview';
import logo from '../assets/logo.png';

interface SummaryPageProps {
  onBackToLive?: () => void;
}

export const SummaryPage = ({ onBackToLive }: SummaryPageProps) => {
  const [summary, setSummary] = useState<MatchSummary | null>(null);
  const [teamsSummary, setTeamsSummary] = useState<MatchSummaryTeamItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedTeams, setExpandedTeams] = useState<Record<string, boolean>>({});
  const isPreviewMode = isFinal9SummaryPreviewEnabled();

  useEffect(() => {
    let mounted = true;
    let refreshTimer: number | null = null;
    let refreshInFlight = false;
    let lastRefreshAt = 0;

    if (isPreviewMode) {
      const preview = buildFinal9SummaryPreviewData();
      setSummary(preview.summary);
      setTeamsSummary(preview.teamsSummary);
      setError(null);
      setLoading(false);
      return () => {
        mounted = false;
      };
    }

    const loadSummary = async (isInitialLoad = false) => {
      if (!isInitialLoad) {
        const now = Date.now();
        if (refreshInFlight || now - lastRefreshAt < 1000) {
          return;
        }
        refreshInFlight = true;
        lastRefreshAt = now;
      }
      try {
        const [s, t] = await Promise.all([getMatchSummary(), getMatchSummaryTeams()]);
        if (!mounted) return;
        setSummary(s);
        const sortedTeams = [...t].sort((a, b) => b.total_score - a.total_score || a.team_display_name.localeCompare(b.team_display_name, 'zh-CN'));
        setTeamsSummary(sortedTeams);
        setExpandedTeams((prev) => {
          const nextState = { ...prev };
          sortedTeams.forEach((team) => {
            if (nextState[team.team_id] === undefined) {
              nextState[team.team_id] = false;
            }
          });
          return nextState;
        });
        setError(null);
      } catch (err) {
        if (!mounted) return;
        console.error('Failed to fetch summary data:', err);
        setError(String(err));
      } finally {
        if (!isInitialLoad) {
          refreshInFlight = false;
        }
        if (mounted && isInitialLoad) {
          setLoading(false);
        }
      }
    };

    void loadSummary(true);
    refreshTimer = window.setInterval(() => {
      void loadSummary(false);
    }, 5000);

    return () => {
      mounted = false;
      if (refreshTimer !== null) {
        window.clearInterval(refreshTimer);
      }
    };
  }, [isPreviewMode]);

  const toggleTeamExpansion = (teamId: string) => {
    setExpandedTeams((prev) => ({
      ...prev,
      [teamId]: !prev[teamId]
    }));
  };

  const handleBackToLive = () => {
    onBackToLive?.();
  };

  const formatTask = (name: string | null | undefined) => {
    if (!name) return '-';
    const formatted = name.toUpperCase();
    if (formatted === 'VMI') return '运动想象';
    if (formatted === 'VME') return '运动执行';
    if (formatted === 'LEFT_VS_REST') return '左手 vs 静息';
    if (formatted === 'RIGHT_VS_REST') return '右手 vs 静息';
    return formatted;
  };

  const translateMatchStatus = (status: string) => {
    const s = status.toLowerCase();
    if (s === 'idle' || s === 'waiting_start') return '等待开始';
    if (s === 'started' || s === 'starting') return '启动中';
    if (s === 'running') return '进行中';
    if (s === 'ended' || s === 'stopped' || s === 'finished') return '已结赛';
    return status.toUpperCase();
  };

  const translateTaskStatus = (status: string) => {
    const normalized = status.toLowerCase();
    if (normalized === 'finished') return '已完成';
    if (normalized === 'running') return '进行中';
    if (normalized === 'pending' || normalized === 'idle') return '未开始';
    return status;
  };

  if (loading) {
    return (
      <div className="h-screen bg-slate-950 text-slate-100 flex items-center justify-center font-sans tracking-wide overflow-y-auto">
        <div className="flex flex-col items-center">
          <Activity className="animate-spin text-blue-500 mb-4" size={48} />
          <h2 className="text-2xl font-bold text-slate-300 tracking-widest">比赛数据汇总生成中...</h2>
        </div>
      </div>
    );
  }

  if (error || !summary) {
    return (
      <div className="h-screen bg-slate-950 text-slate-100 p-8 flex flex-col items-center justify-center overflow-y-auto">
        <AlertTriangle className="text-red-500 mb-4" size={48} />
        <h2 className="text-xl font-bold text-red-400 mb-4">赛果数据获取失败</h2>
        <p className="text-slate-500">{error}</p>
        <button onClick={handleBackToLive} className="mt-8 px-6 py-2 bg-slate-800 hover:bg-slate-700 rounded text-cyan-400 transition-colors">
          返回实时大屏
        </button>
      </div>
    );
  }

  const finalScoreboardRows = (teamsSummary.length > 0
    ? teamsSummary.map((team, index) => ({
        rank: index + 1,
        team_id: team.team_id,
        team_display_name: team.team_display_name,
        total_score: team.total_score,
        mean_accuracy_percent: team.mean_accuracy_percent,
        avg_reaction_time_ms: team.avg_reaction_time_ms,
      }))
    : [...summary.scoreboard]
        .sort((a, b) => b.total_score - a.total_score || a.team_id.localeCompare(b.team_id, 'zh-CN'))
        .map((row, index) => ({
          rank: index + 1,
          team_id: row.team_id,
        team_display_name: row.team_id,
        total_score: row.total_score,
        mean_accuracy_percent: row.mean_accuracy_percent,
        avg_reaction_time_ms: row.avg_reaction_time_ms,
        })));
  const isCompactSummaryLayout = finalScoreboardRows.length >= 9;
  const hasExpandedScoreboardRow = Object.values(expandedTeams).some(Boolean);
  const shouldStretchScoreboardRows = finalScoreboardRows.length === 9 && !hasExpandedScoreboardRow;
  const getPodiumGradientClassName = (rank: number) => {
    if (rank === 1) return 'bg-gradient-to-r from-yellow-400/30 via-amber-500/20 to-transparent border-yellow-300/40';
    if (rank === 2) return 'bg-gradient-to-r from-slate-200/30 via-slate-400/20 to-transparent border-slate-200/40';
    if (rank === 3) return 'bg-gradient-to-r from-orange-700/30 via-amber-800/20 to-transparent border-orange-700/40';
    return '';
  };

  return (
    <div className={`h-screen bg-slate-950 text-slate-100 font-sans ${isCompactSummaryLayout ? 'p-3 md:p-4' : 'p-4 md:p-6'} overflow-hidden select-none flex flex-col`}>
      <div className={`flex justify-between items-center border-b border-slate-800 flex-shrink-0 ${isCompactSummaryLayout ? 'mb-4 pb-3' : 'mb-6 pb-4'}`}>
        <div className={`flex items-center min-w-0 ${isCompactSummaryLayout ? 'gap-3' : 'gap-4'}`}>
          <button 
            onClick={handleBackToLive}
             className="p-2 hover:bg-slate-800 rounded-full transition-colors text-slate-400 hover:text-white"
            title="返回实时监控"
          >
            <ArrowLeft size={24} />
          </button>
          <img src={logo} alt="比赛Logo" className={`w-auto object-contain shrink-0 ${isCompactSummaryLayout ? 'h-16' : 'h-20'}`} />
          <h1 className={`${isCompactSummaryLayout ? 'text-lg' : 'text-xl'} font-black bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400 tracking-tight`}>
            基于感觉肌肉电刺激提示的上肢运动想象分类技术与系统赛总结
          </h1>
        </div>
        <div className="shrink-0">
          <span className="px-3 py-1 bg-green-900/40 text-green-400 border border-green-800/50 rounded-lg text-sm font-bold tracking-widest">
            {translateMatchStatus(summary.match_status)}
          </span>
        </div>
      </div>

      <div className={`grid grid-cols-12 flex-1 min-h-0 animate-fade-in-up ${isCompactSummaryLayout ? 'gap-4' : 'gap-6'}`}>
        <div className="col-span-12 xl:col-span-8 min-w-0 flex flex-col min-h-0">
          <div className={`bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl shadow-xl flex flex-col min-h-0 h-full ${isCompactSummaryLayout ? 'p-4' : 'p-6'}`}>
            <div className={`flex items-center border-b border-slate-800 flex-shrink-0 ${isCompactSummaryLayout ? 'mb-4 pb-3' : 'mb-6 pb-4'}`}>
              <Trophy size={20} className="text-yellow-400 mr-3" />
              <h2 className="text-xl font-bold text-white tracking-widest uppercase">最终成绩总榜</h2>
            </div>
            <div className="grid grid-cols-[16%_1fr_24%_44px] min-w-0 px-2 py-3 text-[16px] text-slate-400 bg-slate-800/40 sticky top-0 backdrop-blur shadow-sm rounded-t-lg flex-shrink-0">
              <div className="text-center whitespace-nowrap">排名</div>
              <div className="text-center whitespace-nowrap">赛队</div>
              <div className="text-center whitespace-nowrap">得分</div>
              <div></div>
            </div>

            <div
              className={`flex-1 min-h-0 overflow-y-auto custom-scrollbar ${shouldStretchScoreboardRows ? 'grid' : 'space-y-0'}`}
              style={shouldStretchScoreboardRows ? { gridTemplateRows: `repeat(9, minmax(0, 1fr))` } : undefined}
            >
              {teamsSummary.length === 0 && finalScoreboardRows.length === 0 && (
                <div className="text-center py-6 text-slate-500">尚无榜单数据</div>
              )}
              {(teamsSummary.length > 0 ? teamsSummary : finalScoreboardRows.map((row) => ({
                team_id: row.team_id,
                team_display_name: row.team_display_name,
                rank: row.rank,
                run_status: 'unknown',
                total_score: row.total_score,
                observed_trial_count: 0,
                mean_accuracy_percent: row.mean_accuracy_percent,
                avg_reaction_time_ms: row.avg_reaction_time_ms,
                configured_task_count: 0,
                started_task_count: 0,
                started_task_names: [],
                timeout_count: 0,
                timeout_rate_percent: 0,
                task_summary_list: [],
                subject_task_summary_list: [],
              }))).map((team, index) => {
                const rank = index + 1;
                const isTop3 = rank <= 3;
                return (
                  <div key={team.team_id} className={`min-h-0 flex flex-col border-b border-slate-800/50 transition-colors ${getPodiumGradientClassName(rank)}`}>
                    <button
                      type="button"
                      className={`w-full min-w-0 ${shouldStretchScoreboardRows ? 'h-full min-h-[52px]' : 'min-h-[56px]'} grid grid-cols-[16%_1fr_24%_44px] items-center px-2 py-2.5 hover:bg-slate-800/50 transition-colors ${expandedTeams[team.team_id] ? 'bg-slate-800/60' : ''}`}
                      onClick={() => toggleTeamExpansion(team.team_id)}
                    >
                      <div className="flex justify-center">
                        <span className={`w-6 h-6 text-xs rounded-full flex items-center justify-center font-bold ${
                          rank === 1 ? 'bg-yellow-500 text-yellow-950' :
                          rank === 2 ? 'bg-slate-300 text-slate-800' :
                          rank === 3 ? 'bg-amber-700 text-amber-100' : 'bg-slate-800 text-slate-400'
                        }`}>
                          {rank}
                        </span>
                      </div>
                      <div className="min-w-0 font-semibold text-white text-center truncate px-2" title={team.team_display_name}>
                        {team.team_display_name}
                      </div>
                      <div className={`text-center font-bold font-mono whitespace-nowrap ${isTop3 ? 'text-yellow-400' : 'text-slate-300'}`}>
                        {Number(team.total_score).toFixed(2)}
                      </div>
                      <div className="flex justify-center text-slate-500">
                        {expandedTeams[team.team_id] ? <ChevronDown size={22} /> : <ChevronRight size={22} />}
                      </div>
                    </button>

                    {expandedTeams[team.team_id] && (
                      <div className={`border-t border-slate-700/50 bg-slate-950/40 shadow-inner ${isCompactSummaryLayout ? 'p-3' : 'p-4'}`}>
                        <h4 className="text-xs text-slate-400 tracking-widest mb-3 flex items-center font-bold">
                          <FileBarChart size={14} className="mr-2" /> 子任务分析明细
                        </h4>
                        <div className="overflow-x-auto custom-scrollbar">
                          <table className="w-full text-sm text-center align-middle" style={{ tableLayout: 'auto' }}>
                             <thead className="text-[10px] text-slate-500 bg-slate-900/60">
                              <tr>
                                 <th className="px-3 py-2 rounded-tl-lg whitespace-nowrap text-center">实验协议 / 任务范式</th>
                                 <th className="px-3 py-2 text-center whitespace-nowrap">状态</th>
                                 <th className="px-3 py-2 text-center whitespace-nowrap normal-case">trial 数量</th>
                                 <th className="px-3 py-2 text-center whitespace-nowrap">准确率(%)</th>
                                 <th className="px-3 py-2 text-center whitespace-nowrap normal-case">平均耗时(ms)</th>
                                 <th className="px-3 py-2 text-center whitespace-nowrap">获得分数</th>
                                 <th className="px-3 py-2 text-center rounded-tr-lg whitespace-nowrap">单项超时</th>
                               </tr>
                             </thead>
                             <tbody>
                                {team.task_summary_list?.length === 0 && (
                                   <tr><td colSpan={7} className="text-center py-4 text-slate-600">无记录的执行任务</td></tr>
                                )}
                                {team.task_summary_list?.map((t, tidx) => (
                                    <tr key={tidx} className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors">
                                    <td className="px-3 py-2 font-medium text-slate-300 whitespace-nowrap text-center">
                                      <span className="text-cyan-400/80">{formatTask(t.exp_name)}</span>
                                      <span className="text-slate-600 mx-1">/</span>
                                      <span className="text-blue-300/80">{formatTask(t.exp_task)}</span>
                                    </td>
                                    <td className="px-3 py-2 text-center whitespace-nowrap">
                                        {t.task_status === 'finished' ? 
                                          <span className="px-2 py-0.5 bg-green-900/30 text-green-400 text-[10px] rounded uppercase border border-green-800/50">已完成</span> : 
                                          <span className="px-2 py-0.5 bg-yellow-900/30 text-yellow-400 text-[10px] rounded uppercase border border-yellow-800/50">{translateTaskStatus(t.task_status)}</span>
                                        }
                                      </td>
                                    <td className="px-3 py-2 text-center text-slate-300 font-mono whitespace-nowrap">{t.observed_trial_count}</td>
                                    <td className="px-3 py-2 text-center font-mono whitespace-nowrap text-emerald-400/90">{Number(t.accuracy_percent).toFixed(2)}</td>
                                    <td className="px-3 py-2 text-center font-mono whitespace-nowrap text-slate-400">{Number(t.avg_reaction_time_ms).toFixed(2)}</td>
                                    <td className="px-3 py-2 text-center font-bold text-yellow-500/90 font-mono whitespace-nowrap">{Number(t.task_score).toFixed(2)}</td>
                                    <td className={`px-3 py-2 text-center font-mono whitespace-nowrap ${t.timeout_count > 0 ? 'text-red-400' : 'text-slate-500'}`}>
                                      {t.timeout_count} <span className="text-[9px] opacity-70">({Number(t.timeout_rate_percent).toFixed(1)}%)</span>
                                    </td>
                                  </tr>
                                ))}
                             </tbody>
                          </table>
                        </div>

                        {team.subject_task_summary_list.length > 0 && (
                          <div className="mt-6">
                            <h4 className="text-xs text-slate-400 tracking-widest mb-3 flex items-center font-bold">
                              <Target size={14} className="mr-2" /> 被试级结果
                            </h4>
                            <div className="overflow-x-auto custom-scrollbar">
                              <table className="w-full text-sm text-center align-middle" style={{ tableLayout: 'auto' }}>
                                <thead className="text-[10px] text-slate-500 bg-slate-900/60">
                                  <tr>
                                    <th className="px-3 py-2 rounded-tl-lg whitespace-nowrap text-center">被试</th>
                                    <th className="px-3 py-2 whitespace-nowrap text-center">实验协议 / 任务范式</th>
                                    <th className="px-3 py-2 text-center whitespace-nowrap">状态</th>
                                    <th className="px-3 py-2 text-center whitespace-nowrap normal-case">trial 数量</th>
                                    <th className="px-3 py-2 text-center rounded-tr-lg whitespace-nowrap">准确率(%)</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {team.subject_task_summary_list.map((item, itemIndex) => (
                                    <tr key={`${item.task_id}-${item.subject_id ?? itemIndex}`} className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors">
                                      <td className="px-3 py-2 text-slate-300 font-mono whitespace-nowrap text-center">{item.subject_id ?? '-'}</td>
                                      <td className="px-3 py-2 font-medium text-slate-300 whitespace-nowrap text-center">
                                        <span className="text-cyan-400/80">{formatTask(item.exp_name)}</span>
                                        <span className="text-slate-600 mx-1">/</span>
                                        <span className="text-blue-300/80">{formatTask(item.exp_task)}</span>
                                      </td>
                                      <td className="px-3 py-2 text-center whitespace-nowrap">
                                        <span className={`px-2 py-0.5 text-[10px] rounded uppercase border ${
                                          item.task_status === 'finished'
                                            ? 'bg-green-900/30 text-green-400 border-green-800/50'
                                            : 'bg-yellow-900/30 text-yellow-400 border-yellow-800/50'
                                        }`}>
                                          {translateTaskStatus(item.task_status)}
                                        </span>
                                      </td>
                                      <td className="px-3 py-2 text-center text-slate-300 font-mono whitespace-nowrap">{item.observed_trial_count}</td>
                                      <td className="px-3 py-2 text-center font-mono whitespace-nowrap text-emerald-400/90">{Number(item.accuracy_percent).toFixed(2)}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
        <div className="col-span-12 xl:col-span-4 min-w-0 min-h-0 flex flex-col">
          <div className={`bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl shadow-xl flex flex-col min-h-0 h-full ${isCompactSummaryLayout ? 'p-4' : 'p-6'}`}>
            <div className={`flex items-center border-b border-slate-800 flex-shrink-0 ${isCompactSummaryLayout ? 'mb-4 pb-3' : 'mb-6 pb-4'}`}>
              <SquareChartGantt size={20} className="text-cyan-400 mr-3" />
              <h2 className={`${isCompactSummaryLayout ? 'text-xl' : 'text-2xl'} font-bold text-white tracking-widest uppercase`}>全局任务完成度</h2>
            </div>
            <div
              className={`flex-1 min-h-0 overflow-y-auto custom-scrollbar pr-1 ${summary.task_summary_list?.length ? 'grid gap-2' : 'space-y-2'}`}
              style={summary.task_summary_list?.length
                ? { gridTemplateRows: `repeat(${summary.task_summary_list.length}, minmax(0, 1fr))` }
                : undefined}
            >
              {summary.task_summary_list?.length === 0 && (
                <div className="text-center py-6 text-slate-500">尚无任务汇总数据</div>
              )}
              {summary.task_summary_list?.map((task, idx) => (
                <div key={idx} className={`min-h-0 h-full bg-slate-950/60 border border-slate-700/50 rounded-lg shadow-inner ${isCompactSummaryLayout ? 'p-3' : 'p-4'}`}>
                  <div className="mb-2 border-b border-slate-800/50 pb-1.5">
                    <span className={`font-bold text-cyan-300 tracking-wide ${isCompactSummaryLayout ? 'text-[14px]' : 'text-[16px]'}`}>
                      {formatTask(task.exp_name)} <span className="text-slate-600 mx-1">/</span> {formatTask(task.exp_task)}
                    </span>
                  </div>
                  <div className={`grid grid-cols-2 h-[calc(100%-2rem)] content-start ${isCompactSummaryLayout ? 'gap-x-4 gap-y-3 text-[15px]' : 'gap-x-4 gap-y-3 text-[15px]'} text-slate-300`}>
                    <div className="flex items-center justify-between">
                      <span>完成</span>
                      <span className="font-bold text-slate-200 ml-2">{task.finished_team_count}/{task.team_count}</span>
                    </div>
                    <div className="flex items-center justify-between normal-case">
                      <span>trial数量</span>
                      <span className="font-bold text-slate-200 ml-2">{task.total_observed_trial_count}</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span>准确率</span>
                      <span className="font-bold text-emerald-400 ml-2">{Number(task.mean_accuracy_percent).toFixed(2)}%</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span>超时率</span>
                      <span className="font-bold text-yellow-500 ml-2">{Number(task.timeout_rate_percent).toFixed(2)}%</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
