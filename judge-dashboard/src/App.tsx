import { useEffect, useRef, useState } from 'react';
import { useSyncLive } from './hooks/useSyncLive';
import { Overview } from './components/Overview';
import { CurrentTrial } from './components/CurrentTrial';
import { TeamCard } from './components/TeamCard';
import { Leaderboard } from './components/Leaderboard';
import { SystemStatus } from './components/SystemStatus';
import { SummaryPage } from './components/SummaryPage';
import { useJudgeStore } from './store/useJudgeStore';
import { getLiveSnapshot, pauseMatch, resumeMatch, resumeStage, startMatch } from './api/rest';
import type { RecoveryStageDescriptor, TeamInfo } from './api/types';
import { RecoveryModal } from './components/RecoveryModal';
import { Play, Pause, RefreshCw } from 'lucide-react';
import { buildFinal9PreviewData, isFinal9PreviewEnabled } from './mock/final9Preview';
import { isFinal9SummaryPreviewEnabled } from './mock/summary9Preview';

const App = () => {
  useSyncLive();
  const { teams, scoreboard, systemStatus, overview, controlStatus, updateFromRest } = useJudgeStore();
  const scoreboardOrderById = new Map(
    scoreboard.map((row, index) => [
      row.team_id,
      {
        rank: row.rank || index + 1,
        totalScore: row.total_score ?? 0,
      },
    ]),
  );
  const teamList = Object.values(teams).sort((a, b) => {
    const scoreA = scoreboardOrderById.get(a.team_id);
    const scoreB = scoreboardOrderById.get(b.team_id);
    const rankA = scoreA?.rank ?? Number.MAX_SAFE_INTEGER;
    const rankB = scoreB?.rank ?? Number.MAX_SAFE_INTEGER;
    if (rankA !== rankB) {
      return rankA - rankB;
    }

    const totalScoreA = scoreA?.totalScore ?? a.current_total_score ?? 0;
    const totalScoreB = scoreB?.totalScore ?? b.current_total_score ?? 0;
    if (totalScoreA !== totalScoreB) {
      return totalScoreB - totalScoreA;
    }

    return a.team_display_name.localeCompare(b.team_display_name, 'zh-CN');
  });
  const teamRankById = new Map(scoreboard.map((row, index) => [row.team_id, row.rank || index + 1]));
  const shouldAutoShowSummary = Boolean(controlStatus?.match_finished);
  const [isStarting, setIsStarting] = useState(false);
  const [isPausing, setIsPausing] = useState(false);
  const [isResuming, setIsResuming] = useState(false);
  const [isRecoveryModalOpen, setIsRecoveryModalOpen] = useState(false);
  const [controlFeedback, setControlFeedback] = useState<string | null>(null);
  const stableFinishedSinceRef = useRef<number | null>(null);
  const summaryRedirectTimerRef = useRef<number | null>(null);
  const autoSummaryRedirectedRef = useRef(false);
  const lastRefreshPromiseRef = useRef<Promise<void> | null>(null);
  const isSummaryPreviewMode = isFinal9SummaryPreviewEnabled();
  const [route, setRoute] = useState<'live' | 'summary'>(() => (
    window.location.hash === '#/summary' || isSummaryPreviewMode ? 'summary' : 'live'
  ));
  const isPreviewMode = isFinal9PreviewEnabled();

  useEffect(() => {
    const handleHashChange = () => {
      setRoute(window.location.hash === '#/summary' ? 'summary' : 'live');
    };

    window.addEventListener('hashchange', handleHashChange);
    return () => {
      window.removeEventListener('hashchange', handleHashChange);
    };
  }, []);

  useEffect(() => {
    if (!isPreviewMode) {
      return;
    }
    const preview = buildFinal9PreviewData();
    const teamRecord = preview.teams.reduce<Record<string, TeamInfo>>((acc, team) => {
      acc[team.team_id] = team;
      return acc;
    }, {});
    updateFromRest({
      overview: preview.overview,
      trial: preview.trial,
      teams: teamRecord,
      scoreboard: preview.scoreboard,
      systemStatus: preview.systemStatus,
      controlStatus: preview.controlStatus,
    });
  }, [isPreviewMode, updateFromRest]);

  useEffect(() => {
    const clearSummaryRedirectTimer = () => {
      if (summaryRedirectTimerRef.current !== null) {
        window.clearTimeout(summaryRedirectTimerRef.current);
        summaryRedirectTimerRef.current = null;
      }
    };

    if (!shouldAutoShowSummary) {
      stableFinishedSinceRef.current = null;
      autoSummaryRedirectedRef.current = false;
      clearSummaryRedirectTimer();
      return;
    }

    if (route === 'summary' || autoSummaryRedirectedRef.current) {
      clearSummaryRedirectTimer();
      return;
    }

    const now = Date.now();
    if (stableFinishedSinceRef.current === null) {
      stableFinishedSinceRef.current = now;
    }

    if (summaryRedirectTimerRef.current === null) {
      const finishedStableForMs = now - stableFinishedSinceRef.current;
      const redirectDelayMs = Math.max(0, 3000 - finishedStableForMs);
      summaryRedirectTimerRef.current = window.setTimeout(() => {
        summaryRedirectTimerRef.current = null;
        autoSummaryRedirectedRef.current = true;
        if (window.location.hash !== '#/summary') {
          window.location.hash = '#/summary';
        }
      }, redirectDelayMs);
    }
  }, [shouldAutoShowSummary, route]);

  useEffect(() => () => {
    if (summaryRedirectTimerRef.current !== null) {
      window.clearTimeout(summaryRedirectTimerRef.current);
    }
  }, []);

  const refreshLiveSnapshot = async () => {
    if (isPreviewMode) {
      const preview = buildFinal9PreviewData();
      const teamRecord = preview.teams.reduce<Record<string, TeamInfo>>((acc, team) => {
        acc[team.team_id] = team;
        return acc;
      }, {});
      updateFromRest({
        overview: preview.overview,
        trial: preview.trial,
        teams: teamRecord,
        scoreboard: preview.scoreboard,
        systemStatus: preview.systemStatus,
        controlStatus: preview.controlStatus,
      });
      return;
    }
    if (lastRefreshPromiseRef.current) {
      await lastRefreshPromiseRef.current;
      return;
    }
    const refreshPromise = (async () => {
      const snapshot = await getLiveSnapshot();
      const teamRecord = snapshot.teams.reduce<Record<string, TeamInfo>>((acc, team) => {
        acc[team.team_id] = team;
        return acc;
      }, {});
      updateFromRest({
        overview: snapshot.overview,
        trial: snapshot.trial,
        teams: teamRecord,
        scoreboard: snapshot.scoreboard,
        systemStatus: snapshot.systemStatus,
        controlStatus: snapshot.controlStatus,
      });
    })();
    lastRefreshPromiseRef.current = refreshPromise;
    try {
      await refreshPromise;
    } finally {
      if (lastRefreshPromiseRef.current === refreshPromise) {
        lastRefreshPromiseRef.current = null;
      }
    }
  };

  const handleStartMatch = async () => {
    try {
      setIsStarting(true);
      setControlFeedback(null);
      await startMatch();
      updateFromRest({
        controlStatus: {
          waiting_start: false,
          match_started: true,
          pause_requested: false,
          paused: false,
          started_at: controlStatus?.started_at ?? Date.now() / 1000,
          paused_at: null,
          resumed_at: null,
          last_seen_start_request_at: Date.now() / 1000,
          last_seen_pause_request_at: controlStatus?.last_seen_pause_request_at ?? null,
          last_seen_resume_request_at: controlStatus?.last_seen_resume_request_at ?? null,
          coordinator_started_at: controlStatus?.coordinator_started_at ?? null,
          updated_at: Date.now() / 1000,
        },
      });
      await refreshLiveSnapshot();
      setControlFeedback('已记录开始比赛请求，比赛状态已切换为进行中。');
    } catch (e) {
      console.error(e);
      const message = e instanceof Error ? e.message : String(e);
      setControlFeedback(message);
      alert('开始比赛失败: ' + message);
    } finally {
      setIsStarting(false);
    }
  };

  const handlePauseMatch = async () => {
    try {
      setIsPausing(true);
      setControlFeedback(null);
      await pauseMatch();
      await refreshLiveSnapshot();
      setControlFeedback('已记录暂停请求。系统会在当前 trial 结束后暂停下一轮放行。');
    } catch (e) {
      alert('暂停操作失败: ' + String(e));
    } finally {
      setIsPausing(false);
    }
  };

  const handleResumeMatch = async () => {
    try {
      setIsResuming(true);
      setControlFeedback(null);
      await resumeMatch();
      await refreshLiveSnapshot();
      setControlFeedback('已记录继续请求。系统将从当前暂停断点继续比赛。');
    } catch (e) {
      alert('继续动作失败: ' + String(e));
    } finally {
      setIsResuming(false);
    }
  };

  const handleRestartStage = async (payload: RecoveryStageDescriptor) => {
    await resumeStage(payload);
    setControlFeedback(
      `已记录指定阶段重跑请求：${payload.subject_id} / ${payload.exp_name} / ${payload.exp_task} / ${payload.session_id}。` +
      '系统将自动重启裁判端，保留此前结果，删除该阶段及之后结果，并从该阶段重新校准、重新开始比赛。'
    );
  };

  const showSummaryPage = route === 'summary';
  if (showSummaryPage) {
    return (
      <SummaryPage
        onBackToLive={() => {
          window.location.hash = '#/live';
        }}
      />
    );
  }

  const isJudgeHealthy = systemStatus?.judge_web?.status === 'running' || systemStatus?.judge_web?.status === 'ready';
  const isBackendReady = isJudgeHealthy && Boolean(systemStatus?.runtime_stage_status);
  const startReadiness = overview?.start_readiness ?? controlStatus?.start_readiness ?? null;
  const canStartMatch = isBackendReady && Boolean(startReadiness?.ready);
  const startReadinessMessage = !isBackendReady
    ? '系统未全部就绪'
    : (startReadiness?.reason_list?.[0] ?? null);
  const matchStatus = controlStatus?.paused
    ? 'paused'
    : controlStatus?.match_started
      ? (overview?.match_status.toLowerCase() ?? 'running')
      : 'waiting_start';
  const isFinalCompactLayout = teamList.length >= 9;

  return (
    <div className={`${isFinalCompactLayout ? 'p-3 md:p-4' : 'p-4 md:p-5'} min-h-screen bg-slate-950 text-slate-100 font-sans overflow-hidden flex flex-col h-screen select-none`}>
      {isRecoveryModalOpen && (
        <RecoveryModal
          onClose={() => setIsRecoveryModalOpen(false)}
          onRestartStage={handleRestartStage}
        />
      )}
      <div className={`w-full flex-shrink-0 animate-fade-in-down ${isFinalCompactLayout ? 'mb-3' : 'mb-4'} h-auto`}>
        <Overview compact={isFinalCompactLayout} />
      </div>

      <div className={`flex-1 w-full grid grid-cols-12 ${isFinalCompactLayout ? 'gap-2.5' : 'gap-4'} min-h-0`}>
        <div className={`col-span-12 lg:col-span-8 flex flex-col h-full min-h-0 ${isFinalCompactLayout ? 'space-y-3' : 'space-y-4'}`}>
          <div className={`flex-shrink-0 animate-fade-in h-auto ${isFinalCompactLayout ? 'min-h-[96px]' : 'min-h-[132px]'}`}>
            <CurrentTrial compact={isFinalCompactLayout} />
          </div>

          <div className={`flex-1 min-h-0 bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl ${isFinalCompactLayout ? 'p-2.5 pr-1.5 overflow-y-scroll custom-scrollbar-strong' : 'p-3 overflow-y-auto'} custom-scrollbar animate-fade-in-up shadow-lg`}>
            <h3 className={`text-slate-400 ${isFinalCompactLayout ? 'mb-2 pb-0.5 text-xs' : 'mb-3 pb-1 text-sm'} sticky top-0 bg-slate-950/90 z-10 uppercase tracking-widest flex justify-between font-bold`}>
              <span>参赛队伍状态</span>
            </h3>

            <div className={`grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 content-start ${isFinalCompactLayout ? 'gap-2 pb-2 auto-rows-max' : 'gap-3 pb-3'}`}>
              {teamList.map((t) => (
                <TeamCard key={t.team_id} team={t} compact={isFinalCompactLayout} rank={teamRankById.get(t.team_id)} />
              ))}
              {teamList.length === 0 && (
                <div className="col-span-full py-12 text-center text-slate-600 font-mono tracking-widest animate-pulse border-2 border-dashed border-slate-800 rounded-xl">
                  等待赛队连接接入...
                </div>
              )}
            </div>
          </div>
        </div>

        <div className={`col-span-12 lg:col-span-4 flex flex-col h-full min-h-0`}>
          <div className={`${isFinalCompactLayout ? 'h-[79%] min-h-0 mb-1.5 flex-none' : 'flex-1 min-h-0 mb-4'} animate-fade-in-right`}>
            <Leaderboard compact={isFinalCompactLayout} />
          </div>

          <div className={`h-auto flex flex-col ${isFinalCompactLayout ? 'space-y-1.5 shrink-0' : 'space-y-4'} animate-fade-in-up`}>
            <div className={`flex flex-col ${isFinalCompactLayout ? 'space-y-2' : 'space-y-3'}`}>
              {controlFeedback && (
                <div className={`${isFinalCompactLayout ? 'px-3 py-2 text-[11px]' : 'px-4 py-3 text-xs'} rounded-xl border border-cyan-800/50 bg-cyan-950/30 leading-relaxed text-cyan-200`}>
                  {controlFeedback}
                </div>
              )}
              {(matchStatus === 'waiting_start' || matchStatus === 'idle') && (
                <>
                  {startReadinessMessage && (
                    <div className={`${isFinalCompactLayout ? 'px-3 py-2 text-[11px]' : 'px-4 py-3 text-xs'} rounded-xl border border-amber-800/50 bg-amber-950/30 leading-relaxed text-amber-200`}>
                      {startReadinessMessage}
                    </div>
                  )}
                  <button
                    disabled={!canStartMatch || isStarting || isPreviewMode}
                    onClick={handleStartMatch}
                    className={`w-full ${isFinalCompactLayout ? 'py-2.5 text-[12px]' : 'py-4'} rounded-xl font-bold tracking-widest uppercase transition-all shadow-xl backdrop-blur-md flex justify-center items-center ${
                      !canStartMatch || isPreviewMode
                        ? 'bg-slate-800/60 text-slate-500 cursor-not-allowed border border-slate-700/50'
                        : 'bg-gradient-to-r from-blue-600/80 to-emerald-600/80 hover:from-blue-500 hover:to-emerald-500 text-white shadow-blue-500/20 hover:shadow-emerald-500/40 border border-blue-500/30'
                    }`}
                  >
                    <Play size={20} className="mr-2" />
                    {isPreviewMode ? '预览模式不可开赛' : (!canStartMatch ? (startReadinessMessage ?? '未满足开赛条件') : (isStarting ? '开始比赛中...' : '开始比赛'))}
                  </button>
                </>
              )}

              {['started', 'starting', 'running'].includes(matchStatus) && (
                <button
                  disabled={isPausing || isPreviewMode}
                  onClick={handlePauseMatch}
                  className={`w-full ${isFinalCompactLayout ? 'py-2.5 text-[12px]' : 'py-4 text-sm'} rounded-xl font-bold tracking-widest transition-all shadow-xl backdrop-blur-md flex justify-center items-center bg-gradient-to-r from-orange-600/80 to-amber-600/80 hover:from-orange-500 hover:to-amber-500 text-white shadow-orange-500/20 hover:shadow-amber-500/40 border border-orange-500/30 disabled:opacity-50`}
                >
                  <Pause size={18} className="mr-2 shrink-0" />
                  <span className="truncate">{isPreviewMode ? '预览模式不可暂停' : (isPausing ? '暂停操作中...' : '当前 trial 结束后暂停下一轮放行')}</span>
                </button>
              )}

              {matchStatus === 'paused' && (
                <div className="flex space-x-3 w-full">
                  <button
                    disabled={isResuming || isPreviewMode}
                    onClick={handleResumeMatch}
                    className={`flex-1 ${isFinalCompactLayout ? 'py-2.5 text-[12px]' : 'py-4 text-sm'} rounded-xl font-bold tracking-widest uppercase transition-all shadow-xl backdrop-blur-md flex justify-center items-center bg-gradient-to-r from-emerald-600/80 to-cyan-600/80 hover:from-emerald-500 hover:to-cyan-500 text-white shadow-emerald-500/20 hover:shadow-cyan-500/40 border border-emerald-500/30 disabled:opacity-50`}
                  >
                    <Play size={18} className="mr-2 shrink-0" />
                    {isPreviewMode ? '预览模式不可继续' : '继续比赛'}
                  </button>
                  <button
                    disabled={isPreviewMode}
                    onClick={() => setIsRecoveryModalOpen(true)}
                    className={`flex-1 ${isFinalCompactLayout ? 'py-2.5 text-[12px]' : 'py-4 text-sm'} rounded-xl font-bold tracking-widest uppercase transition-all shadow-xl backdrop-blur-md flex justify-center items-center bg-gradient-to-r from-red-600/80 to-orange-600/80 hover:from-red-500 hover:to-orange-500 text-white shadow-red-500/20 hover:shadow-orange-500/40 border border-red-500/30 disabled:opacity-50`}
                  >
                    <RefreshCw size={18} className="mr-2 shrink-0" />
                    {isPreviewMode ? '预览模式不可重跑' : '指定阶段重跑'}
                  </button>
                </div>
              )}

              {matchStatus === 'recovery_selecting' && (
                 <button disabled className={`w-full ${isFinalCompactLayout ? 'py-2.5 text-[12px]' : 'py-4 text-sm'} rounded-xl font-bold tracking-widest transition-all shadow-xl backdrop-blur-md flex justify-center items-center bg-slate-800/60 text-slate-400 border border-slate-700/50 cursor-wait`}>
                   <RefreshCw size={18} className="mr-2 animate-spin" />
                   等待恢复确认...
                 </button>
              )}

              {['ended', 'stopped', 'finished'].includes(matchStatus) && (
                 <button disabled className={`w-full ${isFinalCompactLayout ? 'py-2.5 text-sm' : 'py-4 text-lg'} rounded-xl font-bold tracking-widest uppercase transition-all shadow-xl backdrop-blur-md flex justify-center items-center bg-slate-800/60 text-slate-500 border border-slate-700/50 cursor-not-allowed`}>
                   比赛已结束
                 </button>
              )}
            </div>

            <div className="text-center mt-2 text-[10px] text-slate-700 font-mono">
              BCI Competition 2026 裁判系统 • 决赛阶段
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default App;
