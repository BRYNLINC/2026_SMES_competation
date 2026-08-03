import { useMemo } from 'react';
import type { TeamInfo } from '../api/types';
import { Zap, Clock, Activity, AlertTriangle, Brain, Sparkles, SlidersHorizontal } from 'lucide-react';
import { useJudgeStore } from '../store/useJudgeStore';

export const TeamCard = ({ team, compact = false, rank }: { team: TeamInfo; compact?: boolean; rank?: number }) => {
  const { trial } = useJudgeStore();
  const getConnectionColor = (status: string) => {
    switch (status) {
      case 'connected': return 'bg-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.5)]';
      case 'connecting':
      case 'reconnecting': return 'bg-yellow-500 shadow-[0_0_10px_rgba(234,179,8,0.5)] animate-pulse';
      case 'disconnected':
      case 'error': return 'bg-red-500 shadow-[0_0_10px_rgba(239,68,68,0.5)]';
      default: return 'bg-slate-500 shadow-[0_0_10px_rgba(100,116,139,0.5)]'; 
    }
  };

  const formatConnStatus = (status: string) => {
    switch (status) {
      case 'connected': return '在线';
      case 'connecting': return '连接中';
      case 'disconnected': return '已掉线';
      case 'reconnecting': return '恢复连接中';
      case 'error': return '异常';
      case 'closed': return '已断开';
      case 'stopped': return '已停止';
      default: return status;
    }
  };

  const formatRunStatus = (status: string) => {
    switch (status) {
      case 'running': return '运行中';
      case 'starting': return '启动中';
      case 'idle': return '空闲未准备';
      case 'stopped': return '已停止';
      default: return status;
    }
  };

  const formatCalibStatus = (status: string) => {
    switch (status) {
      case 'ready': return '已完成';
      case 'pending': return '待校准';
      default: return status;
    }
  };

  const getCalibColor = (status: string) => {
    switch (status) {
      case 'ready':
        return 'text-emerald-400';
      case 'pending':
        return 'text-amber-400';
      default:
        return 'text-slate-500';
    }
  };
  const isTimeoutWarning = team.is_timeout;

  const acc = useMemo(() => team.current_task_accuracy_percent?.toFixed(2) ?? '0.00', [team.current_task_accuracy_percent]);

  const mapLabel = (label: number | string | null) => {
    if (label === null || label === undefined) return '-';
    const str = String(label);
    if (!trial) return str;
    if (str === '0') return '静息';
    if (str === '1' && trial.exp_task.toLowerCase().includes('left')) return '左手';
    if (str === '1' && trial.exp_task.toLowerCase().includes('right')) return '右手';
    return str;
  }

  const isDisconnected = team.connection_status === 'disconnected' || team.connection_status === 'error';
  const isReconnecting = team.connection_status === 'reconnecting' || team.connection_status === 'connecting';
  const isForfeitCurrentTask = Boolean(team.forfeit_current_task);
  const hasJudgeMessage = Boolean(team.judge_message);

  const resolvedCalibrationStatusLabel = (() => {
    if (isDisconnected) return '已掉线';
    if (isReconnecting) return '等待重连';
    if (isForfeitCurrentTask) return '当前task无效';
    return formatCalibStatus(team.calibration_status);
  })();

  const resolvedCalibrationClassName = (() => {
    if (isDisconnected) return 'text-red-400';
    if (isReconnecting) return 'text-amber-400';
    if (isForfeitCurrentTask) return 'text-orange-400';
    return getCalibColor(team.calibration_status);
  })();

  const resolvedEnvironmentStatusLabel = (() => {
    if (isDisconnected) return '链路断开';
    if (isReconnecting) return '恢复连接中';
    if (isForfeitCurrentTask) return '当前task不计分';
    return formatRunStatus(team.run_status);
  })();

  const resolvedEnvironmentClassName = (() => {
    if (isDisconnected) return 'text-red-400';
    if (isReconnecting) return 'text-amber-400';
    if (isForfeitCurrentTask) return 'text-orange-400';
    if (team.run_status === 'running') return 'text-emerald-400';
    if (team.run_status === 'starting') return 'text-amber-400';
    return 'text-slate-500';
  })();

  const resolvedStatusHint = (() => {
    if (isDisconnected) {
      return team.last_disconnect_reason || team.recovery_advice || null;
    }
    if (hasJudgeMessage) {
      return team.judge_message || null;
    }
    return null;
  })();

  const resolvedStatusHintClassName = (() => {
    if (isDisconnected) return 'text-red-300';
    if (team.is_timeout || team.is_invalid_output) return 'text-orange-300';
    return 'text-slate-400';
  })();
  const podiumClassName = (() => {
    if (rank === 1) return 'border-yellow-300/70 bg-gradient-to-br from-yellow-400/30 via-amber-500/20 to-slate-900/50 shadow-[0_0_22px_rgba(250,204,21,0.22)]';
    if (rank === 2) return 'border-slate-200/60 bg-gradient-to-br from-slate-200/30 via-slate-400/20 to-slate-900/50 shadow-[0_0_22px_rgba(226,232,240,0.18)]';
    if (rank === 3) return 'border-orange-700/70 bg-gradient-to-br from-orange-700/30 via-amber-800/20 to-slate-900/50 shadow-[0_0_22px_rgba(180,83,9,0.2)]';
    return 'bg-slate-900/40';
  })();
  const podiumBorderClassName = rank && rank <= 3 ? '' : (
    isDisconnected ? 'border-red-500/50 shadow-[0_0_15px_rgba(239,68,68,0.2)]' : (isTimeoutWarning ? 'border-red-500/80 shadow-[0_0_20px_rgba(239,68,68,0.3)] animate-pulse' : 'border-slate-700/50 hover:border-slate-500/60')
  );

  return (
    <div className={`${compact ? 'p-2 min-h-[176px]' : 'p-3'} rounded-xl border flex flex-col backdrop-blur-md transition-all duration-300 relative overflow-hidden ${podiumClassName} ${podiumBorderClassName}`}>
      {/* Header */}
      <div className={`flex justify-between items-center border-b border-slate-800 ${compact ? 'pb-1 mb-1' : 'pb-2 mb-2'} relative z-10`}>
        <h3 className={`${compact ? 'text-[15px]' : 'text-lg'} font-bold truncate tracking-tight text-white`} title={team.team_display_name}>
          {team.team_display_name}
        </h3>
        <div className="flex items-center space-x-2 shrink-0">
          <span className={`${compact ? 'w-2 h-2' : 'w-2.5 h-2.5'} rounded-full ${getConnectionColor(team.connection_status)} transition-colors`}></span>
          <span className={`${compact ? 'text-[8px]' : 'text-[9px]'} text-slate-300 font-bold tracking-widest uppercase`}>
            {formatConnStatus(team.connection_status)}
          </span>
        </div>
      </div>
      {/* Real-time Inference */}
      <div className={`flex justify-between items-stretch ${compact ? 'mb-1.5 space-x-1.5' : 'mb-2.5 space-x-2'}`}>
        <div className={`w-1/2 flex flex-col items-center justify-center ${compact ? 'p-1.5 min-h-[62px]' : 'p-2 min-h-[82px]'} bg-slate-950/60 rounded-lg border border-slate-700/50 shadow-inner`}>
           <div className={`flex items-center space-x-1 ${compact ? 'text-[8px]' : 'text-[9px]'} text-slate-500 tracking-wide`}>
             <Brain size={compact ? 10 : 11} />
             <span>当前预测</span>
           </div>
           <div className={`${compact ? 'mt-0.5 text-lg' : 'mt-1 text-lg'} font-black text-center font-sans tracking-widest transition-colors`}>
             <span className={`block text-center ${
               mapLabel(team.predict_label) === '静息' ? 'text-red-400 drop-shadow-[0_0_8px_rgba(239,68,68,0.5)]' :
               (mapLabel(team.predict_label) === '左手' || mapLabel(team.predict_label) === '右手') ? 'text-blue-400 drop-shadow-[0_0_8px_rgba(96,165,250,0.5)]' :
               'text-slate-400'
             }`}>
               {mapLabel(team.predict_label)}
             </span>
           </div>
        </div>

        <div className={`w-1/2 flex flex-col items-center justify-center ${compact ? 'p-1.5 min-h-[62px]' : 'p-2 min-h-[82px]'} bg-slate-950/60 rounded-lg border border-slate-700/50 shadow-inner`}>
           <div className={`flex items-center space-x-1 ${compact ? 'text-[8px]' : 'text-[9px]'} text-slate-500 tracking-wide`}>
             <Clock size={compact ? 10 : 11} />
             <span>当前trial耗时 (ms)</span>
           </div>
           <div className={`${compact ? 'mt-0.5 text-base' : 'mt-1 text-lg'} font-black font-mono transition-colors ${
             isTimeoutWarning ? 'text-red-500 flex items-center drop-shadow-md' : 'text-slate-200'
           }`}>
             {team.predict_time_ms != null ? Number(team.predict_time_ms).toFixed(2) : '-'}
             {isTimeoutWarning && <AlertTriangle size={compact ? 12 : 14} className="ml-2 animate-bounce" />}
           </div>
        </div>
      </div>

      {/* Stats Grid */}
      <div className={`grid grid-cols-2 ${compact ? 'gap-1 p-1.5' : 'gap-2 p-2'} text-center mt-auto bg-slate-800/40 rounded-lg border border-slate-700/50 shadow-inner`}>
        <div className="flex flex-col items-center">
          <span className={`${compact ? 'text-[8px]' : 'text-[9px]'} text-slate-500 uppercase tracking-wide mb-1 flex items-center`}><Activity size={compact ? 8 : 9} className="mr-1"/>本任务分数</span>
          <span className={`font-bold text-yellow-400 ${compact ? 'text-sm' : 'text-base'} shadow-yellow-500/50 drop-shadow-md`}>{Number(team.current_task_score).toFixed(2)}</span>
        </div>
        <div className="flex flex-col items-center">
          <span className={`${compact ? 'text-[8px]' : 'text-[9px]'} text-slate-500 uppercase tracking-wide mb-1 flex items-center`}><Zap size={compact ? 8 : 9} className="mr-1"/>本任务平均准确率</span>
          <span className={`font-bold ${compact ? 'text-[13px]' : 'text-sm'} text-slate-200`}>{acc}<span className={`${compact ? 'text-[9px]' : 'text-[10px]'} text-slate-500`}>%</span></span>
        </div>
      </div>

      {/* Footer Status */}
      <div className={`flex justify-between items-center ${compact ? 'mt-1 pt-1 text-[8px]' : 'mt-2 pt-2 text-[9px]'} border-t border-slate-700/50 tracking-wide`}>
         <span className="text-slate-300 flex items-center gap-1 min-w-0">
           <SlidersHorizontal size={compact ? 9 : 10} className="text-slate-500 shrink-0" />
           <span>校准:</span>
           <span className={`${resolvedCalibrationClassName} truncate`}>{resolvedCalibrationStatusLabel}</span>
         </span>
         <span className="text-slate-400 flex items-center gap-1 min-w-0 justify-end" title={resolvedStatusHint ?? undefined}>
           <Sparkles size={compact ? 9 : 10} className="text-slate-500 shrink-0" />
           <span>状态:</span>
           <span className={`${resolvedEnvironmentClassName} truncate`}>{resolvedEnvironmentStatusLabel}</span>
           {resolvedStatusHint && (
             <>
               <AlertTriangle size={compact ? 9 : 10} className={`${resolvedStatusHintClassName} shrink-0`} />
               <span className={`${resolvedStatusHintClassName} truncate max-w-[72px]`}>
                 {resolvedStatusHint}
               </span>
             </>
           )}
         </span>
      </div>
    </div>
  );
};
