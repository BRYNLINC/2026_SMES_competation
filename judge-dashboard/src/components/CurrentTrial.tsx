import { useJudgeStore } from '../store/useJudgeStore';
import { AlertTriangle, Target, Dna } from 'lucide-react';

export const CurrentTrial = ({ compact = false }: { compact?: boolean }) => {
  const { trial } = useJudgeStore();

  if (!trial) return (
    <div className="p-6 bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl animate-pulse h-full flex flex-col justify-center items-center shadow-lg">
      <div className="text-slate-500 font-bold tracking-widest flex items-center">
        <Target size={20} className="mr-3 text-slate-600" />
        等待当前 Trial 流程数据传入...
      </div>
    </div>
  );

  if (trial.status?.toLowerCase() === 'error') return (
    <div className={`${compact ? 'p-3' : 'p-6'} bg-red-950/35 border border-red-700/70 rounded-xl h-full flex items-center shadow-lg`}>
      <AlertTriangle size={compact ? 28 : 36} className="mr-4 shrink-0 text-red-400" />
      <div className="min-w-0">
        <div className={`${compact ? 'text-base' : 'text-lg'} font-bold text-red-200`}>裁判端数据分发失败</div>
        <div className={`${compact ? 'text-[11px]' : 'text-xs'} mt-1 font-mono text-red-100/80 break-words`}>
          {trial.subject_id} / {trial.exp_name} / {trial.exp_task} / {trial.session_id}
        </div>
        <div className={`${compact ? 'text-[11px]' : 'text-xs'} mt-1 text-red-300 break-words`}>
          {trial.error_type ? `${trial.error_type}: ` : ''}{trial.error_message || '未提供错误详情'}
        </div>
        {trial.recovery_advice && (
          <div className={`${compact ? 'text-[10px]' : 'text-xs'} mt-1 text-amber-200 break-words`}>
            {trial.recovery_advice}
          </div>
        )}
      </div>
    </div>
  );

  const formatTask = (name: string) => {
    const formatted = name.toUpperCase();
    if (formatted === 'VMI') return '运动想象';
    if (formatted === 'VME') return '运动执行';
    if (formatted === 'LEFT_VS_REST') return '左手 vs 静息';
    if (formatted === 'RIGHT_VS_REST') return '右手 vs 静息';
    return formatted;
  };

  const formatSubjectProgress = () => {
    const currentSubjectIndex = trial.current_subject_index;
    const totalSubjectCount = trial.total_subject_count;
    if (
      typeof currentSubjectIndex === 'number'
      && Number.isFinite(currentSubjectIndex)
      && typeof totalSubjectCount === 'number'
      && Number.isFinite(totalSubjectCount)
      && totalSubjectCount > 0
    ) {
      return `当前被试 ${currentSubjectIndex} / ${totalSubjectCount}`;
    }
    return '当前被试 - / -';
  };

  const trueLabelStr = trial.true_label !== null && trial.true_label !== undefined ? String(trial.true_label) : '?';
  const displayLabel = trueLabelStr === '0' ? '静息' :
                       (trueLabelStr === '1' && trial.exp_task.toLowerCase().includes('left') ? '左手' :
                       (trueLabelStr === '1' && trial.exp_task.toLowerCase().includes('right') ? '右手' : trueLabelStr));

  const getLabelColorClass = (label: string) => {
    if (label === '静息') return 'from-pink-300 to-red-500 drop-shadow-[0_0_15px_rgba(236,72,153,0.5)]';
    if (label === '左手' || label === '右手') return 'from-blue-300 to-indigo-500 drop-shadow-[0_0_15px_rgba(99,102,241,0.5)]';
    return 'from-slate-300 to-slate-500';
  };

  const formatSessionId = (value: number | string) => {
    const text = String(value ?? '').trim();
    const match = text.match(/^session\s*(\d+)$/i);
    return match ? match[1] : text;
  };

  return (
    <div className={`${compact ? 'p-2' : 'p-6'} bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl flex items-center justify-between shadow-lg relative overflow-hidden group h-full`}>
      {/* Decorative gradient background */}
      <div className="absolute inset-0 bg-gradient-to-r from-blue-600/10 via-transparent to-pink-600/10 opacity-30 group-hover:opacity-100 transition-opacity"></div>
      
      <div className={`relative z-10 ${compact ? 'w-1/2 space-y-3.5' : 'w-1/2 space-y-4'} flex flex-col justify-center`}>
        <div className={`flex items-center space-x-3 bg-slate-900/60 w-max ${compact ? 'px-3 py-1.5' : 'px-4 py-2'} rounded-lg border border-slate-700/50 shadow-inner`}>
          <Dna size={compact ? 16 : 20} className="text-purple-400" />
          <span className={`${compact ? 'text-sm' : 'text-sm'} font-mono text-purple-100 font-semibold tracking-widest`}>{formatSubjectProgress()}</span>
        </div>
        
        <div className={`${compact ? 'flex items-center gap-2 overflow-hidden' : ''}`}>
          <h2 className={`${compact ? 'text-[22px] shrink-0' : 'text-2xl'} font-bold flex items-center tracking-wide`}>
            {formatTask(trial.exp_name)} <span className="mx-2 text-slate-500">/</span> {formatTask(trial.exp_task)}
          </h2>
          <div className={`${compact ? 'flex min-w-0 flex-nowrap items-center gap-1.5 overflow-hidden text-[11px]' : 'mt-2 flex items-center text-sm'} text-slate-400 font-mono`}>
            <span className={`${compact ? 'px-1.5 py-0.5' : 'px-2 py-1 mr-2'} bg-slate-900/60 border border-slate-700/50 rounded text-xs whitespace-nowrap`}>session: {formatSessionId(trial.session_id)}</span>
            <span className={`${compact ? 'px-1.5 py-0.5' : 'px-2 py-1 mr-2'} bg-slate-900/60 border border-slate-700/50 rounded text-xs whitespace-nowrap`}>block: {trial.block_id}</span>
            <span className={`${compact ? 'px-1.5 py-0.5' : 'px-2 py-1'} bg-blue-900/40 text-blue-300 border border-blue-800/50 rounded text-xs whitespace-nowrap`}>window: {trial.trial_id}</span>
          </div>
        </div>
      </div>
      <div className={`${compact ? 'p-4 min-w-[208px] max-h-28' : 'p-6'} relative z-10 flex items-center justify-center bg-slate-950/60 rounded-2xl border border-slate-700/50 shadow-inner`}>
        <div className="flex flex-col items-center">
          <div className={`flex items-center ${compact ? 'mb-2' : 'mb-2'} text-slate-400`}>
            <Target size={compact ? 18 : 18} className="mr-2 text-slate-400" />
            <span className={`font-semibold uppercase tracking-wider ${compact ? 'text-sm' : 'text-sm'}`}>当前真实标签</span>
          </div>
          {trial.status?.toLowerCase().includes('calib') || trial.exp_task?.toLowerCase().includes('calib') || trial.exp_name?.toLowerCase().includes('calib') ? (
            <span className={`${compact ? 'text-3xl' : 'text-3xl'} font-black font-sans text-transparent bg-clip-text bg-gradient-to-br from-yellow-300 to-amber-500 drop-shadow-[0_0_15px_rgba(245,158,11,0.5)] animate-pulse`}>
              等待新一轮校准
            </span>
          ) : (
            <span className={`${compact ? 'text-4xl' : 'text-4xl'} font-black font-sans text-transparent bg-clip-text bg-gradient-to-br ${getLabelColorClass(displayLabel)}`}>
              {displayLabel}
            </span>
          )}
        </div>
      </div>
    </div>
  );
};
