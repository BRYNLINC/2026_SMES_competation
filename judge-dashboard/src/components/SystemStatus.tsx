import { useJudgeStore } from '../store/useJudgeStore';
import { Server, CheckCircle2, XCircle } from 'lucide-react';

export const SystemStatus = ({ compact = false }: { compact?: boolean }) => {
    const { systemStatus } = useJudgeStore();

    const judgeStatus = systemStatus?.judge_web?.status || 'unknown';
    const isJudgeHealthy = judgeStatus === 'running' || judgeStatus === 'ready';

    return (
        <div className={`${compact ? 'p-2.5 text-xs' : 'p-4 text-sm'} bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl shadow-xl flex flex-col justify-center h-full`}>
            <h3 className={`text-slate-400 uppercase tracking-widest ${compact ? 'text-[10px] mb-1.5' : 'text-xs mb-3'} flex items-center font-bold`}>
                <Server size={compact ? 12 : 14} className="mr-2" />
                业务组件监控
            </h3>
            
            <div className={`flex items-center justify-between bg-slate-800/60 ${compact ? 'p-2' : 'p-3'} rounded-lg border border-slate-700/50 shadow-inner`}>
                <span className="text-slate-300 font-bold tracking-wide">裁判端核心服务</span>
                {isJudgeHealthy ? (
                    <span className="text-emerald-400 font-bold flex items-center"><CheckCircle2 size={compact ? 14 : 16} className="mr-1 drop-shadow" />正常</span>
                ) : (
                    <span className="text-red-500 font-bold flex items-center animate-pulse"><XCircle size={compact ? 14 : 16} className="mr-1 drop-shadow" />离线</span>
                )}
            </div>
        </div>
    );
};
