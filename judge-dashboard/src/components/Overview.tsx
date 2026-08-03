import { useJudgeStore } from '../store/useJudgeStore';
import { Activity, Users, CheckCircle, Wifi } from 'lucide-react';
import logo from '../assets/logo.png';

export const Overview = ({ compact = false }: { compact?: boolean }) => {
  const { overview, liveTransportStatus } = useJudgeStore();
  const matchStatus = overview?.match_status.toLowerCase() ?? 'waiting_start';
  const isRunning = ['started', 'starting', 'running'].includes(matchStatus);
  const statusClassName = (
    isRunning || matchStatus === 'finished'
      ? 'text-green-400'
      : matchStatus === 'paused'
        ? 'text-cyan-400'
        : matchStatus === 'recovery_selecting'
          ? 'text-purple-400'
          : 'text-yellow-400'
  );
  const statusLabel = (
    matchStatus === 'idle' || matchStatus === 'waiting_start' ? '等待开始' :
    matchStatus === 'paused' ? '比赛已暂停' :
    matchStatus === 'recovery_selecting' ? '等待恢复确认' :
    isRunning ? '比赛进行中' :
    matchStatus === 'ended' || matchStatus === 'stopped' || matchStatus === 'finished' ? '已结束' :
    overview?.match_status.toUpperCase() ?? 'UNKNOWN'
  );

  if (!overview) {
    return (
      <div className={`${compact ? 'p-3 min-h-[76px]' : 'p-4 min-h-[88px]'} bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-lg animate-pulse w-full shadow-lg`}></div>
    );
  }

  return (
    <div className={`flex flex-col md:flex-row justify-between items-center bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-lg shadow-xl w-full ${compact ? 'p-3' : 'p-4'}`}>
      <div className={`flex items-center min-w-0 ${compact ? 'gap-3' : 'gap-4'}`}>
        <img src={logo} alt="比赛Logo" className={`w-auto object-contain shrink-0 ${compact ? 'h-16' : 'h-20'}`} />
        <div className="min-w-0 flex-1">
          <h1 className={`${compact ? 'text-lg' : 'text-xl'} font-black bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400 tracking-tight`}>
            基于感觉肌肉电刺激提示的上肢运动想象分类技术与系统赛
          </h1>
          <div className={`flex items-center flex-wrap font-medium ${compact ? 'mt-1 gap-2 text-[11px]' : 'mt-1.5 gap-3 text-xs'}`}>
            <span className={`px-2 py-1 rounded-md bg-slate-800/80 ${statusClassName}`}>
              比赛状态: {statusLabel}
            </span>
            {liveTransportStatus === 'offline' && (
              <span className="flex items-center text-red-400 px-2 py-1 bg-red-950 rounded-md animate-pulse">
                <Activity size={14} className="mr-1" /> 数据连接中断
              </span>
            )}
          </div>
        </div>
      </div>

      <div className={`flex shrink-0 ${compact ? 'space-x-4 mt-2 md:mt-0' : 'space-x-6 mt-3 md:mt-0'}`}>
        <div className="flex flex-col items-center">
          <div className={`${compact ? 'p-2 mb-1' : 'p-2.5 mb-1.5'} bg-slate-800 rounded-full`}><Users size={compact ? 18 : 20} className="text-gray-400" /></div>
          <span className={`${compact ? 'text-lg' : 'text-xl'} font-bold`}>{overview.team_count}</span>
          <span className={`${compact ? 'text-[10px]' : 'text-xs'} text-gray-400 mt-1 font-bold tracking-wider`}>总队伍数</span>
        </div>
        <div className="flex flex-col items-center">
          <div className={`${compact ? 'p-2 mb-1' : 'p-2.5 mb-1.5'} bg-slate-800 rounded-full`}><Wifi size={compact ? 18 : 20} className="text-blue-400" /></div>
          <span className={`${compact ? 'text-lg' : 'text-xl'} font-bold text-blue-400`}>{overview.connected_team_count}</span>
          <span className={`${compact ? 'text-[10px]' : 'text-xs'} text-blue-400 mt-1 font-bold tracking-wider`}>已连接</span>
        </div>
        <div className="flex flex-col items-center">
          <div className={`${compact ? 'p-2 mb-1' : 'p-2.5 mb-1.5'} bg-slate-800 rounded-full`}><CheckCircle size={compact ? 18 : 20} className="text-emerald-400" /></div>
          <span className={`${compact ? 'text-lg' : 'text-xl'} font-bold text-emerald-400`}>{overview.calibrated_team_count}</span>
          <span className={`${compact ? 'text-[10px]' : 'text-xs'} text-emerald-400 mt-1 font-bold tracking-wider`}>已校准</span>
        </div>
      </div>
    </div>
  );
};
