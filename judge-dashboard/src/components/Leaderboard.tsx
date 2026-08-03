import { useJudgeStore } from '../store/useJudgeStore';
import { Trophy } from 'lucide-react';

export const Leaderboard = ({ compact = false }: { compact?: boolean }) => {
  const { scoreboard, teams } = useJudgeStore();

  if (compact) {
    return (
      <div className="p-2 bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl shadow-xl w-full h-full flex flex-col min-h-0 overflow-hidden">
        <div className="flex items-center mb-1.5 pb-1 border-b border-slate-800">
          <Trophy size={18} className="mr-1.5 text-yellow-400" />
          <h2 className="text-lg font-black text-white tracking-widest uppercase">排行榜</h2>
        </div>

        <div className="grid grid-cols-[54px_minmax(0,1.2fr)_0.8fr_0.95fr_1fr] gap-x-2 px-1 py-1 text-[13px] text-slate-300 border-b border-slate-800/80 font-sans">
          <div className="text-slate-300 uppercase font-bold">排名</div>
          <div className="text-slate-300 uppercase font-bold">赛队</div>
          <div className="text-slate-300 uppercase font-bold text-center">平均分</div>
          <div className="text-slate-300 uppercase font-bold text-center">平均准确率</div>
          <div className="text-slate-300 uppercase font-bold text-center normal-case">耗时 (ms)</div>
        </div>

        {scoreboard.length === 0 ? (
          <div className="flex-1 flex items-center justify-center text-slate-500 text-sm">等待数据加载...</div>
        ) : (
          <div
            className={`flex-1 min-h-0 ${scoreboard.length <= 9 ? 'grid' : 'overflow-y-auto custom-scrollbar'}`}
            style={scoreboard.length <= 9 ? { gridTemplateRows: `repeat(${scoreboard.length}, minmax(0, 1fr))` } : undefined}
          >
            {scoreboard.map((row, idx) => {
              const teamName = teams[row.team_id]?.team_display_name || row.team_id;
              const isTop3 = idx < 3;
              const podiumClassName = (
                idx === 0 ? 'bg-gradient-to-r from-yellow-400/30 via-amber-500/20 to-transparent' :
                idx === 1 ? 'bg-gradient-to-r from-slate-200/30 via-slate-400/20 to-transparent' :
                idx === 2 ? 'bg-gradient-to-r from-orange-700/30 via-amber-800/20 to-transparent' :
                ''
              );
              return (
                <div
                  key={row.team_id}
                  className={`grid grid-cols-[54px_minmax(0,1.2fr)_0.8fr_0.95fr_1fr] gap-x-2 gap-y-1 items-center px-1 py-0.5 border-b border-slate-800/60 last:border-b-0 font-sans ${podiumClassName}`}
                >
                  <div className="flex items-center">
                    <span className={`w-7 h-7 text-[14px] rounded-full flex items-center justify-center font-bold ${
                      idx === 0 ? 'bg-yellow-500 text-yellow-950' :
                      idx === 1 ? 'bg-slate-300 text-slate-800' :
                      idx === 2 ? 'bg-amber-700 text-amber-100' : 'bg-slate-800 text-slate-400'
                    }`}>
                      {row.rank}
                    </span>
                  </div>
                  <div className="text-[13px] font-medium text-white truncate" title={teamName}>
                    {teamName}
                  </div>
                  <div className={`text-center text-[15px] font-medium ${isTop3 ? 'text-yellow-300' : 'text-slate-300'}`}>
                    {Number(row.average_score ?? 0).toFixed(2)}
                  </div>
                  <div className="text-center text-[15px] text-slate-300 font-mono">
                    {row.mean_accuracy_percent?.toFixed(2) ?? '0.00'}
                  </div>
                  <div className="text-center text-[15px] text-slate-400 font-mono">
                    {row.avg_reaction_time_ms?.toFixed(2) ?? '0.00'}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="p-4 bg-slate-900/40 backdrop-blur-md border border-slate-700/50 rounded-xl shadow-xl w-full h-full flex flex-col min-h-0">
      <div className="flex items-center mb-4 pb-3 border-b border-slate-800">
        <Trophy size={20} className="mr-2 text-yellow-400" />
        <h2 className="text-xl font-black text-white tracking-widest uppercase">排行榜</h2>
      </div>

      <div className="flex-1 overflow-auto pr-1 custom-scrollbar min-h-0">
        <table className="w-full min-w-[340px] text-xs text-left align-middle" style={{ tableLayout: 'auto' }}>
          <thead className="text-[10px] text-slate-400 uppercase bg-slate-800/40 sticky top-0 backdrop-blur shadow-sm">
            <tr>
              <th className="px-2 py-2.5 rounded-tl-lg whitespace-nowrap min-w-[48px]">排名</th>
              <th className="px-2 py-2.5 whitespace-nowrap min-w-[96px]">赛队</th>
              <th className="px-2 py-2.5 text-center whitespace-nowrap min-w-[64px]">平均分</th>
              <th className="px-2 py-2.5 text-center whitespace-nowrap min-w-[76px]">平均准确率</th>
              <th className="px-2 py-2.5 text-center rounded-tr-lg whitespace-nowrap min-w-[84px] normal-case">耗时 (ms)</th>
            </tr>
          </thead>
          <tbody>
            {scoreboard.length === 0 ? (
              <tr><td colSpan={5} className="text-center py-8 text-slate-500">等待数据加载...</td></tr>
            ) : (
              scoreboard.map((row, idx) => {
                const teamName = teams[row.team_id]?.team_display_name || row.team_id;
                const isTop3 = idx < 3;

                return (
                  <tr key={row.team_id} className={`border-b border-slate-800 hover:bg-slate-800/50 transition-colors ${
                    idx === 0 ? 'bg-gradient-to-r from-yellow-400/30 via-amber-500/20 to-transparent' :
                    idx === 1 ? 'bg-gradient-to-r from-slate-200/30 via-slate-400/20 to-transparent' :
                    idx === 2 ? 'bg-gradient-to-r from-orange-700/30 via-amber-800/20 to-transparent' :
                    ''
                  }`}>
                    <td className="px-2 py-2 font-medium whitespace-nowrap">
                      <div className="flex items-center">
                        <span className={`w-5 h-5 text-[10px] rounded-full flex items-center justify-center font-bold mr-1 ${
                          idx === 0 ? 'bg-yellow-500 text-yellow-950' :
                          idx === 1 ? 'bg-slate-300 text-slate-800' :
                          idx === 2 ? 'bg-amber-700 text-amber-100' : 'bg-slate-800 text-slate-400'
                        }`}>
                          {row.rank}
                        </span>
                      </div>
                    </td>
                    <td className="px-2 py-2 font-semibold text-white max-w-full overflow-hidden text-ellipsis whitespace-nowrap" title={teamName}>
                      <span className="block truncate">{teamName}</span>
                    </td>
                    <td className={`px-2 py-2 text-center font-bold whitespace-nowrap ${isTop3 ? 'text-yellow-400' : 'text-slate-300'}`}>
                      {Number(row.average_score ?? 0).toFixed(2)}
                    </td>
                    <td className="px-2 py-2 text-center text-slate-300 font-mono whitespace-nowrap">
                      {row.mean_accuracy_percent?.toFixed(2) ?? '0.00'}
                    </td>
                    <td className="px-2 py-2 text-center text-slate-400 font-mono whitespace-nowrap">
                      {row.avg_reaction_time_ms?.toFixed(2) ?? '0.00'}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
