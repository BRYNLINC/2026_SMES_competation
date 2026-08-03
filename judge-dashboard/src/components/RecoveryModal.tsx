import { useEffect, useState } from 'react';
import { getRecoveryCheckpoints, getRecoveryStatus } from '../api/rest';
import type { RecoveryCheckpoint, RecoveryStageDescriptor, RecoveryStatus } from '../api/types';
import { AlertTriangle, Database, Play, RefreshCcw, RefreshCw, X } from 'lucide-react';

interface RecoveryModalProps {
  onClose: () => void;
  onRestartStage: (payload: RecoveryStageDescriptor) => Promise<void>;
}

export const RecoveryModal = ({ onClose, onRestartStage }: RecoveryModalProps) => {
  const [checkpoints, setCheckpoints] = useState<RecoveryCheckpoint[]>([]);
  const [recoveryStatus, setRecoveryStatus] = useState<RecoveryStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [subjectId, setSubjectId] = useState('');
  const [expName, setExpName] = useState('');
  const [expTask, setExpTask] = useState('');
  const [sessionId, setSessionId] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    let mounted = true;
    Promise.all([getRecoveryCheckpoints(), getRecoveryStatus()])
      .then(([checkpointList, status]) => {
        if (!mounted) return;
        setCheckpoints(checkpointList);
        setRecoveryStatus(status);
        const pendingStage = status.pending_restart_stage_request;
        if (pendingStage?.subject_id && pendingStage?.exp_name && pendingStage?.exp_task && pendingStage?.session_id) {
          setSubjectId(pendingStage.subject_id);
          setExpName(pendingStage.exp_name);
          setExpTask(pendingStage.exp_task);
          setSessionId(pendingStage.session_id);
        }
        setLoading(false);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(String(err));
        setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, []);

  const autofill = (checkpoint: RecoveryCheckpoint) => {
    setSubjectId(checkpoint.subject_id);
    setExpName(checkpoint.exp_name);
    setExpTask(checkpoint.exp_task);
    setSessionId(checkpoint.session_id);
  };

  const handleStageRestart = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!subjectId || !expName || !expTask || !sessionId) {
      alert('请完整填写受试者、范式名称、任务内容和会话编号。');
      return;
    }
    const message = [
      '确定要执行指定阶段重跑吗？',
      '',
      `目标阶段: ${subjectId} / ${expName} / ${expTask} / ${sessionId}`,
      '效果: 保留该阶段之前的结果，删除该阶段及之后的结果，并从该阶段重新开始。',
      '影响: 会强制打断当前所有赛队的当前 task 流程。',
      '',
      '系统会自动重启裁判端，并从该阶段重新校准、重新开始比赛。',
    ].join('\n');
    if (!window.confirm(message)) return;

    try {
      setIsSubmitting(true);
      await onRestartStage({
        subject_id: subjectId,
        exp_name: expName,
        exp_task: expTask,
        session_id: sessionId,
      });
      onClose();
    } catch (err) {
      alert('指定阶段重跑请求失败: ' + String(err));
      setIsSubmitting(false);
    }
  };

  const formatTime = (timestamp: string | number) => {
    const value = Number(timestamp);
    const date = new Date(value * 1000);
    if (Number.isNaN(date.getTime())) return String(timestamp);
    return date.toLocaleString();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 p-4 backdrop-blur-sm animate-fade-in font-sans">
      <div className="flex max-h-[90vh] w-full max-w-4xl flex-col overflow-hidden rounded-2xl border border-slate-700/80 bg-slate-900 shadow-2xl">
        <div className="flex items-center justify-between border-b border-slate-800 bg-slate-950/50 p-5">
          <div className="flex items-center space-x-3 text-white">
            <RefreshCcw className="text-orange-400" size={24} />
            <h2 className="text-xl font-black uppercase tracking-widest">指定阶段重跑</h2>
          </div>
          <button
            disabled={isSubmitting}
            onClick={onClose}
            className="text-slate-400 transition-colors hover:text-white disabled:opacity-50"
          >
            <X size={24} />
          </button>
        </div>

        <div className="flex flex-1 flex-col overflow-hidden lg:flex-row">
          <div className="flex flex-col overflow-y-auto border-b border-slate-800 bg-slate-900/40 p-5 lg:w-[55%] lg:border-b-0 lg:border-r custom-scrollbar">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="flex items-center text-sm font-bold uppercase tracking-widest text-slate-300">
                <Database size={16} className="mr-2 text-slate-500" /> checkpoint 列表
              </h3>
              {loading && <RefreshCw size={14} className="animate-spin text-blue-500" />}
            </div>

            {error && (
              <div className="mb-4 rounded border border-red-900/50 bg-red-950/30 p-3 text-xs text-red-400">
                获取 checkpoint 失败: {error}
              </div>
            )}

            {!loading && !error && checkpoints.length === 0 && (
              <div className="space-y-2 rounded-lg border-2 border-dashed border-slate-800 py-8 text-center text-sm text-slate-500">
                <div>当前未发现可用 checkpoint 列表。</div>
                <div>可以直接在右侧手工填写 `subject_id / exp_name / exp_task / session_id`。</div>
              </div>
            )}

            <div className="space-y-3">
              {checkpoints.map((checkpoint) => (
                <div
                  key={checkpoint.checkpoint_id}
                  className="flex flex-col rounded-lg border border-slate-800 bg-slate-950 p-3 shadow-inner transition-colors hover:border-orange-500/50"
                >
                  <div className="mb-2 flex items-start justify-between">
                    <span className="rounded bg-slate-900 px-2 py-0.5 font-mono text-[10px] text-slate-500">
                      ID: {checkpoint.checkpoint_id}
                    </span>
                    <span className="rounded bg-slate-900 px-2 py-0.5 text-[10px] text-slate-500">
                      {formatTime(checkpoint.created_at)}
                    </span>
                  </div>
                  <div className="mb-3 flex flex-wrap gap-2 font-mono text-xs font-bold tracking-tight text-slate-300">
                    <span className="rounded border border-blue-900/50 bg-blue-900/30 px-2 py-1 text-blue-300">{checkpoint.subject_id}</span>
                    <span className="rounded border border-cyan-900/50 bg-cyan-900/30 px-2 py-1 text-cyan-300">{checkpoint.exp_name}</span>
                    <span className="rounded border border-indigo-900/50 bg-indigo-900/30 px-2 py-1 text-indigo-300">{checkpoint.exp_task}</span>
                    <span className="rounded border border-emerald-900/50 bg-emerald-900/30 px-2 py-1 text-emerald-300">{checkpoint.session_id}</span>
                  </div>
                  {checkpoint.description && <div className="mb-3 text-xs italic text-slate-500">{checkpoint.description}</div>}
                  <button
                    disabled={isSubmitting}
                    onClick={() => autofill(checkpoint)}
                    className="self-end rounded bg-orange-950/40 px-3 py-1.5 text-xs text-orange-300 transition-colors hover:bg-orange-900/50 hover:text-orange-200 disabled:opacity-50"
                  >
                    作为重跑起点
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div className="overflow-y-auto bg-slate-950/60 p-5 lg:w-[45%]">
            <h3 className="mb-6 flex items-center text-sm font-bold uppercase tracking-widest text-slate-300">
              <Play size={16} className="mr-2 text-slate-500" /> 重跑配置
            </h3>

            <div className="relative mb-6 overflow-hidden rounded-lg border border-red-500/30 bg-red-950/20 p-4">
              <div className="absolute left-0 top-0 h-full w-1 bg-red-500" />
              <div className="mb-2 flex items-start text-red-400">
                <AlertTriangle size={16} className="mr-2 mt-0.5" />
                <span className="text-sm font-bold uppercase tracking-widest">生效语义</span>
              </div>
              <p className="pl-6 text-xs leading-relaxed text-red-300/80">
                指定阶段重跑会 <strong className="mx-1 text-red-400">保留该阶段之前结果</strong>，
                <strong className="mx-1 text-red-400">删除该阶段及之后结果</strong>，随后自动重启裁判端，并从该阶段重新校准、重新开始比赛。
              </p>
              <p className="mt-3 pl-6 text-xs leading-relaxed text-amber-200/90">
                这不是“继续比赛”。继续比赛请直接使用暂停页上的“继续比赛”按钮。可选阶段仅限已经跑过或当前正在跑的阶段。
              </p>
            </div>

            <div className="mb-5 rounded-lg border border-slate-800 bg-slate-950/60 p-3 text-xs text-slate-400">
              <div>结果目录数: {recoveryStatus?.result_team_dir_list?.length ?? 0}</div>
              <div>可选重跑阶段数: {recoveryStatus?.checkpoint_count ?? 0}</div>
              <div>实时状态文件: {recoveryStatus?.live_state_files?.team_live_count ?? 0} 个赛队快照</div>
              {recoveryStatus?.pending_restart_stage_request && (
                <div className="pt-2 text-amber-300">
                  已记录的重跑起点: {recoveryStatus.pending_restart_stage_request.subject_id}
                  {' / '}
                  {recoveryStatus.pending_restart_stage_request.exp_name}
                  {' / '}
                  {recoveryStatus.pending_restart_stage_request.exp_task}
                  {' / '}
                  {recoveryStatus.pending_restart_stage_request.session_id}
                </div>
              )}
            </div>

            <form onSubmit={handleStageRestart} className="space-y-4">
              <div>
                <label className="mb-1.5 block text-xs font-bold uppercase tracking-widest text-slate-400">
                  Subject ID / 受试者编号
                </label>
                <input
                  type="text"
                  value={subjectId}
                  onChange={(event) => setSubjectId(event.target.value)}
                  placeholder="例如: subject_1"
                  disabled={isSubmitting}
                  className="w-full rounded border border-slate-700 bg-slate-900 px-3 py-2 font-mono text-sm text-white transition-colors focus:border-orange-500 focus:outline-none disabled:opacity-50"
                />
              </div>
              <div>
                <label className="mb-1.5 block text-xs font-bold uppercase tracking-widest text-slate-400">
                  Exp Name / 实验方案
                </label>
                <input
                  type="text"
                  value={expName}
                  onChange={(event) => setExpName(event.target.value)}
                  placeholder="例如: VMI"
                  disabled={isSubmitting}
                  className="w-full rounded border border-slate-700 bg-slate-900 px-3 py-2 font-mono text-sm text-white transition-colors focus:border-orange-500 focus:outline-none disabled:opacity-50"
                />
              </div>
              <div>
                <label className="mb-1.5 block text-xs font-bold uppercase tracking-widest text-slate-400">
                  Exp Task / 具体任务
                </label>
                <input
                  type="text"
                  value={expTask}
                  onChange={(event) => setExpTask(event.target.value)}
                  placeholder="例如: left_vs_rest"
                  disabled={isSubmitting}
                  className="w-full rounded border border-slate-700 bg-slate-900 px-3 py-2 font-mono text-sm text-white transition-colors focus:border-orange-500 focus:outline-none disabled:opacity-50"
                />
              </div>
              <div>
                <label className="mb-1.5 block text-xs font-bold uppercase tracking-widest text-slate-400">
                  Session ID / 会话编号
                </label>
                <input
                  type="text"
                  value={sessionId}
                  onChange={(event) => setSessionId(event.target.value)}
                  placeholder="例如: session2"
                  disabled={isSubmitting}
                  className="w-full rounded border border-slate-700 bg-slate-900 px-3 py-2 font-mono text-sm text-white transition-colors focus:border-orange-500 focus:outline-none disabled:opacity-50"
                />
              </div>

              <div className="pt-6">
                <button
                  type="submit"
                  disabled={isSubmitting || !subjectId || !expName || !expTask || !sessionId}
                  className="flex w-full items-center justify-center rounded border border-red-500/50 bg-gradient-to-r from-red-600/90 to-orange-600/90 py-3.5 font-bold uppercase tracking-widest text-white shadow-lg shadow-red-900/30 transition-all hover:from-red-500 hover:to-orange-500 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {isSubmitting ? <RefreshCw size={18} className="animate-spin" /> : '确认指定阶段重跑'}
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};
