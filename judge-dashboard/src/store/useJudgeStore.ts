import { create } from 'zustand';
import type {
  MatchControlStatus,
  MatchOverview,
  CurrentTrial,
  TeamInfo,
  ScoreboardItem,
  SystemStatusData,
} from '../api/types';
import { normalizeLivePayload } from '../api/rest';

export type LiveTransportStatus = 'connecting' | 'websocket' | 'rest_fallback' | 'offline';

export interface JudgeStoreState {
  overview: MatchOverview | null;
  trial: CurrentTrial | null;
  teams: Record<string, TeamInfo>;
  scoreboard: ScoreboardItem[];
  systemStatus: SystemStatusData | null;
  controlStatus: MatchControlStatus | null;
  isConnectedWS: boolean;
  liveTransportStatus: LiveTransportStatus;
  lastLiveSuccessAt: number | null;
  updateFromRest: (data: Partial<JudgeStoreState>) => void;
  updateFromWs: (payload: unknown) => void;
  setWsStatus: (status: boolean) => void;
  setLiveTransportStatus: (status: LiveTransportStatus, successAt?: number | null) => void;
}

export const useJudgeStore = create<JudgeStoreState>((set) => ({
  overview: null,
  trial: null,
  teams: {},
  scoreboard: [],
  systemStatus: null,
  controlStatus: null,
  isConnectedWS: false,
  liveTransportStatus: 'connecting',
  lastLiveSuccessAt: null,
  
  updateFromRest: (data) => set((state) => ({ ...state, ...data })),
  
  updateFromWs: (payload) => set((state) => {
    const normalized = normalizeLivePayload(payload);
    const newState = { ...state };

    if (normalized.overview !== undefined) {
      newState.overview = normalized.overview;
    }

    if (normalized.current !== undefined) {
      newState.trial = normalized.current;
    }

    if (normalized.teams !== undefined) {
      const nextTeams: Record<string, TeamInfo> = { ...state.teams };
      normalized.teams.forEach((t: TeamInfo) => {
        if (t.team_id) {
          nextTeams[t.team_id] = { ...(state.teams[t.team_id] ?? {} as TeamInfo), ...t };
        }
      });
      newState.teams = nextTeams;
    }

    if (normalized.scoreboard !== undefined) {
      newState.scoreboard = normalized.scoreboard;
    }

    if (normalized.system !== undefined) {
      newState.systemStatus = normalized.system;
      if (normalized.system?.match_control_status) {
        newState.controlStatus = normalized.system.match_control_status;
      }
    }

    if (normalized.control !== undefined) {
      newState.controlStatus = normalized.control?.match_control_status ?? null;
    }

    return newState;
  }),
  
  setWsStatus: (status) => set({
    isConnectedWS: status,
    liveTransportStatus: status ? 'websocket' : 'connecting',
  }),
  setLiveTransportStatus: (status, successAt) => set((state) => ({
    isConnectedWS: status === 'websocket',
    liveTransportStatus: status,
    lastLiveSuccessAt: successAt === undefined ? state.lastLiveSuccessAt : successAt,
  })),
}));
