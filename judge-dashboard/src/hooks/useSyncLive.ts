import { useEffect, useRef } from 'react';
import { useJudgeStore, type JudgeStoreState } from '../store/useJudgeStore';
import * as restApi from '../api/rest';
import type { TeamInfo } from '../api/types';
import { isFinal9PreviewEnabled } from '../mock/final9Preview';

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, '');
}

function toWebSocketBaseUrl(value: string): string {
  const normalized = trimTrailingSlash(value);
  if (normalized.startsWith('https://')) {
    return `wss://${normalized.slice('https://'.length)}`;
  }
  if (normalized.startsWith('http://')) {
    return `ws://${normalized.slice('http://'.length)}`;
  }
  return normalized;
}

function resolveWebSocketUrl(): string {
  const envWsBaseUrl = import.meta.env.VITE_WS_BASE_URL;
  if (typeof envWsBaseUrl === 'string' && envWsBaseUrl.trim() !== '') {
    return trimTrailingSlash(envWsBaseUrl.trim());
  }

  const envDevProxyTarget = import.meta.env.VITE_DEV_PROXY_TARGET;
  if (import.meta.env.DEV) {
    const devTarget = typeof envDevProxyTarget === 'string' && envDevProxyTarget.trim() !== ''
      ? envDevProxyTarget.trim()
      : 'http://127.0.0.1:18080';
    return `${toWebSocketBaseUrl(devTarget)}/api/v1/ws/live`;
  }

  const host = window.location.hostname || 'localhost';
  const port = window.location.port ? `:${window.location.port}` : '';
  const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${wsProtocol}://${host}${port}/api/v1/ws/live`;
}

export function useSyncLive() {
  const updateFromRest = useJudgeStore((state) => state.updateFromRest);
  const updateFromWs = useJudgeStore((state) => state.updateFromWs);
  const setLiveTransportStatus = useJudgeStore((state) => state.setLiveTransportStatus);
  const fallbackInterval = useRef<number | null>(null);
  const inFlightRestFetch = useRef(false);
  const lastRestFetchAt = useRef(0);
  const latestWsAt = useRef(0);
  const consecutiveRestFailureCount = useRef(0);

  useEffect(() => {
    if (isFinal9PreviewEnabled()) {
      setLiveTransportStatus('rest_fallback', Date.now());
      return;
    }

    let ws: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let wsHealthTimer: number | null = null;
    let reconnectAttempts = 0;
    let wsOpenedAt = 0;
    let disposed = false;

    const fetchAll = async (updateTransportStatus = false) => {
      if (inFlightRestFetch.current) {
        return;
      }
      const now = Date.now();
      if (now - lastRestFetchAt.current < 250) {
        return;
      }
      inFlightRestFetch.current = true;
      lastRestFetchAt.current = now;
      try {
        const [overview, current, teams, scoreboard, system, controlStatus] = await Promise.allSettled([
          restApi.getOverview(),
          restApi.getCurrentTrial(),
          restApi.getTeams(),
          restApi.getScoreboard(),
          restApi.getSystemComponents(),
          restApi.getControlStatus(),
        ]);

        const nextState: Partial<JudgeStoreState> = {};
        let fulfilledRequestCount = 0;
        if (overview.status === 'fulfilled') {
          nextState.overview = overview.value;
          fulfilledRequestCount++;
        }
        if (current.status === 'fulfilled') {
          nextState.trial = current.value;
          fulfilledRequestCount++;
        }
        if (teams.status === 'fulfilled') {
          const teamRecord: Record<string, TeamInfo> = {};
          teams.value.forEach((t) => {
            teamRecord[t.team_id] = t;
          });
          nextState.teams = teamRecord;
          fulfilledRequestCount++;
        }
        if (scoreboard.status === 'fulfilled') {
          nextState.scoreboard = scoreboard.value;
          fulfilledRequestCount++;
        }
        if (system.status === 'fulfilled') {
          nextState.systemStatus = system.value;
          fulfilledRequestCount++;
        }
        if (controlStatus.status === 'fulfilled') {
          nextState.controlStatus = controlStatus.value;
          fulfilledRequestCount++;
        }

        if (fulfilledRequestCount > 0) {
          updateFromRest(nextState);
          consecutiveRestFailureCount.current = 0;
          if (updateTransportStatus && ws?.readyState !== WebSocket.OPEN) {
            setLiveTransportStatus('rest_fallback', Date.now());
          }
        } else {
          consecutiveRestFailureCount.current += 1;
          if (updateTransportStatus && consecutiveRestFailureCount.current >= 2) {
            setLiveTransportStatus('offline');
          }
        }
      } catch (e) {
        console.error("REST init failed", e);
        consecutiveRestFailureCount.current += 1;
        if (updateTransportStatus && consecutiveRestFailureCount.current >= 2) {
          setLiveTransportStatus('offline');
        }
      } finally {
        inFlightRestFetch.current = false;
      }
    };

    const setupRestFallback = () => {
      if (fallbackInterval.current === null) {
        console.log('WS offline, starting REST fallback polling...');
        void fetchAll(true);
        fallbackInterval.current = window.setInterval(() => {
          if (Date.now() - latestWsAt.current < 1000) {
            return;
          }
          void fetchAll(true);
        }, 1000);
      }
    };

    const clearFallback = () => {
      if (fallbackInterval.current !== null) {
        window.clearInterval(fallbackInterval.current);
        fallbackInterval.current = null;
      }
    };

    const scheduleReconnect = (connectWS: () => void) => {
      if (disposed || reconnectTimer !== null) {
        return;
      }
      const timeout = Math.min(1000 * Math.pow(2, reconnectAttempts), 10000);
      reconnectAttempts++;
      reconnectTimer = window.setTimeout(() => {
        reconnectTimer = null;
        connectWS();
      }, timeout);
    };

    const wsUrl = resolveWebSocketUrl();
    const connectWS = () => {
      if (disposed) {
        return;
      }
      if (fallbackInterval.current === null) {
        setLiveTransportStatus('connecting');
      }
      let socket: WebSocket;
      try {
        socket = new WebSocket(wsUrl);
      } catch (error) {
        console.error('WS construction failed', error);
        setupRestFallback();
        scheduleReconnect(connectWS);
        return;
      }
      ws = socket;

      socket.onopen = () => {
        if (ws !== socket) return;
        console.log('WS Connected');
        wsOpenedAt = Date.now();
        latestWsAt.current = 0;
        setLiveTransportStatus('websocket', wsOpenedAt);
        clearFallback();
        reconnectAttempts = 0;
      };

      socket.onmessage = (event) => {
        if (ws !== socket) return;
        try {
          const payload = JSON.parse(event.data);
          latestWsAt.current = Date.now();
          consecutiveRestFailureCount.current = 0;
          updateFromWs(payload);
          setLiveTransportStatus('websocket', latestWsAt.current);
        } catch {
          console.warn('Ignored invalid WS payload');
        }
      };

      socket.onclose = (event) => {
        if (ws !== socket) return;
        console.warn('WS closed', { code: event.code, reason: event.reason });
        ws = null;
        setupRestFallback();
        scheduleReconnect(connectWS);
      };

      socket.onerror = () => {
        if (ws !== socket) return;
        setupRestFallback();
        socket.close();
      };
    };

    void fetchAll(false);

    connectWS();
    wsHealthTimer = window.setInterval(() => {
      if (ws?.readyState !== WebSocket.OPEN) {
        return;
      }
      const lastActivityAt = latestWsAt.current || wsOpenedAt;
      if (lastActivityAt > 0 && Date.now() - lastActivityAt > 5000) {
        console.warn('WS live snapshot timed out; reconnecting');
        ws.close(4000, 'live snapshot timeout');
      }
    }, 1000);

    return () => {
      disposed = true;
      clearFallback();
      if (reconnectTimer !== null) clearTimeout(reconnectTimer);
      if (wsHealthTimer !== null) clearInterval(wsHealthTimer);
      if (ws) {
        ws.onclose = null;
        ws.close();
      }
    };
  }, [setLiveTransportStatus, updateFromRest, updateFromWs]);
}

