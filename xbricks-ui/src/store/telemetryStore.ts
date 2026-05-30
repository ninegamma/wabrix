import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { useEffect } from 'react';
import type { TMLiveStats } from '../types/TMLiveStats';

export type PinnedMetric = { labelIndex: number; type: 'pps' | 'bps' };

interface TelemetryState {
  liveData: Record<string, TMLiveStats>;
  pinnedMetrics: Record<string, PinnedMetric>;
  updateLiveData: (stats: TMLiveStats) => void;
  togglePin: (rfNodeId: string, labelIndex: number, type: 'pps' | 'bps') => void;
}

export const useTelemetryStore = create<TelemetryState>()(
  persist(
    (set) => ({
      liveData: {},
      pinnedMetrics: {},
      updateLiveData: (stats) =>
        set((state) => ({
          liveData: { ...state.liveData, [stats.tmstats_name]: stats },
        })),
      togglePin: (rfNodeId, labelIndex, type) =>
        set((state) => {
          const current = state.pinnedMetrics[rfNodeId];
          if (current?.labelIndex === labelIndex && current?.type === type) {
            const next = { ...state.pinnedMetrics };
            delete next[rfNodeId];
            return { pinnedMetrics: next };
          }
          return {
            pinnedMetrics: {
              ...state.pinnedMetrics,
              [rfNodeId]: { labelIndex, type },
            },
          };
        }),
    }),
    {
      name: 'xbrix-telemetry-pins',
      partialize: (state) => ({ pinnedMetrics: state.pinnedMetrics }),
    }
  )
);

// Call once at the App root. Opens a single shared WebSocket connection for
// all telemetry consumers; data flows into the Zustand store.
export function useTelemetrySocket() {
  useEffect(() => {
    let stopped = false;
    let socket: WebSocket;

    const connect = () => {
      if (stopped) return;
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      socket = new WebSocket(`${protocol}//${window.location.host}/api/ws/telemetry`);

      socket.onmessage = (event) => {
        try {
          const stats: TMLiveStats = JSON.parse(event.data);
          useTelemetryStore.getState().updateLiveData(stats);
        } catch (_) {}
      };

      socket.onclose = () => {
        if (!stopped) setTimeout(connect, 2000);
      };
    };

    connect();

    return () => {
      stopped = true;
      if (socket && socket.readyState !== WebSocket.CLOSED) socket.close();
    };
  }, []);
}
