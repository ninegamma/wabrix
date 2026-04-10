import { memo, useEffect, useState } from 'react';
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
// Import your generated type if you have one, or define it inline:
import type { InterfaceConfig } from './types/InterfaceConfig';

// The payload sent by the Rust backend
export type LiveStats = {
    iface: string;
    direction: string;
    is_active_run: boolean;
    live_pkts: number;
    sw_drops: number;
    hw_drops: number;
};

// We extend the Node type to accept InterfaceConfig data
export type TelemetryNodeType = Node<InterfaceConfig, 'telemetry'>;

// Use React.memo so the node doesn't re-render unless its specific stats change
export const TelemetryNode = memo(({ data }: NodeProps<TelemetryNodeType>) => {
    const [stats, setStats] = useState<LiveStats | null>(null);
    const [isConnected, setIsConnected] = useState(false);

    useEffect(() => {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws/telemetry`;

        const socket = new WebSocket(wsUrl);

        socket.onopen = () => setIsConnected(true);
        socket.onclose = () => setIsConnected(false);

        socket.onmessage = (event) => {
            try {
                const incomingStats: LiveStats = JSON.parse(event.data);

                // CRITICAL: Only update state if this telemetry message belongs to THIS specific node.
                // We assume data.name holds the interface name (e.g., 'enp2s0f0np0')
                if (incomingStats.iface === data.name) {
                    setStats(incomingStats);
                }
            } catch (err) {
                console.error("Telemetry parse error", err);
            }
        };

        // Cleanup: Close the socket when this node is deleted or the canvas is cleared
        return () => {
            if (socket.readyState === WebSocket.OPEN) {
                socket.close();
            }
        };
    }, [data.name]);

    // UI Colors based on state
    const isActive = stats?.is_active_run;
    const borderColor = isActive ? '#10b981' : isConnected ? '#3b82f6' : '#ef4444';
    const bgColor = isActive ? '#064e3b' : '#1e293b';

    return (
        <div style={{
            background: bgColor,
            border: `2px solid ${borderColor}`,
            borderRadius: '8px',
            padding: '12px',
            minWidth: '220px',
            color: '#f8fafc',
            fontFamily: 'monospace',
            boxShadow: isActive ? '0 0 15px rgba(16, 185, 129, 0.4)' : '0 4px 6px rgba(0,0,0,0.3)',
            transition: 'all 0.2s ease-in-out'
        }}>
            {/* Header Area */}
            <div style={{ borderBottom: `1px solid ${borderColor}`, paddingBottom: '8px', marginBottom: '8px', display: 'flex', justifyContent: 'space-between' }}>
                <strong style={{ fontSize: '14px', color: '#60a5fa' }}>🌐 {data.name}</strong>
                <span style={{ fontSize: '10px', display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: borderColor, animation: isActive ? 'pulse 1s infinite' : 'none' }} />
                    {isActive ? 'LIVE' : isConnected ? 'IDLE' : 'OFFLINE'}
                </span>
            </div>

            {/* Config Data (Static)
            <div style={{ fontSize: '10px', color: '#94a3b8', marginBottom: '10px' }}>
                <div>MAC: {data.mac_if || 'Auto'}</div>
                <div>Queues: {data.rxd} RX / {data.txd} TX</div>
            </div>
            */}
            {/* Live Telemetry Area */}
            {stats ? (
                <div style={{ background: '#0f172a', padding: '8px', borderRadius: '4px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: '#cbd5e1' }}>Packets:</span>
                        <strong style={{ color: '#10b981' }}>{stats.live_pkts.toLocaleString()}</strong>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: '#cbd5e1' }}>SW Drops:</span>
                        <strong style={{ color: stats.sw_drops > 0 ? '#ef4444' : '#94a3b8' }}>{stats.sw_drops.toLocaleString()}</strong>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <span style={{ color: '#cbd5e1' }}>HW Drops:</span>
                        <strong style={{ color: stats.hw_drops > 0 ? '#ef4444' : '#94a3b8' }}>{stats.hw_drops.toLocaleString()}</strong>
                    </div>
                </div>
            ) : (
                <div style={{ fontSize: '11px', color: '#64748b', textAlign: 'center', padding: '10px 0' }}>
                    Waiting for traffic...
                </div>
            )}

            {/* React Flow Handles */}
            <Handle type="target" position={Position.Left} style={{ background: borderColor }} />
            <Handle type="source" position={Position.Right} style={{ background: borderColor }} />
        </div>
    );
});