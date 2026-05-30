import { useState, useEffect, useCallback, useRef, memo } from 'react';
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
import type { InterfaceConfig } from '../types/InterfaceConfig';
import type { UmemConfig } from '../types/UmemConfig';
import type { NodeConfig } from '../types/NodeConfig';
import type { XskMapping } from '../types/XskMapping';
import type { WorkerConfig } from '../types/WorkerConfig';
import { useTelemetryStore } from '../store/telemetryStore';

// ==========================================
// REACT FLOW NODE TYPE EXPORTS
// ==========================================
export type IfaceNodeType = Node<InterfaceConfig, 'iface'>;
export type UmemNodeType = Node<UmemConfig, 'umem'>;
export type PipelineNodeData = NodeConfig & { pipeline_name?: string };
export type XskNodeType = Node<XskMapping, 'xsk'>;
export type WorkerNodeType = Node<WorkerConfig, 'worker'>;
export type PipelineNodeType = Node<PipelineNodeData, 'pipeline'>;
export type PipelineHeaderData = { name: string; workers: number[]; poll_frequency?: number };
export type PipelineHeaderNodeType = Node<PipelineHeaderData, 'pipeline_header'>;

// ==========================================
// 1. THE FLOATING TELEMETRY POPOVER (interface nodes)
// ==========================================
const TelemetryPopover = ({ ifaceName, onClose }: { ifaceName: string, onClose: () => void }) => {
    const stats = useTelemetryStore(state => state.liveData[ifaceName]);
    const [offset, setOffset] = useState({ x: 0, y: 0 });
    const dragOrigin = useRef<{ mx: number; my: number; ox: number; oy: number } | null>(null);

    const onHeaderMouseDown = (e: React.MouseEvent) => {
        e.stopPropagation();
        dragOrigin.current = { mx: e.clientX, my: e.clientY, ox: offset.x, oy: offset.y };

        const onMove = (ev: MouseEvent) => {
            if (!dragOrigin.current) return;
            setOffset({
                x: dragOrigin.current.ox + (ev.clientX - dragOrigin.current.mx),
                y: dragOrigin.current.oy + (ev.clientY - dragOrigin.current.my),
            });
        };
        const onUp = () => {
            dragOrigin.current = null;
            window.removeEventListener('mousemove', onMove);
            window.removeEventListener('mouseup', onUp);
        };
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
    };

    const isActive = stats?.is_active_run;

    return (
        <div className="nodrag" style={{
            position: 'absolute',
            top: `calc(-110px + ${offset.y}px)`,
            left: `calc(50% + ${offset.x}px)`,
            transform: 'translateX(-50%)',
            background: isActive ? '#064e3b' : '#0f172a',
            border: `1px solid ${isActive ? '#10b981' : '#3b82f6'}`,
            borderRadius: '8px',
            padding: '10px',
            width: '180px',
            color: '#f8fafc',
            boxShadow: '0 10px 25px rgba(0,0,0,0.5)',
            zIndex: 1000,
            userSelect: 'none',
        }}>
            <div
                onMouseDown={onHeaderMouseDown}
                style={{
                    display: 'flex', justifyContent: 'space-between',
                    marginBottom: '8px', borderBottom: '1px solid #333', paddingBottom: '4px',
                    cursor: 'grab',
                }}
            >
                <span style={{ fontSize: '10px', color: isActive ? '#10b981' : '#cbd5e1' }}>
                    {isActive ? '🚀 LIVE TRAFFIC' : '⏳ IDLE'}
                </span>
                <button
                    onMouseDown={e => e.stopPropagation()}
                    onClick={onClose}
                    style={{ background: 'transparent', border: 'none', color: '#ef4444', cursor: 'pointer', fontSize: '12px' }}
                >
                    ✖
                </button>
            </div>

            {stats ? (
                <div style={{ fontSize: '12px' }}>
                    {stats.labels.map((label, i) => {
                        const val = Number(stats.counters_pkts[i] ?? 0);
                        const isDropCounter = label.includes('drop');
                        return (
                            <div key={label} style={{ display: 'flex', justifyContent: 'space-between' }}>
                                <span>{label}:</span>
                                <strong style={{ color: isDropCounter && val > 0 ? '#ef4444' : isDropCounter ? '#94a3b8' : '#10b981' }}>
                                    {val.toLocaleString()}
                                </strong>
                            </div>
                        );
                    })}
                </div>
            ) : (
                <div style={{ fontSize: '11px', color: '#64748b', textAlign: 'center' }}>
                    Waiting for data...
                </div>
            )}
        </div>
    );
};

// ==========================================
// 2. THE MAIN INTERFACE NODE
// ==========================================
export const IfaceNode = memo(({ id, data }: NodeProps<IfaceNodeType>) => {
    const [showTelemetry, setShowTelemetry] = useState(false);

    const pinned = useTelemetryStore(state => state.pinnedMetrics[id]);
    const stats = useTelemetryStore(state => state.liveData[data.name]);
    const togglePin = useTelemetryStore(state => state.togglePin);

    return (
        <div style={{
            position: 'relative',
            background: '#1e293b',
            border: '2px solid #3b82f6',
            borderRadius: '8px',
            padding: '12px',
            minWidth: '180px',
            color: '#f8fafc',
            fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
        }}>
            <div
                onClick={() => setShowTelemetry(!showTelemetry)}
                style={{
                    position: 'absolute',
                    top: '-6px',
                    right: '-6px',
                    width: '14px',
                    height: '14px',
                    background: showTelemetry ? '#10b981' : '#3b82f6',
                    border: '2px solid #0f172a',
                    borderRadius: '50%',
                    cursor: 'pointer',
                    boxShadow: showTelemetry ? '0 0 8px #10b981' : 'none',
                    transition: 'all 0.2s',
                    zIndex: 10
                }}
                title="Toggle Live Telemetry"
            />

            {showTelemetry && (
                <TelemetryPopover
                    ifaceName={data.name}
                    onClose={() => setShowTelemetry(false)}
                />
            )}

            <div style={{ borderBottom: '1px solid #475569', paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: '#60a5fa' }}>🌐 IFACE: {data.name}</strong>
            </div>

            {/* Pinned metric badge */}
            {stats && pinned !== undefined && (
                <div style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    padding: '3px 6px', marginBottom: '6px',
                    background: '#0f172a', border: '1px solid #3b82f644',
                    borderRadius: '4px', fontSize: '11px',
                }}>
                    <span style={{ color: '#94a3b8', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {stats.labels[pinned.labelIndex]}
                    </span>
                    <span style={{ color: '#60a5fa', fontFamily: 'monospace', fontWeight: 'bold', marginLeft: '4px' }}>
                        {Number(stats.counters_pkts[pinned.labelIndex] ?? 0).toLocaleString()}
                    </span>
                    <button
                        className="nodrag"
                        onClick={() => togglePin(id, pinned.labelIndex, pinned.type)}
                        style={{ background: 'none', border: 'none', color: '#f59e0b', cursor: 'pointer', fontSize: '12px', padding: '0 0 0 4px' }}
                    >★</button>
                </div>
            )}

            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                {Object.entries(data)
                    .filter(([_, value]) => value !== null && value !== undefined && typeof value !== 'object')
                    .map(([key, value]) => (
                        <div key={key}>
                            <strong style={{ color: '#94a3b8' }}>{key}:</strong> {String(value)}
                        </div>
                    ))}
            </div>

            <Handle type="target" position={Position.Left} style={{ background: '#3b82f6' }} />
            <Handle type="source" position={Position.Right} style={{ background: '#3b82f6' }} />
        </div>
    );
});

// ==========================================
// 3. THE DYNAMIC UMEM NODE
// ==========================================
export function UmemNode({ data }: NodeProps<UmemNodeType>) {
    return (
        <div style={{
            background: '#312e81', border: '2px solid #a855f7', borderRadius: '8px',
            padding: '12px', minWidth: '180px', color: '#f8fafc', fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
        }}>
            <div style={{ borderBottom: '1px solid #4f46e5', paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: '#c084fc' }}>🧠 UMEM [{data.id}]</strong>
            </div>

            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                {Object.entries(data)
                    .filter(([_, value]) => value !== null && value !== undefined)
                    .map(([key, value]) => (
                        <div key={key}>
                            <strong style={{ color: '#94a3b8' }}>{key}:</strong> {String(value)}
                        </div>
                    ))}
            </div>

            <Handle type="target" position={Position.Left} style={{ background: '#a855f7' }} />
            <Handle type="source" position={Position.Right} style={{ background: '#a855f7' }} />
            <Handle id="bottom" type="source" position={Position.Bottom} style={{ background: '#a855f7', width: 12, height: 3, borderRadius: 1 }} />
            <Handle id="bottom-in" type="target" position={Position.Bottom} style={{ background: '#f59e0b', width: 8, height: 8, borderRadius: '50%', bottom: -12 }} />
        </div>
    );
}

// ==========================================
// 3b. XSK NODE
// ==========================================
export function XskNode({ data }: NodeProps<XskNodeType>) {
    return (
        <div style={{
            background: '#0c1a2e', border: '2px solid #38bdf8', borderRadius: '8px',
            padding: '12px', minWidth: '180px', color: '#f8fafc', fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
        }}>
            <div style={{ borderBottom: '1px solid #0369a1', paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: '#7dd3fc' }}>&#x26a1; XSK: {data.iface}</strong>
            </div>
            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                <div><strong style={{ color: '#94a3b8' }}>queues:</strong> {data.queues.join(', ')}</div>
                <div><strong style={{ color: '#94a3b8' }}>umem:</strong> {data.umem}</div>
                <div><strong style={{ color: '#94a3b8' }}>rxq/txq:</strong> {data.rxq_size}/{data.txq_size}</div>
                <div><strong style={{ color: '#94a3b8' }}>mode:</strong> {data.xdp_mode}</div>
                {data.zerocopy && <div><strong style={{ color: '#38bdf8' }}>zerocopy</strong></div>}
            </div>
            <Handle id="top" type="target" position={Position.Top} style={{ background: '#38bdf8', width: 12, height: 3, borderRadius: 1 }} />
            <Handle type="source" position={Position.Right} style={{ background: '#38bdf8' }} />
        </div>
    );
}

// ==========================================
// 3c. WORKER NODE
// ==========================================
export function WorkerNode({ data }: NodeProps<WorkerNodeType>) {
    return (
        <div style={{
            background: '#1c1409', border: '2px solid #f59e0b', borderRadius: '8px',
            padding: '12px', minWidth: '180px', color: '#f8fafc', fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
        }}>
            <Handle id="top" type="source" position={Position.Top} style={{ background: '#f59e0b', width: 12, height: 3, borderRadius: 1 }} />
            <div style={{ borderBottom: '1px solid #78350f', paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: '#fcd34d' }}>&#x1f9f5; Worker {data.id}</strong>
            </div>
            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                <div><strong style={{ color: '#94a3b8' }}>core:</strong> {data.core}</div>
                {data.partitions.length > 0 && (
                    <div><strong style={{ color: '#94a3b8' }}>partitions:</strong>{' '}
                        {data.partitions.map(p => `umem${p.umem}×${p.count}`).join(', ')}
                    </div>
                )}
            </div>
            <Handle type="source" position={Position.Bottom} style={{ background: '#f59e0b', width: 12, height: 3, borderRadius: 1 }} />
        </div>
    );
}

// ==========================================
// 4a. PIPELINE HEADER NODE
// ==========================================
export function PipelineHeaderNode({ data }: NodeProps<PipelineHeaderNodeType>) {
    const c = pipelineColor(data.name);
    const [pollFreq, setPollFreq] = useState<string>(String(data.poll_frequency ?? 1000));
    const [editing, setEditing] = useState(false);

    const commitPollFrequency = async () => {
        setEditing(false);
        const val = parseInt(pollFreq, 10);
        if (isNaN(val) || val < 1) { setPollFreq(String(data.poll_frequency ?? 1000)); return; }
        try {
            await fetch('/api/pipeline/update_config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pipeline_name: data.name, poll_frequency: val }),
            });
            (data as any).poll_frequency = val;
        } catch (e) { console.error('poll_frequency update failed', e); }
    };

    return (
        <div style={{
            background: c.bg,
            border: `2px solid ${c.border}`,
            borderRadius: '8px',
            padding: '12px',
            minWidth: '180px',
            color: '#f8fafc',
            fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)',
        }}>
            <div style={{ borderBottom: `1px solid ${c.divider}`, paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: c.title }}>&#x2261; {data.name}</strong>
            </div>
            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                <div>
                    <strong style={{ color: '#94a3b8' }}>workers:</strong>{' '}
                    <span style={{ color: c.handle }}>[{data.workers.join(', ')}]</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginTop: '2px' }}>
                    <strong style={{ color: c.border }}>poll_freq:</strong>
                    {editing ? (
                        <input
                            className="nodrag"
                            type="number"
                            min={1}
                            value={pollFreq}
                            onChange={e => setPollFreq(e.target.value)}
                            onBlur={commitPollFrequency}
                            onKeyDown={e => { if (e.key === 'Enter') commitPollFrequency(); if (e.key === 'Escape') { setEditing(false); setPollFreq(String(data.poll_frequency ?? 1000)); } }}
                            autoFocus
                            style={{
                                width: '70px', background: '#1e293b', border: `1px solid ${c.border}`,
                                color: '#f8fafc', borderRadius: '3px', padding: '1px 4px',
                                fontSize: '11px', fontFamily: 'monospace',
                            }}
                        />
                    ) : (
                        <span
                            onClick={() => setEditing(true)}
                            title="Click to edit (hot-changeable)"
                            style={{
                                color: c.border, cursor: 'pointer', borderBottom: `1px dashed ${c.border}`,
                                paddingBottom: '1px',
                            }}
                        >{pollFreq} Hz</span>
                    )}
                </div>
            </div>
            <Handle type="source" position={Position.Bottom} style={{ background: c.handle, width: 12, height: 3, borderRadius: 1 }} />
        </div>
    );
}

// ==========================================
// 4b. GENERATOR STATS POPOVER
// ==========================================
const GeneratorStatsPopover = ({ nodeId, onClose }: { nodeId: string; onClose: () => void }) => {
    const stats = useTelemetryStore(state => state.liveData[nodeId]);
    const [offset, setOffset] = useState({ x: 0, y: 0 });
    const dragOrigin = useRef<{ mx: number; my: number; ox: number; oy: number } | null>(null);

    const onHeaderMouseDown = (e: React.MouseEvent) => {
        e.stopPropagation();
        dragOrigin.current = { mx: e.clientX, my: e.clientY, ox: offset.x, oy: offset.y };
        const onMove = (ev: MouseEvent) => {
            if (!dragOrigin.current) return;
            setOffset({
                x: dragOrigin.current.ox + (ev.clientX - dragOrigin.current.mx),
                y: dragOrigin.current.oy + (ev.clientY - dragOrigin.current.my),
            });
        };
        const onUp = () => {
            dragOrigin.current = null;
            window.removeEventListener('mousemove', onMove);
            window.removeEventListener('mouseup', onUp);
        };
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
    };

    // labels: total_generated[0], pkts_this_run[1], total_cycles[2], pps_last_run[3]
    const totalGen   = stats ? Number(stats.counters_pkts[0] ?? 0) : null;
    const thisRun    = stats ? Number(stats.counters_pkts[1] ?? 0) : null;
    const totalCycles = stats ? Number(stats.counters_pkts[2] ?? 0) : null;
    const avgCycles  = (totalGen && totalGen > 0 && totalCycles !== null)
        ? Math.round(totalCycles / totalGen) : null;
    const ppsRaw     = stats ? Number(stats.counters_pkts[3] ?? 0) : null;

    const formatPps = (pps: number): string => {
        if (pps >= 1_000_000) return Number((pps / 1_000_000).toPrecision(5)) + 'M';
        if (pps >= 1_000)     return Number((pps / 1_000).toPrecision(5)) + 'k';
        return pps.toFixed(0);
    };

    return (
        <div className="nodrag" style={{
            position: 'absolute',
            top: `calc(-172px + ${offset.y}px)`,
            left: `calc(50% + ${offset.x}px)`,
            transform: 'translateX(-50%)',
            background: '#0f172a',
            border: '1px solid #f59e0b',
            borderRadius: '8px',
            padding: '10px',
            width: '200px',
            color: '#f8fafc',
            boxShadow: '0 10px 25px rgba(0,0,0,0.5)',
            zIndex: 1000,
            userSelect: 'none',
        }}>
            <div onMouseDown={onHeaderMouseDown} style={{
                display: 'flex', justifyContent: 'space-between',
                marginBottom: '8px', borderBottom: '1px solid #333', paddingBottom: '4px',
                cursor: 'grab',
            }}>
                <span style={{ fontSize: '10px', color: '#f59e0b' }}>&#x1f4ca; GENERATOR STATS</span>
                <button
                    onMouseDown={e => e.stopPropagation()}
                    onClick={onClose}
                    style={{ background: 'transparent', border: 'none', color: '#ef4444', cursor: 'pointer', fontSize: '12px' }}
                >✖</button>
            </div>

            {stats ? (
                <div style={{ fontSize: '12px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: '#94a3b8' }}>total gen'd:</span>
                        <strong style={{ color: '#10b981' }}>{totalGen !== null ? totalGen.toLocaleString() : '—'}</strong>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: '#94a3b8' }}>this run:</span>
                        <strong style={{ color: '#60a5fa' }}>{thisRun !== null ? thisRun.toLocaleString() : '—'}</strong>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: '#94a3b8' }}>avg cycles/pkt:</span>
                        <strong style={{ color: '#f59e0b' }}>
                            {avgCycles !== null && avgCycles > 0 ? avgCycles.toLocaleString() : 'n/a'}
                        </strong>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <span style={{ color: '#94a3b8' }}>pps:</span>
                        <strong style={{ color: '#a78bfa' }}>
                            {ppsRaw !== null && ppsRaw > 0 ? formatPps(ppsRaw) : 'n/a'}
                        </strong>
                    </div>
                </div>
            ) : (
                <div style={{ fontSize: '11px', color: '#64748b', textAlign: 'center' }}>
                    Waiting for data...
                </div>
            )}
        </div>
    );
};

// ==========================================
// PIPELINE PALETTE
// ==========================================
const PIPELINE_PALETTE: Array<{ bg: string; border: string; divider: string; title: string; handle: string }> = [
    { bg: '#052e16', border: '#22c55e', divider: '#166534', title: '#4ade80', handle: '#22c55e' },
    { bg: '#083344', border: '#22d3ee', divider: '#164e63', title: '#67e8f9', handle: '#22d3ee' },
    { bg: '#431407', border: '#f97316', divider: '#7c2d12', title: '#fdba74', handle: '#f97316' },
    { bg: '#2d0a20', border: '#ec4899', divider: '#831843', title: '#f9a8d4', handle: '#ec4899' },
    { bg: '#1e1b4b', border: '#818cf8', divider: '#312e81', title: '#c7d2fe', handle: '#818cf8' },
    { bg: '#422006', border: '#eab308', divider: '#713f12', title: '#fde047', handle: '#eab308' },
    { bg: '#450a0a', border: '#ef4444', divider: '#7f1d1d', title: '#fca5a5', handle: '#ef4444' },
    { bg: '#0c0a1e', border: '#a855f7', divider: '#3b0764', title: '#d8b4fe', handle: '#a855f7' },
];

export function pipelineColor(name: string | undefined) {
    if (!name) return PIPELINE_PALETTE[0];
    let hash = 0;
    for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
    return PIPELINE_PALETTE[hash % PIPELINE_PALETTE.length];
}

// ==========================================
// 4c. GENERATOR CONTROL BUTTON
// ==========================================
type GenState = 'idle' | 'generating';

const GeneratorControlButton = ({
    nodeId,
    pipelineNodeId,
    pipelineName,
}: {
    nodeId: string;
    pipelineNodeId: string;
    pipelineName: string;
}) => {
    const tmStatsKey = `${pipelineName}/${pipelineNodeId}`;
    const isActive = useTelemetryStore(state => state.liveData[tmStatsKey]?.is_active_run ?? false);

    const [genState, setGenState] = useState<GenState>('idle');
    const [pulse, setPulse] = useState(false);
    const prevActiveRef = useRef<boolean | null>(null);

    // Pulse animation tick while generating
    useEffect(() => {
        if (genState !== 'generating') { setPulse(false); return; }
        const t = setInterval(() => setPulse(p => !p), 650);
        return () => clearInterval(t);
    }, [genState]);

    // Sync button state from telemetry and auto-reset when run finishes
    useEffect(() => {
        const wasActive = prevActiveRef.current;
        prevActiveRef.current = isActive;

        if (isActive) {
            setGenState('generating');
        } else if (wasActive === true && !isActive) {
            setGenState('idle');
            const handler = (window as any).__xbricksSendCmdToNode;
            if (handler) handler(nodeId, pipelineName, pipelineNodeId, 'stop');
        }
    }, [isActive, nodeId, pipelineName, pipelineNodeId]);

    const handleClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        const cmd = genState === 'generating' ? 'stop' : 'start';
        setGenState(cmd === 'start' ? 'generating' : 'idle');
        const handler = (window as any).__xbricksSendCmdToNode;
        if (handler) handler(nodeId, pipelineName, pipelineNodeId, cmd);
    };

    const isGenerating = genState === 'generating';

    return (
        <div onMouseDown={e => e.stopPropagation()} style={{ marginTop: '8px' }}>
            <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                padding: '3px 8px',
                background: isGenerating ? '#052e16' : '#0f172a',
                border: `1px solid ${isGenerating ? '#10b981' : '#334155'}`,
                borderBottom: 'none',
                borderRadius: '4px 4px 0 0',
                fontSize: '10px',
                fontWeight: 'bold',
                letterSpacing: '0.08em',
                color: isGenerating ? '#10b981' : '#475569',
                userSelect: 'none',
            }}>
                <div style={{
                    width: '7px',
                    height: '7px',
                    borderRadius: '50%',
                    background: isGenerating ? '#10b981' : '#334155',
                    boxShadow: isGenerating && pulse ? '0 0 5px #10b981' : 'none',
                    opacity: isGenerating ? (pulse ? 1 : 0.4) : 1,
                    transition: 'opacity 0.3s, box-shadow 0.3s',
                    flexShrink: 0,
                }} />
                {isGenerating ? 'GENERATING' : 'IDLE'}
            </div>
            <button
                onClick={handleClick}
                style={{
                    display: 'block',
                    width: '100%',
                    padding: '5px 0',
                    background: isGenerating ? '#7f1d1d' : '#14532d',
                    border: `1px solid ${isGenerating ? '#ef4444' : '#22c55e'}`,
                    borderRadius: '0 0 4px 4px',
                    color: isGenerating ? '#ef4444' : '#22c55e',
                    cursor: 'pointer',
                    fontFamily: 'monospace',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    letterSpacing: '0.05em',
                }}
            >
                {isGenerating ? '⏹ STOP' : '▶ START'}
            </button>
        </div>
    );
};

// ==========================================
// 5. PIPELINE NODE
// ==========================================
export function PipelineNode({ data, id }: NodeProps<PipelineNodeType>) {
    const [showTelemetry, setShowTelemetry] = useState(false);
    const closeTelemetry = useCallback(() => setShowTelemetry(false), []);

    const tmStatsKey = data.pipeline_name ? `${data.pipeline_name}/${data.id}` : data.id;

    // Zustand store subscriptions (selector-scoped: only this node re-renders on its data changes)
    const nodeStats = useTelemetryStore(state => state.liveData[tmStatsKey]);
    const pinned = useTelemetryStore(state => state.pinnedMetrics[id]);
    const togglePin = useTelemetryStore(state => state.togglePin);

    // DropNode: live dropped-packet counter from store
    const droppedCount = data.type === 'DropNode' && nodeStats
        ? Number(nodeStats.counters_pkts[0] ?? 0)
        : null;

    const label = data.id;
    const colors = pipelineColor(data.pipeline_name);
    const routes: string[] = Array.isArray(data.routes) ? data.routes : [];
    const args = data.args && Object.keys(data.args).length > 0 ? data.args : null;
    const hotArgs = (data as any).hot_args && Object.keys((data as any).hot_args).length > 0 ? (data as any).hot_args as Record<string, string> : null;
    const [wasmDataEdits, setWasmDataEdits] = useState<Record<string, string>>(data.wasm_data ?? {});
    const wasmDataFocusedRef = useRef(false);
    useEffect(() => {
        if (!wasmDataFocusedRef.current) setWasmDataEdits(data.wasm_data ?? {});
    }, [data.wasm_data]);
    const hasWasmData = Object.keys(wasmDataEdits).length > 0;

    const [opState, setOpState] = useState<0 | 1 | 2 | null>(null);
    const sendOpState = useCallback((state: 0 | 1 | 2) => {
        if (!data.pipeline_name) return;
        setOpState(state);
        fetch('/api/control/event', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                address_pattern: `*/${data.pipeline_name}/${data.id}/*`,
                opcode: 0x0001,
                payload: [state],
            }),
        }).catch(console.error);
    }, [data.pipeline_name, data.id]);

    const sendFlush = useCallback(() => {
        if (!data.pipeline_name) return;
        fetch('/api/control/event', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                address_pattern: `*/${data.pipeline_name}/${data.id}/*`,
                opcode: 0x0002,
                payload: [1],
            }),
        }).catch(console.error);
    }, [data.pipeline_name, data.id]);

    const outgoing: { id: string; label: string }[] = [];
    const seen = new Set<string>();
    const addOut = (id: string, lbl: string) => {
        if (id && !seen.has(id)) { seen.add(id); outgoing.push({ id, label: lbl }); }
    };
    routes.forEach(r => addOut(r, r));

    const handleSpacing = outgoing.length > 1
        ? outgoing.map((_, i) => `${((i + 1) / (outgoing.length + 1)) * 100}%`)
        : ['50%'];

    // Pinned metric badge: only show when rates are available (pipeline nodes have them)
    const hasPinnedBadge = pinned !== undefined && nodeStats && nodeStats.rates_pkts.length > 0;

    return (
        <div style={{
            background: colors.bg, border: `2px solid ${colors.border}`, borderRadius: '8px',
            padding: '12px', minWidth: '180px', color: '#f8fafc', fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)', position: 'relative',
        }}>
            {/* Telemetry toggle dot */}
            <div
                onClick={() => setShowTelemetry(!showTelemetry)}
                style={{
                    position: 'absolute',
                    top: '-6px',
                    right: '-6px',
                    width: '14px',
                    height: '14px',
                    background: showTelemetry ? '#10b981' : colors.border,
                    border: '2px solid #0f172a',
                    borderRadius: '50%',
                    cursor: 'pointer',
                    boxShadow: showTelemetry ? '0 0 8px #10b981' : 'none',
                    transition: 'all 0.2s',
                    zIndex: 10,
                }}
                title="Toggle Live Telemetry"
            />

            {showTelemetry && (
                data.type === 'PacketGenerator'
                    ? <GeneratorStatsPopover nodeId={tmStatsKey} onClose={closeTelemetry} />
                    : <TelemetryPopover ifaceName={tmStatsKey} onClose={closeTelemetry} />
            )}

            <Handle type="target" position={Position.Left} style={{ background: colors.handle }} />

            <div style={{ borderBottom: `1px solid ${colors.divider}`, paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '13px', color: colors.title }}>&#x2699; {label}</strong>
                <div style={{ fontSize: '10px', color: colors.border, marginTop: '2px' }}>{data.type}</div>
            </div>

            {/* Pinned metric badge */}
            {hasPinnedBadge && (
                <div style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    padding: '3px 6px', marginBottom: '6px',
                    background: '#0f172a', border: `1px solid ${colors.border}44`,
                    borderRadius: '4px', fontSize: '11px',
                }}>
                    <span style={{ color: '#94a3b8', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {nodeStats!.labels[pinned!.labelIndex]}
                    </span>
                    <span style={{ color: '#60a5fa', fontFamily: 'monospace', fontWeight: 'bold', marginLeft: '4px' }}>
                        {pinned!.type === 'pps'
                            ? `${Number(nodeStats!.rates_pkts[pinned!.labelIndex] ?? 0).toLocaleString()} pps`
                            : `${Number(nodeStats!.rates_bytes[pinned!.labelIndex] ?? 0).toLocaleString()} Bps`
                        }
                    </span>
                    <button
                        className="nodrag"
                        onClick={() => togglePin(id, pinned!.labelIndex, pinned!.type)}
                        style={{ background: 'none', border: 'none', color: '#f59e0b', cursor: 'pointer', fontSize: '12px', padding: '0 0 0 4px' }}
                    >★</button>
                </div>
            )}

            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                {data.queues && data.queues.length > 0 && (
                    <div><strong style={{ color: '#94a3b8' }}>queues:</strong> {data.queues.join(', ')}</div>
                )}
                {(data as any).umem && (data as any).umem.length > 0 && (
                    <div><strong style={{ color: '#94a3b8' }}>umem:</strong> {(data as any).umem.join(', ')}</div>
                )}
                {args && Object.entries(args).map(([k, v]) => (
                    <div key={k}>
                        <strong style={{ color: '#94a3b8' }}>{k === 'wasm_file_path' ? 'wasm_file' : k}:</strong>{' '}
                        {k === 'wasm_file_path' ? v.split('/').pop() : v}
                    </div>
                ))}
            </div>
            {hotArgs && (
                <div style={{ fontSize: '11px', lineHeight: '1.6', color: colors.border, marginTop: '4px' }}>
                    {Object.entries(hotArgs).map(([k, v]) => (
                        <div key={k}>
                            <strong style={{ color: colors.border }}>{k}:</strong>{' '}{v}
                        </div>
                    ))}
                </div>
            )}

            {hasWasmData && (
                <div style={{
                    fontSize: '11px', lineHeight: '1.6', marginTop: '6px',
                    borderTop: `1px solid ${colors.divider}`, paddingTop: '4px',
                }}>
                    <div style={{ fontSize: '10px', color: '#64748b', marginBottom: '3px' }}>wasm_data</div>
                    {Object.entries(wasmDataEdits).map(([k, v]) => (
                        <div key={k} style={{ display: 'flex', alignItems: 'center', gap: '4px', marginBottom: '2px' }}>
                            <strong style={{ color: '#94a3b8', flexShrink: 0, minWidth: '90px' }}>{k}:</strong>
                            <input
                                className="nodrag"
                                type="text"
                                value={v}
                                onFocus={() => { wasmDataFocusedRef.current = true; }}
                                onChange={e => setWasmDataEdits(prev => ({ ...prev, [k]: e.target.value }))}
                                onBlur={() => {
                                    wasmDataFocusedRef.current = false;
                                    if (!data.pipeline_name) return;
                                    fetch('/api/pipeline/update', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({
                                            pipeline_name: data.pipeline_name,
                                            node_id: data.id,
                                            wasm_data: wasmDataEdits,
                                        }),
                                    }).catch(console.error);
                                }}
                                style={{
                                    background: '#0f172a',
                                    border: '1px solid #334155',
                                    borderRadius: '3px',
                                    color: '#f8fafc',
                                    fontFamily: 'monospace',
                                    fontSize: '11px',
                                    padding: '1px 4px',
                                    flex: 1,
                                    minWidth: 0,
                                }}
                            />
                        </div>
                    ))}
                </div>
            )}

            {data.type === 'WasmEngine' && (
                <div style={{ marginTop: '6px', borderTop: `1px solid ${colors.divider}`, paddingTop: '4px' }}>
                    <div style={{ fontSize: '10px', color: '#64748b', marginBottom: '3px' }}>operational state</div>
                    <div style={{ display: 'flex', gap: '4px' }}>
                        {([['Offline', 0, '#ef4444', '#450a0a'], ['Online', 1, '#10b981', '#052e16'], ['Bypass', 2, '#f59e0b', '#1c1409']] as const).map(([label, val, fg, bg]) => (
                            <button
                                key={label}
                                className="nodrag"
                                onClick={() => sendOpState(val)}
                                style={{
                                    flex: 1,
                                    padding: '2px 0',
                                    fontSize: '10px',
                                    fontFamily: 'monospace',
                                    fontWeight: opState === val ? 'bold' : 'normal',
                                    cursor: 'pointer',
                                    borderRadius: '3px',
                                    border: `1px solid ${opState === val ? fg : '#334155'}`,
                                    background: opState === val ? bg : '#0f172a',
                                    color: opState === val ? fg : '#64748b',
                                    transition: 'all 0.15s',
                                }}
                            >
                                {label}
                            </button>
                        ))}
                        <button
                            className="nodrag"
                            onClick={sendFlush}
                            style={{
                                flex: 1,
                                padding: '2px 0',
                                fontSize: '10px',
                                fontFamily: 'monospace',
                                fontWeight: 'normal',
                                cursor: 'pointer',
                                borderRadius: '3px',
                                border: '1px solid #334155',
                                background: '#0f172a',
                                color: '#38bdf8',
                                transition: 'all 0.15s',
                            }}
                        >
                            Flush
                        </button>
                    </div>
                </div>
            )}

            {data.type === 'DropNode' && (
                <div style={{
                    marginTop: '6px',
                    padding: '4px 6px',
                    background: droppedCount !== null && droppedCount > 0 ? '#450a0a' : '#1e293b',
                    border: `1px solid ${droppedCount !== null && droppedCount > 0 ? '#ef4444' : '#475569'}`,
                    borderRadius: '4px',
                    textAlign: 'center',
                    fontSize: '12px',
                    color: droppedCount !== null && droppedCount > 0 ? '#ef4444' : '#64748b',
                    fontWeight: 'bold',
                }}>
                    {droppedCount !== null ? `${droppedCount.toLocaleString()} dropped` : 'dropped: —'}
                </div>
            )}

            {data.type === 'PacketGenerator' && (
                <GeneratorControlButton
                    nodeId={id}
                    pipelineNodeId={(data as any).id}
                    pipelineName={(data as any).pipeline_name}
                />
            )}

            {outgoing.length > 0
                ? outgoing.map((o, i) => (
                    <Handle
                        key={o.id}
                        id={o.id}
                        type="source"
                        position={Position.Right}
                        style={{ background: colors.handle, top: handleSpacing[i] }}
                    />
                ))
                : <Handle type="source" position={Position.Right} style={{ background: colors.handle }} />
            }
        </div>
    );
}
