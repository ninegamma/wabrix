import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';

// ==========================================
// 1. TYPESCRIPT DEFINITIONS (Matches Rust)
// ==========================================

export type IfaceData = {
    name: string;
    rxd: number | null;
    txd: number | null;
    irq_core: number | null;
    type: string | null;
    namespace?: string | null;
    peer?: string | null;
    mac_if: string | null;
    ipnet: string | null;
};

export type UmemData = {
    id: number;
    frame_size: number;
    frame_count: number;
    fq_size: number;
    cq_size: number;
    head_room: number;
    partitions: number;
    reserved: number;
};

export type IfaceNodeType = Node<IfaceData, 'iface'>;
export type UmemNodeType = Node<UmemData, 'umem'>;

// ==========================================
// 2. THE INTERFACE NODE COMPONENT
// ==========================================
// ==========================================
// 2. THE DYNAMIC INTERFACE NODE
// ==========================================

export function IfaceNode({ data }: NodeProps<IfaceNodeType>) {
    return (
        <div style={{
            background: '#1e293b', border: '2px solid #3b82f6', borderRadius: '8px',
            padding: '12px', minWidth: '180px', color: '#f8fafc', fontFamily: 'monospace',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
        }}>
            <div style={{ borderBottom: '1px solid #475569', paddingBottom: '8px', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: '#60a5fa' }}>🌐 IFACE: {data.name}</strong>
            </div>

            <div style={{ fontSize: '11px', lineHeight: '1.6', color: '#cbd5e1' }}>
                {Object.entries(data)
                    .filter(([_, value]) => value !== null && value !== undefined) // <-- This hides the nulls!
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
}

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
                    .filter(([_, value]) => value !== null && value !== undefined) // <-- This hides the nulls!
                    .map(([key, value]) => (
                        <div key={key}>
                            <strong style={{ color: '#94a3b8' }}>{key}:</strong> {String(value)}
                        </div>
                    ))}
            </div>

            <Handle type="target" position={Position.Left} style={{ background: '#a855f7' }} />
            <Handle type="source" position={Position.Right} style={{ background: '#a855f7' }} />
        </div>
    );
}