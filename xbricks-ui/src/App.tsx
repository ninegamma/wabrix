import { useEffect, useState } from 'react';
import { ReactFlow, Background, Controls, useNodesState, useEdgesState } from '@xyflow/react';
import type { Node, Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import axios from 'axios';
import dagre from 'dagre';
import { IfaceNode, UmemNode, XskNode, WorkerNode, PipelineNode } from './components/CustomNodes.tsx';
import { TerminalWindow } from './TerminalWindow';
import { TelemetryNode } from './TelemetryNode';

const nodeTypes = {
    iface: IfaceNode,
    telemetry: TelemetryNode,
    umem: UmemNode,
    xsk: XskNode,
    worker: WorkerNode,
    pipeline: PipelineNode,
};

const NODE_W = 200;
const NODE_H = 100;
const TOP_GAP = 300;    // vertical gap between top row and pipeline nodes
const TOP_SPACING = 40; // horizontal gap between top-row nodes

const ROW0_TYPES = new Set(['iface', 'umem']); // top row
const ROW1_TYPES = new Set(['xsk']);              // second row
const ROW2_TYPES = new Set(['worker']);           // third row

// Fields that should render as radio buttons instead of a text input
const ENUM_OPTIONS: Record<string, string[]> = {
    xdp_mode: ['auto', 'native', 'skb'],
};

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'LR') => {
    const row0 = nodes.filter(n => ROW0_TYPES.has(n.type ?? ''));
    const row1 = nodes.filter(n => ROW1_TYPES.has(n.type ?? ''));
    const row2 = nodes.filter(n => ROW2_TYPES.has(n.type ?? ''));
    const pipelineNodes = nodes.filter(n =>
        !ROW0_TYPES.has(n.type ?? '') && !ROW1_TYPES.has(n.type ?? '') && !ROW2_TYPES.has(n.type ?? ''));

    // Run Dagre only on pipeline nodes
    const g = new dagre.graphlib.Graph();
    g.setDefaultEdgeLabel(() => ({}));
    g.setGraph({ rankdir: direction });

    pipelineNodes.forEach(node => g.setNode(node.id, { width: NODE_W, height: NODE_H }));
    edges.forEach(edge => {
        if (g.hasNode(edge.source) && g.hasNode(edge.target)) {
            g.setEdge(edge.source, edge.target);
        }
    });

    dagre.layout(g);

    const ROW_GAP = 40;
    const row1Y = row1.length > 0 ? NODE_H + ROW_GAP : 0;
    const row2Y = row2.length > 0 ? row1Y + (row1.length > 0 ? NODE_H + ROW_GAP : 0) : row1Y;
    const pipelineOffsetY = row2Y + (row2.length > 0 ? NODE_H + TOP_GAP : row1.length > 0 ? NODE_H + TOP_GAP : row0.length > 0 ? NODE_H + TOP_GAP : 0);

    pipelineNodes.forEach(node => {
        const pos = g.node(node.id);
        node.position = {
            x: pos.x - NODE_W / 2,
            y: pos.y - NODE_H / 2 + pipelineOffsetY,
        };
    });

    row0.forEach((node, i) => {
        node.position = { x: i * (NODE_W + TOP_SPACING), y: 0 };
    });
    row1.forEach((node, i) => {
        node.position = { x: i * (NODE_W + TOP_SPACING), y: row1Y };
    });
    row2.forEach((node, i) => {
        node.position = { x: i * (NODE_W + TOP_SPACING), y: row2Y };
    });

    return { nodes, edges };
};


// Styles for the main buttons in the top bar
const topMenuBtnStyle: React.CSSProperties = {
    background: 'transparent',
    color: '#cbd5e1',
    border: 'none',
    padding: '6px 12px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontSize: '14px',
    fontWeight: '500',
    transition: 'background 0.2s',
};

// Style for when a menu is currently clicked/open
const topMenuBtnActiveStyle: React.CSSProperties = {
    ...topMenuBtnStyle,
    background: '#1e293b',
    color: '#ffffff',
};

// The dropdown container itself
const dropdownMenuStyle: React.CSSProperties = {
    position: 'absolute',
    top: '100%',
    left: 0,
    marginTop: '4px',
    background: '#1e293b',
    border: '1px solid #333',
    borderRadius: '6px',
    minWidth: '200px',
    display: 'flex',
    flexDirection: 'column',
    padding: '4px 0',
    boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.5)',
    zIndex: 1001, // Stays above the click-away overlay
};

// Individual items inside the dropdown
const dropdownItemStyle: React.CSSProperties = {
    background: 'transparent',
    color: '#e2e8f0',
    border: 'none',
    padding: '10px 16px',
    textAlign: 'left',
    cursor: 'pointer',
    fontSize: '14px',
    display: 'flex',
    gap: '10px',
    alignItems: 'center',
    width: '100%'
};

// A subtle horizontal line to separate menu items
const menuSeparatorStyle: React.CSSProperties = {
    height: '1px',
    background: '#333',
    margin: '4px 0'
};

export default function App() {
    const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
    const [error, setError] = useState<string | null>(null);
    const [configPath, setConfigPath] = useState<string>('xbricks.toml');
    const [showFileBrowser, setShowFileBrowser] = useState(false);
    const [pendingRestart, setPendingRestart] = useState(true);
    // Tracks which menu is currently open ('File', 'System', or null)
    const [activeMenu, setActiveMenu] = useState<string | null>(null);

    const doHardwareSave = async (savePath: string) => {
        setShowFileBrowser(false);
        try {
            const configResponse = await axios.get('/api/config');
            const fullConfig = configResponse.data;

            fullConfig.xbricks.ifaces = nodes
                .filter(n => n.type === 'iface')
                .map(n => n.data);
            fullConfig.xbricks.umem = nodes
                .filter(n => n.type === 'umem')
                .map(n => n.data);
            fullConfig.xbricks.xsk = nodes
                .filter(n => n.type === 'xsk')
                .sort((a, b) => parseInt(a.id.split('_')[1]) - parseInt(b.id.split('_')[1]))
                .map(n => n.data);
            fullConfig.xbricks.workers = nodes
                .filter(n => n.type === 'worker')
                .sort((a, b) => (a.data.id as number) - (b.data.id as number))
                .map(n => n.data);

            const params: Record<string, string | boolean> = { save_path: savePath };
            if (!pendingRestart) params.restart = false;

            await axios.post('/api/hardware/update', fullConfig, { params });
            setConfigPath(savePath);
            alert(pendingRestart
                ? "Configuration saved! Data plane is restarting..."
                : "Configuration saved to disk.");
        } catch (err) {
            console.error(err);
            alert("Failed to save configuration.");
        }
    };

    // Opens the file browser; restart=true for sidebar button, false for File→Save to Disk
    const handleHardwareSave = (restart = true) => {
        setPendingRestart(restart);
        setShowFileBrowser(true);
    };

    // Pushes the current UI node state into the backend's in-memory config
    // without writing to disk or restarting the dataplane.
    const syncConfigToBackend = async () => {
        const configResponse = await axios.get('/api/config');
        const fullConfig = configResponse.data;
        fullConfig.xbricks.ifaces = nodes.filter(n => n.type === 'iface').map(n => n.data);
        fullConfig.xbricks.umem   = nodes.filter(n => n.type === 'umem').map(n => n.data);
        fullConfig.xbricks.xsk    = nodes
            .filter(n => n.type === 'xsk')
            .sort((a, b) => parseInt(a.id.split('_')[1]) - parseInt(b.id.split('_')[1]))
            .map(n => n.data);
        fullConfig.xbricks.workers = nodes
            .filter(n => n.type === 'worker')
            .sort((a, b) => (a.data.id as number) - (b.data.id as number))
            .map(n => n.data);
        await axios.post('/api/hardware/update', fullConfig, {
            params: { restart: false, save_to_disk: false },
        });
    };

    const sendDataplaneCommand = async (command: 'start' | 'stop' | 'restart' | 'check' | 'shutdown') => {
        setActiveMenu(null);
        try {
            if (command === 'check') {
                await syncConfigToBackend();
            }
            await axios.post('/api/dataplane/command', { command });
        } catch (err) {
            console.error(err);
            alert(`Failed to send ${command} command.`);
        }
    };

    const handleLiveUpdate = async () => {
        if (!selectedNode) return;

        // 1. Ensure we know which pipeline this node belongs to.
        // NOTE: Make sure your `/api/topology` endpoint includes `pipeline_name`
        // inside the `data` object for each pipeline node!
        const pipelineName = selectedNode.data.pipeline_name as string;

        if (!pipelineName) {
            alert("Cannot hot-swap: Pipeline name missing from node data.");
            return;
        }

        // 2. Format the routes.
        // If the user typed "route_A, route_B" into the text input, we split it into an array.
        let parsedRoutes: string[] | undefined = undefined;
        if (typeof selectedNode.data.routes === 'string') {
            parsedRoutes = selectedNode.data.routes
                .split(',')
                .map((s: string) => s.trim())
                .filter((s: string) => s.length > 0);
        } else if (Array.isArray(selectedNode.data.routes)) {
            parsedRoutes = selectedNode.data.routes;
        }

        // 3. Construct the payload matching our Rust `NodeUpdatePayload`
        const payload = {
            pipeline_name: pipelineName,
            node_id: selectedNode.id,
            routes: parsedRoutes,
            // UI might store it as 'default' or 'default_route' depending on your topology JSON
            default_route: (selectedNode.data.default_route || selectedNode.data.default) as string | undefined,
        };

        try {
            await axios.post('/api/pipeline/update', payload); // Adjust endpoint if needed
            alert("⚡ Live update applied instantly with zero dropped packets!");
        } catch (err: any) {
            console.error(err);
            alert(`Failed to apply live update: ${err.response?.data || err.message}`);
        }
    };


    useEffect(() => {
        axios.get('/api/topology')
            .then((response) => {
                const knownTypes = new Set(['iface', 'umem', 'xsk', 'worker']);

                const hasSavedLayout = !!response.data?.has_saved_layout;
                if (response.data?.config_path) {
                    setConfigPath(response.data.config_path);
                }

                const initialNodes: any[] = [];

                (response.data?.nodes || []).forEach((n: any) => {
                    const isPipeline = !knownTypes.has(n.type);

                    // 1. Push the standard node (Iface, Umem, Worker, etc.)
                    initialNodes.push({
                        id: n.id,
                        type: isPipeline ? 'pipeline' : n.type,
                        position: hasSavedLayout ? (n.position ?? { x: 0, y: 0 }) : { x: 0, y: 0 },
                        data: isPipeline ? { ...n.data, type: n.type } : n.data,
                    });

                    // 2. If the node we just pushed was an Interface, inject a companion Telemetry node!
                    if (n.type === 'iface') {
                        initialNodes.push({
                            id: `telemetry_${n.data.name}`,
                            type: 'telemetry',
                            // If we have a saved layout, put it exactly where the user saved it.
                            // If not, offset it slightly below the Interface node so they don't overlap.
                            position: hasSavedLayout
                                ? (response.data.layout?.[`telemetry_${n.data.name}`] ?? { x: n.position?.x || 0, y: (n.position?.y || 0) + 200 })
                                : { x: 0, y: 0 },
                            data: n.data, // Pass the exact same interface config data so it knows what to monitor
                        });
                    }
                });

                // Build edges from backend + derive from pipeline node routes
                const edgeStyle = { stroke: '#61da5c', strokeWidth: 3 };

                const backendEdges = (response.data?.edges || []).map((e: any) => ({
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    animated: true,
                    style: edgeStyle,
                }));

                // XSK → iface and XSK → umem edges
                const xskEdgeStyle = { stroke: '#38bdf8', strokeWidth: 4 };
                const xskEdges: Edge[] = [];
                initialNodes
                    .filter((n: any) => n.type === 'xsk')
                    .forEach((n: any) => {
                        const ifaceTarget = `iface_${n.data.iface}`;
                        const umemTarget = `umem_${n.data.umem}`;
                        const nodeIds = new Set(initialNodes.map((x: any) => x.id));
                        if (nodeIds.has(ifaceTarget)) {
                            xskEdges.push({ id: `e-${n.id}-iface`, source: ifaceTarget, sourceHandle: 'bottom', target: n.id, targetHandle: 'top', animated: false, style: xskEdgeStyle });
                        }
                        if (nodeIds.has(umemTarget)) {
                            xskEdges.push({ id: `e-${n.id}-umem`, source: umemTarget, sourceHandle: 'bottom', target: n.id, targetHandle: 'top', animated: false, style: xskEdgeStyle });
                        }
                    });

                // Worker → UMEM partition edges (with count label)
                const workerEdgeStyle = { stroke: '#f59e0b', strokeWidth: 3 };
                const workerEdges: Edge[] = [];
                const nodeIds2 = new Set(initialNodes.map((n: any) => n.id));
                initialNodes
                    .filter((n: any) => n.type === 'worker')
                    .forEach((n: any) => {
                        (n.data.partitions || []).forEach((p: any) => {
                            const umemTarget = `umem_${p.umem}`;
                            if (nodeIds2.has(umemTarget)) {
                                workerEdges.push({
                                    id: `e-${n.id}-umem${p.umem}`,
                                    source: n.id,
                                    sourceHandle: 'top',
                                    target: umemTarget,
                                    targetHandle: 'bottom-in',
                                    animated: false,
                                    label: String(p.count),
                                    labelStyle: { fill: '#fcd34d', fontFamily: 'monospace', fontSize: 11, fontWeight: 'bold' },
                                    labelBgStyle: { fill: '#1c1409', fillOpacity: 0.85 },
                                    style: workerEdgeStyle,
                                });
                            }
                        });
                    });

                const routeEdges: Edge[] = [];
                const nodeIds = new Set(initialNodes.map((n: any) => n.id));
                initialNodes
                    .filter((n: any) => n.type === 'pipeline')
                    .forEach((n: any) => {
                        const routes: string[] = Array.isArray(n.data.routes) ? n.data.routes : [];
                        routes.forEach((target: string) => {
                            if (nodeIds.has(target)) {
                                routeEdges.push({ id: `e-${n.id}-${target}`, source: n.id, sourceHandle: target, target, animated: true, style: edgeStyle });
                            }
                        });
                        const def: string | null = n.data.default ?? null;
                        if (def && nodeIds.has(def) && !routes.includes(def)) {
                            routeEdges.push({ id: `e-${n.id}-default-${def}`, source: n.id, sourceHandle: def, target: def, animated: true, style: { ...edgeStyle, strokeDasharray: '5,5' } });
                        }
                        const next: string | null = n.data.next ?? null;
                        if (next && nodeIds.has(next) && !routes.includes(next) && next !== def) {
                            routeEdges.push({ id: `e-${n.id}-next-${next}`, source: n.id, sourceHandle: next, target: next, animated: true, style: edgeStyle });
                        }
                    });

                const allEdges = [...backendEdges, ...xskEdges, ...workerEdges, ...routeEdges];

                if (hasSavedLayout) {
                    setNodes(initialNodes);
                    setEdges(allEdges);
                } else {
                    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(initialNodes, allEdges, 'LR');
                    setNodes(layoutedNodes);
                    setEdges(layoutedEdges);
                }
            })
            .catch((err) => setError(err.message));
    }, [setNodes, setEdges]);


    // Save all node positions to /api/layout after any drag
    const handleNodeDragStop = () => {
        setNodes(currentNodes => {
            const positions: Record<string, { x: number; y: number }> = {};
            currentNodes.forEach(n => { positions[n.id] = n.position; });
            axios.post('/api/layout', positions).catch(err => console.error('Failed to save layout:', err));
            return currentNodes;
        });
    };

    const updateNodeData = (nodeId: string, key: string, value: string | number | number[] | boolean) => {
        setNodes((nds) =>
            nds.map((node) => {
                if (node.id === nodeId) {
                    return { ...node, data: { ...node.data, [key]: value } };
                }
                return node;
            })
        );
    };

    if (error) return <div style={{ color: 'red', padding: '20px' }}>Failed to load graph: {error}</div>;


    // --- NEW: Find the currently selected node ---
    const selectedNode = nodes.find((n) => n.selected);

    return (
        <>
        {/* MAIN WRAPPER: Top to Bottom Column */}
        <div style={{
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden'
        }}>
            {/* ========================================== */}
            {/* 1. TOP MENU BAR (Dropdown Navigation)        */}
            {/* ========================================== */}
            <div style={{
                height: '50px',
                background: '#0f172a',
                borderBottom: '1px solid #333',
                padding: '0 20px',
                display: 'flex',
                alignItems: 'center',
                flexShrink: 0,
                position: 'relative', // CRITICAL: So dropdowns anchor to this bar
                zIndex: 1000 // CRITICAL: Keeps menus on top of the graph
            }}>
                <h3 style={{ color: '#60a5fa', margin: 0, marginRight: '25px', fontSize: '18px' }}>
                    🧱 XBricks DataPlane
                </h3>

                {/* Menu Buttons Area */}
                <div style={{ display: 'flex', gap: '5px' }}>

                    {/* --- FILE MENU --- */}
                    <div style={{ position: 'relative' }}>
                        <button
                            onClick={() => setActiveMenu(activeMenu === 'File' ? null : 'File')}
                            style={activeMenu === 'File' ? topMenuBtnActiveStyle : topMenuBtnStyle}
                        >
                            File
                        </button>

                        {activeMenu === 'File' && (
                            <div style={dropdownMenuStyle}>
                                <button onClick={() => { setActiveMenu(null); alert("Load Action"); }} style={dropdownItemStyle}>📂 Load Config</button>
                                <button onClick={() => { setActiveMenu(null); handleHardwareSave(false); }} style={dropdownItemStyle}>💾 Save to Disk</button>
                                <div style={menuSeparatorStyle} />
                                <button onClick={() => sendDataplaneCommand('check')} style={dropdownItemStyle}>✅ Check Config</button>
                            </div>
                        )}
                    </div>

                    {/* --- DATAPLANE MENU --- */}
                    <div style={{ position: 'relative' }}>
                        <button
                            onClick={() => setActiveMenu(activeMenu === 'Dataplane' ? null : 'Dataplane')}
                            style={activeMenu === 'Dataplane' ? topMenuBtnActiveStyle : topMenuBtnStyle}
                        >
                            Dataplane
                        </button>

                        {activeMenu === 'Dataplane' && (
                            <div style={dropdownMenuStyle}>
                                <button onClick={() => sendDataplaneCommand('start')}    style={dropdownItemStyle}>▶ Start</button>
                                <button onClick={() => sendDataplaneCommand('restart')}  style={dropdownItemStyle}>🔄 Restart</button>
                                <button onClick={() => sendDataplaneCommand('stop')}     style={dropdownItemStyle}>⏹ Stop</button>
                                <div style={menuSeparatorStyle} />
                                <button onClick={() => sendDataplaneCommand('shutdown')} style={{ ...dropdownItemStyle, color: '#ef4444' }}>⏻ Shutdown</button>
                            </div>
                        )}
                    </div>

                    {/* --- SYSTEM MENU --- */}
                    <div style={{ position: 'relative' }}>
                        <button
                            onClick={() => setActiveMenu(activeMenu === 'System' ? null : 'System')}
                            style={activeMenu === 'System' ? topMenuBtnActiveStyle : topMenuBtnStyle}
                        >
                            System
                        </button>

                        {activeMenu === 'System' && (
                            <div style={dropdownMenuStyle}>
                                <button onClick={() => { setActiveMenu(null); alert("Status"); }} style={dropdownItemStyle}>📊 View Status</button>
                                <button onClick={() => { setActiveMenu(null); alert("Settings"); }} style={dropdownItemStyle}>⚙️ Settings</button>
                            </div>
                        )}
                    </div>

                </div>

                {/* INVISIBLE OVERLAY: Closes menus if you click anywhere else on the screen */}
                {activeMenu && (
                    <div
                        onClick={() => setActiveMenu(null)}
                        style={{ position: 'fixed', top: '50px', left: 0, right: 0, bottom: 0, zIndex: 999 }}
                    />
                )}
            </div>
            {/* MIDDLE: The Graph and Sidebar area */}
            <div style={{
                flex: 1,
                minHeight: 0, // CRITICAL: Allows flex child to shrink below content size
                display: 'flex',
                flexDirection: 'row',
                overflow: 'hidden'
            }}>
                {/* The Graph */}
                <div style={{ flexGrow: 1, position: 'relative' }}>
                    <ReactFlow
                        nodes={nodes}
                        edges={edges}
                        nodeTypes={nodeTypes}
                        onNodesChange={onNodesChange}
                        onEdgesChange={onEdgesChange}
                        onNodeDragStop={handleNodeDragStop}
                        fitView
                        colorMode="dark"
                    >
                        <Background gap={16} size={1} />
                        <Controls />
                    </ReactFlow>
                </div>

                {/* The Sidebar (Only shows if a node is selected) */}
                {selectedNode && (
                    <div style={{
                        width: '350px',
                        background: '#1e1e1e',
                        borderLeft: '1px solid #333',
                        padding: '20px',
                        color: 'white',
                        overflowY: 'auto' // Local scrollbar only
                    }}>
                        <h2 style={{ borderBottom: '1px solid #444', paddingBottom: '10px' }}>
                            Edit {selectedNode.type === 'iface' ? 'Interface' : selectedNode.type === 'umem' ? 'UMEM' : (selectedNode.data as any).type || selectedNode.id}
                        </h2>
                        <div style={{ marginTop: '20px', display: 'flex', flexDirection: 'column', gap: '15px' }}>
                            {Object.entries(selectedNode.data)
                                .filter(([_, value]) => {
                                    if (value === null || value === undefined) return false;
                                    // Skip array-of-objects (e.g. partitions) — not safely inline-editable
                                    if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') return false;
                                    // Skip plain objects
                                    if (!Array.isArray(value) && typeof value === 'object') return false;
                                    return true;
                                })
                                .map(([key, value]) => {
                                    const isNumArr = Array.isArray(value) && (value.length === 0 || typeof value[0] === 'number');
                                    const isNumber = typeof value === 'number';
                                    const displayValue = isNumArr
                                        ? (value as number[]).join(', ')
                                        : (value as string | number);

                                    // Boolean fields: Yes / No radio buttons
                                    if (typeof value === 'boolean') {
                                        return (
                                            <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                                                <label style={{ fontSize: '12px', color: '#aaa', textTransform: 'uppercase' }}>{key}</label>
                                                <div style={{ display: 'flex', gap: '20px' }}>
                                                    {[true, false].map(opt => (
                                                        <label key={String(opt)} style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '13px', color: '#e2e8f0' }}>
                                                            <input
                                                                type="radio"
                                                                name={`${selectedNode.id}-${key}`}
                                                                checked={value === opt}
                                                                onChange={() => updateNodeData(selectedNode.id, key, opt)}
                                                            />
                                                            {opt ? 'Yes' : 'No'}
                                                        </label>
                                                    ))}
                                                </div>
                                            </div>
                                        );
                                    }

                                    // Enum fields: inline radio buttons
                                    const enumOpts = ENUM_OPTIONS[key];
                                    if (enumOpts) {
                                        return (
                                            <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                                                <label style={{ fontSize: '12px', color: '#aaa', textTransform: 'uppercase' }}>{key}</label>
                                                <div style={{ display: 'flex', gap: '14px', flexWrap: 'wrap' }}>
                                                    {enumOpts.map(opt => (
                                                        <label key={opt} style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '13px', color: '#e2e8f0' }}>
                                                            <input
                                                                type="radio"
                                                                name={`${selectedNode.id}-${key}`}
                                                                checked={value === opt}
                                                                onChange={() => updateNodeData(selectedNode.id, key, opt)}
                                                            />
                                                            {opt}
                                                        </label>
                                                    ))}
                                                </div>
                                            </div>
                                        );
                                    }

                                    // Array-of-number fields: uncontrolled input, commit on blur.
                                    // Using defaultValue + key lets the user type freely (commas,
                                    // spaces) without the input snapping back on every keystroke.
                                    if (isNumArr) {
                                        return (
                                            <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                                                <label style={{ fontSize: '12px', color: '#aaa', textTransform: 'uppercase' }}>{key}</label>
                                                <input
                                                    key={`${selectedNode.id}-${key}-${JSON.stringify(value)}`}
                                                    type="text"
                                                    defaultValue={displayValue}
                                                    onBlur={(e) => {
                                                        const parsed = e.target.value
                                                            .split(/[\s,]+/)
                                                            .map(s => parseInt(s.trim(), 10))
                                                            .filter(n => !isNaN(n));
                                                        updateNodeData(selectedNode.id, key, parsed);
                                                    }}
                                                    style={{ padding: '8px', background: '#2a2a2a', border: '1px solid #555', borderRadius: '4px', color: 'white', fontFamily: 'monospace' }}
                                                />
                                            </div>
                                        );
                                    }

                                    return (
                                        <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                                            <label style={{ fontSize: '12px', color: '#aaa', textTransform: 'uppercase' }}>{key}</label>
                                            <input
                                                type={isNumber ? 'number' : 'text'}
                                                value={displayValue}
                                                onChange={(e) => updateNodeData(selectedNode.id, key,
                                                    isNumber ? Number(e.target.value) : e.target.value)}
                                                style={{ padding: '8px', background: '#2a2a2a', border: '1px solid #555', borderRadius: '4px', color: 'white', fontFamily: 'monospace' }}
                                            />
                                        </div>
                                    );
                                })}
                        </div>
                        {!['iface', 'umem', 'xsk', 'worker'].includes(selectedNode.type ?? '') && (
                            <button
                                onClick={handleLiveUpdate}
                                style={{
                                    marginTop: '30px', width: '100%', padding: '12px',
                                    background: '#10b981',
                                    color: 'white', border: 'none', borderRadius: '4px',
                                    cursor: 'pointer', fontWeight: 'bold'
                                }}
                            >
                                ⚡ APPLY LIVE UPDATE
                            </button>
                        )}
                    </div>
                )}
            </div>

            {/* BOTTOM: The Terminal area */}
            <div style={{
                height: '250px',
                width: '100%',
                background: '#1e1e1e',
                borderTop: '2px solid #3b82f6',
                flexShrink: 0,
                overflow: 'hidden' // Keeps the terminal's internal scroller contained
            }}>
                <TerminalWindow />
            </div>

        </div>

        {/* FILE BROWSER DIALOG — rendered as portal-style overlay */}
        {showFileBrowser && (
            <FileBrowserDialog
                initialPath={configPath}
                restartOnSave={pendingRestart}
                onConfirm={doHardwareSave}
                onCancel={() => setShowFileBrowser(false)}
            />
        )}
        </>
    );
}