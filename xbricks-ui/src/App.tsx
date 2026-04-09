import { useEffect, useState } from 'react';
import { ReactFlow, Background, Controls, useNodesState, useEdgesState } from '@xyflow/react';
import type { Node, Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import axios from 'axios';
import dagre from 'dagre';
import { IfaceNode, UmemNode, PipelineNode } from './components/CustomNodes.tsx';
import { TerminalWindow } from './TerminalWindow';

const nodeTypes = {
    iface: IfaceNode,
    umem: UmemNode,
    pipeline: PipelineNode,
};

const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'LR') => {
    dagreGraph.setGraph({ rankdir: direction });

    nodes.forEach((node) => {
        dagreGraph.setNode(node.id, { width: 200, height: 100 });
    });

    edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target);
    });

    dagre.layout(dagreGraph);

    nodes.forEach((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        node.position = {
            x: nodeWithPosition.x - 200 / 2,
            y: nodeWithPosition.y - 100 / 2,
        };
    });

    return { nodes, edges };
};

export default function App() {
    const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
    const [error, setError] = useState<string | null>(null);

    const handleHardwareSave = async () => {
        try {
            // We need to reconstruct the full config object.
            // For now, let's assume we are just updating the ifaces and umems
            // inside the existing config structure.

            const updatedIfaces = nodes
                .filter(n => n.type === 'iface')
                .map(n => n.data);

            const updatedUmems = nodes
                .filter(n => n.type === 'umem')
                .map(n => n.data);

            // Fetch the raw configuration object from the backend
            const configResponse = await axios.get('/api/config');
            const fullConfig = configResponse.data;

            // Update the relevant parts
            fullConfig.xbricks.ifaces = updatedIfaces;
            fullConfig.xbricks.umem = updatedUmems;

            await axios.post('/api/hardware/update', fullConfig);
            alert("Configuration saved! Data plane is restarting...");
        } catch (err) {
            console.error(err);
            alert("Failed to save configuration.");
        }
    };

    // Inside your App component

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
                const knownTypes = new Set(['iface', 'umem']);

                const initialNodes = (response.data?.nodes || []).map((n: any) => {
                    const isPipeline = !knownTypes.has(n.type);
                    return {
                        id: n.id,
                        type: isPipeline ? 'pipeline' : n.type,
                        position: { x: 0, y: 0 },
                        data: isPipeline ? { ...n.data, type: n.type } : n.data,
                    };
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

                const allEdges = [...backendEdges, ...routeEdges];
                const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(initialNodes, allEdges, 'LR');

                setNodes(layoutedNodes);
                setEdges(layoutedEdges);
            })
            .catch((err) => setError(err.message));
    }, [setNodes, setEdges]);

    // --- NEW: Function to update node data ---
    const updateNodeData = (nodeId: string, key: string, value: string | number) => {
        setNodes((nds) =>
            nds.map((node) => {
                if (node.id === nodeId) {
                    // Create a completely new object for React to detect the change
                    return {
                        ...node,
                        data: {
                            ...node.data,
                            [key]: value,
                        },
                    };
                }
                return node;
            })
        );
    };

    if (error) return <div style={{ color: 'red', padding: '20px' }}>Failed to load graph: {error}</div>;

    // --- NEW: Find the currently selected node ---
    const selectedNode = nodes.find((n) => n.selected);

    return (
        // MAIN WRAPPER: Top to Bottom Column
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
            {/* TOP: The Graph and Sidebar area */}
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
                                .filter(([_, value]) => value !== null && value !== undefined)
                                .map(([key, value]) => {
                                    const isNumber = typeof value === 'number';
                                    // Format arrays to strings for easy text-box editing
                                    const displayValue = Array.isArray(value) ? value.join(', ') : (value as string | number);

                                    return (
                                        <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                                            <label style={{ fontSize: '12px', color: '#aaa', textTransform: 'uppercase' }}>{key}</label>
                                            <input
                                                type={isNumber ? 'number' : 'text'}
                                                value={displayValue}
                                                onChange={(e) => updateNodeData(
                                                    selectedNode.id,
                                                    key,
                                                    isNumber ? Number(e.target.value) : e.target.value
                                                )}
                                                style={{ padding: '8px', background: '#2a2a2a', border: '1px solid #555', borderRadius: '4px', color: 'white', fontFamily: 'monospace' }}
                                            />
                                        </div>
                                    );
                                })}
                        </div>
                        {['iface', 'umem'].includes(selectedNode.type ?? '') ? (
                            <button
                                onClick={handleHardwareSave} // Your original full-save function
                                style={{
                                    marginTop: '30px', width: '100%', padding: '12px',
                                    background: '#ef4444', // Red for dangerous/restarting actions
                                    color: 'white', border: 'none', borderRadius: '4px',
                                    cursor: 'pointer', fontWeight: 'bold'
                                }}
                            >
                                🔄 SAVE & RESTART DATAPLANE
                            </button>
                        ) : (
                            <button
                                onClick={handleLiveUpdate}
                                style={{
                                    marginTop: '30px', width: '100%', padding: '12px',
                                    background: '#10b981', // Green for fast/safe actions
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
    );
}