import { useEffect, useState } from 'react';
import { ReactFlow, Background, Controls, useNodesState, useEdgesState } from '@xyflow/react';
import type { Node, Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import axios from 'axios';
import dagre from 'dagre';
import { IfaceNode, UmemNode } from './CustomNodes';
import { TerminalWindow } from './TerminalWindow';

const nodeTypes = {
    iface: IfaceNode,
    umem: UmemNode,
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

    const handleSave = async () => {
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

            // Fetch current full config from state (you might want to store the
            // full original config in a useRef or state)
            // For this example, we'll send the updated parts.
            // In a production app, you'd merge this with the 'pipelines' etc.

            // Simplest approach: Send a request to a "Partial Update" endpoint
            // or fetch the full state first.

            // Let's assume we have a way to get the full original config:
            const response = await axios.get('/api/topology');
            const fullConfig = response.data.full_config_structure; // Hypothetical

            // Update the relevant parts
            fullConfig.xbricks.ifaces = updatedIfaces;
            fullConfig.xbricks.umem = updatedUmems;

            await axios.post('/api/save-config', fullConfig);
            alert("Configuration saved to hardware and disk!");
        } catch (err) {
            console.error(err);
            alert("Failed to save configuration.");
        }
    };

    useEffect(() => {
        axios.get('/api/topology')
            .then((response) => {
                const initialNodes = (response.data?.nodes || []).map((n: any) => ({
                    id: n.id,
                    type: n.type,
                    position: { x: 0, y: 0 },
                    data: n.data,
                }));

                const initialEdges = (response.data?.edges || []).map((e: any) => ({
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    animated: true,
                }));

                const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(initialNodes, initialEdges, 'LR');

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
                            Edit {selectedNode.type === 'iface' ? 'Interface' : 'UMEM'}
                        </h2>
                        <div style={{ marginTop: '20px', display: 'flex', flexDirection: 'column', gap: '15px' }}>
                            {Object.entries(selectedNode.data)
                                .filter(([_, value]) => value !== null && value !== undefined)
                                .map(([key, value]) => {
                                    const isNumber = typeof value === 'number';
                                    return (
                                        <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                                            <label style={{ fontSize: '12px', color: '#aaa', textTransform: 'uppercase' }}>{key}</label>
                                            <input
                                                type={isNumber ? 'number' : 'text'}
                                                value={value as string | number}
                                                onChange={(e) => updateNodeData(selectedNode.id, key, isNumber ? Number(e.target.value) : e.target.value)}
                                                style={{ padding: '8px', background: '#2a2a2a', border: '1px solid #555', borderRadius: '4px', color: 'white', fontFamily: 'monospace' }}
                                            />
                                        </div>
                                    );
                                })}
                        </div>
                        <button
                            onClick={handleSave}
                            style={{
                                marginTop: '30px',
                                width: '100%',
                                padding: '12px',
                                background: '#10b981', // Green
                                color: 'white',
                                border: 'none',
                                borderRadius: '4px',
                                cursor: 'pointer',
                                fontWeight: 'bold'
                            }}
                        >
                            💾 SAVE TO DISK
                        </button>
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