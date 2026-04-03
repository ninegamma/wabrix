import { useEffect, useState } from 'react';
import { ReactFlow, Background, Controls, useNodesState, useEdgesState } from '@xyflow/react';
import type { Node, Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import axios from 'axios';
import dagre from 'dagre';
import { IfaceNode, UmemNode } from './CustomNodes';

// --- THE AUTO-LAYOUT ENGINE ---
const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

// You can change 'LR' (Left-to-Right) to 'TB' (Top-to-Bottom)
const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'LR') => {
    dagreGraph.setGraph({ rankdir: direction });

    // 1. Feed the nodes to Dagre (we tell it roughly how big our boxes are)
    nodes.forEach((node) => {
        dagreGraph.setNode(node.id, { width: 150, height: 50 });
    });

    // 2. Feed the connections to Dagre
    edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target);
    });

    // 3. Let Dagre calculate the perfect X/Y coordinates!
    dagre.layout(dagreGraph);

    // 4. Apply the calculated coordinates back to our React Flow nodes
    nodes.forEach((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        node.position = {
            x: nodeWithPosition.x - 150 / 2, // Shift by half width to center it
            y: nodeWithPosition.y - 50 / 2,  // Shift by half height to center it
        };
    });

    return { nodes, edges };
};
// Map the Rust string "type" to the React component
const nodeTypes = {
    iface: IfaceNode,
    umem: UmemNode,
};

    // ... rest of your App.tsx code
export default function App() {
    // 1. Replace standard useState with React Flow's interactive state hooks
    const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        axios.get('/api/topology')
            .then((response) => {
                const initialNodes = (response.data?.nodes || []).map((n: any) => ({
                    id: n.id,
                    type: n.type,         // 1. Tell React Flow if it's an "iface" or "umem"
                    position: { x: 0, y: 0 }, // Dagre will overwrite this
                    data: n.data,         // 2. Pass the raw TOML data straight to the Custom Node!
                }));

                const initialEdges = (response.data?.edges || []).map((e: any) => ({
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    animated: true,
                }));

                // Run the auto-layout before saving it to state
                const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
                    initialNodes,
                    initialEdges,
                    'LR' // Change to 'TB' if you prefer vertical pipelines!
                );
                setNodes(layoutedNodes);
                setEdges(layoutedEdges);
            })
            .catch((err) => setError(err.message));
    }, []);

    if (error) return <div style={{ color: 'red', padding: '20px' }}>Failed to load graph: {error}</div>;

    return (
        <div style={{ width: '100vw', height: '100vh', background: '#0f0f0f' }}>
            <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes} // <--- Add this prop!
                onNodesChange={onNodesChange} // 2. Tell the graph how to handle dragging!
                onEdgesChange={onEdgesChange} // 3. Tell the graph how to handle edge changes!
                fitView
                colorMode="dark"
            >
                <Background gap={16} size={1} />
                <Controls />
            </ReactFlow>
        </div>
    );
}