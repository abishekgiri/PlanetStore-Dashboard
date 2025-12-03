import React from 'react';
import { Server, Activity, Database } from 'lucide-react';
import type { NodeStat } from '../api';
import { clsx } from 'clsx';

interface NodeListProps {
    nodes: NodeStat[];
}

export const NodeList: React.FC<NodeListProps> = ({ nodes }) => {
    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
            {nodes.map((node) => (
                <div key={node.node_id} className="bg-white p-4 rounded-lg shadow-md border border-gray-200">
                    <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center space-x-2">
                            <Server className="w-5 h-5 text-blue-500" />
                            <h3 className="font-semibold text-lg">{node.node_id}</h3>
                        </div>
                        <div className={clsx(
                            "w-3 h-3 rounded-full",
                            node.latency_ms < 100 ? "bg-green-500" : "bg-yellow-500"
                        )} />
                    </div>
                    <div className="space-y-1 text-sm text-gray-600">
                        <div className="flex items-center justify-between">
                            <span className="flex items-center gap-1"><Activity className="w-4 h-4" /> Latency</span>
                            <span>{node.latency_ms} ms</span>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="flex items-center gap-1"><Database className="w-4 h-4" /> Objects</span>
                            <span>{node.object_count}</span>
                        </div>
                        <div className="text-xs text-gray-400 mt-2 truncate">
                            {node.base_url}
                        </div>
                    </div>
                </div>
            ))}
        </div>
    );
};
