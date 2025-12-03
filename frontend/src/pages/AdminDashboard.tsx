import { useEffect, useState } from 'react';
import { BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Activity, HardDrive, Database, TrendingUp } from 'lucide-react';
import { api } from '../api';

interface ClusterMetrics {
    cluster: {
        total_objects: number;
        total_size_bytes: number;
        total_versions: number;
        unique_content: number;
        dedup_savings_percent: number;
        node_count: number;
    };
    buckets: Array<{ name: string; objects: number; size_bytes: number }>;
    nodes: Array<{ node_id: string; shard_count: number; status: string }>;
}

const COLORS = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#6366f1'];

export function AdminDashboard() {
    const [metrics, setMetrics] = useState<ClusterMetrics | null>(null);
    const [loading, setLoading] = useState(true);
    const [events, setEvents] = useState<any[]>([]);

    useEffect(() => {
        fetchMetrics();
        const interval = setInterval(fetchMetrics, 5000); // Refresh every 5s

        // WebSocket Connection
        const ws = new WebSocket('ws://localhost:8000/ws/events');

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            setEvents(prev => [data, ...prev].slice(0, 10)); // Keep last 10 events
            // Also refresh metrics immediately on event
            fetchMetrics();
        };

        return () => {
            clearInterval(interval);
            ws.close();
        };
    }, []);

    const fetchMetrics = async () => {
        try {
            const response = await api.get<ClusterMetrics>('/admin/metrics');
            setMetrics(response.data);
            setLoading(false);
        } catch (error) {
            console.error('Failed to fetch metrics', error);
        }
    };

    if (loading || !metrics) {
        return (
            <div className="flex items-center justify-center h-screen bg-gray-100">
                <div className="text-gray-500">Loading metrics...</div>
            </div>
        );
    }

    const formatBytes = (bytes: number) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
    };

    return (
        <div className="min-h-screen bg-gray-100 py-8 px-4">
            <div className="max-w-7xl mx-auto">
                {/* Header */}
                <div className="mb-8 flex justify-between items-center">
                    <div>
                        <h1 className="text-3xl font-bold text-gray-900 flex items-center gap-3">
                            <Activity className="w-8 h-8 text-blue-600" />
                            PlanetStore Admin Dashboard
                        </h1>
                        <p className="text-gray-500 mt-2">Real-time cluster monitoring</p>
                    </div>
                    <div className="flex items-center gap-2">
                        <span className="relative flex h-3 w-3">
                            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                            <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
                        </span>
                        <span className="text-sm font-medium text-green-600">Live Connected</span>
                    </div>
                </div>

                {/* Live Activity Feed */}
                <div className="bg-white p-6 rounded-lg shadow-md mb-8 border-l-4 border-indigo-500">
                    <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                        <Activity className="w-5 h-5 text-indigo-500" />
                        Live Activity Feed
                    </h2>
                    <div className="space-y-3 max-h-60 overflow-y-auto">
                        {events.length === 0 ? (
                            <p className="text-gray-400 text-sm italic">Waiting for events...</p>
                        ) : (
                            events.map((evt, idx) => (
                                <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 rounded border border-gray-100 animate-fade-in">
                                    <div className="flex items-center gap-3">
                                        <span className={`px-2 py-1 rounded text-xs font-bold uppercase ${evt.type === 'upload' ? 'bg-green-100 text-green-700' :
                                            evt.type === 'delete' ? 'bg-red-100 text-red-700' : 'bg-gray-100 text-gray-700'
                                            }`}>
                                            {evt.type}
                                        </span>
                                        <span className="text-sm font-medium text-gray-700">
                                            {evt.bucket} / {evt.key}
                                        </span>
                                    </div>
                                    <div className="flex items-center gap-4 text-xs text-gray-500">
                                        {evt.size && <span>{formatBytes(evt.size)}</span>}
                                        {evt.deduplicated && (
                                            <span className="flex items-center gap-1 text-purple-600 font-medium">
                                                <Database className="w-3 h-3" /> Deduped
                                            </span>
                                        )}
                                        <span className="bg-gray-200 px-2 py-0.5 rounded text-gray-600 uppercase">{evt.method}</span>
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* Stats Cards */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                    <div className="bg-white p-6 rounded-lg shadow-md border-l-4 border-blue-500">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-sm text-gray-500">Total Objects</p>
                                <p className="text-2xl font-bold text-gray-900">{metrics.cluster.total_objects}</p>
                            </div>
                            <Database className="w-10 h-10 text-blue-500 opacity-50" />
                        </div>
                    </div>

                    <div className="bg-white p-6 rounded-lg shadow-md border-l-4 border-purple-500">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-sm text-gray-500">Total Size</p>
                                <p className="text-2xl font-bold text-gray-900">{formatBytes(metrics.cluster.total_size_bytes)}</p>
                            </div>
                            <HardDrive className="w-10 h-10 text-purple-500 opacity-50" />
                        </div>
                    </div>

                    <div className="bg-white p-6 rounded-lg shadow-md border-l-4 border-green-500">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-sm text-gray-500">Dedup Savings</p>
                                <p className="text-2xl font-bold text-gray-900">{metrics.cluster.dedup_savings_percent}%</p>
                            </div>
                            <TrendingUp className="w-10 h-10 text-green-500 opacity-50" />
                        </div>
                    </div>

                    <div className="bg-white p-6 rounded-lg shadow-md border-l-4 border-orange-500">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-sm text-gray-500">Total Versions</p>
                                <p className="text-2xl font-bold text-gray-900">{metrics.cluster.total_versions}</p>
                            </div>
                            <Activity className="w-10 h-10 text-orange-500 opacity-50" />
                        </div>
                    </div>
                </div>

                {/* Charts */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {/* Node Distribution */}
                    <div className="bg-white p-6 rounded-lg shadow-md">
                        <h2 className="text-lg font-semibold text-gray-900 mb-4">Node Distribution</h2>
                        <ResponsiveContainer width="100%" height={300}>
                            <BarChart data={metrics.nodes}>
                                <XAxis dataKey="node_id" />
                                <YAxis />
                                <Tooltip />
                                <Bar dataKey="shard_count" fill="#3b82f6" />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>

                    {/* Bucket Usage */}
                    <div className="bg-white p-6 rounded-lg shadow-md">
                        <h2 className="text-lg font-semibold text-gray-900 mb-4">Bucket Usage</h2>
                        <ResponsiveContainer width="100%" height={300}>
                            <PieChart>
                                <Pie
                                    data={metrics.buckets}
                                    dataKey="objects"
                                    nameKey="name"
                                    cx="50%"
                                    cy="50%"
                                    outerRadius={80}
                                    label={(entry: any) => `${entry.name}: ${entry.value}`}
                                >
                                    {metrics.buckets.map((_, index) => (
                                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                    ))}
                                </Pie>
                                <Tooltip />
                                <Legend />
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* Node Status Table */}
                <div className="bg-white p-6 rounded-lg shadow-md mt-6">
                    <h2 className="text-lg font-semibold text-gray-900 mb-4">Node Health</h2>
                    <table className="w-full">
                        <thead className="bg-gray-50 border-b">
                            <tr>
                                <th className="px-4 py-3 text-left text-sm font-medium text-gray-500">Node ID</th>
                                <th className="px-4 py-3 text-left text-sm font-medium text-gray-500">Shards</th>
                                <th className="px-4 py-3 text-left text-sm font-medium text-gray-500">Status</th>
                            </tr>
                        </thead>
                        <tbody>
                            {metrics.nodes.map((node) => (
                                <tr key={node.node_id} className="border-b hover:bg-gray-50">
                                    <td className="px-4 py-3 font-medium text-gray-700">{node.node_id}</td>
                                    <td className="px-4 py-3 text-gray-600">{node.shard_count}</td>
                                    <td className="px-4 py-3">
                                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-700">
                                            ● {node.status}
                                        </span>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}
