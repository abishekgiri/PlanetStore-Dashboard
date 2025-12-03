import React, { useState } from 'react';
import { Folder, Plus } from 'lucide-react';
import type { Bucket } from '../api';
import { clsx } from 'clsx';

interface BucketListProps {
    buckets: Bucket[];
    selectedBucket: string | null;
    onSelectBucket: (name: string) => void;
    onCreateBucket: (name: string) => void;
}

export const BucketList: React.FC<BucketListProps> = ({ buckets, selectedBucket, onSelectBucket, onCreateBucket }) => {
    const [newBucketName, setNewBucketName] = useState('');

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        if (newBucketName.trim()) {
            onCreateBucket(newBucketName.trim());
            setNewBucketName('');
        }
    };

    return (
        <div className="bg-white rounded-lg shadow-md border border-gray-200 h-full flex flex-col">
            <div className="p-4 border-b border-gray-200">
                <h2 className="font-semibold text-lg mb-4">Buckets</h2>
                <form onSubmit={handleSubmit} className="flex gap-2">
                    <input
                        type="text"
                        value={newBucketName}
                        onChange={(e) => setNewBucketName(e.target.value)}
                        placeholder="New bucket..."
                        className="flex-1 px-3 py-1.5 text-sm border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                    <button
                        type="submit"
                        className="p-1.5 bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors"
                    >
                        <Plus className="w-4 h-4" />
                    </button>
                </form>
            </div>
            <div className="flex-1 overflow-y-auto p-2 space-y-1">
                {buckets.map((bucket) => (
                    <button
                        key={bucket.name}
                        onClick={() => onSelectBucket(bucket.name)}
                        className={clsx(
                            "w-full flex items-center space-x-3 px-3 py-2 rounded-md text-sm transition-colors",
                            selectedBucket === bucket.name
                                ? "bg-blue-50 text-blue-700 font-medium"
                                : "text-gray-700 hover:bg-gray-100"
                        )}
                    >
                        <Folder className={clsx("w-4 h-4", selectedBucket === bucket.name ? "text-blue-500" : "text-gray-400")} />
                        <span>{bucket.name}</span>
                    </button>
                ))}
                {buckets.length === 0 && (
                    <div className="text-center text-gray-400 text-sm py-4">
                        No buckets yet
                    </div>
                )}
            </div>
        </div>
    );
};
