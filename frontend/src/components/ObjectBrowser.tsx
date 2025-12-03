import React, { useRef } from 'react';
import { File as FileIcon, Download, Trash2, Upload } from 'lucide-react';
import { getDownloadUrl, type ObjectInfo } from '../api';


interface ObjectBrowserProps {
    bucketName: string;
    objects: ObjectInfo[];
    onUpload: (file: File) => void;
    onDelete: (key: string) => void;
    isUploading: boolean;
}

export const ObjectBrowser: React.FC<ObjectBrowserProps> = ({ bucketName, objects, onUpload, onDelete, isUploading }) => {
    const fileInputRef = useRef<HTMLInputElement>(null);

    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (e.target.files && e.target.files[0]) {
            onUpload(e.target.files[0]);
            // Reset input
            if (fileInputRef.current) {
                fileInputRef.current.value = '';
            }
        }
    };

    const formatSize = (bytes: number) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    return (
        <div className="bg-white rounded-lg shadow-md border border-gray-200 h-full flex flex-col">
            <div className="p-4 border-b border-gray-200 flex justify-between items-center">
                <h2 className="font-semibold text-lg flex items-center gap-2">
                    <FolderOpenIcon className="w-5 h-5 text-blue-500" />
                    {bucketName}
                </h2>
                <div>
                    <input
                        type="file"
                        ref={fileInputRef}
                        onChange={handleFileChange}
                        className="hidden"
                    />
                    <button
                        onClick={() => fileInputRef.current?.click()}
                        disabled={isUploading}
                        className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 transition-colors disabled:opacity-50"
                    >
                        {isUploading ? (
                            <span className="animate-spin">⌛</span>
                        ) : (
                            <Upload className="w-4 h-4" />
                        )}
                        Upload Object
                    </button>
                </div>
            </div>
            <div className="flex-1 overflow-auto">
                <table className="w-full text-left text-sm">
                    <thead className="bg-gray-50 text-gray-500 font-medium border-b border-gray-200">
                        <tr>
                            <th className="px-4 py-3">Name</th>
                            <th className="px-4 py-3">Size</th>
                            <th className="px-4 py-3">Shards</th>
                            <th className="px-4 py-3 text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                        {objects.map((obj) => (
                            <tr key={obj.key} className="hover:bg-gray-50 group">
                                <td className="px-4 py-3 flex items-center gap-2">
                                    <FileIcon className="w-4 h-4 text-gray-400" />
                                    <span className="font-medium text-gray-700">{obj.key}</span>
                                </td>
                                <td className="px-4 py-3 text-gray-500">{formatSize(obj.size_bytes)}</td>
                                <td className="px-4 py-3">
                                    <div className="flex items-center gap-2">
                                        <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-700">
                                            {obj.shards_count} shards
                                        </span>
                                        {obj.is_latest && (
                                            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-700">
                                                Latest
                                            </span>
                                        )}
                                    </div>
                                </td>
                                <td className="px-4 py-3 text-right">
                                    <div className="flex items-center justify-end gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                                        <a
                                            href={getDownloadUrl(bucketName, obj.key)}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="p-1.5 text-gray-500 hover:text-blue-600 hover:bg-blue-50 rounded"
                                            title="Download"
                                        >
                                            <Download className="w-4 h-4" />
                                        </a>
                                        <button
                                            onClick={() => onDelete(obj.key)}
                                            className="p-1.5 text-gray-500 hover:text-red-600 hover:bg-red-50 rounded"
                                            title="Delete"
                                        >
                                            <Trash2 className="w-4 h-4" />
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        ))}
                        {objects.length === 0 && (
                            <tr>
                                <td colSpan={4} className="px-4 py-8 text-center text-gray-400">
                                    No objects in this bucket
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

// Helper icon
const FolderOpenIcon = ({ className }: { className?: string }) => (
    <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className={className}
    >
        <path d="M4 20h16a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.93a2 2 0 0 1-1.66-.9l-.82-1.2A2 2 0 0 0 7.93 2H4a2 2 0 0 0-2 2v13c0 1.1.9 2 2 2Z" />
        <path d="M2 10h20" />
    </svg>
);
