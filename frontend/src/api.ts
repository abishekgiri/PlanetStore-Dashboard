import axios from 'axios';

const API_URL = 'http://localhost:8000';

export const api = axios.create({
    baseURL: API_URL,
});

export interface NodeStat {
    node_id: string;
    base_url: string;
    latency_ms: number;
    object_count: number;
}

export interface Bucket {
    name: string;
    versioning_enabled: boolean;
}

export interface ObjectInfo {
    key: string;
    size_bytes: number;
    version_id: string;
    is_latest: boolean;
    shards_count: number;
}

export const getNodes = async () => {
    const response = await api.get<NodeStat[]>('/nodes');
    return response.data;
};

export const getBuckets = async () => {
    const response = await api.get<Bucket[]>('/buckets');
    return response.data;
};

export const createBucket = async (name: string, versioning: boolean = false) => {
    const response = await api.post<Bucket>('/buckets', { name, versioning });
    return response.data;
};

export const getObjects = async (bucket: string) => {
    const response = await api.get<ObjectInfo[]>(`/buckets/${bucket}/objects`);
    return response.data;
};

export const uploadObject = async (bucket: string, key: string, file: File) => {
    const formData = new FormData();
    formData.append('file', file);
    const response = await api.put(`/buckets/${bucket}/objects/${key}`, formData, {
        headers: {
            'Content-Type': 'multipart/form-data',
        },
    });
    return response.data;
};

export const deleteObject = async (bucket: string, key: string) => {
    const response = await api.delete(`/buckets/${bucket}/objects/${key}`);
    return response.data;
};

export const getDownloadUrl = (bucket: string, key: string) => {
    return `${API_URL}/buckets/${bucket}/objects/${key}`;
};
