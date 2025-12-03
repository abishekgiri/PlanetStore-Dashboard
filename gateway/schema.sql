-- Create tables for Planetstore metadata

CREATE TABLE IF NOT EXISTS buckets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    versioning_enabled BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ix_buckets_name ON buckets(name);

CREATE TABLE IF NOT EXISTS objects (
    id SERIAL PRIMARY KEY,
    bucket_name VARCHAR(255) NOT NULL,
    object_key VARCHAR(2048) NOT NULL,
    version_id VARCHAR(36) NOT NULL,
    is_latest BOOLEAN DEFAULT TRUE,
    size_bytes BIGINT,
    shards TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bucket_key_version ON objects(bucket_name, object_key, version_id);
