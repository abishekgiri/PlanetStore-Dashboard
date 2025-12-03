-- Add content_store table for deduplication
CREATE TABLE IF NOT EXISTS content_store (
    content_hash VARCHAR(64) PRIMARY KEY,
    size_bytes BIGINT NOT NULL,
    shards TEXT NOT NULL,
    refcount INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add content_hash column to objects table
ALTER TABLE objects ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_objects_content_hash ON objects(content_hash);
