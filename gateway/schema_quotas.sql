-- Bucket quotas table for custom quota configuration
CREATE TABLE IF NOT EXISTS bucket_quotas (
    bucket_name VARCHAR(255) PRIMARY KEY,
    max_size_bytes BIGINT NOT NULL,
    max_objects INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
