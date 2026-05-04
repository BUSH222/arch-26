-- Event Store: Immutable append-only log
CREATE TABLE IF NOT EXISTS event_store (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id INT,
    aggregate_type VARCHAR(50) NOT NULL,
    event_data JSONB NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    version INT NOT NULL,
    UNIQUE(aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS idx_event_store_aggregate_id 
    ON event_store(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_event_store_event_type 
    ON event_store(event_type);
CREATE INDEX IF NOT EXISTS idx_event_store_created_at 
    ON event_store(created_at);

-- User Projections: Read model built from events
CREATE TABLE IF NOT EXISTS user_projections (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    projection_version INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_projections_created_at 
    ON user_projections(created_at);

-- Users List Projection: For efficient list queries
CREATE TABLE IF NOT EXISTS users_list_projection (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- Audit Projection: For compliance/history
CREATE TABLE IF NOT EXISTS user_audit_projection (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id INT NOT NULL,
    change_type VARCHAR(50) NOT NULL,
    old_value JSONB,
    new_value JSONB,
    changed_at TIMESTAMPTZ NOT NULL,
    changed_by VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_user_audit_projection_aggregate_id 
    ON user_audit_projection(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_user_audit_projection_changed_at 
    ON user_audit_projection(changed_at);

-- Projection Checkpoints: Track which events have been processed
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name VARCHAR(100) PRIMARY KEY,
    last_processed_event_id BIGINT DEFAULT 0,
    last_processed_at TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'active'
);
