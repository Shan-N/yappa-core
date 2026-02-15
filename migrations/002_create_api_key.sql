CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prefix TEXT NOT NULL,          -- e.g., "sk_live_"
    hash TEXT NOT NULL,            -- SHA-256 hash of the full key
    tenant_id TEXT NOT NULL,       -- The tenant this key belongs to
    permissions TEXT[] DEFAULT '{}', -- Optional: scopes like "read", "write"
    revoked BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

CREATE INDEX idx_api_keys_hash ON api_keys(hash);
