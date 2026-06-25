CREATE TABLE IF NOT EXISTS groups (
    conversation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    name            TEXT NOT NULL,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS group_members (
    conversation_id UUID NOT NULL REFERENCES groups(conversation_id) ON DELETE CASCADE,
    tenant_id       TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    joined_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (conversation_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_groups_tenant ON groups (tenant_id);
CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members (tenant_id, user_id);
