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

INSERT INTO groups (conversation_id, tenant_id, name, created_by, created_at)
SELECT DISTINCT 
    conversation_id,
    tenant_id,
    channel_id as name,
    (ARRAY_AGG(sender_id ORDER BY created_at ASC))[1] as created_by,
    MIN(created_at) as created_at
FROM messages
WHERE channel_type = 'Group' OR channel_type = 'group'
GROUP BY conversation_id, tenant_id, channel_id
ON CONFLICT (conversation_id) DO NOTHING;

INSERT INTO groups (conversation_id, tenant_id, name, created_by, created_at)
SELECT DISTINCT 
    conversation_id,
    tenant_id,
    channel_id as name,
    (ARRAY_AGG(sender_id ORDER BY created_at ASC))[1] as created_by,
    MIN(created_at) as created_at
FROM messages
WHERE channel_type = 'Group' OR channel_type = 'group'
GROUP BY conversation_id, tenant_id, channel_id
ON CONFLICT (tenant_id, name) DO NOTHING;

INSERT INTO group_members (conversation_id, tenant_id, user_id, joined_at)
SELECT DISTINCT 
    m.conversation_id,
    m.tenant_id,
    m.sender_id as user_id,
    MIN(m.created_at) as joined_at
FROM messages m
WHERE (m.channel_type = 'Group' OR m.channel_type = 'group')
  AND m.conversation_id IN (SELECT conversation_id FROM groups)
GROUP BY m.conversation_id, m.tenant_id, m.sender_id
ON CONFLICT (conversation_id, user_id) DO NOTHING;
