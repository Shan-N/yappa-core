CREATE TABLE IF NOT EXISTS groups (
    conversation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    name            TEXT NOT NULL,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'groups_tenant_id_name_key') THEN
        DELETE FROM groups g
        WHERE g.ctid NOT IN (
            SELECT MIN(g2.ctid)
            FROM groups g2
            GROUP BY g2.tenant_id, g2.name
        );
        ALTER TABLE groups ADD CONSTRAINT groups_tenant_id_name_key UNIQUE (tenant_id, name);
    END IF;
END $$;

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
WHERE channel_type ILIKE 'group'
  AND NOT EXISTS (SELECT 1 FROM groups WHERE groups.conversation_id = messages.conversation_id)
GROUP BY conversation_id, tenant_id, channel_id
HAVING conversation_id IS NOT NULL;

INSERT INTO group_members (conversation_id, tenant_id, user_id, joined_at)
SELECT DISTINCT 
    m.conversation_id,
    m.tenant_id,
    m.sender_id as user_id,
    MIN(m.created_at) as joined_at
FROM messages m
WHERE m.channel_type ILIKE 'group'
  AND m.conversation_id IN (SELECT conversation_id FROM groups)
  AND NOT EXISTS (
      SELECT 1 FROM group_members gm 
      WHERE gm.conversation_id = m.conversation_id AND gm.user_id = m.sender_id
  )
GROUP BY m.conversation_id, m.tenant_id, m.sender_id;
