CREATE TABLE IF NOT EXISTS messages (
    message_id   UUID PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    conversation_id UUID NOT NULL,
    sender_id    TEXT NOT NULL,
    content      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_tenant ON messages (tenant_id);
CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages (conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_created ON messages (created_at);
