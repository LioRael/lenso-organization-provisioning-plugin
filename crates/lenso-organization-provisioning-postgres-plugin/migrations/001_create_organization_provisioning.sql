CREATE TABLE organization_provisionings (
    provisioning_id UUID PRIMARY KEY,
    requested_by TEXT NOT NULL,
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    slug TEXT NOT NULL CHECK (length(slug) BETWEEN 1 AND 100),
    owner_subject TEXT NOT NULL CHECK (length(owner_subject) BETWEEN 1 AND 256),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'manual_review', 'cleanup_pending', 'cleanup_running', 'cleanup_completed', 'cleanup_failed')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    organization_id TEXT,
    owner_membership_id TEXT,
    cleanup_requested BOOLEAN NOT NULL DEFAULT FALSE,
    organization_residue BOOLEAN NOT NULL DEFAULT FALSE,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    cleanup_completed_at TIMESTAMPTZ,
    CHECK ((organization_id IS NULL) = (owner_membership_id IS NULL)),
    CHECK (NOT organization_residue OR organization_id IS NOT NULL)
);

CREATE INDEX organization_provisionings_requester_page_idx
    ON organization_provisionings (requested_by, created_at DESC, provisioning_id DESC);
CREATE INDEX organization_provisionings_status_idx
    ON organization_provisionings (status, updated_at ASC, provisioning_id ASC);

CREATE TABLE organization_provisioning_effects (
    effect_id UUID PRIMARY KEY,
    provisioning_id UUID NOT NULL REFERENCES organization_provisionings(provisioning_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('create_organization', 'put_entitlement', 'revoke_entitlement')),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 64),
    state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight', 'applied', 'failed', 'unknown', 'compensated', 'skipped')),
    downstream_key TEXT NOT NULL UNIQUE,
    subject_kind TEXT CHECK (subject_kind IN ('organization', 'owner')),
    feature TEXT,
    limit_value BIGINT CHECK (limit_value IS NULL OR limit_value > 0),
    source_effect_id UUID REFERENCES organization_provisioning_effects(effect_id) ON DELETE RESTRICT,
    external_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    last_error TEXT,
    provider_receipt JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    UNIQUE (provisioning_id, kind, ordinal),
    CHECK ((state = 'in_flight' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'in_flight' AND lease_token IS NULL AND lease_until IS NULL)),
    CHECK ((kind = 'create_organization' AND ordinal = 0 AND subject_kind IS NULL AND feature IS NULL AND limit_value IS NULL AND source_effect_id IS NULL)
        OR (kind = 'put_entitlement' AND subject_kind IS NOT NULL AND feature IS NOT NULL AND source_effect_id IS NULL)
        OR (kind = 'revoke_entitlement' AND subject_kind IS NOT NULL AND feature IS NOT NULL AND source_effect_id IS NOT NULL))
);

CREATE INDEX organization_provisioning_effects_due_idx
    ON organization_provisioning_effects (created_at ASC, effect_id ASC)
    WHERE state IN ('pending', 'in_flight');
CREATE INDEX organization_provisioning_effects_saga_idx
    ON organization_provisioning_effects (provisioning_id, kind, ordinal, effect_id);

CREATE TABLE organization_provisioning_mutations (
    caller_instance TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    provisioning_id UUID REFERENCES organization_provisionings(provisioning_id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('started', 'completed')),
    response JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (caller_instance, idempotency_key)
);

CREATE INDEX organization_provisioning_mutations_resource_idx
    ON organization_provisioning_mutations (provisioning_id);

CREATE TABLE organization_provisioning_activity (
    activity_id UUID PRIMARY KEY,
    provisioning_id UUID NOT NULL REFERENCES organization_provisionings(provisioning_id) ON DELETE CASCADE,
    effect_id UUID REFERENCES organization_provisioning_effects(effect_id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    provisioning_revision BIGINT NOT NULL CHECK (provisioning_revision > 0),
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX organization_provisioning_activity_idx
    ON organization_provisioning_activity (provisioning_id, created_at ASC, activity_id ASC);
