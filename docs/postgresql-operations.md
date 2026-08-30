# PostgreSQL operations

## Schema ownership

Choose a dedicated PostgreSQL schema for each Plugin Instance. The configured
schema is the only database namespace this Plugin owns. Runtime SQL uses that
owned search path and never accesses Organization, membership, Entitlements,
Auth, or Secrets tables.

Schema changes are operator-managed:

```rust
OrganizationProvisioningOperator::setup(
    database_url,
    "organization_provisioning_v1",
)
.await?;

OrganizationProvisioningOperator::upgrade(
    database_url,
    "organization_provisioning_v1",
)
.await?;
```

`setup` creates the owned schema and installs the authored migration plan.
`upgrade` applies pending authored migrations and is safe to repeat. Runtime
activation fails closed when the schema or migration history is missing or
incompatible; it never performs DDL.

Use PostgreSQL 17 or a separately validated compatible release. Give the
runtime database role only the privileges required in its owned schema. Keep
schema administration credentials outside the Plugin Instance. Do not edit an
already-applied migration; append a new authored migration and run `upgrade`.

## Secret and activation setup

Configure `database_url_secret` as a bounded reference resolved through
`lenso.secrets@1`. Do not put a database URL directly in Plugin configuration.
The configured Auth issuer and verification key are not fetched from the
database, and neither management nor worker callers gain authority from
database access.

Before activation:

1. run `setup` or `upgrade` with operator credentials;
2. verify all authored migrations are present and checksummed;
3. grant the runtime role access only to the selected schema;
4. bind exactly one Secrets, Organization Admin, and Entitlements Admin
   provider;
5. activate the Plugin and verify the owned schema opens without DDL.

There is no in-memory fallback. If Secrets resolution, PostgreSQL preparation,
or migration validation fails, keep the Plugin unavailable and repair the
operator-owned dependency.

## Consistency unit

Back up and restore these four tables together:

- `organization_provisionings`;
- `organization_provisioning_effects`;
- `organization_provisioning_mutations`;
- `organization_provisioning_activity`.

The saga row, ordered effects, idempotency receipts, and activity evidence form
one consistency unit. Restoring only a subset can cause a duplicate start, lose
the evidence needed to reconcile an unknown outcome, accept a stale revision,
or misreport cleanup residue.

## Worker scheduling

Call `process_due` from an exact allowlisted worker Instance with an Actor
Assertion for `lenso.organization-provisioning-worker@1/process_due`.

- `limit` must be 1 through 100;
- `lease_seconds` must be 5 through 300;
- continue immediately while `has_more=true`;
- when `has_more=false`, poll with bounded jitter appropriate to the App;
- do not issue direct SQL updates to force completion or clear a lease.

At the beginning of a batch, the worker quarantines expired in-flight effects
up to the batch limit. It then claims due saga rows with
`FOR UPDATE SKIP LOCKED`, preserving effect order within a saga while allowing
different sagas to progress concurrently. A claim persists a fresh UUID fence;
finalization is accepted only with that exact token.

Alert on any of the following:

- `manual_review > 0` or `quarantined > 0`;
- sagas in `manual_review`, `failed`, or `cleanup_failed`;
- repeated revision or idempotency conflicts;
- growing `pending` age or a worker that stops returning progress;
- a mismatch between local provider receipts and downstream records.

## Manual-review runbook

A Runtime failure can occur after the downstream provider applied the effect.
The same is true when a worker loses its lease before committing the receipt.
The Plugin therefore marks the effect `unknown`, sets the saga to
`manual_review`, invalidates the old fence, and refuses automatic replay.

To resolve one unknown effect:

1. pause automated mutation of that saga and call `get` as its original actor;
2. record the returned revision and identify the single `unknown` step;
3. inspect the authoritative downstream provider using the stable
   provisioning/effect evidence and expected Organization, scope, subject, and
   feature;
4. capture a concise, non-secret evidence statement;
5. call `retry` with a new caller-scoped idempotency key, the exact revision,
   and one of the resolutions below;
6. resume `process_due` only after the retry response leaves `manual_review`.

Use `confirm_applied` as follows:

- `create_organization`: provide both `observed_organization_id` and
  `observed_owner_membership_id`, and no grant id;
- `put_entitlement`: provide `observed_grant_id`, and no Organization or
  membership id;
- `revoke_entitlement`: provide no observed ids after verifying the grant is
  absent.

Use `confirm_not_applied` with no observed ids only when authoritative evidence
shows the effect did not occur; the effect returns to `pending`. Do not use
`retry_failed` for `manual_review`. That resolution is reserved for a known
domain failure in `failed` or `cleanup_failed` and also requires no observed
ids. A revision conflict means another transition won; fetch the saga again and
repeat the reconciliation rather than substituting the new revision blindly.

## Cleanup runbook

`request_cleanup` is valid only for `completed` or `failed` and requires the
original actor, a new caller-scoped idempotency key, exact revision, and
nonempty evidence. Resolve `manual_review` before requesting cleanup.

The cleanup transaction:

1. marks unfinished `put_entitlement` effects skipped;
2. creates one `revoke_entitlement` effect for each grant recorded as applied;
3. links each revoke to its source effect and persisted grant id;
4. records the number of reversible grants and whether an Organization remains;
5. returns `cleanup_pending`, or `cleanup_completed` when no grant needs
   compensation.

Continue `process_due` until cleanup completes or needs intervention. A known
revoke `not_found` result is treated as already absent. Any unknown revoke
outcome still enters `manual_review` and requires downstream reconciliation.

Cleanup never deletes the Organization or owner membership. If Organization
creation was applied, `organization_residue=true` is expected before, during,
and after cleanup. Do not clear the flag with SQL or describe
`cleanup_completed` as a full rollback.

## Backup, restore, and restart

Back up the entire owned schema atomically. On restore:

1. restore all owned tables to the same recovery point;
2. run the authored `upgrade` workflow;
3. activate the Plugin against the restored schema;
4. inspect every `in_flight`, `manual_review`, `failed`, and `cleanup_failed`
   saga before resuming the worker;
5. allow expired in-flight leases to quarantine naturally; do not erase or
   shorten them to force replay;
6. resume `process_due` with the configured worker identity.

Restoring an older snapshot does not undo Organizations or grants in their
provider Plugins. Reconcile downstream state before processing effects whose
local receipt may have been lost. Preserve activity and mutation receipts until
the product's retention policy explicitly permits deletion.

## Acceptance-test safety

The `postgres-acceptance` feature reads
`LENSO_ORGANIZATION_PROVISIONING_TEST_DATABASE_URL`. Use a disposable PostgreSQL
17 database. Every test creates and drops a unique owned schema, but the
database itself must still be isolated from production.

```sh
createdb organization_provisioning_test
LENSO_ORGANIZATION_PROVISIONING_TEST_DATABASE_URL=postgresql://postgres@localhost:5432/organization_provisioning_test \
  cargo test --locked -p lenso-organization-provisioning-postgres-plugin \
  --all-targets --all-features
```

The suite covers restart-stable replay, actor-local keyset listing, concurrent
idempotent start, ordered effects, revision CAS, UUID fencing, unknown-outcome
manual review, grant-only cleanup with explicit Organization residue, and
concurrent `SKIP LOCKED` workers.
