# Lenso Organization Provisioning Plugin

Removable, PostgreSQL-backed Organization bootstrap for Lenso Apps. The Plugin
durably coordinates Organization Admin creation with a configured set of
Entitlements Admin grants, exposes the saga's current evidence, and supports
operator-confirmed recovery and compensating grant cleanup.

The Plugin provides two portable roles:

- `lenso.organization-provisioning@1`: `start`, `get`, `list`, `retry`, and
  `request_cleanup`;
- `lenso.organization-provisioning-worker@1`: bounded `process_due` batches.

`start` commits the complete local intent before any downstream call. It
snapshots the configured bootstrap grants, creates a stable provisioning UUID,
and returns a `pending` saga. `process_due` then invokes
`lenso.organization-admin@2` to create the Organization before converging each
configured grant through `lenso.entitlements-admin@1`.

Known downstream domain failures are recorded as `failed`. A Runtime failure,
timeout, lost worker, or expired lease means the external outcome is unknown;
the effect and saga move to `manual_review` and are never replayed
automatically. An operator must reconcile the downstream provider and call
`retry` with `confirm_applied` or `confirm_not_applied`, an exact revision, and
durable evidence.

## Safety and authority

Every public or worker call requires both:

1. the exact calling Plugin Instance in the role-specific immutable allowlist;
2. a valid Actor Assertion issued by the configured Auth issuer and audienced
   to the exact Capability operation.

Actors may be users or service accounts. Public resources are local to the
asserted actor: `get`, `list`, `retry`, and `request_cleanup` can only observe or
mutate sagas whose `requested_by` subject matches that assertion. Supplying a
provisioning UUID is not authority.

Mutations use caller-scoped idempotency keys and persist the request hash plus
the exact response. Reusing a key with a different operation, actor, or payload
fails with `idempotency_conflict`. `retry` and `request_cleanup` additionally
require the current positive decimal revision as a compare-and-swap token.
Lists return at most 100 records with an opaque `(created_at, provisioning_id)`
keyset cursor.

Cleanup is deliberately compensating, not destructive. It can revoke only
bootstrap grants that this saga recorded as applied. It never deletes the
Organization, removes its owner membership, or rewrites another Plugin's
tables. Once Organization creation has been confirmed, responses continue to
set `organization_residue=true`, including after `cleanup_completed`.

See [the Plugin card](docs/plugin-card.md) for the complete Capability,
authority, state, and ownership model. See
[PostgreSQL operations](docs/postgresql-operations.md) for schema setup,
worker recovery, manual review, backup, and restore procedures.

## Operator workflow

1. Create or upgrade one Plugin-owned PostgreSQL schema with
   `OrganizationProvisioningOperator::setup` or `upgrade`. Activation validates
   the authored migration history but never runs DDL.
2. Store the PostgreSQL URL behind a `lenso.secrets@1` reference. Configure the
   Auth issuer and Actor Assertion public key, exact management and worker
   caller Instance keys, and zero to 64 unique bootstrap grant templates.
3. Bind one Secrets provider, one Organization Admin provider, and one
   Entitlements Admin provider.
4. Call `start`. Schedule `process_due` with a limit from 1 to 100 and a lease
   from 5 to 300 seconds. Continue while `has_more=true`.
5. Alert on `failed`, `cleanup_failed`, `manual_review`, or a nonzero
   `quarantined` count. Resolve unknown outcomes against the owning downstream
   provider before allowing another attempt.

A grant template targets either the newly created Organization or the requested
owner. Every grant uses Organization scope, a configured feature, an optional
positive decimal limit, and no expiry. Templates are snapshotted when `start`
commits, so later configuration changes do not alter an existing saga.

There is no in-memory persistence fallback and no background worker hidden in
activation. Removing the Plugin Instance removes the behavior and bindings;
dropping its owned schema is a separate operator decision. Organizations and
Entitlements remain facts of their provider Plugins.

## Verification

The repository pins Rust 1.94 and CI runs against PostgreSQL 17:

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
lenso-contract-codegen check \
  crates/lenso-capability-organization-provisioning/capability.json \
  --rust crates/lenso-capability-organization-provisioning/src/generated.rs
lenso-contract-codegen check \
  crates/lenso-capability-organization-provisioning-worker/capability.json \
  --rust crates/lenso-capability-organization-provisioning-worker/src/generated.rs
./scripts/check-repository-boundary.sh
./scripts/check-public-packages.sh
```

The real PostgreSQL acceptance suite uses a disposable database and creates a
unique schema per test:

```sh
LENSO_ORGANIZATION_PROVISIONING_TEST_DATABASE_URL=postgresql://postgres@localhost:5432/organization_provisioning_test \
  cargo test --locked --workspace --all-targets --all-features
```

Publication and crates.io Trusted Publisher setup are documented in
[the release process](docs/release-process.md).
