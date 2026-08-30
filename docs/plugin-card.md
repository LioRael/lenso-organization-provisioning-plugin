# Organization Provisioning Plugin card

## Outcome

An authorized caller can start one durable Organization bootstrap, inspect its
progress, and recover a known or operator-reconciled failure. A bounded worker
creates the Organization first and then applies the configured bootstrap
Entitlements. An authorized caller can later request compensating cleanup of
only those grants, while the created Organization remains explicit residue.

## Package and slots

- Plugin package: `lenso-organization-provisioning-postgres-plugin`
- Plugin id: `lenso.organization-provisioning.postgres`
- Root slot: `organization-provisioning`
- Public Capability: `lenso.organization-provisioning@1`
- Worker Capability: `lenso.organization-provisioning-worker@1`

## Provides

The public role has five request operations:

- `start`: persist a caller-idempotent saga and snapshot the configured grant
  templates without crossing a downstream Plugin boundary;
- `get`: return one actor-local saga with its ordered effect steps;
- `list`: filter the asserted actor's sagas by status and return a bounded
  keyset page;
- `retry`: retry a known failed effect or resolve one unknown outcome using
  revision CAS, observed identifiers, and operator evidence;
- `request_cleanup`: after `completed` or `failed`, create compensation effects
  for exactly the grants recorded as applied.

The worker role has one request operation:

- `process_due`: quarantine expired leases, claim up to 100 due effects with
  `FOR UPDATE SKIP LOCKED`, execute them in saga order, and finalize only under
  the exact UUID fence.

`process_due.limit` is 1 through 100 and `lease_seconds` is 5 through 300. Its
response distinguishes claimed, completed, failed, manual-review, and
quarantined counts and reports whether more local work is due.

## Requires

- `lenso.secrets@1`: resolve the configured PostgreSQL URL during activation;
- `lenso.organization-admin@2`: idempotent `create_organization` with the
  requested name, slug, and owner subject;
- `lenso.entitlements-admin@1`: converge configured `put_grant` effects and
  compensate them with `revoke_grant`;
- signed Auth Actor Assertions verified at the exact target operation.

All required Capability bindings have cardinality `one`. The implementation
uses generated clients and never reads or writes another Plugin's tables.

## Configuration

One immutable Plugin Instance configures:

- an owned PostgreSQL `schema` and one `database_url_secret` reference;
- an `auth_issuer` and base64 Actor Assertion verification key;
- 1 to 64 unique exact `management_callers` and `worker_callers`;
- up to 64 unique `(subject_kind, feature)` bootstrap grants.

`subject_kind` is `organization` or `owner`. Each feature is a bounded
identifier and each optional limit is a positive decimal integer. The Plugin
copies these templates into every new saga, preserving the provisioning intent
even if Instance configuration changes later.

## Authorization and resource scope

Every management call checks the exact calling Plugin Instance against
`management_callers`; every worker call checks `worker_callers`. Both roles then
verify an Actor Assertion whose audience names the exact Capability operation.
Actor kind may be `user` or `service_account`.

Caller identity and actor identity serve different purposes. Caller identity
scopes idempotency keys and proves which Plugin Instance invoked the role. Actor
identity is persisted as `requested_by`, scopes all public reads and mutations,
and is attached to activity evidence. A different actor receives
`provisioning_not_found` even if it knows the provisioning UUID. Worker actors
can claim due work globally but are recorded on each worker activity.

There is no ambient administrator bypass, bearer-UUID authorization, or direct
Access Control table lookup in this Plugin. The App must place only intended
surface and worker Instances in the exact caller allowlists.

## Owned facts

The owned PostgreSQL schema contains:

- `organization_provisionings`: requested intent, actor, status, revision,
  provider identifiers, residue flag, errors, and lifecycle timestamps;
- `organization_provisioning_effects`: ordered create/grant/revoke effects,
  stable downstream keys, attempts, leases, UUID fences, provider receipts, and
  completion state;
- `organization_provisioning_mutations`: caller-scoped idempotency reservations,
  request hashes, actors, and exact completed responses;
- `organization_provisioning_activity`: append-only local transition evidence
  with actor and provisioning revision.

It does not own Organizations, Organization memberships, Entitlements, Auth
credentials, Secrets, RBAC policy, or either downstream provider's receipts and
retention policy.

## Saga ordering and state

`start` creates one `create_organization` effect at ordinal zero followed by the
snapshotted `put_entitlement` effects. The worker cannot claim a grant until the
Organization effect has recorded both the Organization id and owner membership
id. Grants use Organization scope; their subject is either that Organization id
or the requested owner subject.

| Current | Event | Next | Guard |
| --- | --- | --- | --- |
| none | `start` commits | `pending` | exact caller, exact actor, new or exact-replay idempotency key |
| `pending` | worker claims | `running` | ordered due effect, row lock, fresh lease fence |
| `running` | known success | `pending` / `completed` | exact fence and provider receipt |
| `running` | known domain rejection | `failed` | exact fence and bounded error code |
| `running` | unknown result or expired lease | `manual_review` | effect becomes `unknown`; no automatic replay |
| `failed` | `retry_failed` | `pending` | exact actor, revision CAS, evidence |
| `manual_review` | `confirm_not_applied` | `pending` | provider reconciliation and revision CAS |
| `manual_review` | `confirm_applied` | `pending` / `completed` | effect-specific observed ids and evidence |
| `completed` / `failed` | `request_cleanup` | `cleanup_pending` / `cleanup_completed` | revision CAS; applied grants only |
| `cleanup_pending` | worker claims revoke | `cleanup_running` | row lock and fresh lease fence |
| `cleanup_running` | known revoke result | `cleanup_pending` / `cleanup_completed` | exact fence; not-found means already absent |
| `cleanup_running` | known failure | `cleanup_failed` | exact fence |
| `cleanup_running` | unknown result | `manual_review` | explicit reconciliation required |

`running` and `cleanup_running` describe a persisted in-flight lease. Revision
increments on claims, finalization, retry, cleanup request, and quarantine, so a
stale operator response cannot mutate a newer saga state.

## Idempotency, concurrency, and fencing

Public mutation identity is `(caller_instance, idempotency_key)`. The persisted
operation, actor, and SHA-256 request hash must all match for an exact replay;
otherwise the result is `idempotency_conflict`. The response is read back from
PostgreSQL, so replay remains exact across process restart.

Worker claims lock the saga with `FOR UPDATE SKIP LOCKED`, allowing independent
sagas to progress concurrently while serializing effects within one saga. Each
claim writes a fresh UUID lease token. Completion, failure, or unknown-outcome
finalization matches that token; a late result from a stale worker is
superseded. If a lease expires before a receipt commits, the effect is
quarantined as unknown because the downstream call may have completed.

## Manual review

Unknown effects are never converted directly to an ordinary retry. An operator
must inspect the authoritative Organization or Entitlements provider, then use:

- `confirm_applied` with Organization and owner-membership ids for
  `create_organization`;
- `confirm_applied` with a grant id for `put_entitlement`;
- `confirm_applied` with no observed ids for a verified grant revocation;
- `confirm_not_applied` with no observed ids when the effect definitely did not
  happen.

Every decision requires the current revision and nonempty evidence. A known
domain failure uses `retry_failed` instead and supplies no observed ids.

## Cleanup and residue

Cleanup can start only from `completed` or `failed`; `manual_review` must be
resolved first. Pending or failed grant creations are marked skipped. Each
applied grant produces one `revoke_entitlement` effect linked to its source
effect and recorded provider grant id.

There is deliberately no Organization deletion operation. Owner membership is
also preserved. `organization_residue` becomes true as soon as Organization
creation is confirmed and remains true after grant cleanup. Consumers must show
this field rather than describing `cleanup_completed` as a full rollback.

## Lifecycle and removal

`activate` resolves the database URL, verifies the authored migration history,
and opens the owned schema. It never creates, upgrades, or repairs schema
objects and does not start a worker. `deactivate` closes the PostgreSQL pool.

Removing the Plugin Instance and its bindings stops provisioning behavior.
Dropping its schema removes the local saga, mutation, effect, and activity
evidence. Created Organizations, memberships, and any grants that were not
successfully compensated remain owned by their provider Plugins.

## Deliberate v1 limits

- Provisioning coordinates one Organization create and a static snapshot of
  zero to 64 Entitlements grants; it is not a generic workflow engine.
- There is no automatic reconciliation query for unknown outcomes. Human or
  operator automation must provide evidence through `retry`.
- Cleanup does not delete an Organization, revoke owner membership, or touch
  grants that this saga did not record as applied.
- Lists are actor-local and status-filtered; there is no cross-actor operator
  search role in v1.
- Activity is local operational evidence, not a replacement for an independent
  audit-log product or the downstream providers' records.
