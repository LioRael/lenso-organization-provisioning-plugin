# Security policy

Please report suspected vulnerabilities privately to the repository owner. Do
not open a public issue containing a database URL, secret reference, Actor
Assertion, Auth verification key, downstream provider receipt, manual-review
evidence, Organization identifier, owner subject, or entitlement grant
identifier.

Supported releases receive fixes on the latest published minor line. Reports
should include the affected crate version, Capability and operation, expected
trust boundary, observed result, and a minimal reproduction with identities,
credentials, and provider evidence removed.

## Security invariants

Operators must preserve all of these controls:

- keep management and worker allowlists limited to exact Plugin Instance keys;
- verify Actor Assertions against the configured issuer, key, and exact target
  Capability operation;
- treat provisioning resources as local to their `requested_by` actor and never
  authorize a request from possession of a UUID alone;
- keep the database URL in `lenso.secrets@1` and give the database role access
  only to the Plugin-owned schema;
- apply schema changes through the explicit operator workflow, never by adding
  activation-time DDL;
- retain mutation, effect, activity, and provider-receipt rows as one recovery
  unit;
- require revision compare-and-swap and independent evidence for manual
  resolution or cleanup.

An unknown downstream outcome is not a retryable failure. Runtime failures and
expired effect leases enter `manual_review` because Organization creation or an
Entitlements change may already have succeeded. Inspect the authoritative
provider before calling `retry`; never manufacture `confirm_applied` identifiers
or use `confirm_not_applied` without evidence that the effect did not occur.

Cleanup is intentionally incomplete in the destructive sense. It revokes only
grants recorded as applied by this saga. It does not delete the Organization or
owner membership, and `organization_residue=true` must remain visible after
cleanup whenever Organization creation was applied. Treat requests to hide or
clear that residue as a security-sensitive ownership-boundary change.

Worker leases are fenced by a UUID. A stale worker result must not be accepted
after its fence is superseded, and expired leases must be quarantined for manual
review rather than reclaimed as if no downstream call occurred. Monitor
`manual_review`, `cleanup_failed`, nonzero `quarantined`, repeated domain
failures, unexpected revision conflicts, and idempotency conflicts.
