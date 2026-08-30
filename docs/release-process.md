# Release process

This repository has three public Rust crates, published in dependency order:

1. `lenso-capability-organization-provisioning`
2. `lenso-capability-organization-provisioning-worker`
3. `lenso-organization-provisioning-postgres-plugin`

Publication is manual-only from a clean, reviewed `main` checkout through
`.github/workflows/release-plz.yml`. The workflow has no push trigger and does
not create an automatic Release-plz PR. Version and changelog changes must
therefore be reviewed on an ordinary pull request before a release run.

The manual workflow has two inputs:

- `dry_run=true` validates the release plan without publishing;
- `dry_run=false` requests live publication and additionally requires
  `live_guard=publish` on `main`.

Run a dry run first and inspect every planned package, version, dependency, tag,
and release. A successful dry run proves only that Release-plz accepted the
current plan; it does not prove a crate was published.

Before the Plugin can publish, the exact compatible versions of Organization
Admin, Entitlements Admin, Secrets, Auth SDK, Lenso runtime, Lenso authoring,
Lenso contract runtime, and PostgreSQL Kit dependencies must already exist on
crates.io. Git revisions in this repository are development provenance; Cargo
normalizes publishable dependencies to their declared version requirements.

## crates.io Trusted Publisher

Trusted Publishing cannot allocate an unowned crate name. For the first release
only, allocate each `0.1.0` crate name in the order above with a temporary
crates.io token restricted to new-package publication, then revoke it
immediately. Do not store that token in Cargo credentials, GitHub secrets,
workflow logs, or shell history.

Configure one crates.io Trusted Publisher for each crate:

- owner: `LioRael`
- repository: `lenso-organization-provisioning-plugin`
- workflow: `release-plz.yml`
- environment: unset

The workflow has no `CARGO_REGISTRY_TOKEN` fallback and no GitHub Actions
environment. Only the guarded live job requests `id-token: write`; Release-plz
exchanges GitHub's OIDC identity for the short-lived crates.io credential.

## Required gates

Use the pinned Rust toolchain and PostgreSQL 17:

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
LENSO_ORGANIZATION_PROVISIONING_TEST_DATABASE_URL=postgresql://postgres@localhost:5432/organization_provisioning_test \
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

Review the package archives, normalized consumer graph, public Capability
descriptors, generated projections, configuration schema, migrations, README,
security policy, and release metadata. Confirm that cleanup documentation still
states that Organizations are never deleted and that `organization_residue`
remains visible.

## Live release and proof

1. Merge the reviewed version change and required gates to `main`.
2. Run `release-plz.yml` on `main` with `dry_run=true`.
3. Inspect the dry-run output and confirm all prerequisites are published.
4. Run it again with `dry_run=false` and `live_guard=publish`.
5. Verify the workflow completed through GitHub OIDC with no registry token.
6. Verify every expected GitHub tag and release points at the released commit.
7. Verify each exact crate version with `cargo info <crate>@<version>` and a
   fresh consumer resolution; do not rely on a tag or workflow badge alone.

If the workflow response is interrupted or ambiguous, read the workflow run,
GitHub releases, repository refs, and crates.io state before retrying. Re-running
publication without that readback can turn an uncertain result into confusing
duplicate release activity.
