#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-organization-provisioning\nlenso-capability-organization-provisioning-worker\nlenso-organization-provisioning-postgres-plugin'
actual_crates="$(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 | xargs -0 sed -n 's/^name = "\([^"]*\)"/\1/p' | sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  echo "unexpected workspace crate boundary" >&2
  diff -u <(printf '%s\n' "$expected_crates") <(printf '%s\n' "$actual_crates") || true
  exit 1
fi

if rg -n 'path\s*=\s*"(\.\./\.\./|/)' --glob 'Cargo.toml' .; then
  echo "cross-repository or absolute path dependencies are not allowed" >&2
  exit 1
fi

if rg -n 'HashMap|Mutex<.*Vec|in.memory|memory fallback' crates --glob '*.rs'; then
  echo "ambient in-memory durable state is not allowed" >&2
  exit 1
fi

if rg -n 'CREATE (TABLE|INDEX|SCHEMA)|ALTER TABLE|DROP (TABLE|SCHEMA)' \
  crates/lenso-organization-provisioning-postgres-plugin/src/lib.rs \
  crates/lenso-organization-provisioning-postgres-plugin/src/storage.rs; then
  echo "runtime DDL is not allowed; migrations are operator-managed" >&2
  exit 1
fi

if rg -n 'CREATE TABLE (organizations|organization_memberships|entitlement_grants|entitlements)' \
  crates/lenso-organization-provisioning-postgres-plugin/migrations; then
  echo "migration crosses another Plugin's owned fact boundary" >&2
  exit 1
fi

if rg -n 'lenso-platform-|lenso-module-|HostBuilder|HostLinkedModule|ModuleManifest' \
  Cargo.toml crates README.md docs --glob '!**/generated.rs'; then
  echo "legacy Lenso framework dependency or API found" >&2
  exit 1
fi

for capability in \
  'lenso.organization-provisioning@1' \
  'lenso.organization-provisioning-worker@1' \
  'lenso.secrets@1' \
  'lenso.organization-admin@2' \
  'lenso.entitlements-admin@1'; do
  if ! rg -q "$capability" README.md docs crates; then
    echo "documented Organization Provisioning Capability boundary is missing: $capability" >&2
    exit 1
  fi
done

for table in \
  organization_provisionings \
  organization_provisioning_effects \
  organization_provisioning_mutations \
  organization_provisioning_activity; do
  if ! rg -q "$table" crates/lenso-organization-provisioning-postgres-plugin; then
    echo "owned PostgreSQL table is missing from implementation: $table" >&2
    exit 1
  fi
done

for forbidden in \
  'delete_organization' \
  'DROP TABLE organizations' \
  'DELETE FROM organizations'; do
  if rg -n "$forbidden" crates; then
    echo "irreversible Organization cleanup is forbidden: $forbidden" >&2
    exit 1
  fi
done
