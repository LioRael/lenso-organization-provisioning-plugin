#!/usr/bin/env bash
set -euo pipefail

cargo_bin="${LENSO_CARGO_BIN:-cargo}"
flags=(--locked)
if [[ "${LENSO_PACKAGE_ALLOW_DIRTY:-0}" == "1" ]]; then
  flags+=(--allow-dirty)
fi

for manifest in crates/*/Cargo.toml; do
  rg -qx 'publish = true' "$manifest" || {
    printf '%s is not explicitly publishable\n' "$manifest" >&2
    exit 1
  }
done

for package in \
  lenso-capability-organization-provisioning \
  lenso-capability-organization-provisioning-worker; do
  "$cargo_bin" package --quiet "${flags[@]}" -p "$package"
done

"$cargo_bin" package --quiet "${flags[@]}" --no-verify \
  -p lenso-organization-provisioning-postgres-plugin \
  --config 'patch.crates-io.lenso-capability-organization-provisioning.path="crates/lenso-capability-organization-provisioning"' \
  --config 'patch.crates-io.lenso-capability-organization-provisioning-worker.path="crates/lenso-capability-organization-provisioning-worker"'

printf 'public Organization Provisioning package archives are valid\n'
