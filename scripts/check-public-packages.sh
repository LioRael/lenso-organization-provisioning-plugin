#!/usr/bin/env bash
set -euo pipefail

cargo_bin="${LENSO_CARGO_BIN:-cargo}"
consumer_cargo_bin="${LENSO_CONSUMER_CARGO_BIN:-cargo}"
repository_root="$(git rev-parse --show-toplevel)"
verification_root="$(mktemp -d "${TMPDIR:-/tmp}/lenso-organization-provisioning-packages.XXXXXX")"
workspace_copy="$verification_root/repository"

cleanup() {
  if [[ "${LENSO_KEEP_PACKAGE_TMP:-0}" == "1" ]]; then
    printf 'kept package verification root: %s\n' "$verification_root" >&2
  else
    rm -r "$verification_root"
  fi
}
trap cleanup EXIT

mkdir -p "$workspace_copy"
tar --exclude=.git --exclude=target -C "$repository_root" -cf - . | tar -C "$workspace_copy" -xf -

offline_flags=()
if [[ "${LENSO_PACKAGE_OFFLINE:-0}" == "1" ]]; then
  offline_flags+=(--offline)
fi

source_checkout() {
  local environment_name="$1"
  local repository="$2"
  local revision="$3"
  local directory="$4"
  local configured="${!environment_name:-}"
  if [[ -n "$configured" ]]; then
    git -C "$configured" rev-parse --is-inside-work-tree >/dev/null || return
    git -C "$configured" cat-file -e "$revision^{commit}" || return
    printf '%s\n' "$configured"
    return
  fi
  local checkout="$verification_root/$directory"
  git clone --quiet --filter=blob:none --no-checkout "$repository" "$checkout" || return
  git -C "$checkout" checkout --quiet --detach "$revision" || return
  printf '%s\n' "$checkout"
}

organization_root="$(source_checkout LENSO_ORGANIZATION_SOURCE https://github.com/LioRael/lenso-organization-plugin 9572afd465ba2f952b646ec16935c0274f66c82a organization)"
entitlements_root="$(source_checkout LENSO_ENTITLEMENTS_SOURCE https://github.com/LioRael/lenso-entitlements-plugin bc953a0c6de9aefe5489f7c7e3ef2d215cc25c13 entitlements)"
secrets_root="$(source_checkout LENSO_SECRETS_SOURCE https://github.com/LioRael/lenso-secrets-plugin c31aa142ff59b4536e2bf3e9785ccbb5bb5c0e6a secrets)"

package_dependency() {
  local source_root="$1"
  local package_name="$2"
  "$cargo_bin" package --quiet --locked "${offline_flags[@]}" \
    --manifest-path "$source_root/Cargo.toml" -p "$package_name" || return
  local metadata
  metadata="$("$cargo_bin" metadata --no-deps --format-version=1 --manifest-path "$source_root/Cargo.toml")" || return
  local target_directory
  target_directory="$(jq -r '.target_directory' <<<"$metadata")" || return
  local version
  version="$(jq -r --arg name "$package_name" '.packages[] | select(.name == $name) | .version' <<<"$metadata")" || return
  local archive="$target_directory/package/$package_name-$version.crate"
  [[ -f "$archive" ]] || return
  tar -xzf "$archive" -C "$verification_root" || return
  printf '%s\n' "$verification_root/$package_name-$version"
}

# Normalize provider-neutral Capability dependencies exactly as crates.io would.
# This prevents source-workspace path patches from hiding trait-identity breaks.
organization_admin_package="$(package_dependency "$organization_root" lenso-capability-organization-admin)"
entitlements_admin_package="$(package_dependency "$entitlements_root" lenso-capability-entitlements-admin)"
secrets_package="$(package_dependency "$secrets_root" lenso-capability-secrets)"

for capability in \
  lenso-capability-organization-provisioning \
  lenso-capability-organization-provisioning-worker; do
  "$cargo_bin" package --quiet --locked "${offline_flags[@]}" \
    --manifest-path "$workspace_copy/Cargo.toml" -p "$capability"
done

patches=(
  --config "patch.crates-io.lenso-capability-organization-provisioning.path=\"$workspace_copy/crates/lenso-capability-organization-provisioning\""
  --config "patch.crates-io.lenso-capability-organization-provisioning-worker.path=\"$workspace_copy/crates/lenso-capability-organization-provisioning-worker\""
  --config "patch.crates-io.lenso-capability-organization-admin.path=\"$organization_root/crates/lenso-capability-organization-admin\""
  --config "patch.crates-io.lenso-capability-entitlements-admin.path=\"$entitlements_root/crates/lenso-capability-entitlements-admin\""
  --config "patch.crates-io.lenso-capability-secrets.path=\"$secrets_root/crates/lenso-capability-secrets\""
)

"$cargo_bin" "${patches[@]}" package --quiet --no-verify "${offline_flags[@]}" \
  --manifest-path "$workspace_copy/Cargo.toml" \
  -p lenso-organization-provisioning-postgres-plugin

metadata="$("$cargo_bin" metadata --no-deps --format-version=1 --manifest-path "$workspace_copy/Cargo.toml")"
target_directory="$(jq -r '.target_directory' <<<"$metadata")"
public_version="$(jq -r '.packages[] | select(.name == "lenso-capability-organization-provisioning") | .version' <<<"$metadata")"
worker_version="$(jq -r '.packages[] | select(.name == "lenso-capability-organization-provisioning-worker") | .version' <<<"$metadata")"
plugin_version="$(jq -r '.packages[] | select(.name == "lenso-organization-provisioning-postgres-plugin") | .version' <<<"$metadata")"

for archive in \
  "$target_directory/package/lenso-capability-organization-provisioning-$public_version.crate" \
  "$target_directory/package/lenso-capability-organization-provisioning-worker-$worker_version.crate" \
  "$target_directory/package/lenso-organization-provisioning-postgres-plugin-$plugin_version.crate"; do
  [[ -f "$archive" ]]
  tar -xzf "$archive" -C "$verification_root"
done

public_package="$verification_root/lenso-capability-organization-provisioning-$public_version"
worker_package="$verification_root/lenso-capability-organization-provisioning-worker-$worker_version"
plugin_package="$verification_root/lenso-organization-provisioning-postgres-plugin-$plugin_version"

# Cargo-clippy performs independent Cargo discovery. Persist patches only inside
# this disposable extracted consumer, never in the source workspace.
mkdir -p "$plugin_package/.cargo"
{
  printf '[patch.crates-io]\n'
  printf 'lenso-capability-organization-provisioning = { path = "%s" }\n' "$public_package"
  printf 'lenso-capability-organization-provisioning-worker = { path = "%s" }\n' "$worker_package"
  printf 'lenso-capability-organization-admin = { path = "%s" }\n' "$organization_admin_package"
  printf 'lenso-capability-entitlements-admin = { path = "%s" }\n' "$entitlements_admin_package"
  printf 'lenso-capability-secrets = { path = "%s" }\n' "$secrets_package"
} >"$plugin_package/.cargo/config.toml"

(
  cd "$plugin_package"
  "$consumer_cargo_bin" generate-lockfile "${offline_flags[@]}"
  "$consumer_cargo_bin" check --quiet --locked --all-targets --all-features "${offline_flags[@]}"
  "$consumer_cargo_bin" test --quiet --locked --all-targets --all-features "${offline_flags[@]}"
  "$consumer_cargo_bin" clippy --quiet --locked --all-targets --all-features "${offline_flags[@]}" -- -D warnings
)
