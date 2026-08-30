//! PostgreSQL-backed durable Organization Provisioning saga.

mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod schema;
mod storage;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration};

use lenso::prelude::*;
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionVerifier, ActorProjectionError, AssertionClock, TypedActor,
};
use lenso_capability_entitlements_admin as entitlements;
use lenso_capability_organization_admin as organization;
use lenso_capability_organization_provisioning as public;
use lenso_capability_organization_provisioning_worker as worker;
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_kernel::{PluginDependencies, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;
use zeroize::Zeroizing;

pub use operator::{OrganizationProvisioningOperator, OrganizationProvisioningOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLERS: usize = 64;
const MAX_ID_BYTES: usize = 512;
const MAX_IDEMPOTENCY_BYTES: usize = 200;
const MAX_EVIDENCE_BYTES: usize = 4_000;
const MAX_BOOTSTRAP_GRANTS: usize = 64;

/// One immutable entitlement grant template snapshotted into every new saga.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapGrantConfig {
    subject_kind: String,
    feature: String,
    limit: Option<String>,
}

impl BootstrapGrantConfig {
    /// Builds one organization- or owner-scoped bootstrap grant template.
    pub fn new(
        subject_kind: impl Into<String>,
        feature: impl Into<String>,
        limit: Option<String>,
    ) -> Self {
        Self {
            subject_kind: subject_kind.into(),
            feature: feature.into(),
            limit,
        }
    }
}

/// Immutable configuration for one Organization Provisioning Plugin Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrganizationProvisioningConfig {
    schema: String,
    database_url_secret: String,
    auth_issuer: String,
    auth_assertion_public_key: String,
    management_callers: Vec<String>,
    worker_callers: Vec<String>,
    bootstrap_grants: Vec<BootstrapGrantConfig>,
}

impl OrganizationProvisioningConfig {
    /// Creates and validates immutable Organization Provisioning configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        auth_issuer: impl Into<String>,
        auth_assertion_public_key: impl Into<String>,
        management_callers: Vec<String>,
        worker_callers: Vec<String>,
        bootstrap_grants: Vec<BootstrapGrantConfig>,
    ) -> Result<Self, OrganizationProvisioningConfigError> {
        let config = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            auth_issuer: auth_issuer.into(),
            auth_assertion_public_key: auth_assertion_public_key.into(),
            management_callers,
            worker_callers,
            bootstrap_grants,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), OrganizationProvisioningConfigError> {
        schema::schema_plan(self.schema.clone())
            .map_err(|_| OrganizationProvisioningConfigError::InvalidSchema)?;
        if !valid_secret_reference(&self.database_url_secret) {
            return Err(OrganizationProvisioningConfigError::InvalidSecretReference);
        }
        if !valid_identifier(&self.auth_issuer, 256) {
            return Err(OrganizationProvisioningConfigError::InvalidAuthIssuer);
        }
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| OrganizationProvisioningConfigError::InvalidAuthPublicKey)?;
        validate_callers(&self.management_callers)
            .map_err(|()| OrganizationProvisioningConfigError::InvalidManagementCallers)?;
        validate_callers(&self.worker_callers)
            .map_err(|()| OrganizationProvisioningConfigError::InvalidWorkerCallers)?;
        if self.bootstrap_grants.len() > MAX_BOOTSTRAP_GRANTS {
            return Err(OrganizationProvisioningConfigError::InvalidBootstrapGrants);
        }
        let mut unique = BTreeSet::new();
        for grant in &self.bootstrap_grants {
            if !matches!(grant.subject_kind.as_str(), "organization" | "owner")
                || !valid_dimension(&grant.feature, 128)
                || !grant
                    .limit
                    .as_deref()
                    .is_none_or(|value| value.parse::<i64>().is_ok_and(|value| value > 0))
                || !unique.insert((grant.subject_kind.as_str(), grant.feature.as_str()))
            {
                return Err(OrganizationProvisioningConfigError::InvalidBootstrapGrants);
            }
        }
        Ok(())
    }

    fn verifier(&self) -> Result<ActorAssertionVerifier, RuntimeFailure> {
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| RuntimeFailure::InvalidResolvedPlan {
            detail: "Organization Provisioning Auth verification key is invalid".to_owned(),
        })
    }

    fn grant_snapshots(&self) -> Result<Vec<storage::BootstrapGrant>, RuntimeFailure> {
        self.bootstrap_grants
            .iter()
            .map(|grant| {
                Ok(storage::BootstrapGrant {
                    subject_kind: grant.subject_kind.clone(),
                    feature: grant.feature.clone(),
                    limit: grant
                        .limit
                        .as_deref()
                        .map(str::parse)
                        .transpose()
                        .map_err(|_| RuntimeFailure::InvalidResolvedPlan {
                            detail: "Organization Provisioning grant limit is invalid".to_owned(),
                        })?,
                })
            })
            .collect()
    }
}

/// Invalid immutable Organization Provisioning configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum OrganizationProvisioningConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("invalid Auth issuer")]
    InvalidAuthIssuer,
    #[error("invalid Auth assertion public key")]
    InvalidAuthPublicKey,
    #[error("management_callers must contain unique exact Instance keys")]
    InvalidManagementCallers,
    #[error("worker_callers must contain unique exact Instance keys")]
    InvalidWorkerCallers,
    #[error("bootstrap_grants must be bounded, unique, and valid")]
    InvalidBootstrapGrants,
}

fn validate_config(config: &OrganizationProvisioningConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("Organization Provisioning configuration is invalid: {error}"),
        })
}

#[derive(Clone, Debug)]
struct PreparedOrganizationProvisioning {
    postgres: OwnedPostgres,
}

#[lenso::plugin(
    lifecycle,
    configuration_schema = "configuration.schema.json",
    validate = validate_config
)]
#[derive(Clone)]
struct PostgresOrganizationProvisioningPlugin {
    #[config]
    config: OrganizationProvisioningConfig,
    secrets: Port<secrets::SecretsClient>,
    organization: Port<organization::OrganizationAdminClient>,
    entitlements: Port<entitlements::EntitlementsAdminClient>,
    prepared: Rc<RefCell<Option<PreparedOrganizationProvisioning>>>,
}

impl fmt::Debug for PostgresOrganizationProvisioningPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresOrganizationProvisioningPlugin")
            .field("schema", &self.config.schema)
            .field("prepared", &self.prepared.borrow().is_some())
            .field("bootstrap_grant_count", &self.config.bootstrap_grants.len())
            .finish_non_exhaustive()
    }
}

#[lenso::provides(
    public::OrganizationProvisioning,
    worker::OrganizationProvisioningWorker
)]
impl PostgresOrganizationProvisioningPlugin {}

impl PostgresOrganizationProvisioningPlugin {
    async fn start(
        &self,
        context: Ctx,
        request: public::StartRequest,
    ) -> PluginResult<public::StartResponse, public::StartError> {
        let (caller, actor) = self.authorize_public::<public::StartError>(
            &context,
            public::CAPABILITY_ID,
            public::START_OPERATION,
        )?;
        let name = request.name.trim();
        if !valid_organization_name(name)
            || !valid_slug(&request.slug)
            || !valid_opaque_id(&request.owner_subject, 256)
            || !valid_idempotency_key(&request.idempotency_key)
        {
            return Err(PluginError::domain(public::StartError::InvalidRequest));
        }
        let hash = request_hash(&request)?;
        let grants = self
            .config
            .grant_snapshots()
            .map_err(PluginError::runtime)?;
        let response = storage::start(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &actor,
            &request.idempotency_key,
            &hash,
            Uuid::new_v4(),
            name,
            &request.slug,
            &request.owner_subject,
            &grants,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(public::StartError::from_storage(failure)))?;
        wire_cast(&response)
    }

    async fn get(
        &self,
        context: Ctx,
        request: public::GetRequest,
    ) -> PluginResult<public::GetResponse, public::GetError> {
        let (_, actor) = self.authorize_public::<public::GetError>(
            &context,
            public::CAPABILITY_ID,
            public::GET_OPERATION,
        )?;
        let provisioning_id = Uuid::parse_str(&request.provisioning_id)
            .map_err(|_| PluginError::domain(public::GetError::InvalidRequest))?;
        let response = storage::get(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            provisioning_id,
            &actor,
        )
        .await
        .map_err(storage_runtime)?
        .ok_or_else(|| PluginError::domain(public::GetError::ProvisioningNotFound))?;
        wire_cast(&response)
    }

    async fn list(
        &self,
        context: Ctx,
        request: public::ListRequest,
    ) -> PluginResult<public::ListResponse, public::ListError> {
        let (_, actor) = self.authorize_public::<public::ListError>(
            &context,
            public::CAPABILITY_ID,
            public::LIST_OPERATION,
        )?;
        if !(1..=100).contains(&request.limit)
            || request
                .status
                .as_deref()
                .is_some_and(|value| !valid_status(value))
        {
            return Err(PluginError::domain(public::ListError::InvalidRequest));
        }
        let cursor = match request.cursor.as_deref() {
            Some(value) => Some(
                storage::decode_cursor(value)
                    .ok_or_else(|| PluginError::domain(public::ListError::InvalidRequest))?,
            ),
            None => None,
        };
        let mut items = storage::list(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &storage::ListFilters {
                actor: &actor,
                status: request.status.as_deref(),
                cursor: cursor.as_ref(),
                limit: request.limit + 1,
            },
        )
        .await
        .map_err(storage_runtime)?;
        let next_cursor = if items.len() > usize::try_from(request.limit).unwrap_or(0) {
            items.truncate(usize::try_from(request.limit).unwrap_or(0));
            items
                .last()
                .map(storage::encode_cursor)
                .transpose()
                .map_err(storage_runtime)?
        } else {
            None
        };
        wire_cast(&serde_json::json!({"items": items, "next_cursor": next_cursor}))
    }

    async fn retry(
        &self,
        context: Ctx,
        request: public::RetryRequest,
    ) -> PluginResult<public::RetryResponse, public::RetryError> {
        let (caller, actor) = self.authorize_public::<public::RetryError>(
            &context,
            public::CAPABILITY_ID,
            public::RETRY_OPERATION,
        )?;
        let (provisioning_id, expected_revision) = parse_mutation(
            &request.provisioning_id,
            &request.expected_revision,
            &request.idempotency_key,
            &request.evidence,
        )
        .ok_or_else(|| PluginError::domain(public::RetryError::InvalidRequest))?;
        if [
            request.observed_organization_id.as_deref(),
            request.observed_owner_membership_id.as_deref(),
            request.observed_grant_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| !valid_opaque_id(value, MAX_ID_BYTES))
        {
            return Err(PluginError::domain(public::RetryError::InvalidRequest));
        }
        let resolution = match request.resolution {
            public::RetryRequestResolution::RetryFailed => storage::RetryResolution::RetryFailed,
            public::RetryRequestResolution::ConfirmApplied => {
                storage::RetryResolution::ConfirmApplied
            }
            public::RetryRequestResolution::ConfirmNotApplied => {
                storage::RetryResolution::ConfirmNotApplied
            }
        };
        let hash = request_hash(&request)?;
        let response = storage::retry(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &storage::RetryInput {
                caller: &caller,
                actor: &actor,
                idempotency_key: &request.idempotency_key,
                request_hash: &hash,
                provisioning_id,
                expected_revision,
                resolution,
                observed_organization_id: request.observed_organization_id.as_deref(),
                observed_owner_membership_id: request.observed_owner_membership_id.as_deref(),
                observed_grant_id: request.observed_grant_id.as_deref(),
                evidence: &request.evidence,
            },
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(public::RetryError::from_storage(failure)))?;
        wire_cast(&response)
    }

    async fn request_cleanup(
        &self,
        context: Ctx,
        request: public::RequestCleanupRequest,
    ) -> PluginResult<public::RequestCleanupResponse, public::RequestCleanupError> {
        let (caller, actor) = self.authorize_public::<public::RequestCleanupError>(
            &context,
            public::CAPABILITY_ID,
            public::REQUEST_CLEANUP_OPERATION,
        )?;
        let (provisioning_id, expected_revision) = parse_mutation(
            &request.provisioning_id,
            &request.expected_revision,
            &request.idempotency_key,
            &request.evidence,
        )
        .ok_or_else(|| PluginError::domain(public::RequestCleanupError::InvalidRequest))?;
        let hash = request_hash(&request)?;
        let response = storage::request_cleanup(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &storage::CleanupInput {
                caller: &caller,
                actor: &actor,
                idempotency_key: &request.idempotency_key,
                request_hash: &hash,
                provisioning_id,
                expected_revision,
                evidence: &request.evidence,
            },
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| {
            PluginError::domain(public::RequestCleanupError::from_storage(failure))
        })?;
        wire_cast(&response)
    }
}

impl PostgresOrganizationProvisioningPlugin {
    async fn process_due(
        &self,
        context: Ctx,
        request: worker::ProcessDueRequest,
    ) -> PluginResult<worker::ProcessDueResponse, worker::ProcessDueError> {
        let (_, actor) = self.authorize_worker(&context, worker::PROCESS_DUE_OPERATION)?;
        if !(1..=100).contains(&request.limit) || !(5..=300).contains(&request.lease_seconds) {
            return Err(PluginError::domain(worker::ProcessDueError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        let quarantined = storage::quarantine_expired(&prepared.postgres, &actor, request.limit)
            .await
            .map_err(storage_runtime)?;
        let mut claimed = 0_i64;
        let mut completed = 0_i64;
        let mut failed = 0_i64;
        let mut manual_review = i64::try_from(quarantined).unwrap_or(i64::MAX);
        let remaining = request
            .limit
            .saturating_sub(i64::try_from(quarantined).unwrap_or(i64::MAX));
        for _ in 0..remaining {
            let Some(effect) =
                storage::claim_next(&prepared.postgres, &actor, request.lease_seconds)
                    .await
                    .map_err(storage_runtime)?
            else {
                break;
            };
            claimed = claimed.saturating_add(1);
            let outcome = self.execute_effect(&context, &effect).await;
            match storage::finalize_effect(&prepared.postgres, &effect, &actor, outcome)
                .await
                .map_err(storage_runtime)?
            {
                storage::FinalizeResult::Applied => completed = completed.saturating_add(1),
                storage::FinalizeResult::Failed => failed = failed.saturating_add(1),
                storage::FinalizeResult::ManualReview => {
                    manual_review = manual_review.saturating_add(1);
                }
                storage::FinalizeResult::Superseded => {}
            }
        }
        let has_more = storage::has_more(&prepared.postgres)
            .await
            .map_err(storage_runtime)?;
        Ok(worker::ProcessDueResponse {
            claimed,
            completed,
            failed,
            manual_review,
            quarantined: i64::try_from(quarantined).unwrap_or(i64::MAX),
            has_more,
        })
    }

    async fn execute_effect(
        &self,
        context: &Ctx,
        effect: &storage::EffectRecord,
    ) -> storage::EffectOutcome {
        match effect.kind.as_str() {
            "create_organization" => self.execute_create_organization(context, effect).await,
            "put_entitlement" => self.execute_put_entitlement(context, effect).await,
            "revoke_entitlement" => self.execute_revoke_entitlement(context, effect).await,
            _ => storage::EffectOutcome::Failed {
                error_code: "invalid_effect_kind".to_owned(),
            },
        }
    }

    async fn execute_create_organization(
        &self,
        context: &Ctx,
        effect: &storage::EffectRecord,
    ) -> storage::EffectOutcome {
        match self
            .organization
            .create_organization_with_context(
                context.clone(),
                organization::CreateOrganizationRequest {
                    idempotency_key: effect.downstream_key.clone(),
                    name: effect.name.clone(),
                    owner_subject: effect.owner_subject.clone(),
                    slug: effect.slug.clone(),
                },
            )
            .await
        {
            Ok(response) => storage::EffectOutcome::OrganizationApplied {
                organization_id: response.organization_id.clone(),
                owner_membership_id: response.owner_membership_id.clone(),
                receipt: serde_json::json!({
                    "created": response.created,
                    "organization_id": response.organization_id,
                    "owner_membership_id": response.owner_membership_id,
                }),
            },
            Err(organization::OrganizationAdminInvocationError::Domain(error)) => {
                storage::EffectOutcome::Failed {
                    error_code: organization_error_code(&error),
                }
            }
            Err(organization::OrganizationAdminInvocationError::Runtime(_)) => {
                storage::EffectOutcome::Unknown {
                    error_code: "organization_outcome_unknown".to_owned(),
                }
            }
        }
    }

    async fn execute_put_entitlement(
        &self,
        context: &Ctx,
        effect: &storage::EffectRecord,
    ) -> storage::EffectOutcome {
        let Some(organization_id) = effect.organization_id.as_deref() else {
            return storage::EffectOutcome::Failed {
                error_code: "missing_organization_id".to_owned(),
            };
        };
        let Some(subject_kind) = effect.subject_kind.as_deref() else {
            return storage::EffectOutcome::Failed {
                error_code: "missing_entitlement_subject_kind".to_owned(),
            };
        };
        let subject = if subject_kind == "organization" {
            organization_id
        } else if subject_kind == "owner" {
            &effect.owner_subject
        } else {
            return storage::EffectOutcome::Failed {
                error_code: "invalid_entitlement_subject_kind".to_owned(),
            };
        };
        let Some(feature) = effect.feature.as_deref() else {
            return storage::EffectOutcome::Failed {
                error_code: "missing_entitlement_feature".to_owned(),
            };
        };
        match self
            .entitlements
            .put_grant_with_context(
                context.clone(),
                entitlements::PutGrantRequest {
                    scope_kind: "organization".to_owned(),
                    scope_id: organization_id.to_owned(),
                    subject: subject.to_owned(),
                    feature: feature.to_owned(),
                    limit: effect.limit.map(|value| value.to_string()),
                    expires_at: None,
                },
            )
            .await
        {
            Ok(response) => storage::EffectOutcome::EntitlementApplied {
                grant_id: response.grant_id.clone(),
                receipt: serde_json::json!({
                    "grant_id": response.grant_id,
                    "changed": response.changed,
                    "policy_revision": response.policy_revision,
                }),
            },
            Err(entitlements::EntitlementsAdminPutGrantInvocationError::Domain(error)) => {
                storage::EffectOutcome::Failed {
                    error_code: put_grant_error_code(&error),
                }
            }
            Err(entitlements::EntitlementsAdminPutGrantInvocationError::Runtime(_)) => {
                storage::EffectOutcome::Unknown {
                    error_code: "entitlement_put_outcome_unknown".to_owned(),
                }
            }
        }
    }

    async fn execute_revoke_entitlement(
        &self,
        context: &Ctx,
        effect: &storage::EffectRecord,
    ) -> storage::EffectOutcome {
        let Some(grant_id) = effect.external_id.as_deref() else {
            return storage::EffectOutcome::Failed {
                error_code: "missing_cleanup_grant_id".to_owned(),
            };
        };
        match self
            .entitlements
            .revoke_grant_with_context(
                context.clone(),
                entitlements::RevokeGrantRequest {
                    grant_id: grant_id.to_owned(),
                },
            )
            .await
        {
            Ok(response) => storage::EffectOutcome::Compensated {
                receipt: serde_json::json!({
                    "grant_id": grant_id,
                    "changed": response.changed,
                    "policy_revision": response.policy_revision,
                }),
            },
            Err(entitlements::EntitlementsAdminRevokeGrantInvocationError::Domain(
                entitlements::RevokeGrantError::NotFound,
            )) => storage::EffectOutcome::Compensated {
                receipt: serde_json::json!({
                    "grant_id": grant_id,
                    "already_absent": true,
                }),
            },
            Err(entitlements::EntitlementsAdminRevokeGrantInvocationError::Domain(error)) => {
                storage::EffectOutcome::Failed {
                    error_code: revoke_grant_error_code(&error),
                }
            }
            Err(entitlements::EntitlementsAdminRevokeGrantInvocationError::Runtime(_)) => {
                storage::EffectOutcome::Unknown {
                    error_code: "entitlement_revoke_outcome_unknown".to_owned(),
                }
            }
        }
    }

    fn authorize_public<E: PublicRoleError>(
        &self,
        context: &Ctx,
        capability: &str,
        operation: &str,
    ) -> Result<(String, String), PluginError<E>> {
        let caller = allowed_caller(context, &self.config.management_callers)
            .ok_or_else(|| PluginError::domain(E::forbidden()))?;
        let actor = self
            .authenticated_subject(context, capability, operation)
            .map_err(|()| PluginError::domain(E::unauthenticated()))?;
        Ok((caller, actor))
    }

    fn authorize_worker(
        &self,
        context: &Ctx,
        operation: &str,
    ) -> Result<(String, String), PluginError<worker::ProcessDueError>> {
        let caller = allowed_caller(context, &self.config.worker_callers)
            .ok_or_else(|| PluginError::domain(worker::ProcessDueError::Forbidden))?;
        let actor = self
            .authenticated_subject(context, worker::CAPABILITY_ID, operation)
            .map_err(|()| PluginError::domain(worker::ProcessDueError::Unauthenticated))?;
        Ok((caller, actor))
    }

    fn authenticated_subject(
        &self,
        context: &Ctx,
        capability: &str,
        operation: &str,
    ) -> Result<String, ()> {
        let actor = self
            .config
            .verifier()
            .map_err(|_| ())?
            .project_context::<ProvisioningActor>(context, capability, operation, &UtcClock)
            .map_err(|_| ())?;
        valid_opaque_id(&actor.subject, MAX_ID_BYTES)
            .then_some(actor.subject)
            .ok_or(())
    }

    fn prepared(&self) -> Result<PreparedOrganizationProvisioning, RuntimeFailure> {
        self.prepared
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Organization Provisioning Plugin is not prepared".to_owned(),
            })
    }
}

impl Lifecycle for PostgresOrganizationProvisioningPlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let database_url = resolve_secret(
            &self.secrets,
            context.dependencies(),
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        self.prepared
            .borrow_mut()
            .replace(PreparedOrganizationProvisioning { postgres });
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ProvisioningActor {
    subject: String,
}

impl TypedActor for ProvisioningActor {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        if !matches!(assertion.actor_kind(), "user" | "service_account") {
            return Err(ActorProjectionError::UnexpectedActorKind {
                expected: "user or service_account".to_owned(),
                actual: assertion.actor_kind().to_owned(),
            });
        }
        Ok(Self {
            subject: assertion.subject().to_owned(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct UtcClock;

impl AssertionClock for UtcClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

trait PublicRoleError: Sized {
    fn unauthenticated() -> Self;
    fn forbidden() -> Self;
    fn from_storage(failure: storage::DomainFailure) -> Self;
}

macro_rules! impl_public_error {
    ($($error:path),+ $(,)?) => {
        $(impl PublicRoleError for $error {
            fn unauthenticated() -> Self { Self::Unauthenticated }
            fn forbidden() -> Self { Self::Forbidden }
            fn from_storage(failure: storage::DomainFailure) -> Self {
                match failure {
                    storage::DomainFailure::ProvisioningNotFound => Self::ProvisioningNotFound,
                    storage::DomainFailure::RevisionConflict => Self::RevisionConflict,
                    storage::DomainFailure::IdempotencyConflict => Self::IdempotencyConflict,
                    storage::DomainFailure::InvalidTransition => Self::InvalidTransition,
                    storage::DomainFailure::ManualResolutionRequired => Self::ManualResolutionRequired,
                }
            }
        })+
    };
}

impl_public_error!(
    public::StartError,
    public::GetError,
    public::ListError,
    public::RetryError,
    public::RequestCleanupError,
);

async fn resolve_secret(
    secrets: &SecretsClient,
    dependencies: &PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|response| Zeroizing::new(response.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("Organization Provisioning secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}

fn request_hash<T: Serialize, E>(request: &T) -> Result<Vec<u8>, PluginError<E>> {
    serde_json::to_vec(request)
        .map(|wire| Sha256::digest(wire).to_vec())
        .map_err(serialization_runtime)
}

fn wire_cast<T: DeserializeOwned, E>(value: &impl Serialize) -> Result<T, PluginError<E>> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(serialization_runtime)
}

#[allow(clippy::needless_pass_by_value)]
fn serialization_runtime<E>(error: serde_json::Error) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::Internal {
        detail: format!("Organization Provisioning wire serialization failed: {error}"),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn storage_runtime<E>(error: storage::StorageError) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    })
}

fn allowed_caller(context: &Ctx, allowed: &[String]) -> Option<String> {
    context.caller_instance().and_then(|caller| {
        allowed
            .iter()
            .any(|entry| entry == caller)
            .then(|| caller.to_owned())
    })
}

fn parse_mutation(
    provisioning_id: &str,
    expected_revision: &str,
    idempotency_key: &str,
    evidence: &str,
) -> Option<(Uuid, i64)> {
    if !valid_idempotency_key(idempotency_key) || !valid_text(evidence, MAX_EVIDENCE_BYTES) {
        return None;
    }
    Some((
        Uuid::parse_str(provisioning_id).ok()?,
        expected_revision.parse().ok().filter(|value| *value > 0)?,
    ))
}

fn valid_organization_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 200 && !value.chars().any(char::is_control)
}

fn valid_slug(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !value.starts_with('-')
        && !value.ends_with('-')
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum
        && !value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

fn valid_dimension(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn valid_idempotency_key(value: &str) -> bool {
    valid_opaque_id(value, MAX_IDEMPOTENCY_BYTES)
}

fn valid_opaque_id(value: &str, maximum: usize) -> bool {
    valid_dimension(value, maximum)
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    valid_opaque_id(value, maximum) && !value.contains('/')
}

fn valid_secret_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.starts_with('/')
        && !reference.ends_with('/')
        && !reference.contains("//")
        && reference
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && valid_opaque_id(reference, 256)
}

fn validate_callers(callers: &[String]) -> Result<(), ()> {
    if callers.is_empty()
        || callers.len() > MAX_CALLERS
        || callers.iter().any(|caller| !valid_identifier(caller, 256))
        || callers.iter().collect::<BTreeSet<_>>().len() != callers.len()
    {
        Err(())
    } else {
        Ok(())
    }
}

fn valid_status(value: &str) -> bool {
    matches!(
        value,
        "pending"
            | "running"
            | "completed"
            | "failed"
            | "manual_review"
            | "cleanup_pending"
            | "cleanup_running"
            | "cleanup_completed"
            | "cleanup_failed"
    )
}

fn organization_error_code(error: &organization::CreateOrganizationError) -> String {
    match error {
        organization::CreateOrganizationError::Forbidden => "organization_forbidden".to_owned(),
        organization::CreateOrganizationError::IdempotencyConflict => {
            "organization_idempotency_conflict".to_owned()
        }
        organization::CreateOrganizationError::InvalidOrganization => {
            "organization_invalid".to_owned()
        }
        organization::CreateOrganizationError::SlugConflict => {
            "organization_slug_conflict".to_owned()
        }
        organization::CreateOrganizationError::Unknown(value) => {
            bounded_external_code("organization_domain", &value.code)
        }
    }
}

fn put_grant_error_code(error: &entitlements::PutGrantError) -> String {
    match error {
        entitlements::PutGrantError::Forbidden => "entitlement_put_forbidden".to_owned(),
        entitlements::PutGrantError::InvalidGrant => "entitlement_put_invalid".to_owned(),
        entitlements::PutGrantError::Unknown(value) => {
            bounded_external_code("entitlement_put_domain", &value.code)
        }
    }
}

fn revoke_grant_error_code(error: &entitlements::RevokeGrantError) -> String {
    match error {
        entitlements::RevokeGrantError::Forbidden => "entitlement_revoke_forbidden".to_owned(),
        entitlements::RevokeGrantError::InvalidGrant => "entitlement_revoke_invalid".to_owned(),
        entitlements::RevokeGrantError::NotFound => "entitlement_revoke_not_found".to_owned(),
        entitlements::RevokeGrantError::Unknown(value) => {
            bounded_external_code("entitlement_revoke_domain", &value.code)
        }
    }
}

fn bounded_external_code(prefix: &str, value: &str) -> String {
    let mut result = String::with_capacity(prefix.len() + 1 + value.len().min(128));
    result.push_str(prefix);
    result.push(':');
    result.extend(
        value
            .chars()
            .filter(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
            })
            .take(128),
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_auth_sdk::ActorAssertionIssuer;

    fn config() -> OrganizationProvisioningConfig {
        let issuer = ActorAssertionIssuer::new("auth.users", b"provisioning-test-key");
        OrganizationProvisioningConfig::new(
            "organization_provisioning",
            "organization-provisioning/database-url",
            "auth.users",
            issuer.public_key_base64(),
            vec!["provisioning-api".to_owned()],
            vec!["provisioning-worker".to_owned()],
            vec![
                BootstrapGrantConfig::new("organization", "projects", Some("100".to_owned())),
                BootstrapGrantConfig::new("owner", "admin-console", None),
            ],
        )
        .unwrap()
    }

    #[test]
    fn configuration_snapshots_unique_bounded_grants() {
        let config = config();
        let grants = config.grant_snapshots().unwrap();
        assert_eq!(grants.len(), 2);
        assert_eq!(grants[0].limit, Some(100));

        let mut duplicate = config;
        duplicate.bootstrap_grants.push(BootstrapGrantConfig::new(
            "organization",
            "projects",
            None,
        ));
        assert_eq!(
            duplicate.validate(),
            Err(OrganizationProvisioningConfigError::InvalidBootstrapGrants)
        );
    }

    #[test]
    fn identifiers_and_statuses_are_fail_closed() {
        assert!(valid_slug("acme-labs"));
        assert!(!valid_slug("Acme"));
        assert!(valid_status("manual_review"));
        assert!(!valid_status("retrying"));
        assert_eq!(
            bounded_external_code("downstream", "bad code/with spaces"),
            "downstream:badcodewithspaces"
        );
    }
}
