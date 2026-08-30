use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use thiserror::Error;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("{operation}: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("invalid stored Organization Provisioning data: {detail}")]
    InvalidStoredData { detail: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DomainFailure {
    ProvisioningNotFound,
    RevisionConflict,
    IdempotencyConflict,
    InvalidTransition,
    ManualResolutionRequired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BootstrapGrant {
    pub(crate) subject_kind: String,
    pub(crate) feature: String,
    pub(crate) limit: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ProvisioningView {
    pub(crate) provisioning_id: Uuid,
    pub(crate) status: String,
    #[serde(with = "decimal_i64")]
    pub(crate) revision: i64,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) owner_subject: String,
    pub(crate) requested_by: String,
    pub(crate) organization_id: Option<String>,
    pub(crate) owner_membership_id: Option<String>,
    pub(crate) cleanup_requested: bool,
    pub(crate) organization_residue: bool,
    pub(crate) last_error: Option<String>,
    pub(crate) steps: Vec<StepView>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) completed_at: Option<String>,
    pub(crate) cleanup_completed_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct StepView {
    pub(crate) effect_id: Uuid,
    pub(crate) kind: String,
    pub(crate) ordinal: i32,
    pub(crate) status: String,
    pub(crate) attempt: i32,
    pub(crate) external_id: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) updated_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ProvisioningListItem {
    pub(crate) provisioning_id: Uuid,
    pub(crate) status: String,
    #[serde(with = "decimal_i64")]
    pub(crate) revision: i64,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) owner_subject: String,
    pub(crate) organization_id: Option<String>,
    pub(crate) cleanup_requested: bool,
    pub(crate) organization_residue: bool,
    pub(crate) last_error: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PageCursor {
    created_at: OffsetDateTime,
    provisioning_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryResolution {
    RetryFailed,
    ConfirmApplied,
    ConfirmNotApplied,
}

#[derive(Clone, Debug)]
pub(crate) struct RetryInput<'a> {
    pub(crate) caller: &'a str,
    pub(crate) actor: &'a str,
    pub(crate) idempotency_key: &'a str,
    pub(crate) request_hash: &'a [u8],
    pub(crate) provisioning_id: Uuid,
    pub(crate) expected_revision: i64,
    pub(crate) resolution: RetryResolution,
    pub(crate) observed_organization_id: Option<&'a str>,
    pub(crate) observed_owner_membership_id: Option<&'a str>,
    pub(crate) observed_grant_id: Option<&'a str>,
    pub(crate) evidence: &'a str,
}

#[derive(Clone, Debug)]
pub(crate) struct CleanupInput<'a> {
    pub(crate) caller: &'a str,
    pub(crate) actor: &'a str,
    pub(crate) idempotency_key: &'a str,
    pub(crate) request_hash: &'a [u8],
    pub(crate) provisioning_id: Uuid,
    pub(crate) expected_revision: i64,
    pub(crate) evidence: &'a str,
}

#[derive(Clone, Debug)]
pub(crate) struct EffectRecord {
    pub(crate) effect_id: Uuid,
    pub(crate) provisioning_id: Uuid,
    pub(crate) kind: String,
    pub(crate) downstream_key: String,
    pub(crate) subject_kind: Option<String>,
    pub(crate) feature: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) external_id: Option<String>,
    pub(crate) lease_token: Uuid,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) owner_subject: String,
    pub(crate) organization_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum EffectOutcome {
    OrganizationApplied {
        organization_id: String,
        owner_membership_id: String,
        receipt: serde_json::Value,
    },
    EntitlementApplied {
        grant_id: String,
        receipt: serde_json::Value,
    },
    Compensated {
        receipt: serde_json::Value,
    },
    Failed {
        error_code: String,
    },
    Unknown {
        error_code: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FinalizeResult {
    Applied,
    Failed,
    ManualReview,
    Superseded,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    provisioning_id: Uuid,
    name: &str,
    slug: &str,
    owner_subject: &str,
    grants: &[BootstrapGrant],
) -> Result<Result<ProvisioningView, DomainFailure>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin provisioning start", source))?;
    match begin_mutation::<ProvisioningView>(
        &mut transaction,
        caller,
        idempotency_key,
        "start",
        actor,
        request_hash,
        None,
    )
    .await?
    {
        Err(failure) => return Ok(Err(failure)),
        Ok(MutationReplay::Replay(response)) => {
            transaction
                .commit()
                .await
                .map_err(|source| database("commit provisioning start replay", source))?;
            return Ok(Ok(response));
        }
        Ok(MutationReplay::Execute) => {}
    }

    sqlx::query(
        "INSERT INTO organization_provisionings(provisioning_id,requested_by,name,slug,owner_subject,status,revision) VALUES($1,$2,$3,$4,$5,'pending',1)",
    )
    .bind(provisioning_id)
    .bind(actor)
    .bind(name)
    .bind(slug)
    .bind(owner_subject)
    .execute(&mut *transaction)
    .await
    .map_err(|source| database("insert provisioning saga", source))?;
    sqlx::query(
        "UPDATE organization_provisioning_mutations SET provisioning_id=$3 WHERE caller_instance=$1 AND idempotency_key=$2",
    )
    .bind(caller)
    .bind(idempotency_key)
    .bind(provisioning_id)
    .execute(&mut *transaction)
    .await
    .map_err(|source| database("attach start mutation to provisioning saga", source))?;

    let create_effect_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO organization_provisioning_effects(effect_id,provisioning_id,kind,ordinal,state,downstream_key) VALUES($1,$2,'create_organization',0,'pending',$3)",
    )
    .bind(create_effect_id)
    .bind(provisioning_id)
    .bind(format!("organization-provisioning:{provisioning_id}:create:v1"))
    .execute(&mut *transaction)
    .await
    .map_err(|source| database("insert organization creation effect", source))?;

    for (ordinal, grant) in grants.iter().enumerate() {
        let ordinal =
            i32::try_from(ordinal).map_err(|_| invalid_data("bootstrap grant ordinal overflow"))?;
        sqlx::query(
            "INSERT INTO organization_provisioning_effects(effect_id,provisioning_id,kind,ordinal,state,downstream_key,subject_kind,feature,limit_value) VALUES($1,$2,'put_entitlement',$3,'pending',$4,$5,$6,$7)",
        )
        .bind(Uuid::new_v4())
        .bind(provisioning_id)
        .bind(ordinal)
        .bind(format!(
            "organization-provisioning:{provisioning_id}:entitlement:{ordinal}:v1"
        ))
        .bind(&grant.subject_kind)
        .bind(&grant.feature)
        .bind(grant.limit)
        .execute(&mut *transaction)
        .await
        .map_err(|source| database("insert entitlement bootstrap effect", source))?;
    }

    insert_activity(
        &mut transaction,
        provisioning_id,
        None,
        "provisioning_started",
        actor,
        1,
        serde_json::json!({"bootstrap_grants": grants.len()}),
    )
    .await?;
    let response = load_view_tx(&mut transaction, provisioning_id, actor)
        .await?
        .ok_or_else(|| invalid_data("new provisioning saga disappeared"))?;
    complete_mutation(&mut transaction, caller, idempotency_key, &response).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit provisioning start", source))?;
    Ok(Ok(response))
}

pub(crate) async fn get(
    postgres: &OwnedPostgres,
    provisioning_id: Uuid,
    actor: &str,
) -> Result<Option<ProvisioningView>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin provisioning read", source))?;
    let view = load_view_tx(&mut transaction, provisioning_id, actor).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit provisioning read", source))?;
    Ok(view)
}

pub(crate) struct ListFilters<'a> {
    pub(crate) actor: &'a str,
    pub(crate) status: Option<&'a str>,
    pub(crate) cursor: Option<&'a PageCursor>,
    pub(crate) limit: i64,
}

pub(crate) async fn list(
    postgres: &OwnedPostgres,
    filters: &ListFilters<'_>,
) -> Result<Vec<ProvisioningListItem>, StorageError> {
    let cursor_time = filters.cursor.map(|cursor| cursor.created_at);
    let cursor_id = filters.cursor.map(|cursor| cursor.provisioning_id);
    let rows = sqlx::query(
        "SELECT provisioning_id,status,revision,name,slug,owner_subject,organization_id,cleanup_requested,organization_residue,last_error,created_at,updated_at FROM organization_provisionings WHERE requested_by=$1 AND ($2::text IS NULL OR status=$2) AND ($3::timestamptz IS NULL OR (created_at,provisioning_id)<($3,$4)) ORDER BY created_at DESC,provisioning_id DESC LIMIT $5",
    )
    .bind(filters.actor)
    .bind(filters.status)
    .bind(cursor_time)
    .bind(cursor_id)
    .bind(filters.limit)
    .fetch_all(postgres.pool())
    .await
    .map_err(|source| database("list provisioning sagas", source))?;
    rows.iter().map(list_item_from_row).collect()
}

pub(crate) fn encode_cursor(item: &ProvisioningListItem) -> Result<String, StorageError> {
    let created_at = OffsetDateTime::parse(&item.created_at, &Rfc3339)
        .map_err(|error| invalid_data(format!("list cursor time is invalid: {error}")))?;
    Ok(format!(
        "{}:{}",
        created_at.unix_timestamp_nanos() / 1_000,
        item.provisioning_id
    ))
}

pub(crate) fn decode_cursor(value: &str) -> Option<PageCursor> {
    let (micros, provisioning_id) = value.split_once(':')?;
    let nanos = micros.parse::<i128>().ok()?.checked_mul(1_000)?;
    Some(PageCursor {
        created_at: OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?,
        provisioning_id: Uuid::parse_str(provisioning_id).ok()?,
    })
}

pub(crate) async fn retry(
    postgres: &OwnedPostgres,
    input: &RetryInput<'_>,
) -> Result<Result<ProvisioningView, DomainFailure>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin provisioning retry", source))?;
    let saga = lock_saga(&mut transaction, input.provisioning_id, input.actor)
        .await?
        .ok_or(DomainFailure::ProvisioningNotFound);
    let saga = match saga {
        Ok(saga) => saga,
        Err(failure) => return Ok(Err(failure)),
    };
    match begin_mutation::<ProvisioningView>(
        &mut transaction,
        input.caller,
        input.idempotency_key,
        "retry",
        input.actor,
        input.request_hash,
        Some(input.provisioning_id),
    )
    .await?
    {
        Err(failure) => return Ok(Err(failure)),
        Ok(MutationReplay::Replay(response)) => {
            transaction
                .commit()
                .await
                .map_err(|source| database("commit provisioning retry replay", source))?;
            return Ok(Ok(response));
        }
        Ok(MutationReplay::Execute) => {}
    }
    if saga.revision != input.expected_revision {
        return Ok(Err(DomainFailure::RevisionConflict));
    }

    let effect = if saga.status == "manual_review" {
        sqlx::query(
            "SELECT effect_id,kind FROM organization_provisioning_effects WHERE provisioning_id=$1 AND state='unknown' ORDER BY created_at ASC,effect_id ASC LIMIT 1 FOR UPDATE",
        )
        .bind(input.provisioning_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| database("lock unknown provisioning effect", source))?
        .ok_or(DomainFailure::ManualResolutionRequired)
    } else if matches!(saga.status.as_str(), "failed" | "cleanup_failed") {
        if input.resolution != RetryResolution::RetryFailed {
            return Ok(Err(DomainFailure::InvalidTransition));
        }
        sqlx::query(
            "SELECT effect_id,kind FROM organization_provisioning_effects WHERE provisioning_id=$1 AND state='failed' ORDER BY created_at ASC,effect_id ASC LIMIT 1 FOR UPDATE",
        )
        .bind(input.provisioning_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| database("lock failed provisioning effect", source))?
        .ok_or(DomainFailure::InvalidTransition)
    } else {
        Err(DomainFailure::InvalidTransition)
    };
    let effect = match effect {
        Ok(effect) => effect,
        Err(failure) => return Ok(Err(failure)),
    };
    let effect_id: Uuid = effect
        .try_get("effect_id")
        .map_err(|source| database("decode retry effect id", source))?;
    let kind: String = effect
        .try_get("kind")
        .map_err(|source| database("decode retry effect kind", source))?;

    match input.resolution {
        RetryResolution::RetryFailed => {
            if saga.status == "manual_review"
                || input.observed_organization_id.is_some()
                || input.observed_owner_membership_id.is_some()
                || input.observed_grant_id.is_some()
            {
                return Ok(Err(DomainFailure::ManualResolutionRequired));
            }
            reset_effect_to_pending(&mut transaction, effect_id).await?;
        }
        RetryResolution::ConfirmNotApplied => {
            if saga.status != "manual_review"
                || input.observed_organization_id.is_some()
                || input.observed_owner_membership_id.is_some()
                || input.observed_grant_id.is_some()
            {
                return Ok(Err(DomainFailure::InvalidTransition));
            }
            reset_effect_to_pending(&mut transaction, effect_id).await?;
        }
        RetryResolution::ConfirmApplied => match kind.as_str() {
            "create_organization" => {
                let (Some(organization_id), Some(owner_membership_id), None) = (
                    input.observed_organization_id,
                    input.observed_owner_membership_id,
                    input.observed_grant_id,
                ) else {
                    return Ok(Err(DomainFailure::ManualResolutionRequired));
                };
                sqlx::query(
                    "UPDATE organization_provisioning_effects SET state='applied',external_id=$2,last_error=NULL,lease_token=NULL,lease_until=NULL,provider_receipt=$3,completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='unknown'",
                )
                .bind(effect_id)
                .bind(organization_id)
                .bind(serde_json::json!({"manual_confirmation": true, "owner_membership_id": owner_membership_id}))
                .execute(&mut *transaction)
                .await
                .map_err(|source| database("confirm organization creation effect", source))?;
                sqlx::query(
                    "UPDATE organization_provisionings SET organization_id=$2,owner_membership_id=$3,organization_residue=TRUE WHERE provisioning_id=$1",
                )
                .bind(input.provisioning_id)
                .bind(organization_id)
                .bind(owner_membership_id)
                .execute(&mut *transaction)
                .await
                .map_err(|source| database("record manually confirmed organization", source))?;
            }
            "put_entitlement" => {
                let (None, None, Some(grant_id)) = (
                    input.observed_organization_id,
                    input.observed_owner_membership_id,
                    input.observed_grant_id,
                ) else {
                    return Ok(Err(DomainFailure::ManualResolutionRequired));
                };
                sqlx::query(
                    "UPDATE organization_provisioning_effects SET state='applied',external_id=$2,last_error=NULL,lease_token=NULL,lease_until=NULL,provider_receipt=$3,completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='unknown'",
                )
                .bind(effect_id)
                .bind(grant_id)
                .bind(serde_json::json!({"manual_confirmation": true}))
                .execute(&mut *transaction)
                .await
                .map_err(|source| database("confirm entitlement grant effect", source))?;
            }
            "revoke_entitlement" => {
                if input.observed_organization_id.is_some()
                    || input.observed_owner_membership_id.is_some()
                    || input.observed_grant_id.is_some()
                {
                    return Ok(Err(DomainFailure::ManualResolutionRequired));
                }
                sqlx::query(
                    "UPDATE organization_provisioning_effects SET state='compensated',last_error=NULL,lease_token=NULL,lease_until=NULL,provider_receipt=$2,completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='unknown'",
                )
                .bind(effect_id)
                .bind(serde_json::json!({"manual_confirmation": true}))
                .execute(&mut *transaction)
                .await
                .map_err(|source| database("confirm entitlement revocation effect", source))?;
            }
            _ => return Err(invalid_data("unknown effect kind during manual resolution")),
        },
    }

    let revision = refresh_saga_status(&mut transaction, input.provisioning_id, true).await?;
    insert_activity(
        &mut transaction,
        input.provisioning_id,
        Some(effect_id),
        "provisioning_retried",
        input.actor,
        revision,
        serde_json::json!({
            "resolution": retry_resolution_name(input.resolution),
            "evidence": input.evidence,
            "effect_kind": kind,
        }),
    )
    .await?;
    let response = load_view_tx(&mut transaction, input.provisioning_id, input.actor)
        .await?
        .ok_or_else(|| invalid_data("retried provisioning saga disappeared"))?;
    complete_mutation(
        &mut transaction,
        input.caller,
        input.idempotency_key,
        &response,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit provisioning retry", source))?;
    Ok(Ok(response))
}

pub(crate) async fn request_cleanup(
    postgres: &OwnedPostgres,
    input: &CleanupInput<'_>,
) -> Result<Result<ProvisioningView, DomainFailure>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin provisioning cleanup request", source))?;
    let saga = lock_saga(&mut transaction, input.provisioning_id, input.actor)
        .await?
        .ok_or(DomainFailure::ProvisioningNotFound);
    let saga = match saga {
        Ok(saga) => saga,
        Err(failure) => return Ok(Err(failure)),
    };
    match begin_mutation::<ProvisioningView>(
        &mut transaction,
        input.caller,
        input.idempotency_key,
        "request_cleanup",
        input.actor,
        input.request_hash,
        Some(input.provisioning_id),
    )
    .await?
    {
        Err(failure) => return Ok(Err(failure)),
        Ok(MutationReplay::Replay(response)) => {
            transaction
                .commit()
                .await
                .map_err(|source| database("commit provisioning cleanup request replay", source))?;
            return Ok(Ok(response));
        }
        Ok(MutationReplay::Execute) => {}
    }
    if saga.revision != input.expected_revision {
        return Ok(Err(DomainFailure::RevisionConflict));
    }
    if saga.cleanup_requested {
        return Ok(Err(DomainFailure::InvalidTransition));
    }
    if saga.status == "manual_review" {
        return Ok(Err(DomainFailure::ManualResolutionRequired));
    }
    if !matches!(saga.status.as_str(), "completed" | "failed") {
        return Ok(Err(DomainFailure::InvalidTransition));
    }

    let grants = sqlx::query(
        "SELECT effect_id,ordinal,subject_kind,feature,limit_value,external_id FROM organization_provisioning_effects WHERE provisioning_id=$1 AND kind='put_entitlement' AND state='applied' ORDER BY ordinal ASC,effect_id ASC FOR UPDATE",
    )
    .bind(input.provisioning_id)
    .fetch_all(&mut *transaction)
    .await
    .map_err(|source| database("lock applied bootstrap grants for cleanup", source))?;
    sqlx::query(
        "UPDATE organization_provisioning_effects SET state='skipped',last_error='cleanup_requested',updated_at=CURRENT_TIMESTAMP WHERE provisioning_id=$1 AND kind='put_entitlement' AND state IN ('pending','failed')",
    )
    .bind(input.provisioning_id)
    .execute(&mut *transaction)
    .await
    .map_err(|source| database("skip unfinished bootstrap grants", source))?;

    for row in &grants {
        let source_effect_id: Uuid = row
            .try_get("effect_id")
            .map_err(|source| database("decode cleanup source effect", source))?;
        let ordinal: i32 = row
            .try_get("ordinal")
            .map_err(|source| database("decode cleanup grant ordinal", source))?;
        let subject_kind: String = row
            .try_get("subject_kind")
            .map_err(|source| database("decode cleanup subject kind", source))?;
        let feature: String = row
            .try_get("feature")
            .map_err(|source| database("decode cleanup feature", source))?;
        let limit: Option<i64> = row
            .try_get("limit_value")
            .map_err(|source| database("decode cleanup grant limit", source))?;
        let grant_id: String = row
            .try_get::<Option<String>, _>("external_id")
            .map_err(|source| database("decode cleanup grant id", source))?
            .ok_or_else(|| invalid_data("applied entitlement has no grant id"))?;
        sqlx::query(
            "INSERT INTO organization_provisioning_effects(effect_id,provisioning_id,kind,ordinal,state,downstream_key,subject_kind,feature,limit_value,source_effect_id,external_id) VALUES($1,$2,'revoke_entitlement',$3,'pending',$4,$5,$6,$7,$8,$9)",
        )
        .bind(Uuid::new_v4())
        .bind(input.provisioning_id)
        .bind(ordinal)
        .bind(format!(
            "organization-provisioning:{}:cleanup:{ordinal}:v1",
            input.provisioning_id
        ))
        .bind(subject_kind)
        .bind(feature)
        .bind(limit)
        .bind(source_effect_id)
        .bind(grant_id)
        .execute(&mut *transaction)
        .await
        .map_err(|source| database("insert entitlement cleanup effect", source))?;
    }
    sqlx::query(
        "UPDATE organization_provisionings SET cleanup_requested=TRUE,last_error=NULL WHERE provisioning_id=$1",
    )
    .bind(input.provisioning_id)
    .execute(&mut *transaction)
    .await
    .map_err(|source| database("mark provisioning cleanup requested", source))?;
    let revision = refresh_saga_status(&mut transaction, input.provisioning_id, true).await?;
    insert_activity(
        &mut transaction,
        input.provisioning_id,
        None,
        "cleanup_requested",
        input.actor,
        revision,
        serde_json::json!({
            "reversible_grants": grants.len(),
            "organization_residue": saga.organization_id.is_some(),
            "evidence": input.evidence,
        }),
    )
    .await?;
    let response = load_view_tx(&mut transaction, input.provisioning_id, input.actor)
        .await?
        .ok_or_else(|| invalid_data("cleanup provisioning saga disappeared"))?;
    complete_mutation(
        &mut transaction,
        input.caller,
        input.idempotency_key,
        &response,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit provisioning cleanup request", source))?;
    Ok(Ok(response))
}

pub(crate) async fn quarantine_expired(
    postgres: &OwnedPostgres,
    actor: &str,
    limit: i64,
) -> Result<usize, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin expired effect quarantine", source))?;
    let mut quarantined = 0_usize;
    for _ in 0..limit {
        let saga = sqlx::query(
            "SELECT p.provisioning_id FROM organization_provisionings p WHERE p.status IN ('running','cleanup_running') AND EXISTS (SELECT 1 FROM organization_provisioning_effects e WHERE e.provisioning_id=p.provisioning_id AND e.state='in_flight' AND e.lease_until<CURRENT_TIMESTAMP) ORDER BY p.updated_at ASC,p.provisioning_id ASC LIMIT 1 FOR UPDATE OF p SKIP LOCKED",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| database("claim saga with expired effect", source))?;
        let Some(saga) = saga else {
            break;
        };
        let provisioning_id: Uuid = saga
            .try_get("provisioning_id")
            .map_err(|source| database("decode expired effect saga", source))?;
        let effect = sqlx::query(
            "SELECT effect_id,kind,lease_token FROM organization_provisioning_effects WHERE provisioning_id=$1 AND state='in_flight' AND lease_until<CURRENT_TIMESTAMP ORDER BY updated_at ASC,effect_id ASC LIMIT 1 FOR UPDATE",
        )
        .bind(provisioning_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| database("lock expired provisioning effect", source))?
        .ok_or_else(|| invalid_data("expired effect disappeared while saga was locked"))?;
        let effect_id: Uuid = effect
            .try_get("effect_id")
            .map_err(|source| database("decode expired effect id", source))?;
        let kind: String = effect
            .try_get("kind")
            .map_err(|source| database("decode expired effect kind", source))?;
        let previous_fence: Uuid = effect
            .try_get("lease_token")
            .map_err(|source| database("decode expired effect fence", source))?;
        sqlx::query(
            "UPDATE organization_provisioning_effects SET state='unknown',lease_token=NULL,lease_until=NULL,last_error='effect_outcome_unknown_after_lease_expiry',updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1",
        )
        .bind(effect_id)
        .execute(&mut *transaction)
        .await
        .map_err(|source| database("quarantine expired provisioning effect", source))?;
        let revision: i64 = sqlx::query_scalar(
            "UPDATE organization_provisionings SET status='manual_review',revision=revision+1,last_error='effect_outcome_unknown_after_lease_expiry',updated_at=CURRENT_TIMESTAMP WHERE provisioning_id=$1 RETURNING revision",
        )
        .bind(provisioning_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| database("mark expired effect for manual review", source))?;
        insert_activity(
            &mut transaction,
            provisioning_id,
            Some(effect_id),
            "effect_outcome_unknown",
            actor,
            revision,
            serde_json::json!({
                "effect_kind": kind,
                "reason": "lease_expired",
                "invalidated_fence": previous_fence,
            }),
        )
        .await?;
        quarantined = quarantined.saturating_add(1);
    }
    transaction
        .commit()
        .await
        .map_err(|source| database("commit expired effect quarantine", source))?;
    Ok(quarantined)
}

pub(crate) async fn claim_next(
    postgres: &OwnedPostgres,
    actor: &str,
    lease_seconds: i64,
) -> Result<Option<EffectRecord>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin provisioning effect claim", source))?;
    let saga = sqlx::query(
        "SELECT p.provisioning_id,p.cleanup_requested FROM organization_provisionings p WHERE ((p.status='pending' AND EXISTS (SELECT 1 FROM organization_provisioning_effects e WHERE e.provisioning_id=p.provisioning_id AND e.state='pending' AND e.kind IN ('create_organization','put_entitlement'))) OR (p.status='cleanup_pending' AND EXISTS (SELECT 1 FROM organization_provisioning_effects e WHERE e.provisioning_id=p.provisioning_id AND e.state='pending' AND e.kind='revoke_entitlement'))) ORDER BY p.updated_at ASC,p.provisioning_id ASC LIMIT 1 FOR UPDATE OF p SKIP LOCKED",
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|source| database("claim due provisioning saga", source))?;
    let Some(saga) = saga else {
        transaction
            .commit()
            .await
            .map_err(|source| database("commit empty provisioning effect claim", source))?;
        return Ok(None);
    };
    let provisioning_id: Uuid = saga
        .try_get("provisioning_id")
        .map_err(|source| database("decode claimed provisioning saga", source))?;
    let cleanup_requested: bool = saga
        .try_get("cleanup_requested")
        .map_err(|source| database("decode claimed cleanup phase", source))?;
    let effect = if cleanup_requested {
        sqlx::query(
            "SELECT effect_id,kind,ordinal,downstream_key,subject_kind,feature,limit_value,external_id FROM organization_provisioning_effects WHERE provisioning_id=$1 AND kind='revoke_entitlement' AND state='pending' ORDER BY ordinal ASC,effect_id ASC LIMIT 1 FOR UPDATE",
        )
        .bind(provisioning_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| database("lock entitlement cleanup effect", source))?
    } else {
        sqlx::query(
            "SELECT effect_id,kind,ordinal,downstream_key,subject_kind,feature,limit_value,external_id FROM organization_provisioning_effects WHERE provisioning_id=$1 AND kind IN ('create_organization','put_entitlement') AND state='pending' ORDER BY CASE kind WHEN 'create_organization' THEN 0 ELSE 1 END ASC,ordinal ASC,effect_id ASC LIMIT 1 FOR UPDATE",
        )
        .bind(provisioning_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| database("lock provisioning effect", source))?
    };
    let effect_id: Uuid = effect
        .try_get("effect_id")
        .map_err(|source| database("decode claimed effect id", source))?;
    let fence = Uuid::new_v4();
    let lease_until = OffsetDateTime::now_utc()
        .checked_add(Duration::seconds(lease_seconds))
        .ok_or_else(|| invalid_data("effect lease overflow"))?;
    let attempt: i32 = sqlx::query_scalar(
        "UPDATE organization_provisioning_effects SET state='in_flight',attempts=attempts+1,lease_token=$2,lease_until=$3,last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 RETURNING attempts",
    )
    .bind(effect_id)
    .bind(fence)
    .bind(lease_until)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("lease provisioning effect", source))?;
    let status = if cleanup_requested {
        "cleanup_running"
    } else {
        "running"
    };
    let revision: i64 = sqlx::query_scalar(
        "UPDATE organization_provisionings SET status=$2,revision=revision+1,last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE provisioning_id=$1 RETURNING revision",
    )
    .bind(provisioning_id)
    .bind(status)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("mark provisioning effect running", source))?;
    let kind: String = effect
        .try_get("kind")
        .map_err(|source| database("decode claimed effect kind", source))?;
    insert_activity(
        &mut transaction,
        provisioning_id,
        Some(effect_id),
        "effect_claimed",
        actor,
        revision,
        serde_json::json!({"effect_kind": kind, "attempt": attempt, "fence": fence}),
    )
    .await?;
    let saga = sqlx::query(
        "SELECT name,slug,owner_subject,organization_id FROM organization_provisionings WHERE provisioning_id=$1",
    )
    .bind(provisioning_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("read claimed provisioning inputs", source))?;
    let record = EffectRecord {
        effect_id,
        provisioning_id,
        kind,
        downstream_key: effect
            .try_get("downstream_key")
            .map_err(|source| database("decode claimed downstream key", source))?,
        subject_kind: effect
            .try_get("subject_kind")
            .map_err(|source| database("decode claimed subject kind", source))?,
        feature: effect
            .try_get("feature")
            .map_err(|source| database("decode claimed feature", source))?,
        limit: effect
            .try_get("limit_value")
            .map_err(|source| database("decode claimed grant limit", source))?,
        external_id: effect
            .try_get("external_id")
            .map_err(|source| database("decode claimed external id", source))?,
        lease_token: fence,
        name: saga
            .try_get("name")
            .map_err(|source| database("decode claimed organization name", source))?,
        slug: saga
            .try_get("slug")
            .map_err(|source| database("decode claimed organization slug", source))?,
        owner_subject: saga
            .try_get("owner_subject")
            .map_err(|source| database("decode claimed owner", source))?,
        organization_id: saga
            .try_get("organization_id")
            .map_err(|source| database("decode claimed organization id", source))?,
    };
    transaction
        .commit()
        .await
        .map_err(|source| database("commit provisioning effect claim", source))?;
    Ok(Some(record))
}

pub(crate) async fn finalize_effect(
    postgres: &OwnedPostgres,
    effect: &EffectRecord,
    actor: &str,
    outcome: EffectOutcome,
) -> Result<FinalizeResult, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin provisioning effect finalization", source))?;
    sqlx::query("SELECT provisioning_id FROM organization_provisionings WHERE provisioning_id=$1 FOR UPDATE")
        .bind(effect.provisioning_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| database("lock provisioning saga for effect finalization", source))?;
    let current = sqlx::query(
        "SELECT kind FROM organization_provisioning_effects WHERE effect_id=$1 AND state='in_flight' AND lease_token=$2 FOR UPDATE",
    )
    .bind(effect.effect_id)
    .bind(effect.lease_token)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|source| database("fence provisioning effect finalization", source))?;
    if current.is_none() {
        transaction
            .commit()
            .await
            .map_err(|source| database("commit superseded effect finalization", source))?;
        return Ok(FinalizeResult::Superseded);
    }

    let (result, activity_kind, evidence) = match outcome {
        EffectOutcome::OrganizationApplied {
            organization_id,
            owner_membership_id,
            receipt,
        } => {
            if effect.kind != "create_organization" {
                return Err(invalid_data(
                    "organization receipt applied to non-organization effect",
                ));
            }
            sqlx::query(
                "UPDATE organization_provisioning_effects SET state='applied',external_id=$2,lease_token=NULL,lease_until=NULL,last_error=NULL,provider_receipt=$3,completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='in_flight' AND lease_token=$4",
            )
            .bind(effect.effect_id)
            .bind(&organization_id)
            .bind(&receipt)
            .bind(effect.lease_token)
            .execute(&mut *transaction)
            .await
            .map_err(|source| database("complete organization creation effect", source))?;
            sqlx::query(
                "UPDATE organization_provisionings SET organization_id=$2,owner_membership_id=$3,organization_residue=TRUE WHERE provisioning_id=$1",
            )
            .bind(effect.provisioning_id)
            .bind(&organization_id)
            .bind(&owner_membership_id)
            .execute(&mut *transaction)
            .await
            .map_err(|source| database("record created organization", source))?;
            (
                FinalizeResult::Applied,
                "effect_applied",
                serde_json::json!({"organization_id": organization_id, "owner_membership_id": owner_membership_id}),
            )
        }
        EffectOutcome::EntitlementApplied { grant_id, receipt } => {
            if effect.kind != "put_entitlement" {
                return Err(invalid_data(
                    "grant receipt applied to non-entitlement effect",
                ));
            }
            sqlx::query(
                "UPDATE organization_provisioning_effects SET state='applied',external_id=$2,lease_token=NULL,lease_until=NULL,last_error=NULL,provider_receipt=$3,completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='in_flight' AND lease_token=$4",
            )
            .bind(effect.effect_id)
            .bind(&grant_id)
            .bind(&receipt)
            .bind(effect.lease_token)
            .execute(&mut *transaction)
            .await
            .map_err(|source| database("complete entitlement grant effect", source))?;
            (
                FinalizeResult::Applied,
                "effect_applied",
                serde_json::json!({"grant_id": grant_id}),
            )
        }
        EffectOutcome::Compensated { receipt } => {
            if effect.kind != "revoke_entitlement" {
                return Err(invalid_data(
                    "compensation receipt applied to non-cleanup effect",
                ));
            }
            sqlx::query(
                "UPDATE organization_provisioning_effects SET state='compensated',lease_token=NULL,lease_until=NULL,last_error=NULL,provider_receipt=$2,completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='in_flight' AND lease_token=$3",
            )
            .bind(effect.effect_id)
            .bind(&receipt)
            .bind(effect.lease_token)
            .execute(&mut *transaction)
            .await
            .map_err(|source| database("complete entitlement compensation effect", source))?;
            (
                FinalizeResult::Applied,
                "effect_compensated",
                serde_json::json!({"grant_id": effect.external_id}),
            )
        }
        EffectOutcome::Failed { error_code } => {
            sqlx::query(
                "UPDATE organization_provisioning_effects SET state='failed',lease_token=NULL,lease_until=NULL,last_error=$2,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='in_flight' AND lease_token=$3",
            )
            .bind(effect.effect_id)
            .bind(&error_code)
            .bind(effect.lease_token)
            .execute(&mut *transaction)
            .await
            .map_err(|source| database("fail provisioning effect", source))?;
            (
                FinalizeResult::Failed,
                "effect_failed",
                serde_json::json!({"error_code": error_code}),
            )
        }
        EffectOutcome::Unknown { error_code } => {
            sqlx::query(
                "UPDATE organization_provisioning_effects SET state='unknown',lease_token=NULL,lease_until=NULL,last_error=$2,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state='in_flight' AND lease_token=$3",
            )
            .bind(effect.effect_id)
            .bind(&error_code)
            .bind(effect.lease_token)
            .execute(&mut *transaction)
            .await
            .map_err(|source| database("quarantine unknown provisioning effect", source))?;
            (
                FinalizeResult::ManualReview,
                "effect_outcome_unknown",
                serde_json::json!({"error_code": error_code}),
            )
        }
    };
    let revision = refresh_saga_status(&mut transaction, effect.provisioning_id, true).await?;
    insert_activity(
        &mut transaction,
        effect.provisioning_id,
        Some(effect.effect_id),
        activity_kind,
        actor,
        revision,
        evidence,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|source| database("commit provisioning effect finalization", source))?;
    Ok(result)
}

pub(crate) async fn has_more(postgres: &OwnedPostgres) -> Result<bool, StorageError> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM organization_provisioning_effects e JOIN organization_provisionings p USING(provisioning_id) WHERE (e.state='pending' AND p.status IN ('pending','cleanup_pending')) OR (e.state='in_flight' AND e.lease_until<CURRENT_TIMESTAMP AND p.status IN ('running','cleanup_running')))",
    )
    .fetch_one(postgres.pool())
    .await
    .map_err(|source| database("check remaining provisioning effects", source))
}

#[derive(Clone, Debug)]
struct LockedSaga {
    status: String,
    revision: i64,
    cleanup_requested: bool,
    organization_id: Option<String>,
}

async fn lock_saga(
    transaction: &mut Transaction<'_, Postgres>,
    provisioning_id: Uuid,
    actor: &str,
) -> Result<Option<LockedSaga>, StorageError> {
    sqlx::query(
        "SELECT status,revision,cleanup_requested,organization_id FROM organization_provisionings WHERE provisioning_id=$1 AND requested_by=$2 FOR UPDATE",
    )
    .bind(provisioning_id)
    .bind(actor)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| database("lock provisioning saga", source))?
    .map(|row| {
        Ok(LockedSaga {
            status: row
                .try_get("status")
                .map_err(|source| database("decode provisioning status", source))?,
            revision: row
                .try_get("revision")
                .map_err(|source| database("decode provisioning revision", source))?,
            cleanup_requested: row
                .try_get("cleanup_requested")
                .map_err(|source| database("decode cleanup request flag", source))?,
            organization_id: row
                .try_get("organization_id")
                .map_err(|source| database("decode created organization id", source))?,
        })
    })
    .transpose()
}

async fn reset_effect_to_pending(
    transaction: &mut Transaction<'_, Postgres>,
    effect_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE organization_provisioning_effects SET state='pending',lease_token=NULL,lease_until=NULL,last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE effect_id=$1 AND state IN ('failed','unknown')",
    )
    .bind(effect_id)
    .execute(&mut **transaction)
    .await
    .map_err(|source| database("reset provisioning effect", source))?;
    Ok(())
}

async fn refresh_saga_status(
    transaction: &mut Transaction<'_, Postgres>,
    provisioning_id: Uuid,
    bump_revision: bool,
) -> Result<i64, StorageError> {
    let saga = sqlx::query(
        "SELECT cleanup_requested,organization_id FROM organization_provisionings WHERE provisioning_id=$1 FOR UPDATE",
    )
    .bind(provisioning_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| database("lock provisioning status refresh", source))?;
    let cleanup_requested: bool = saga
        .try_get("cleanup_requested")
        .map_err(|source| database("decode cleanup status refresh", source))?;
    let organization_id: Option<String> = saga
        .try_get("organization_id")
        .map_err(|source| database("decode residue status refresh", source))?;
    let rows = sqlx::query(
        "SELECT kind,state,last_error FROM organization_provisioning_effects WHERE provisioning_id=$1 ORDER BY CASE kind WHEN 'create_organization' THEN 0 WHEN 'put_entitlement' THEN 1 ELSE 2 END ASC,ordinal ASC,effect_id ASC",
    )
    .bind(provisioning_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|source| database("read provisioning effects for status", source))?;
    let mut unknown = None;
    let mut failed = None;
    let mut pending = false;
    for row in &rows {
        let kind: String = row
            .try_get("kind")
            .map_err(|source| database("decode status effect kind", source))?;
        let state: String = row
            .try_get("state")
            .map_err(|source| database("decode status effect state", source))?;
        let error: Option<String> = row
            .try_get("last_error")
            .map_err(|source| database("decode status effect error", source))?;
        let relevant = if cleanup_requested {
            kind == "revoke_entitlement"
        } else {
            matches!(kind.as_str(), "create_organization" | "put_entitlement")
        };
        if !relevant {
            continue;
        }
        match state.as_str() {
            "unknown" => unknown = unknown.or(error),
            "failed" => failed = failed.or(error),
            "pending" | "in_flight" => pending = true,
            _ => {}
        }
    }
    let (status, last_error) = if let Some(error) = unknown {
        ("manual_review", Some(error))
    } else if let Some(error) = failed {
        (
            if cleanup_requested {
                "cleanup_failed"
            } else {
                "failed"
            },
            Some(error),
        )
    } else if pending {
        (
            if cleanup_requested {
                "cleanup_pending"
            } else {
                "pending"
            },
            None,
        )
    } else if cleanup_requested {
        ("cleanup_completed", None)
    } else {
        ("completed", None)
    };
    let revision: i64 = sqlx::query_scalar(
        "UPDATE organization_provisionings SET status=$2,revision=revision+$3,last_error=$4,organization_residue=($5::text IS NOT NULL),updated_at=CURRENT_TIMESTAMP,completed_at=CASE WHEN $2='completed' THEN COALESCE(completed_at,CURRENT_TIMESTAMP) ELSE completed_at END,cleanup_completed_at=CASE WHEN $2='cleanup_completed' THEN COALESCE(cleanup_completed_at,CURRENT_TIMESTAMP) ELSE cleanup_completed_at END WHERE provisioning_id=$1 RETURNING revision",
    )
    .bind(provisioning_id)
    .bind(status)
    .bind(i64::from(bump_revision))
    .bind(last_error)
    .bind(organization_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| database("refresh provisioning status", source))?;
    Ok(revision)
}

async fn load_view_tx(
    transaction: &mut Transaction<'_, Postgres>,
    provisioning_id: Uuid,
    actor: &str,
) -> Result<Option<ProvisioningView>, StorageError> {
    let row = sqlx::query(
        "SELECT provisioning_id,status,revision,name,slug,owner_subject,requested_by,organization_id,owner_membership_id,cleanup_requested,organization_residue,last_error,created_at,updated_at,completed_at,cleanup_completed_at FROM organization_provisionings WHERE provisioning_id=$1 AND requested_by=$2",
    )
    .bind(provisioning_id)
    .bind(actor)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| database("read provisioning saga", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let steps = sqlx::query(
        "SELECT effect_id,kind,ordinal,state,attempts,external_id,last_error,updated_at FROM organization_provisioning_effects WHERE provisioning_id=$1 ORDER BY CASE kind WHEN 'create_organization' THEN 0 WHEN 'put_entitlement' THEN 1 ELSE 2 END ASC,ordinal ASC,effect_id ASC",
    )
    .bind(provisioning_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|source| database("read provisioning steps", source))?
    .iter()
    .map(step_from_row)
    .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(view_from_row(&row, steps)?))
}

fn view_from_row(row: &PgRow, steps: Vec<StepView>) -> Result<ProvisioningView, StorageError> {
    Ok(ProvisioningView {
        provisioning_id: row
            .try_get("provisioning_id")
            .map_err(|source| database("decode provisioning id", source))?,
        status: row
            .try_get("status")
            .map_err(|source| database("decode provisioning view status", source))?,
        revision: row
            .try_get("revision")
            .map_err(|source| database("decode provisioning view revision", source))?,
        name: row
            .try_get("name")
            .map_err(|source| database("decode organization name", source))?,
        slug: row
            .try_get("slug")
            .map_err(|source| database("decode organization slug", source))?,
        owner_subject: row
            .try_get("owner_subject")
            .map_err(|source| database("decode organization owner", source))?,
        requested_by: row
            .try_get("requested_by")
            .map_err(|source| database("decode provisioning requester", source))?,
        organization_id: row
            .try_get("organization_id")
            .map_err(|source| database("decode provisioning organization", source))?,
        owner_membership_id: row
            .try_get("owner_membership_id")
            .map_err(|source| database("decode owner membership", source))?,
        cleanup_requested: row
            .try_get("cleanup_requested")
            .map_err(|source| database("decode cleanup requested", source))?,
        organization_residue: row
            .try_get("organization_residue")
            .map_err(|source| database("decode organization residue", source))?,
        last_error: row
            .try_get("last_error")
            .map_err(|source| database("decode provisioning error", source))?,
        steps,
        created_at: format_time(
            row.try_get("created_at")
                .map_err(|source| database("decode provisioning creation time", source))?,
        )?,
        updated_at: format_time(
            row.try_get("updated_at")
                .map_err(|source| database("decode provisioning update time", source))?,
        )?,
        completed_at: format_optional_time(
            row.try_get("completed_at")
                .map_err(|source| database("decode provisioning completion time", source))?,
        )?,
        cleanup_completed_at: format_optional_time(
            row.try_get("cleanup_completed_at")
                .map_err(|source| database("decode cleanup completion time", source))?,
        )?,
    })
}

fn step_from_row(row: &PgRow) -> Result<StepView, StorageError> {
    Ok(StepView {
        effect_id: row
            .try_get("effect_id")
            .map_err(|source| database("decode step id", source))?,
        kind: row
            .try_get("kind")
            .map_err(|source| database("decode step kind", source))?,
        ordinal: row
            .try_get("ordinal")
            .map_err(|source| database("decode step ordinal", source))?,
        status: row
            .try_get("state")
            .map_err(|source| database("decode step status", source))?,
        attempt: row
            .try_get("attempts")
            .map_err(|source| database("decode step attempt", source))?,
        external_id: row
            .try_get("external_id")
            .map_err(|source| database("decode step external id", source))?,
        last_error: row
            .try_get("last_error")
            .map_err(|source| database("decode step error", source))?,
        updated_at: format_time(
            row.try_get("updated_at")
                .map_err(|source| database("decode step update time", source))?,
        )?,
    })
}

fn list_item_from_row(row: &PgRow) -> Result<ProvisioningListItem, StorageError> {
    Ok(ProvisioningListItem {
        provisioning_id: row
            .try_get("provisioning_id")
            .map_err(|source| database("decode listed provisioning id", source))?,
        status: row
            .try_get("status")
            .map_err(|source| database("decode listed provisioning status", source))?,
        revision: row
            .try_get("revision")
            .map_err(|source| database("decode listed provisioning revision", source))?,
        name: row
            .try_get("name")
            .map_err(|source| database("decode listed organization name", source))?,
        slug: row
            .try_get("slug")
            .map_err(|source| database("decode listed organization slug", source))?,
        owner_subject: row
            .try_get("owner_subject")
            .map_err(|source| database("decode listed owner", source))?,
        organization_id: row
            .try_get("organization_id")
            .map_err(|source| database("decode listed organization id", source))?,
        cleanup_requested: row
            .try_get("cleanup_requested")
            .map_err(|source| database("decode listed cleanup flag", source))?,
        organization_residue: row
            .try_get("organization_residue")
            .map_err(|source| database("decode listed residue", source))?,
        last_error: row
            .try_get("last_error")
            .map_err(|source| database("decode listed error", source))?,
        created_at: format_time(
            row.try_get("created_at")
                .map_err(|source| database("decode listed creation time", source))?,
        )?,
        updated_at: format_time(
            row.try_get("updated_at")
                .map_err(|source| database("decode listed update time", source))?,
        )?,
    })
}

enum MutationReplay<T> {
    Execute,
    Replay(T),
}

#[allow(clippy::too_many_arguments)]
async fn begin_mutation<T: DeserializeOwned>(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    idempotency_key: &str,
    operation: &str,
    actor: &str,
    request_hash: &[u8],
    provisioning_id: Option<Uuid>,
) -> Result<Result<MutationReplay<T>, DomainFailure>, StorageError> {
    let inserted = sqlx::query(
        "INSERT INTO organization_provisioning_mutations(caller_instance,idempotency_key,operation,actor_subject,request_hash,provisioning_id,status) VALUES($1,$2,$3,$4,$5,$6,'started') ON CONFLICT(caller_instance,idempotency_key) DO NOTHING",
    )
    .bind(caller)
    .bind(idempotency_key)
    .bind(operation)
    .bind(actor)
    .bind(request_hash)
    .bind(provisioning_id)
    .execute(&mut **transaction)
    .await
    .map_err(|source| database("reserve provisioning mutation", source))?
    .rows_affected()
        == 1;
    if inserted {
        return Ok(Ok(MutationReplay::Execute));
    }
    let row = sqlx::query(
        "SELECT operation,actor_subject,request_hash,status,response FROM organization_provisioning_mutations WHERE caller_instance=$1 AND idempotency_key=$2",
    )
    .bind(caller)
    .bind(idempotency_key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| database("read provisioning mutation replay", source))?;
    let stored_operation: String = row
        .try_get("operation")
        .map_err(|source| database("decode mutation operation", source))?;
    let stored_actor: String = row
        .try_get("actor_subject")
        .map_err(|source| database("decode mutation actor", source))?;
    let stored_hash: Vec<u8> = row
        .try_get("request_hash")
        .map_err(|source| database("decode mutation request hash", source))?;
    if stored_operation != operation || stored_actor != actor || stored_hash != request_hash {
        return Ok(Err(DomainFailure::IdempotencyConflict));
    }
    let status: String = row
        .try_get("status")
        .map_err(|source| database("decode mutation status", source))?;
    if status != "completed" {
        return Err(invalid_data(
            "committed provisioning mutation is incomplete",
        ));
    }
    let response: serde_json::Value = row
        .try_get::<Option<serde_json::Value>, _>("response")
        .map_err(|source| database("decode mutation response", source))?
        .ok_or_else(|| invalid_data("completed provisioning mutation has no response"))?;
    serde_json::from_value(response)
        .map(MutationReplay::Replay)
        .map(Ok)
        .map_err(|error| invalid_data(format!("invalid mutation response: {error}")))
}

async fn complete_mutation<T: Serialize>(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    idempotency_key: &str,
    response: &T,
) -> Result<(), StorageError> {
    let response = serde_json::to_value(response).map_err(|error| {
        invalid_data(format!("mutation response cannot be serialized: {error}"))
    })?;
    let affected = sqlx::query(
        "UPDATE organization_provisioning_mutations SET status='completed',response=$3 WHERE caller_instance=$1 AND idempotency_key=$2 AND status='started'",
    )
    .bind(caller)
    .bind(idempotency_key)
    .bind(response)
    .execute(&mut **transaction)
    .await
    .map_err(|source| database("complete provisioning mutation", source))?
    .rows_affected();
    if affected != 1 {
        return Err(invalid_data(
            "provisioning mutation completion lost ownership",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_activity(
    transaction: &mut Transaction<'_, Postgres>,
    provisioning_id: Uuid,
    effect_id: Option<Uuid>,
    kind: &str,
    actor: &str,
    revision: i64,
    evidence: serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO organization_provisioning_activity(activity_id,provisioning_id,effect_id,kind,actor_subject,provisioning_revision,evidence) VALUES($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(Uuid::new_v4())
    .bind(provisioning_id)
    .bind(effect_id)
    .bind(kind)
    .bind(actor)
    .bind(revision)
    .bind(evidence)
    .execute(&mut **transaction)
    .await
    .map_err(|source| database("insert provisioning activity", source))?;
    Ok(())
}

fn retry_resolution_name(resolution: RetryResolution) -> &'static str {
    match resolution {
        RetryResolution::RetryFailed => "retry_failed",
        RetryResolution::ConfirmApplied => "confirm_applied",
        RetryResolution::ConfirmNotApplied => "confirm_not_applied",
    }
}

fn format_time(value: OffsetDateTime) -> Result<String, StorageError> {
    value
        .format(&Rfc3339)
        .map_err(|error| invalid_data(format!("timestamp cannot be formatted: {error}")))
}

fn format_optional_time(value: Option<OffsetDateTime>) -> Result<Option<String>, StorageError> {
    value.map(format_time).transpose()
}

const fn database(operation: &'static str, source: sqlx::Error) -> StorageError {
    StorageError::Database { operation, source }
}

fn invalid_data(detail: impl Into<String>) -> StorageError {
    StorageError::InvalidStoredData {
        detail: detail.into(),
    }
}

mod decimal_i64 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn serialize<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}
