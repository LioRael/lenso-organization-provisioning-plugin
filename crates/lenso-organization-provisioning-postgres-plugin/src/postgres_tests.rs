//! Real `PostgreSQL` acceptance coverage lives in this module.

use lenso_postgres_kit::OwnedPostgres;
use sha2::{Digest as _, Sha256};
use sqlx::{AssertSqlSafe, Row};
use uuid::Uuid;

use super::{OrganizationProvisioningOperator, schema, storage};

const DATABASE_ENV: &str = "LENSO_ORGANIZATION_PROVISIONING_TEST_DATABASE_URL";

struct TestDatabase {
    postgres: OwnedPostgres,
    schema: String,
}

impl TestDatabase {
    async fn setup() -> Option<Self> {
        let database_url = std::env::var(DATABASE_ENV).ok()?;
        let schema = format!("organization_provisioning_test_{}", Uuid::new_v4().simple());
        OrganizationProvisioningOperator::setup(&database_url, &schema)
            .await
            .expect("set up Organization Provisioning acceptance schema");
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(schema.clone()).expect("build acceptance schema plan"),
        )
        .await
        .expect("prepare Organization Provisioning acceptance storage");
        Some(Self { postgres, schema })
    }

    async fn cleanup(self) {
        let statement = format!("DROP SCHEMA \"{}\" CASCADE", self.schema);
        sqlx::query(AssertSqlSafe(statement))
            .execute(self.postgres.pool())
            .await
            .expect("drop Organization Provisioning acceptance schema");
        self.postgres.pool().close().await;
    }
}

fn hash(value: &str) -> Vec<u8> {
    Sha256::digest(value.as_bytes()).to_vec()
}

fn grants() -> Vec<storage::BootstrapGrant> {
    vec![
        storage::BootstrapGrant {
            subject_kind: "organization".to_owned(),
            feature: "projects".to_owned(),
            limit: Some(100),
        },
        storage::BootstrapGrant {
            subject_kind: "owner".to_owned(),
            feature: "admin-console".to_owned(),
            limit: None,
        },
    ]
}

async fn start_saga(
    database: &TestDatabase,
    actor: &str,
    key: &str,
    name: &str,
    grants: &[storage::BootstrapGrant],
) -> storage::ProvisioningView {
    start_saga_as(database, "provisioning-api", actor, key, name, grants).await
}

async fn start_saga_as(
    database: &TestDatabase,
    caller: &str,
    actor: &str,
    key: &str,
    name: &str,
    grants: &[storage::BootstrapGrant],
) -> storage::ProvisioningView {
    storage::start(
        &database.postgres,
        caller,
        actor,
        key,
        &hash(name),
        Uuid::new_v4(),
        name,
        &format!("{}-slug", name.to_ascii_lowercase()),
        actor,
        grants,
    )
    .await
    .expect("start storage call")
    .expect("start provisioning saga")
}

#[tokio::test]
async fn restart_replays_exact_response_and_list_is_actor_scoped_keyset() {
    let Some(database) = TestDatabase::setup().await else {
        return;
    };
    let first = start_saga(&database, "user:one", "start-1", "Acme", &grants()).await;
    assert_eq!(first.status, "pending");
    assert_eq!(first.steps.len(), 3);
    assert!(first.steps.iter().all(|step| step.attempt == 0));

    let restarted = OwnedPostgres::prepare(
        &std::env::var(DATABASE_ENV).unwrap(),
        schema::schema_plan(database.schema.clone()).unwrap(),
    )
    .await
    .unwrap();
    let replay = storage::start(
        &restarted,
        "provisioning-api",
        "user:one",
        "start-1",
        &hash("Acme"),
        Uuid::new_v4(),
        "Acme",
        "acme-slug",
        "user:one",
        &grants(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(replay.provisioning_id, first.provisioning_id);
    assert_eq!(replay.revision, first.revision);
    assert_eq!(replay.updated_at, first.updated_at);
    let conflict = storage::start(
        &restarted,
        "provisioning-api",
        "user:one",
        "start-1",
        &hash("different"),
        Uuid::new_v4(),
        "Acme",
        "acme-slug",
        "user:one",
        &grants(),
    )
    .await
    .unwrap();
    assert!(matches!(
        conflict,
        Err(storage::DomainFailure::IdempotencyConflict)
    ));

    let second = start_saga(&database, "user:one", "start-2", "Beta", &[]).await;
    let other = start_saga_as(
        &database,
        "other-provisioning-api",
        "user:two",
        "start-1",
        "Gamma",
        &[],
    )
    .await;
    assert_ne!(other.provisioning_id, first.provisioning_id);
    assert!(
        storage::get(&database.postgres, first.provisioning_id, "user:two")
            .await
            .unwrap()
            .is_none()
    );

    let first_page = storage::list(
        &database.postgres,
        &storage::ListFilters {
            actor: "user:one",
            status: Some("pending"),
            cursor: None,
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(first_page.len(), 1);
    let cursor = storage::decode_cursor(&storage::encode_cursor(&first_page[0]).unwrap()).unwrap();
    let second_page = storage::list(
        &database.postgres,
        &storage::ListFilters {
            actor: "user:one",
            status: Some("pending"),
            cursor: Some(&cursor),
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(second_page.len(), 1);
    assert_eq!(
        [
            first_page[0].provisioning_id,
            second_page[0].provisioning_id
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>(),
        [first.provisioning_id, second.provisioning_id]
            .into_iter()
            .collect()
    );
    restarted.pool().close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_start_reserves_one_caller_scoped_saga() {
    let Some(database) = TestDatabase::setup().await else {
        return;
    };
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let request_hash = hash("same request");
    let first = storage::start(
        &database.postgres,
        "provisioning-api",
        "user:one",
        "same-key",
        &request_hash,
        first_id,
        "Acme",
        "acme",
        "user:one",
        &[],
    );
    let second = storage::start(
        &database.postgres,
        "provisioning-api",
        "user:one",
        "same-key",
        &request_hash,
        second_id,
        "Acme",
        "acme",
        "user:one",
        &[],
    );
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();
    assert_eq!(first.provisioning_id, second.provisioning_id);
    assert!(matches!(first.provisioning_id, id if id == first_id || id == second_id));
    let saga_count: i64 = sqlx::query_scalar("SELECT count(*) FROM organization_provisionings")
        .fetch_one(database.postgres.pool())
        .await
        .unwrap();
    let mutation_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM organization_provisioning_mutations")
            .fetch_one(database.postgres.pool())
            .await
            .unwrap();
    assert_eq!((saga_count, mutation_count), (1, 1));
    database.cleanup().await;
}

#[tokio::test]
async fn worker_orders_effects_and_fenced_success_completes_the_saga() {
    let Some(database) = TestDatabase::setup().await else {
        return;
    };
    let saga = start_saga(
        &database,
        "service:provisioner",
        "ordered",
        "Acme",
        &grants()[..1],
    )
    .await;
    let create = storage::claim_next(&database.postgres, "worker:one", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(create.kind, "create_organization");
    assert_eq!(
        create.downstream_key,
        format!(
            "organization-provisioning:{}:create:v1",
            saga.provisioning_id
        )
    );
    assert_eq!(
        storage::finalize_effect(
            &database.postgres,
            &create,
            "worker:one",
            storage::EffectOutcome::OrganizationApplied {
                organization_id: "org_acme".to_owned(),
                owner_membership_id: "member_owner".to_owned(),
                receipt: serde_json::json!({"created": true}),
            },
        )
        .await
        .unwrap(),
        storage::FinalizeResult::Applied
    );
    let grant = storage::claim_next(&database.postgres, "worker:one", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(grant.kind, "put_entitlement");
    assert_eq!(grant.organization_id.as_deref(), Some("org_acme"));
    assert_eq!(grant.subject_kind.as_deref(), Some("organization"));
    assert_eq!(
        storage::finalize_effect(
            &database.postgres,
            &grant,
            "worker:one",
            storage::EffectOutcome::EntitlementApplied {
                grant_id: "grant_projects".to_owned(),
                receipt: serde_json::json!({"changed": true}),
            },
        )
        .await
        .unwrap(),
        storage::FinalizeResult::Applied
    );
    let completed = storage::get(
        &database.postgres,
        saga.provisioning_id,
        "service:provisioner",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(completed.status, "completed");
    assert!(completed.organization_residue);
    assert_eq!(completed.organization_id.as_deref(), Some("org_acme"));
    assert_eq!(
        storage::finalize_effect(
            &database.postgres,
            &create,
            "stale-worker",
            storage::EffectOutcome::Unknown {
                error_code: "late_result".to_owned(),
            },
        )
        .await
        .unwrap(),
        storage::FinalizeResult::Superseded
    );
    database.cleanup().await;
}

#[tokio::test]
async fn expired_fence_requires_manual_resolution_before_replay() {
    let Some(database) = TestDatabase::setup().await else {
        return;
    };
    let saga = start_saga(&database, "user:one", "manual", "Acme", &[]).await;
    let abandoned = storage::claim_next(&database.postgres, "worker:one", 30)
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "UPDATE organization_provisioning_effects SET lease_until=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE effect_id=$1",
    )
    .bind(abandoned.effect_id)
    .execute(database.postgres.pool())
    .await
    .unwrap();
    assert_eq!(
        storage::quarantine_expired(&database.postgres, "worker:two", 10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        storage::finalize_effect(
            &database.postgres,
            &abandoned,
            "worker:one",
            storage::EffectOutcome::OrganizationApplied {
                organization_id: "org_late".to_owned(),
                owner_membership_id: "member_late".to_owned(),
                receipt: serde_json::json!({}),
            },
        )
        .await
        .unwrap(),
        storage::FinalizeResult::Superseded
    );
    let manual = storage::get(&database.postgres, saga.provisioning_id, "user:one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manual.status, "manual_review");

    let wrong_revision = storage::retry(
        &database.postgres,
        &storage::RetryInput {
            caller: "provisioning-api",
            actor: "user:one",
            idempotency_key: "resolve-wrong",
            request_hash: &hash("resolve-wrong"),
            provisioning_id: saga.provisioning_id,
            expected_revision: manual.revision + 1,
            resolution: storage::RetryResolution::ConfirmApplied,
            observed_organization_id: Some("org_acme"),
            observed_owner_membership_id: Some("member_owner"),
            observed_grant_id: None,
            evidence: "verified in Organization Directory",
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        wrong_revision,
        Err(storage::DomainFailure::RevisionConflict)
    ));
    let resolved = storage::retry(
        &database.postgres,
        &storage::RetryInput {
            caller: "provisioning-api",
            actor: "user:one",
            idempotency_key: "resolve-applied",
            request_hash: &hash("resolve-applied"),
            provisioning_id: saga.provisioning_id,
            expected_revision: manual.revision,
            resolution: storage::RetryResolution::ConfirmApplied,
            observed_organization_id: Some("org_acme"),
            observed_owner_membership_id: Some("member_owner"),
            observed_grant_id: None,
            evidence: "verified in Organization Directory",
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resolved.status, "completed");
    assert_eq!(resolved.organization_id.as_deref(), Some("org_acme"));
    database.cleanup().await;
}

#[tokio::test]
async fn cleanup_revokes_only_applied_grants_and_reports_organization_residue() {
    let Some(database) = TestDatabase::setup().await else {
        return;
    };
    let saga = start_saga(&database, "user:one", "cleanup", "Acme", &grants()).await;
    let create = storage::claim_next(&database.postgres, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    storage::finalize_effect(
        &database.postgres,
        &create,
        "worker",
        storage::EffectOutcome::OrganizationApplied {
            organization_id: "org_acme".to_owned(),
            owner_membership_id: "member_owner".to_owned(),
            receipt: serde_json::json!({}),
        },
    )
    .await
    .unwrap();
    let first_grant = storage::claim_next(&database.postgres, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    storage::finalize_effect(
        &database.postgres,
        &first_grant,
        "worker",
        storage::EffectOutcome::EntitlementApplied {
            grant_id: "grant_projects".to_owned(),
            receipt: serde_json::json!({}),
        },
    )
    .await
    .unwrap();
    let second_grant = storage::claim_next(&database.postgres, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    storage::finalize_effect(
        &database.postgres,
        &second_grant,
        "worker",
        storage::EffectOutcome::Failed {
            error_code: "entitlement_put_forbidden".to_owned(),
        },
    )
    .await
    .unwrap();
    let failed = storage::get(&database.postgres, saga.provisioning_id, "user:one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, "failed");
    let cleanup = storage::request_cleanup(
        &database.postgres,
        &storage::CleanupInput {
            caller: "provisioning-api",
            actor: "user:one",
            idempotency_key: "cleanup-request",
            request_hash: &hash("cleanup-request"),
            provisioning_id: saga.provisioning_id,
            expected_revision: failed.revision,
            evidence: "customer requested rollback",
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(cleanup.status, "cleanup_pending");
    assert!(cleanup.organization_residue);
    assert_eq!(
        cleanup
            .steps
            .iter()
            .filter(|step| step.kind == "revoke_entitlement")
            .count(),
        1
    );
    let revoke = storage::claim_next(&database.postgres, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revoke.kind, "revoke_entitlement");
    assert_eq!(revoke.external_id.as_deref(), Some("grant_projects"));
    storage::finalize_effect(
        &database.postgres,
        &revoke,
        "worker",
        storage::EffectOutcome::Compensated {
            receipt: serde_json::json!({"changed": true}),
        },
    )
    .await
    .unwrap();
    let cleaned = storage::get(&database.postgres, saga.provisioning_id, "user:one")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cleaned.status, "cleanup_completed");
    assert!(cleaned.organization_residue);
    assert_eq!(cleaned.organization_id.as_deref(), Some("org_acme"));
    let cleanup_activity: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM organization_provisioning_activity WHERE provisioning_id=$1 AND kind='cleanup_requested' AND evidence->>'organization_residue'='true'",
    )
    .bind(saga.provisioning_id)
    .fetch_one(database.postgres.pool())
    .await
    .unwrap();
    assert_eq!(cleanup_activity, 1);
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_workers_skip_locked_across_sagas() {
    let Some(database) = TestDatabase::setup().await else {
        return;
    };
    let first = start_saga(&database, "user:one", "worker-a", "Acme", &[]).await;
    let second = start_saga(&database, "user:two", "worker-b", "Beta", &[]).await;
    let (left, right) = tokio::join!(
        storage::claim_next(&database.postgres, "worker:left", 30),
        storage::claim_next(&database.postgres, "worker:right", 30),
    );
    let left = left.unwrap().unwrap();
    let right = right.unwrap().unwrap();
    assert_ne!(left.provisioning_id, right.provisioning_id);
    assert_eq!(
        [left.provisioning_id, right.provisioning_id]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        [first.provisioning_id, second.provisioning_id]
            .into_iter()
            .collect()
    );
    for effect in [&left, &right] {
        storage::finalize_effect(
            &database.postgres,
            effect,
            "worker",
            storage::EffectOutcome::OrganizationApplied {
                organization_id: format!("org_{}", effect.provisioning_id.simple()),
                owner_membership_id: format!("member_{}", effect.provisioning_id.simple()),
                receipt: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
    }
    let running: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM organization_provisionings WHERE status='running'",
    )
    .fetch_one(database.postgres.pool())
    .await
    .unwrap();
    assert_eq!(running, 0);
    let evidence_rows = sqlx::query(
        "SELECT evidence FROM organization_provisioning_activity WHERE kind='effect_claimed'",
    )
    .fetch_all(database.postgres.pool())
    .await
    .unwrap();
    assert_eq!(evidence_rows.len(), 2);
    assert!(evidence_rows.iter().all(|row| {
        row.try_get::<serde_json::Value, _>("evidence")
            .ok()
            .and_then(|value| value.get("fence").cloned())
            .is_some()
    }));
    database.cleanup().await;
}
