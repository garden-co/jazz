//! Synthetic device-only schema and canonical byte fixtures. Scope rows need
//! explicit authorization: separate local roots do not make public rows private.
use jazz::groove::records::{OwnedRecord, RecordDescriptor, Value as RecordValue, ValueType};
use jazz::ids::AuthorSubject;
use jazz::query::Query;
use jazz::tools::policy_expr::{eq, session};
use jazz::tools::{ColumnType, Schema, SchemaBuilder, TablePolicies, TableSchemaBuilder};

pub fn schema() -> Schema {
    schema_with_policy(true)
}

fn schema_with_policy(protected: bool) -> Schema {
    let owner = eq("owner", session("user"));
    SchemaBuilder::new()
        .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
        .table(
            TableSchemaBuilder::new("scope_rows")
                .column("title", ColumnType::Text)
                .column("owner", ColumnType::Text)
                .policies(if protected {
                    TablePolicies::new()
                        .with_select(owner.clone())
                        .with_insert(owner.clone())
                        .with_update(Some(owner.clone()), owner.clone())
                        .with_delete(owner)
                } else {
                    TablePolicies::default()
                }),
        )
        .build()
}

pub fn owner(scope: &str) -> String {
    AuthorSubject::authenticated(
        jazz_server::TEST_JWT_ISSUER,
        &format!("rn-device-private-{scope}"),
    )
    .expect("synthetic authenticated owner")
    .canonical()
    .to_owned()
}

fn scope_record(scope: &str) -> OwnedRecord {
    let descriptor =
        RecordDescriptor::new([("title", ValueType::String), ("owner", ValueType::String)]);
    let raw = descriptor
        .create(&[
            RecordValue::String(format!("scope-{scope}-private-row")),
            RecordValue::String(owner(scope)),
        ])
        .expect("synthetic owner cells");
    OwnedRecord::new(raw, descriptor)
}

pub fn fixture() -> serde_json::Value {
    let cells = |scope: &str| {
        jazz::binding_codec::encode_named_cells(&scope_record(scope))
            .expect("canonical named scope cells")
    };
    serde_json::json!({
        "schema": schema(),
        "scopeQuery": postcard::to_allocvec(&Query::from("scope_rows")).unwrap(),
        "scopeCells": {"a": cells("a"), "b": cells("b")}
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::binding_codec::decode_named_cells;
    use jazz::tools::{AppContext, ClientStorage, DurabilityTier, Value};
    use jazz_server::{JazzServer, TestJwtIssuer};
    use jazz_testkit::{connect, native_connector, wait_for_query};
    use std::time::Duration;

    #[test]
    fn generated_device_schema_and_bytes_match_rust_producer() {
        let checked: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../dev/rn-device-acceptance/native/device-fixture.json"
        ))
        .unwrap();
        assert_eq!(fixture(), checked);
    }

    #[test]
    fn generated_scope_cells_use_the_shared_named_cell_envelope() {
        let checked: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../dev/rn-device-acceptance/native/device-fixture.json"
        ))
        .unwrap();
        for scope in ["a", "b"] {
            let bytes: Vec<u8> = serde_json::from_value(checked["scopeCells"][scope].clone())
                .expect("checked scope cells are bytes");
            let cells = decode_named_cells(&bytes).expect("fixture uses the shared named-cell ABI");
            assert_eq!(
                cells.get("title"),
                Some(&RecordValue::String(format!("scope-{scope}-private-row")))
            );
            assert_eq!(cells.get("owner"), Some(&RecordValue::String(owner(scope))));

            let record = scope_record(scope);
            let generic = postcard::to_allocvec(&(record.descriptor(), record.raw()))
                .expect("generic execution serde plant");
            assert!(
                decode_named_cells(&generic).is_err(),
                "execution descriptor serde must not enter the native named-cell ABI"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn explicit_owner_policy_isolates_scopes_and_policy_omission_shares_rows() {
        tokio::task::LocalSet::new()
            .run_until(async {
                for protected in [true, false] {
                    let issuer = TestJwtIssuer::start().await;
                    let schema = schema_with_policy(protected);
                    let server = JazzServer::builder()
                        .with_schema(schema.clone())
                        .with_jwks_url(issuer.endpoint())
                        .with_native_transport_connector(native_connector())
                        .start()
                        .await;
                    let mut clients = Vec::new();
                    let mut dirs = Vec::new();
                    for scope in ["a", "b"] {
                        let dir = tempfile::tempdir().unwrap();
                        let client = connect(AppContext {
                            app_id: server.app_id(),
                            client_id: None,
                            schema: schema.clone(),
                            server_url: server.base_url(),
                            data_dir: dir.path().to_owned(),
                            storage: ClientStorage::Memory,
                            storage_factory: None,
                            jwt_token: Some(TestJwtIssuer::jwt_for_user(&format!(
                                "rn-device-private-{scope}"
                            ))),
                            backend_secret: None,
                            admin_secret: None,
                        })
                        .await
                        .unwrap();
                        client.insert("scope_rows", jazz::row_input! {
                        "title" => format!("scope-{scope}-private-row"), "owner" => owner(scope)
                    }).unwrap();
                        clients.push(client);
                        dirs.push(dir);
                    }
                    // Both real authenticated writers must reach authority before
                    // checking visibility. This is not a race against replication.
                    for (client, scope) in clients.iter().zip(["a", "b"]) {
                        wait_for_query(
                            client,
                            Query::from("scope_rows"),
                            Some(DurabilityTier::GlobalServer),
                            Duration::from_secs(20),
                            "owner write reaches authority",
                            |rows| {
                                rows.into_iter()
                                    .any(|(_, values)| {
                                        values.contains(&Value::Text(format!(
                                            "scope-{scope}-private-row"
                                        )))
                                    })
                                    .then_some(())
                            },
                        )
                        .await;
                    }
                    for (client, scope) in clients.iter().zip(["a", "b"]) {
                        let rows = client
                            .query_with_read_tier(
                                Query::from("scope_rows"),
                                jazz::tools::ReadTier::Remote,
                            )
                            .await
                            .unwrap();
                        assert_eq!(
                            rows.len(),
                            if protected { 1 } else { 2 },
                            "owner={scope}, protected={protected}"
                        );
                        assert!(rows.iter().any(|(_, values)| {
                            values.contains(&Value::Text(format!("scope-{scope}-private-row")))
                        }));
                        if protected {
                            let other = if scope == "a" { "b" } else { "a" };
                            assert!(!rows.iter().any(|(_, values)| {
                                values.contains(&Value::Text(format!("scope-{other}-private-row")))
                            }));
                        }
                    }
                }
            })
            .await;
    }
}
