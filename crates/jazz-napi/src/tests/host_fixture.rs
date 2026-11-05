//! Unit fixtures call the shared host entrypoints used immediately before the
//! NAPI Env-only callback registration. Real Node tests cover those callbacks.
use super::*;

pub(crate) fn fixture_schema() -> Vec<u8> {
    let source = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build();
    serde_json::to_vec(&source).unwrap()
}
pub(crate) fn fixture() -> NapiDb {
    NapiDb::open_memory(
        Uint8Array::from(fixture_schema()),
        Uint8Array::from(encode_persistent_open_config(
            CoreAuthorSubject::for_test_bytes([0x47; 16]),
        )),
        None,
    )
    .unwrap()
}
pub(super) fn label_cells(label: &str) -> Uint8Array {
    let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
    let record = jazz::records::OwnedRecord::new(
        descriptor
            .create(&[CoreValue::String(label.into())])
            .unwrap(),
        descriptor,
    );
    jazz::binding_codec::encode_named_cells(&record)
        .unwrap()
        .into()
}
pub(super) fn pending_read(
    future: Pin<Box<dyn Future<Output = napi::Result<Uint8Array>>>>,
) -> PendingNativeRead {
    let future: ReadFuture = Box::pin(async move {
        future
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| HostError::Argument(e.reason))
    });
    PendingNativeRead {
        future: lifecycle::Resource::new(None, future),
    }
}
pub(super) fn core_block_on<F: Future>(future: F) -> F::Output {
    block_on(future)
}
pub(super) fn core_author_id_from_bytes(bytes: &[u8]) -> napi::Result<CoreAuthorSubject> {
    jazz::binding_args::author_subject(bytes).map_err(napi_error)
}
pub(super) fn core_open_backend_identity(
    config: &CoreOpenDbConfig,
) -> napi::Result<CoreDbIdentity> {
    binding_host::open_identity(&mut config.clone(), OpenAs::Backend)
        .map(|(id, _)| id)
        .map_err(host_error)
}
pub(super) fn core_claims_from_json(
    author: CoreAuthorSubject,
    claims: Option<JsonValue>,
) -> napi::Result<BTreeMap<String, CoreValue>> {
    binding_host::claims(author, json_or_null(claims)).map_err(host_error)
}
pub(super) fn core_claim_value_from_json(
    value: JsonValue,
) -> std::result::Result<Option<CoreValue>, String> {
    jazz::tools::policy_claims::json_value_to_policy_claim(
        value,
        jazz::tools::policy_claims::NumericClaimOrigin::JavaScript,
    )
}
pub(super) fn core_read_tier_from_str(tier: &str) -> napi::Result<jazz::db::ReadTier> {
    jazz::binding_args::read_tier(tier).map_err(napi_error)
}
pub(super) fn core_durability_tier_from_str(tier: &str) -> napi::Result<DurabilityTier> {
    jazz::binding_args::durability_tier(tier).map_err(napi_error)
}
pub(super) fn core_read_opts_from_json(
    value: Option<JsonValue>,
) -> napi::Result<jazz::db::ReadOpts> {
    binding_host::read_opts(&json_or_null(value)).map_err(host_error)
}
pub(super) fn core_insert_options(
    value: Option<InsertOptions>,
) -> napi::Result<jazz::db::InsertOptions> {
    binding_host::insert_options(&insert_options(value)?).map_err(host_error)
}
pub(super) fn core_update_options(
    value: Option<UpdateOptions>,
) -> napi::Result<jazz::db::UpdateOptions> {
    binding_host::update_options(&update_options(value)?).map_err(host_error)
}
pub(super) fn core_restore_options(
    value: Option<RestoreOptions>,
) -> napi::Result<jazz::db::RestoreOptions> {
    binding_host::restore_options(&restore_options(value)?).map_err(host_error)
}
pub(super) fn core_subscription_event_to_napi(
    event: &CoreSubscriptionEvent,
) -> napi::Result<SubscriptionEvent> {
    subscription_event(event.clone())
}
pub(super) fn unknown_transaction_kind_message(kind: &str) -> String {
    binding_host::TransactionKind::parse(kind)
        .unwrap_err()
        .to_string()
}
pub(super) fn server_binding(server: CoreJazzServer) -> JazzServer {
    let (changed, _) = tokio::sync::watch::channel(());
    JazzServer {
        lifecycle: Arc::new(JazzServerLifecycle {
            state: Mutex::new(JazzServerState::Running(Box::new(server))),
            changed,
        }),
    }
}
// These adapters exercise production open_as and Binding without inventing an
// unsafe Node Env. There is no alternate identity, storage or mutation path.
pub(super) trait NativeTestOpen {
    fn try_open_persistent(
        path: String,
        schema: Uint8Array,
        config: Uint8Array,
        cached: Option<Uint8Array>,
    ) -> std::result::Result<NapiDb, PersistentOpenError>;
    fn try_open_persistent_as_backend(
        path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> std::result::Result<NapiDb, PersistentOpenError>;
    fn try_open_persistent_with_self_signed_proof(
        path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app: String,
        claimed: String,
        cached: Option<Uint8Array>,
    ) -> std::result::Result<NapiDb, PersistentOpenError>;
}
impl NativeTestOpen for NapiDb {
    fn try_open_persistent(
        path: String,
        schema: Uint8Array,
        config: Uint8Array,
        cached: Option<Uint8Array>,
    ) -> std::result::Result<NapiDb, PersistentOpenError> {
        Self::open_as(
            &schema,
            &config,
            Some(&path),
            OpenAs::Untrusted,
            cached.as_deref(),
        )
    }
    fn try_open_persistent_as_backend(
        path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> std::result::Result<NapiDb, PersistentOpenError> {
        Self::open_as(&schema, &config, Some(&path), OpenAs::Backend, None)
    }
    fn try_open_persistent_with_self_signed_proof(
        path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app: String,
        claimed: String,
        cached: Option<Uint8Array>,
    ) -> std::result::Result<NapiDb, PersistentOpenError> {
        Self::open_as(
            &schema,
            &config,
            Some(&path),
            OpenAs::SelfSigned {
                token: &token,
                app_id: &app,
                claimed_author: &claimed,
            },
            cached.as_deref(),
        )
    }
}
pub(super) trait NativeTestWrites {
    fn insert_test(
        &self,
        table: String,
        cells: Uint8Array,
        opts: Option<InsertOptions>,
    ) -> napi::Result<binding_host::Write>;
    fn update_large_test(
        &self,
        table: String,
        row: Uint8Array,
        cells: Uint8Array,
        updates: JsonValue,
        opts: Option<f64>,
    ) -> napi::Result<binding_host::Write>;
    fn commit_test(&self, id: String, kind: Option<String>) -> napi::Result<binding_host::Write>;
    fn publish_test(&self, token: String) -> napi::Result<binding_host::Write>;
}
impl NativeTestWrites for NapiDb {
    fn insert_test(
        &self,
        table: String,
        cells: Uint8Array,
        opts: Option<InsertOptions>,
    ) -> napi::Result<binding_host::Write> {
        self.binding()?
            .insert(&table, &cells, &insert_options(opts)?)
            .map_err(host_error)
    }
    fn update_large_test(
        &self,
        table: String,
        row: Uint8Array,
        cells: Uint8Array,
        updates: JsonValue,
        opts: Option<f64>,
    ) -> napi::Result<binding_host::Write> {
        self.binding()?
            .update_large_values(&table, &row, &cells, large_value_updates(updates)?, opts)
            .map_err(host_error)
    }
    fn commit_test(&self, id: String, kind: Option<String>) -> napi::Result<binding_host::Write> {
        self.binding()?
            .commit_transaction(&id, kind.as_deref())
            .map_err(host_error)
    }
    fn publish_test(&self, token: String) -> napi::Result<binding_host::Write> {
        let inner = self.open_inner()?;
        let result = inner.binding.publish_initialization_transaction(&token);
        if inner.binding.check_initialization_token(&token).is_err() {
            inner.view.forget_initialization(&token);
        }
        result.map_err(host_error)
    }
}
pub(super) trait NativeTestFinish {
    fn finish_test(&mut self) -> napi::Result<binding_host::Write>;
}
impl NativeTestFinish for StreamingMutation {
    fn finish_test(&mut self) -> napi::Result<binding_host::Write> {
        let write = self.upload.as_ref().ok_or_else(closed)?.finish()?;
        self.upload.take();
        Ok(write)
    }
}

pub(super) fn empty_cells() -> Uint8Array {
    let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
    jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
        descriptor.create(&[]).unwrap(),
        descriptor,
    ))
    .unwrap()
    .into()
}
pub(super) fn labels(db: &NapiDb) -> Vec<String> {
    let core = db.binding().unwrap();
    let query = core
        .db
        .prepare_query(&jazz::query::Query::from("items"))
        .unwrap();
    let rows = block_on(core.db.all(
        &query,
        jazz::db::ReadOpts {
            tier: jazz::db::ReadTier::LocalOnly,
            ..Default::default()
        },
    ))
    .unwrap();
    let mut labels = rows
        .into_iter()
        .map(|row| {
            match row
                .cells()
                .find(|(name, _)| *name == "label")
                .unwrap()
                .1
                .unwrap()
            {
                CoreValue::String(label) => label.clone(),
                _ => panic!("label"),
            }
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels
}
pub(super) fn delta_labels(events: Vec<SubscriptionEvent>) -> Vec<String> {
    let mut labels = Vec::new();
    for event in events {
        let Either3::A(delta) = event else {
            panic!("unexpected terminal event")
        };
        let (added, updated) =
            jazz::binding_codec::decode_subscription_delta_rows(&delta.delta).unwrap();
        for batch in added.into_iter().chain(updated) {
            for row in batch.rows {
                if let CoreValue::String(label) = &row.cells["label"] {
                    labels.push(label.clone());
                } else {
                    panic!("label")
                }
            }
        }
    }
    labels
}
pub(super) fn finish_close(db: &NapiDb) {
    if let Either::B(pending) = db.close().unwrap() {
        for _ in 0..1024 {
            if pending.poll().unwrap().is_some() {
                return;
            }
            db.tick().unwrap();
        }
        panic!("close did not settle");
    }
}
