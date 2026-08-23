use super::*;

impl Database {
    /// Open a schema-aware database over an ordered key/value store.
    ///
    /// `Database::new` does not create storage column families itself. The
    /// caller supplies storage that already has the table/index families needed
    /// by the schema; [`crate::storage::MemoryStorage`] is convenient for tests
    /// and examples.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// use groove::db::Database;
    /// use groove::schema::{
    ///     ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType,
    ///     PrimaryKey, TableSchema,
    /// };
    /// use groove::storage::MemoryStorage;
    ///
    /// let schema = DatabaseSchema::new([TableSchema::new(
    ///     "albums",
    ///     [
    ///         ColumnSchema::new("id", ColumnType::U64),
    ///         ColumnSchema::new("title", ColumnType::String),
    ///         ColumnSchema::new("year", ColumnType::U64),
    ///     ],
    /// )
    /// .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    /// .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    /// let storage = MemoryStorage::new(&["albums", "indices"]);
    ///
    /// let database = Database::new(schema, storage).await?;
    /// assert!(database.last_commit_metrics().is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub async fn new<S>(schema: DatabaseSchema, storage: S) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_with_storage_layout(schema, storage, StorageLayout::Identity).await
    }

    pub async fn new_with_storage_layout<S>(
        schema: DatabaseSchema,
        storage: S,
        storage_layout: StorageLayout,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        validate_durable_key_schema(&schema)?;
        let mut ivm_runtime = IvmRuntime::new(schema)?;
        let chunk_storage: Rc<dyn crate::chunks::ChunkStorage> =
            Rc::new(crate::chunks::MemoryChunkStorage::new());
        let chunk_resolver: Rc<dyn crate::chunks::MissingChunkResolver> =
            Rc::new(crate::chunks::UnavailableChunkResolver);
        let storage = Rc::new(LayoutStorage::new(storage, storage_layout).await?);
        ivm_runtime.set_chunk_provider(Rc::new(
            crate::chunks::StorageChunkProvider::with_resolver_and_observer(
                chunk_storage.clone(),
                chunk_resolver.clone(),
                Rc::new(MetadataChunkInstallObserver {
                    storage: Rc::downgrade(&storage),
                }),
            ),
        ));
        Ok(Self {
            storage,
            chunk_storage,
            chunk_resolver,
            ivm_runtime,
            last_commit_metrics: None,
            last_tick_metrics: None,
            storage_read_metrics: Rc::new(RefCell::new(StorageReadMetrics::default())),
            next_publication_id: 1,
            durable_publication_frontier: None,
            resident_publications: BTreeMap::new(),
            persisted_publications: BTreeSet::new(),
            resident_writes: Rc::new(RefCell::new(StagedWriteState::default())),
            publication_persistence: Rc::new(RefCell::new(PersistenceOrder {
                next: 1,
                waiters: BTreeMap::new(),
                failure: None,
            })),
            abandoned_application: Rc::new(Cell::new(false)),
            poisoned: false,
        })
    }

    pub fn durable_publication_frontier(&self) -> Option<PublicationId> {
        self.durable_publication_frontier
    }

    pub(super) fn resident_storage(&self) -> StagedWriteOverlay<'_, LayoutStorage> {
        StagedWriteOverlay::new(&self.storage, &self.resident_writes)
    }

    /// Reject any host operation after an ambiguous durable finalization.
    #[doc(hidden)]
    pub fn ensure_usable(&self) -> Result<(), Error> {
        self.ensure_not_poisoned()
    }

    /// Return approximate live bytes for one backing class/column family when
    /// the storage backend exposes that optional capability.
    pub async fn approximate_class_bytes(&self, cf: &str) -> Result<Option<u64>, Error> {
        Ok(self.storage.approximate_class_bytes(cf.to_owned()).await?)
    }

    pub fn into_storage(self) -> BoxedStorage {
        Rc::try_unwrap(self.storage)
            .unwrap_or_else(|_| panic!("database storage still has an outstanding operation"))
            .into_inner()
    }

    pub async fn close(&self) -> Result<(), Error> {
        Ok(self.storage.close().await?)
    }

    /// Configure explicit storage durability boundaries for future committed
    /// write batches.
    pub async fn set_write_flush_cadence(&self, every: usize) -> Result<(), Error> {
        Ok(self.storage.set_write_flush_cadence(every).await?)
    }

    /// Complete the current storage durability boundary.
    pub async fn flush_write_boundary(&self) -> Result<(), Error> {
        self.ensure_not_poisoned()?;
        Ok(self.storage.flush_write_boundary().await?)
    }

    pub fn set_auto_direct_family_enabled(&mut self, enabled: bool) {
        self.ivm_runtime.set_auto_direct_family_enabled(enabled);
    }

    /// Install the immutable-chunk provider used by indirect scalar evaluation.
    pub fn set_chunk_provider(&mut self, provider: Rc<dyn crate::chunks::ChunkProvider>) {
        self.ivm_runtime.set_chunk_provider(provider);
    }

    /// Install an existing verified-cache context, preserving it across a
    /// host-side rebuild of the Groove database facade.
    pub fn set_owned_chunk_provider(&mut self, provider: crate::chunks::OwnedChunkProvider) {
        self.ivm_runtime.set_owned_chunk_provider(provider);
    }

    pub fn owned_chunk_provider(&self) -> crate::chunks::OwnedChunkProvider {
        self.ivm_runtime.chunk_provider()
    }

    pub fn local_chunk_reader(&self) -> crate::chunks::LocalChunkReader {
        crate::chunks::LocalChunkReader::new(self.chunk_storage.clone())
    }

    /// Install Groove's policy-blind immutable chunk storage and route future
    /// evaluation reads directly through it.
    pub fn set_chunk_storage(&mut self, storage: Rc<dyn crate::chunks::ChunkStorage>) {
        self.ivm_runtime.set_chunk_provider(Rc::new(
            crate::chunks::StorageChunkProvider::with_resolver_and_observer(
                storage.clone(),
                self.chunk_resolver.clone(),
                Rc::new(MetadataChunkInstallObserver {
                    storage: Rc::downgrade(&self.storage),
                }),
            ),
        ));
        self.chunk_storage = storage;
    }

    pub fn set_missing_chunk_resolver(
        &mut self,
        resolver: Rc<dyn crate::chunks::MissingChunkResolver>,
    ) {
        self.ivm_runtime.set_chunk_provider(Rc::new(
            crate::chunks::StorageChunkProvider::with_resolver_and_observer(
                self.chunk_storage.clone(),
                resolver.clone(),
                Rc::new(MetadataChunkInstallObserver {
                    storage: Rc::downgrade(&self.storage),
                }),
            ),
        ));
        self.chunk_resolver = resolver;
    }

    fn fresh_chunk_locator(_: crate::large_values::ContentHash) -> crate::large_values::Locator {
        crate::large_values::Locator(uuid::Uuid::new_v4().as_bytes().to_vec())
    }

    /// Prepare and stage a complete logical value entirely inside Groove.
    pub async fn prepare_and_stage_large_value(
        &self,
        kind: crate::large_values::LargeValueKind,
        bytes: &[u8],
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let prepared = crate::large_values::prepare(kind, bytes, Self::fresh_chunk_locator)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        self.stage_large_value_preparation(prepared).await
    }

    /// Persist all immutable nodes emitted by a Groove preparation.
    pub async fn stage_large_value_preparation(
        &self,
        prepared: crate::large_values::PreparedLargeValue,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let accounting = crate::large_values::StagedLargeValueAccounting {
            encoded_bytes: prepared
                .staged_chunks
                .iter()
                .try_fold(0_u64, |total, chunk| {
                    total.checked_add(chunk.encoded.len() as u64)
                })
                .ok_or_else(|| {
                    Error::InvalidLargeValueMetadata("staged byte count overflow".to_owned())
                })?,
            node_count: prepared.staged_chunks.len() as u64,
        };
        self.chunk_storage
            .stage(prepared.staged_chunks)
            .await
            .map_err(crate::chunks::ChunkError::from)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        self.register_staged_large_value(prepared.value_ref, accounting)
            .await
    }

    async fn register_staged_large_value(
        &self,
        value_ref: crate::large_values::LargeValueRef,
        accounting: crate::large_values::StagedLargeValueAccounting,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let kind = value_ref.kind;
        let staged = crate::large_values::StagedLargeValue {
            id: crate::large_values::StagedLargeValueId(*uuid::Uuid::new_v4().as_bytes()),
            value_ref,
            accounting,
        };
        let encoded = postcard::to_allocvec(&staged).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot encode staged root: {error}"))
        })?;
        let root_key = large_value_root_key(&staged.value_ref.root)?;
        let mut references = match self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), root_key.clone())
            .await?
        {
            Some(encoded) => postcard::from_bytes(&encoded).map_err(|error| {
                Error::InvalidLargeValueMetadata(format!("cannot decode root references: {error}"))
            })?,
            None => LargeValueRootReferences::default(),
        };
        let activate_root = references.durable == 0 && references.staged == 0;
        if activate_root {
            references.node_active = true;
        }
        references.staged = references.staged.checked_add(1).ok_or_else(|| {
            Error::InvalidLargeValueMetadata("staged root count overflow".to_owned())
        })?;
        let references = postcard::to_allocvec(&references).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot encode root references: {error}"))
        })?;
        let mut operations = vec![
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: staged_large_value_key(staged.id),
                value: encoded,
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: root_key,
                value: references,
            },
        ];
        let mut node_updates =
            BTreeMap::<crate::large_values::NodeRef, LargeValueNodeReferences>::new();
        let mut pending = if activate_root {
            vec![staged.value_ref.root.clone()]
        } else {
            Vec::new()
        };
        while let Some(node_ref) = pending.pop() {
            let mut metadata = if let Some(metadata) = node_updates.remove(&node_ref) {
                metadata
            } else if let Some(encoded) = self
                .storage
                .get(
                    LARGE_VALUE_METADATA_CF.to_owned(),
                    large_value_node_key(&node_ref)?,
                )
                .await?
            {
                postcard::from_bytes(&encoded).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode node references: {error}"
                    ))
                })?
            } else {
                let encoded = self
                    .chunk_storage
                    .get(node_ref.locator.0.clone(), node_ref.object_hash)
                    .await
                    .map_err(crate::chunks::ChunkError::from)
                    .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
                let children =
                    match crate::large_values::decode_node(kind, node_ref.object_hash, &encoded)
                        .map_err(crate::ivm::runtime::IvmRuntimeError::from)?
                    {
                        crate::large_values::ChunkNode::Leaf { .. } => Vec::new(),
                        crate::large_values::ChunkNode::Branch { children, .. } => {
                            children.into_iter().map(|child| child.node_ref).collect()
                        }
                    };
                LargeValueNodeReferences {
                    references: 0,
                    children,
                }
            };
            let activate_children = metadata.references == 0;
            metadata.references = metadata.references.checked_add(1).ok_or_else(|| {
                Error::InvalidLargeValueMetadata("node reference count overflow".to_owned())
            })?;
            if activate_children {
                pending.extend(metadata.children.iter().cloned());
            }
            node_updates.insert(node_ref, metadata);
        }
        for (node_ref, metadata) in node_updates {
            operations.push(OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_node_key(&node_ref)?,
                value: postcard::to_allocvec(&metadata).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode node references: {error}"
                    ))
                })?,
            });
        }
        self.storage.write_many(operations).await?;
        Ok(staged)
    }

    /// Idempotently evict one unaccepted staging root. Jazz uses this mechanism
    /// to enforce its own quota/expiry policy; Groove owns the persisted count
    /// transition and eventual orphan reclamation.
    pub async fn evict_staged_large_value(
        &self,
        id: crate::large_values::StagedLargeValueId,
    ) -> Result<bool, Error> {
        let staged_key = staged_large_value_key(id);
        let Some(encoded) = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), staged_key.clone())
            .await?
        else {
            return Ok(false);
        };
        let staged: crate::large_values::StagedLargeValue = postcard::from_bytes(&encoded)
            .map_err(|error| {
                Error::InvalidLargeValueMetadata(format!(
                    "cannot decode staged root for eviction: {error}"
                ))
            })?;
        let root_key = large_value_root_key(&staged.value_ref.root)?;
        let encoded = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), root_key.clone())
            .await?
            .ok_or_else(|| {
                Error::InvalidLargeValueMetadata("staged root count is missing".to_owned())
            })?;
        let mut references: LargeValueRootReferences =
            postcard::from_bytes(&encoded).map_err(|error| {
                Error::InvalidLargeValueMetadata(format!(
                    "cannot decode staged root references: {error}"
                ))
            })?;
        references.staged = references.staged.checked_sub(1).ok_or_else(|| {
            Error::InvalidLargeValueMetadata("staged root count underflow".to_owned())
        })?;
        let deactivate_root =
            references.staged == 0 && references.durable == 0 && references.node_active;
        if deactivate_root {
            references.node_active = false;
        }
        let mut operations = vec![
            OwnedWriteOperation::Delete {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: staged_key,
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: root_key,
                value: postcard::to_allocvec(&references).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode staged root references: {error}"
                    ))
                })?,
            },
        ];
        let mut pending = if deactivate_root {
            vec![staged.value_ref.root.clone()]
        } else {
            Vec::new()
        };
        while let Some(node_ref) = pending.pop() {
            let key = large_value_node_key(&node_ref)?;
            let encoded = self
                .storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                .await?
                .ok_or_else(|| {
                    Error::InvalidLargeValueMetadata(
                        "reachable node reference metadata is missing".to_owned(),
                    )
                })?;
            let mut metadata: LargeValueNodeReferences =
                postcard::from_bytes(&encoded).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode node references: {error}"
                    ))
                })?;
            metadata.references = metadata.references.checked_sub(1).ok_or_else(|| {
                Error::InvalidLargeValueMetadata("node reference count underflow".to_owned())
            })?;
            if metadata.references == 0 {
                pending.extend(metadata.children.iter().cloned());
            }
            operations.push(OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key,
                value: postcard::to_allocvec(&metadata).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode node references: {error}"
                    ))
                })?,
            });
            if metadata.references == 0 {
                operations.push(OwnedWriteOperation::Set {
                    cf: LARGE_VALUE_METADATA_CF.to_owned(),
                    key: large_value_reclaim_key(&node_ref)?,
                    value: postcard::to_allocvec(&node_ref).map_err(|error| {
                        Error::InvalidLargeValueMetadata(format!(
                            "cannot encode reclaim entry: {error}"
                        ))
                    })?,
                });
            }
        }
        self.storage.write_many(operations).await?;
        Ok(true)
    }

    /// Drain persisted orphan work without walking row history. Each entry was
    /// produced by an atomic reference-count transition; deletion is exact and
    /// idempotent, so a crash leaves the queue entry available for retry.
    pub async fn reclaim_orphaned_large_value_chunks(&self, limit: usize) -> Result<usize, Error> {
        let mut scan = self
            .storage
            .scan(crate::storage::ScanRequest::prefix(
                LARGE_VALUE_METADATA_CF.to_owned(),
                b"reclaim/".to_vec(),
            ))
            .await?;
        let mut reclaimed = 0;
        'batches: while let Some(batch) = scan.next_batch().await? {
            for (queue_key, encoded_ref) in batch {
                if reclaimed >= limit {
                    break 'batches;
                }
                let node_ref: crate::large_values::NodeRef = postcard::from_bytes(&encoded_ref)
                    .map_err(|error| {
                        Error::InvalidLargeValueMetadata(format!(
                            "cannot decode reclaim entry: {error}"
                        ))
                    })?;
                let node_key = large_value_node_key(&node_ref)?;
                let Some(encoded_metadata) = self
                    .storage
                    .get(LARGE_VALUE_METADATA_CF.to_owned(), node_key.clone())
                    .await?
                else {
                    self.storage
                        .delete(LARGE_VALUE_METADATA_CF.to_owned(), queue_key)
                        .await?;
                    continue;
                };
                let metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded_metadata)
                    .map_err(|error| {
                        Error::InvalidLargeValueMetadata(format!(
                            "cannot decode reclaim node references: {error}"
                        ))
                    })?;
                if metadata.references != 0 {
                    self.storage
                        .delete(LARGE_VALUE_METADATA_CF.to_owned(), queue_key)
                        .await?;
                    continue;
                }
                self.chunk_storage
                    .delete(node_ref.locator.0.clone(), node_ref.object_hash)
                    .await
                    .map_err(crate::chunks::ChunkError::from)
                    .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
                self.storage
                    .write_many(vec![
                        OwnedWriteOperation::Delete {
                            cf: LARGE_VALUE_METADATA_CF.to_owned(),
                            key: queue_key,
                        },
                        OwnedWriteOperation::Delete {
                            cf: LARGE_VALUE_METADATA_CF.to_owned(),
                            key: node_key,
                        },
                    ])
                    .await?;
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }

    /// Low-level streaming construction hook. High-level callers use Groove's
    /// owned staging API; this callback surface exists for storage adapters and
    /// bounded-memory conformance tests.
    pub fn prepare_large_value_streaming<R: std::io::Read>(
        &self,
        kind: crate::large_values::LargeValueKind,
        reader: R,
        locator_for: impl FnMut(crate::large_values::ContentHash) -> crate::large_values::Locator,
        stage: impl FnMut(crate::large_values::StagedChunk) -> Result<(), crate::large_values::Error>,
    ) -> Result<
        (
            crate::large_values::LargeValueRef,
            crate::large_values::StreamingPrepareStats,
        ),
        Error,
    > {
        crate::large_values::prepare_streaming(kind, reader, locator_for, stage)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)
            .map_err(Into::into)
    }

    /// Stream, persist, and register a staging root without retaining the
    /// logical input or emitted chunk bytes in Jazz.
    #[cfg(not(target_family = "wasm"))]
    pub async fn prepare_and_stage_large_value_streaming<R>(
        &self,
        kind: crate::large_values::LargeValueKind,
        reader: R,
    ) -> Result<
        (
            crate::large_values::StagedLargeValue,
            crate::large_values::StreamingPrepareStats,
        ),
        Error,
    >
    where
        R: std::io::Read + Send + 'static,
    {
        use futures::{SinkExt, StreamExt};

        let (mut chunks_tx, mut chunks_rx) = futures::channel::mpsc::channel(8);
        let (result_tx, result_rx) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            let result = crate::large_values::prepare_streaming(
                kind,
                reader,
                Self::fresh_chunk_locator,
                |chunk| {
                    futures::executor::block_on(chunks_tx.send(chunk))
                        .map_err(|_| crate::large_values::Error::MalformedScalar)
                },
            );
            drop(chunks_tx);
            let _ = result_tx.send(result);
        });

        let mut encoded_bytes = 0_u64;
        let mut node_count = 0_u64;
        while let Some(chunk) = chunks_rx.next().await {
            encoded_bytes = encoded_bytes
                .checked_add(chunk.encoded.len() as u64)
                .ok_or_else(|| {
                    Error::InvalidLargeValueMetadata("staged byte count overflow".to_owned())
                })?;
            node_count = node_count.checked_add(1).ok_or_else(|| {
                Error::InvalidLargeValueMetadata("staged node count overflow".to_owned())
            })?;
            self.chunk_storage
                .stage(vec![chunk])
                .await
                .map_err(crate::chunks::ChunkError::from)
                .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        }
        let (value_ref, stats) = result_rx
            .await
            .map_err(|_| crate::large_values::Error::MalformedScalar)
            .and_then(|result| result)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        let staged = self
            .register_staged_large_value(
                value_ref,
                crate::large_values::StagedLargeValueAccounting {
                    encoded_bytes,
                    node_count,
                },
            )
            .await?;
        Ok((staged, stats))
    }

    /// Consolidate a bounded edit tail inside one Groove-owned resumable
    /// preparation. The host supplies fresh opaque locators but never drives a
    /// missing-chunk retry loop; this future retains completed local splices.
    pub async fn consolidate_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        mut fresh_locator: impl FnMut(crate::large_values::ContentHash) -> crate::large_values::Locator,
    ) -> Result<crate::large_values::PreparedLargeValue, Error> {
        let mut continuation = crate::large_values::ConsolidationContinuation::new(value)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        let mut inputs = crate::ivm::runtime::evaluation_session::EvaluationInputs::default();
        let provider = self.ivm_runtime.chunk_provider();
        loop {
            match continuation.step(&mut inputs, &mut fresh_locator) {
                Ok(Some(prepared)) => return Ok(prepared),
                Ok(None) => continue,
                Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked) => {
                    let requests = inputs.take_missing_chunks();
                    if requests.is_empty() {
                        return Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked.into());
                    }
                    for request in requests {
                        let bytes = provider
                            .get(request.clone())
                            .await
                            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
                        inputs.install_chunk_from_provider(request, bytes);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Consolidate and persist every newly emitted immutable node inside
    /// Groove, returning only the publishable physical descriptor.
    pub async fn consolidate_and_stage_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::LargeValueRef, Error> {
        let prepared = self
            .consolidate_large_value(value, Self::fresh_chunk_locator)
            .await?;
        Ok(self
            .stage_large_value_preparation(prepared)
            .await?
            .value_ref)
    }

    /// Prepare an append using the bounded edit tail, consolidating through a
    /// localized Groove continuation when the canonical tail limit is crossed.
    pub async fn append_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        bytes: Vec<u8>,
        fresh_locator: impl FnMut(crate::large_values::ContentHash) -> crate::large_values::Locator,
    ) -> Result<crate::large_values::PreparedLargeValue, Error> {
        match crate::large_values::append_tail(&value, bytes)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?
        {
            crate::large_values::TailAppendOutcome::Updated(value_ref) => {
                Ok(crate::large_values::PreparedLargeValue {
                    value_ref,
                    staged_chunks: Vec::new(),
                })
            }
            crate::large_values::TailAppendOutcome::ConsolidationRequired(transient) => {
                self.consolidate_large_value(transient, fresh_locator).await
            }
        }
    }

    /// Apply a bounded append and persist any consolidation nodes in Groove.
    pub async fn append_and_stage_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        bytes: Vec<u8>,
    ) -> Result<crate::large_values::LargeValueRef, Error> {
        let prepared = self
            .append_large_value(value, bytes, Self::fresh_chunk_locator)
            .await?;
        Ok(self
            .stage_large_value_preparation(prepared)
            .await?
            .value_ref)
    }

    /// Prepare an arbitrary byte-coordinate splice. Text boundary/UTF-16 and
    /// JSON replacement validation remain inside Groove and may suspend on the
    /// exact deleted range before producing a descriptor.
    pub async fn edit_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        offset: u64,
        delete_length: u64,
        insert_bytes: Vec<u8>,
        fresh_locator: impl FnMut(crate::large_values::ContentHash) -> crate::large_values::Locator,
    ) -> Result<crate::large_values::PreparedLargeValue, Error> {
        let mut inputs = crate::ivm::runtime::evaluation_session::EvaluationInputs::default();
        let provider = self.ivm_runtime.chunk_provider();
        let outcome = loop {
            match crate::large_values::replace_tail_attempt(
                &value,
                offset,
                delete_length,
                insert_bytes.clone(),
                &mut inputs,
            ) {
                Ok(outcome) => break outcome,
                Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked) => {
                    let requests = inputs.take_missing_chunks();
                    if requests.is_empty() {
                        return Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked.into());
                    }
                    for request in requests {
                        let bytes = provider
                            .get(request.clone())
                            .await
                            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
                        inputs.install_chunk_from_provider(request, bytes);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        match outcome {
            crate::large_values::TailEditOutcome::Updated(value_ref) => {
                Ok(crate::large_values::PreparedLargeValue {
                    value_ref,
                    staged_chunks: Vec::new(),
                })
            }
            crate::large_values::TailEditOutcome::ConsolidationRequired(transient) => {
                self.consolidate_large_value(transient, fresh_locator).await
            }
        }
    }

    /// Apply a byte-coordinate splice and persist any emitted nodes in Groove.
    pub async fn edit_and_stage_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        offset: u64,
        delete_length: u64,
        insert_bytes: Vec<u8>,
    ) -> Result<crate::large_values::LargeValueRef, Error> {
        let prepared = self
            .edit_large_value(
                value,
                offset,
                delete_length,
                insert_bytes,
                Self::fresh_chunk_locator,
            )
            .await?;
        Ok(self
            .stage_large_value_preparation(prepared)
            .await?
            .value_ref)
    }

    /// Read one byte-coordinate range from the final logical scalar while the
    /// evaluator-owned request set discovers only intersecting tree paths.
    pub async fn read_large_value_range(
        &self,
        value: &crate::large_values::LargeValueRef,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, Error> {
        let mut inputs = crate::ivm::runtime::evaluation_session::EvaluationInputs::default();
        let provider = self.ivm_runtime.chunk_provider();
        loop {
            match crate::large_values::byte_range_attempt(value, range.clone(), &mut inputs) {
                Ok(bytes) => return Ok(bytes),
                Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked) => {
                    let requests = inputs.take_missing_chunks();
                    if requests.is_empty() {
                        return Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked.into());
                    }
                    for request in requests {
                        let bytes = provider
                            .get(request.clone())
                            .await
                            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
                        inputs.install_chunk_from_provider(request, bytes);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Consume the next bounded window of a sequential logical-value cursor.
    /// Cursor position advances only after the complete window is available;
    /// suspension, cancellation, and failure expose no partial progress.
    pub async fn read_large_value_cursor_next(
        &self,
        cursor: &mut crate::large_values::LargeValueCursor,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(range) = cursor.next_range() else {
            return Ok(None);
        };
        let bytes = self
            .read_large_value_range(cursor.value(), range.clone())
            .await?;
        cursor.advance_to(range.end);
        Ok(Some(bytes))
    }

    /// Compute a BLAKE3 checksum without materializing the complete logical
    /// value. Work is split into bounded read windows and explicitly yields
    /// after each `max_bytes_per_turn`, including when every chunk is already
    /// resident. The checksum is published only after the cursor completes.
    pub async fn checksum_large_value_streaming(
        &self,
        value: crate::large_values::LargeValueRef,
        window_bytes: usize,
        max_bytes_per_turn: usize,
    ) -> Result<
        (
            crate::large_values::ContentHash,
            crate::large_values::StreamingExecutionStats,
        ),
        Error,
    > {
        let mut operator =
            crate::large_values::StreamingChecksum::new(value, window_bytes, max_bytes_per_turn)
                .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;

        while operator.cursor().remaining_bytes() != 0 {
            let range = operator
                .cursor()
                .next_range()
                .expect("non-complete cursor has a next range");
            let window = self
                .read_large_value_range(operator.cursor().value(), range)
                .await?;
            if operator
                .consume_window(&window)
                .map_err(crate::ivm::runtime::IvmRuntimeError::from)?
            {
                cooperative_yield_once().await;
                operator
                    .record_yield()
                    .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
            }
        }
        operator
            .finish()
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)
            .map_err(Error::from)
    }

    /// Read a UTF-16 code-unit range from a large string or JSON value. Range
    /// boundaries must not split a surrogate pair. Tree descent uses aggregate
    /// UTF-16 metrics, so a narrow late range does not scan its byte prefix.
    pub async fn read_large_text_utf16_range(
        &self,
        value: &crate::large_values::LargeValueRef,
        range: std::ops::Range<u64>,
    ) -> Result<String, Error> {
        let mut inputs = crate::ivm::runtime::evaluation_session::EvaluationInputs::default();
        let provider = self.ivm_runtime.chunk_provider();
        loop {
            match crate::large_values::utf16_range_attempt(value, range.clone(), &mut inputs) {
                Ok(bytes) => {
                    return String::from_utf8(bytes).map_err(|_| {
                        crate::ivm::runtime::IvmRuntimeError::from(
                            crate::large_values::Error::InvalidUtf8,
                        )
                        .into()
                    });
                }
                Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked) => {
                    let requests = inputs.take_missing_chunks();
                    if requests.is_empty() {
                        return Err(crate::ivm::runtime::IvmRuntimeError::EvaluationBlocked.into());
                    }
                    for request in requests {
                        let bytes = provider
                            .get(request.clone())
                            .await
                            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
                        inputs.install_chunk_from_provider(request, bytes);
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Resolve a JSON Pointer against literal validated JSON source. The
    /// returned value is an ordinary owned JSON value; the physical tree and
    /// locators remain private.
    pub async fn read_large_json_pointer(
        &self,
        value: &crate::large_values::LargeValueRef,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        if value.kind != crate::large_values::LargeValueKind::Json {
            return Err(crate::ivm::runtime::IvmRuntimeError::from(
                crate::large_values::Error::InvalidJson,
            )
            .into());
        }
        // JSON validity is a write-admission invariant. Reads deliberately do
        // not revalidate an unread suffix: they parse only the source demanded
        // by the pointer and fail safely if that demanded portion is invalid.
        let mut source = Vec::new();
        let mut offset = 0_u64;
        while offset < value.byte_length {
            let end = offset
                .saturating_add(crate::large_values::LEAF_MIN_BYTES as u64)
                .min(value.byte_length);
            source.extend(self.read_large_value_range(value, offset..end).await?);
            match crate::large_values::json_pointer_prefix(&source, pointer)
                .map_err(crate::ivm::runtime::IvmRuntimeError::from)?
            {
                crate::large_values::JsonPointerPrefix::Found(value) => return Ok(value),
                crate::large_values::JsonPointerPrefix::NeedMore => offset = end,
                crate::large_values::JsonPointerPrefix::RequiresFullDocument => {
                    if end < value.byte_length {
                        source.extend(
                            self.read_large_value_range(value, end..value.byte_length)
                                .await?,
                        );
                    }
                    break;
                }
            }
        }
        let json: serde_json::Value = serde_json::from_slice(&source).map_err(|_| {
            crate::ivm::runtime::IvmRuntimeError::from(crate::large_values::Error::InvalidJson)
        })?;
        Ok(json.pointer(pointer).cloned())
    }
}

async fn cooperative_yield_once() {
    let mut yielded = false;
    std::future::poll_fn(move |context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

impl Database {
    /// Include arrangement and recursive-state size walks in future tick metrics.
    ///
    /// The default is `false` because those walks are diagnostic-only and scale
    /// with retained runtime state rather than with the current commit.
    pub fn set_tick_runtime_stats_enabled(&mut self, enabled: bool) {
        self.ivm_runtime.set_tick_runtime_stats_enabled(enabled);
    }

    /// Compute full runtime stats on demand.
    pub fn runtime_stats(&self) -> RuntimeStats {
        self.ivm_runtime.stats()
    }

    pub(super) fn durable_indices_store_with_storage<'a, T>(
        &'a self,
        storage: &'a T,
        descriptor: &'a RecordDescriptor,
    ) -> RecordStore<'a, T>
    where
        T: OrderedKvStorage,
    {
        RecordStore::new(storage, "indices", descriptor)
    }

    pub fn open_batch(&self) -> DatabaseBatch {
        DatabaseBatch::default()
    }

    /// Test helper whose reads observe writes already added to the batch.
    #[cfg(test)]
    pub(crate) fn open_staged_batch(&mut self) -> StagedDatabaseBatch<'_> {
        StagedDatabaseBatch {
            database: self,
            batch: DatabaseBatch::default(),
        }
    }

    /// Return a typed handle for a schema-declared direct record store.
    ///
    /// Direct stores use record encoding and order-preserving typed primary
    /// keys, but bypass table batches, index maintenance, query planning, and
    /// IVM ticks.
    ///
    /// ```rust
    /// # futures::executor::block_on(async {
    /// use groove::db::Database;
    /// use groove::records::{RecordDescriptor, Value, ValueType};
    /// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
    /// use groove::storage::MemoryStorage;
    ///
    /// let schema = DatabaseSchema::new([]).with_direct_record_store(
    ///     DirectRecordStoreSchema::new(
    ///         "album_art",
    ///         RecordDescriptor::new([("album_id", ValueType::U64), ("side", ValueType::String)]),
    ///         RecordDescriptor::new([("bytes", ValueType::Bytes)]),
    ///     ),
    /// );
    /// let column_families = schema.column_families();
    /// let storage = MemoryStorage::new(&column_families);
    /// let database = Database::new(schema, storage).await?;
    ///
    /// let art = database.direct_record_store("album_art")?;
    /// art.set(
    ///     &[Value::U64(1), Value::String("front".into())],
    ///     &[Value::Bytes(b"front-cover-bytes".to_vec())],
    /// ).await?;
    ///
    /// let stored = art.get(&[Value::U64(1), Value::String("front".into())]).await?;
    /// assert_eq!(stored.unwrap().get("bytes")?, Value::Bytes(b"front-cover-bytes".to_vec()));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }).unwrap();
    /// ```
    pub fn direct_record_store(&self, name: &str) -> Result<DirectRecordStore<'_>, Error> {
        let schema = self.direct_record_store_schema(name)?;
        Ok(DirectRecordStore {
            storage: &self.storage,
            name: schema.name.clone(),
            key: schema.key_descriptor(),
            value: schema.value_descriptor(),
        })
    }
}
