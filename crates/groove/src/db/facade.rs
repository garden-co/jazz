use super::*;

fn is_retryable_upload_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Storage(_)
            | Error::IvmRuntime(
                crate::ivm::runtime::IvmRuntimeError::Storage(_)
                    | crate::ivm::runtime::IvmRuntimeError::Chunk(
                        crate::chunks::ChunkError::Backend(_)
                            | crate::chunks::ChunkError::Unavailable
                            | crate::chunks::ChunkError::Retryable { .. }
                    )
            )
    )
}

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
        let storage = Rc::new(LayoutStorage::new(storage, storage_layout).await?);
        let chunk_storage: Rc<dyn crate::chunks::ChunkStorage> =
            Rc::new(crate::chunks::ManagedChunkStorage::new(Rc::new(
                crate::chunks::OrderedChunkStorage::new(Rc::downgrade(&storage)),
            )));
        let chunk_resolver: Rc<dyn crate::chunks::MissingChunkResolver> =
            Rc::new(crate::chunks::UnavailableChunkResolver);
        let large_value_lifecycle = std::sync::Arc::new(futures::lock::Mutex::new(()));
        ivm_runtime.set_chunk_provider(Rc::new(
            crate::chunks::StorageChunkProvider::with_resolver_observer_and_journal(
                chunk_storage.clone(),
                chunk_resolver.clone(),
                Rc::new(MetadataChunkInstallObserver {
                    storage: Rc::downgrade(&storage),
                    lifecycle: std::sync::Arc::downgrade(&large_value_lifecycle),
                    resident_install: None,
                }),
                Rc::new(MetadataChunkInstallJournal {
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
            stored_record_descriptors: RefCell::new(BTreeMap::new()),
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
            large_value_lifecycle,
            large_value_publication_lifecycle_guard: None,
            large_value_lifecycle_held: Rc::new(Cell::new(false)),
            large_value_lifecycle_publications: BTreeSet::new(),
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

    pub fn large_value_upload_cursor(
        &self,
        value: &crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::LargeValueUploadCursor, crate::large_values::Error> {
        crate::large_values::LargeValueUploadCursor::new(value, self.owned_chunk_provider())
    }

    /// Install Groove's policy-blind immutable chunk storage and route future
    /// evaluation reads directly through it.
    pub fn set_chunk_storage(&mut self, storage: Rc<dyn crate::chunks::ChunkStorage>) {
        self.ivm_runtime.set_chunk_provider(Rc::new(
            crate::chunks::StorageChunkProvider::with_resolver_observer_and_journal(
                storage.clone(),
                self.chunk_resolver.clone(),
                Rc::new(MetadataChunkInstallObserver {
                    storage: Rc::downgrade(&self.storage),
                    lifecycle: std::sync::Arc::downgrade(&self.large_value_lifecycle),
                    resident_install: None,
                }),
                Rc::new(MetadataChunkInstallJournal {
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
            crate::chunks::StorageChunkProvider::with_resolver_observer_and_journal(
                self.chunk_storage.clone(),
                resolver.clone(),
                Rc::new(MetadataChunkInstallObserver {
                    storage: Rc::downgrade(&self.storage),
                    lifecycle: std::sync::Arc::downgrade(&self.large_value_lifecycle),
                    resident_install: None,
                }),
                Rc::new(MetadataChunkInstallJournal {
                    storage: Rc::downgrade(&self.storage),
                }),
            ),
        ));
        self.chunk_resolver = resolver;
    }

    /// Prepare and stage a complete logical value entirely inside Groove.
    pub async fn prepare_and_stage_large_value(
        &self,
        kind: crate::large_values::LargeValueKind,
        bytes: &[u8],
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let prepared = crate::large_values::prepare(kind, bytes)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        self.stage_large_value_preparation(prepared).await
    }

    /// Persist all immutable nodes emitted by a Groove preparation.
    pub(crate) async fn stage_large_value_preparation(
        &self,
        prepared: crate::large_values::PreparedLargeValue,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let upload_id = crate::large_values::StagedLargeValueId(*uuid::Uuid::new_v4().as_bytes());
        self.stage_large_value_chunk_batch(
            upload_id,
            prepared.value_ref.kind,
            prepared.staged_chunks,
        )
        .await?;
        self.finalize_large_value_upload(upload_id, prepared.value_ref)
            .await
    }

    /// Stage a descriptor produced by Groove from an already authenticated
    /// local value. Such edits deliberately reuse unchanged base-tree nodes,
    /// so bind their exact derived descriptor before finalization rather than
    /// applying the raw-upload rule that every reachable node be newly owned.
    async fn stage_derived_large_value_preparation(
        &self,
        prepared: crate::large_values::PreparedLargeValue,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let upload_id = crate::large_values::StagedLargeValueId(*uuid::Uuid::new_v4().as_bytes());
        self.stage_large_value_chunk_batch(
            upload_id,
            prepared.value_ref.kind,
            prepared.staged_chunks,
        )
        .await?;
        self.bind_pending_upload_descriptor(upload_id, &prepared.value_ref)
            .await?;
        self.finalize_large_value_upload(upload_id, prepared.value_ref)
            .await
    }

    /// Install one bounded batch belonging to a remote push upload. Receipt
    /// creation remains a separate finalize operation so Groove never buffers
    /// the complete tree in memory.
    pub async fn stage_large_value_chunk_batch(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        kind: crate::large_values::LargeValueKind,
        chunks: Vec<crate::large_values::StagedChunk>,
    ) -> Result<(), Error> {
        self.stage_large_value_chunk_batch_with_presence_and_pending_limit(
            upload_id, kind, chunks, false, None,
        )
        .await?;
        Ok(())
    }

    /// Install a batch only while the exact pending journal remains present.
    /// The presence check and durable write share the lifecycle lock, so
    /// maintenance eviction cannot race a continuation into recreating it.
    pub async fn stage_large_value_chunk_batch_if_current(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        kind: crate::large_values::LargeValueKind,
        chunks: Vec<crate::large_values::StagedChunk>,
    ) -> Result<bool, Error> {
        self.stage_large_value_chunk_batch_with_presence_and_pending_limit(
            upload_id, kind, chunks, true, None,
        )
        .await
    }

    async fn stage_large_value_chunk_batch_with_presence_and_pending_limit(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        kind: crate::large_values::LargeValueKind,
        chunks: Vec<crate::large_values::StagedChunk>,
        require_existing: bool,
        pending_limit: Option<usize>,
    ) -> Result<bool, Error> {
        let _lifecycle = self.large_value_lifecycle.lock().await;
        // This must precede both chunk staging and metadata mutation. In
        // particular, a valid first child followed by a malformed second child
        // cannot strand the first in durable chunk storage.
        crate::large_values::validate_staged_chunk_batch(kind, &chunks)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        let key = pending_large_value_upload_key(upload_id);
        let mut upload: crate::large_values::PendingLargeValueUpload = if let Some(encoded) = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
            .await?
        {
            let upload: crate::large_values::PendingLargeValueUpload =
                postcard::from_bytes(&encoded).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode pending large-value upload: {error}"
                    ))
                })?;
            upload
        } else {
            if require_existing {
                return Ok(false);
            }
            if let Some(limit) = pending_limit
                && self.pending_large_value_upload_limit_reached(limit).await?
            {
                return Err(Error::PendingLargeValueUploadLimitExceeded { limit });
            }
            crate::large_values::PendingLargeValueUpload {
                id: upload_id,
                descriptor: None,
                receipt_id: None,
                accounting: crate::large_values::StagedLargeValueAccounting::default(),
                created_at_ms: web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                chunks: Vec::new(),
            }
        };
        let mut new_members = BTreeSet::new();
        for chunk in &chunks {
            if !upload.chunks.contains(&chunk.node_ref) {
                upload.accounting.encoded_bytes = upload
                    .accounting
                    .encoded_bytes
                    .checked_add(chunk.encoded.len() as u64)
                    .ok_or_else(|| {
                        Error::InvalidLargeValueMetadata("upload byte count overflow".to_owned())
                    })?;
                upload.accounting.node_count =
                    upload.accounting.node_count.checked_add(1).ok_or_else(|| {
                        Error::InvalidLargeValueMetadata("upload node count overflow".to_owned())
                    })?;
                upload.chunks.push(chunk.node_ref.clone());
                new_members.insert(chunk.node_ref.clone());
            }
        }
        let mut operations = vec![OwnedWriteOperation::Set {
            cf: LARGE_VALUE_METADATA_CF.to_owned(),
            key,
            value: postcard::to_allocvec(&upload).map_err(|error| {
                Error::InvalidLargeValueMetadata(format!(
                    "cannot encode pending large-value upload: {error}"
                ))
            })?,
        }];
        for chunk in &chunks {
            if !new_members.remove(&chunk.node_ref) {
                continue;
            }
            let node_key = large_value_node_key(&chunk.node_ref)?;
            let metadata = if let Some(encoded) = self
                .storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), node_key.clone())
                .await?
            {
                let mut metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded)
                    .map_err(|error| {
                        Error::InvalidLargeValueMetadata(format!(
                            "cannot decode pushed chunk metadata: {error}"
                        ))
                    })?;
                metadata.upload_references =
                    metadata.upload_references.checked_add(1).ok_or_else(|| {
                        Error::InvalidLargeValueMetadata(
                            "upload reference count overflow".to_owned(),
                        )
                    })?;
                metadata
            } else {
                let node = crate::large_values::decode_authenticated_node(
                    chunk.node_ref.object_hash,
                    &chunk.encoded,
                )
                .map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode pushed chunk metadata: {error}"
                    ))
                })?;
                let children = unique_large_value_children(&node);
                LargeValueNodeReferences {
                    references: 0,
                    upload_references: 1,
                    children,
                }
            };
            operations.push(OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: node_key,
                value: postcard::to_allocvec(&metadata).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode pushed chunk metadata: {error}"
                    ))
                })?,
            });
        }
        // The pending-upload record and its per-node upload references are a
        // durable intent journal. It is committed before any separate blob
        // backend put, so a crash before or during chunk staging leaves every
        // possibly-written locator discoverable by expiry/reclamation.
        self.storage.write_many(operations).await?;
        self.chunk_storage
            .stage(chunks)
            .await
            .map_err(crate::chunks::ChunkError::from)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)
            .map_err(Error::from)?;
        Ok(true)
    }

    async fn pending_large_value_upload_limit_reached(&self, limit: usize) -> Result<bool, Error> {
        if limit == 0 {
            return Ok(true);
        }
        let mut cursor = self
            .storage
            .scan(crate::storage::ScanRequest::prefix(
                LARGE_VALUE_METADATA_CF.to_owned(),
                b"upload/".to_vec(),
            ))
            .await?;
        let mut count = 0_usize;
        while let Some(batch) = cursor.next_batch().await? {
            count = count.saturating_add(batch.len());
            if count >= limit {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn descriptor_upload_id(
        value_ref: &crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::StagedLargeValueId, Error> {
        let encoded = postcard::to_allocvec(value_ref).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot encode upload descriptor: {error}"))
        })?;
        let digest = blake3::derive_key("groove pending descriptor upload v1", &encoded);
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        Ok(crate::large_values::StagedLargeValueId(id))
    }

    /// Start or resume a descriptor-keyed root-first upload.
    pub async fn begin_large_value_upload(
        &self,
        value_ref: crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::LargeValueUploadProgress, Error> {
        self.begin_large_value_upload_inner(value_ref, None).await
    }

    /// Start or resume an upload without creating more than `pending_limit`
    /// restart-persistent incomplete-upload records.
    pub async fn begin_large_value_upload_with_pending_limit(
        &self,
        value_ref: crate::large_values::LargeValueRef,
        pending_limit: usize,
    ) -> Result<crate::large_values::LargeValueUploadProgress, Error> {
        self.begin_large_value_upload_inner(value_ref, Some(pending_limit))
            .await
    }

    async fn begin_large_value_upload_inner(
        &self,
        value_ref: crate::large_values::LargeValueRef,
        pending_limit: Option<usize>,
    ) -> Result<crate::large_values::LargeValueUploadProgress, Error> {
        let upload_id = Self::descriptor_upload_id(&value_ref)?;
        self.stage_large_value_chunk_batch_with_presence_and_pending_limit(
            upload_id,
            value_ref.kind,
            Vec::new(),
            false,
            pending_limit,
        )
        .await?;
        self.bind_pending_upload_descriptor(upload_id, &value_ref)
            .await?;
        self.large_value_upload_progress(upload_id, value_ref, false)
            .await?
            .ok_or_else(|| Error::InvalidLargeValueMetadata("pending upload is missing".to_owned()))
    }

    /// Bind a descriptor-keyed peer upload before the first authenticated
    /// frontier is disclosed. Raw chunk staging deliberately has no such
    /// authority; its finalizer must prove exact chunk membership instead.
    async fn bind_pending_upload_descriptor(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        value_ref: &crate::large_values::LargeValueRef,
    ) -> Result<(), Error> {
        let _lifecycle = self.large_value_lifecycle.lock().await;
        let key = pending_large_value_upload_key(upload_id);
        let encoded = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
            .await?
            .ok_or_else(|| {
                Error::InvalidLargeValueMetadata("pending upload is missing".to_owned())
            })?;
        let mut upload: crate::large_values::PendingLargeValueUpload =
            postcard::from_bytes(&encoded).map_err(|error| {
                Error::InvalidLargeValueMetadata(format!(
                    "cannot decode pending large-value upload: {error}"
                ))
            })?;
        if let Some(bound) = &upload.descriptor {
            if bound == value_ref {
                return Ok(());
            }
            return Err(Error::InvalidLargeValueMetadata(
                "pending upload is bound to a different descriptor".to_owned(),
            ));
        }
        upload.descriptor = Some(value_ref.clone());
        self.storage
            .write_many(vec![OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key,
                value: postcard::to_allocvec(&upload).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode bound pending large-value upload: {error}"
                    ))
                })?,
            }])
            .await?;
        Ok(())
    }

    /// Install receiver-requested nodes and derive the next missing frontier.
    pub async fn continue_large_value_upload(
        &self,
        value_ref: crate::large_values::LargeValueRef,
        chunks: Vec<crate::large_values::StagedChunk>,
    ) -> Result<crate::large_values::LargeValueUploadProgress, Error> {
        self.continue_large_value_upload_with_presence(value_ref, chunks, false)
            .await?
            .ok_or_else(|| Error::InvalidLargeValueMetadata("pending upload is missing".to_owned()))
    }

    /// Continue a descriptor-bound upload only while its original pending
    /// journal remains present.
    pub async fn continue_large_value_upload_if_current(
        &self,
        value_ref: crate::large_values::LargeValueRef,
        chunks: Vec<crate::large_values::StagedChunk>,
    ) -> Result<Option<crate::large_values::LargeValueUploadProgress>, Error> {
        self.continue_large_value_upload_with_presence(value_ref, chunks, true)
            .await
    }

    async fn continue_large_value_upload_with_presence(
        &self,
        value_ref: crate::large_values::LargeValueRef,
        chunks: Vec<crate::large_values::StagedChunk>,
        require_existing: bool,
    ) -> Result<Option<crate::large_values::LargeValueUploadProgress>, Error> {
        const FRONTIER_LIMIT: usize = 64;
        let upload_id = Self::descriptor_upload_id(&value_ref)?;
        let requested = match crate::large_values::missing_upload_frontier(
            &value_ref,
            self.local_chunk_reader(),
            FRONTIER_LIMIT,
        )
        .await
        {
            Ok(requested) => requested,
            Err(crate::large_values::ReachabilityError::LargeValue(error)) => {
                return self
                    .reject_large_value_upload(
                        upload_id,
                        crate::ivm::runtime::IvmRuntimeError::from(error).into(),
                    )
                    .await;
            }
            // A corrupt locally durable chunk is terminal for this upload;
            // transport/backend availability remains retryable.
            Err(crate::large_values::ReachabilityError::Chunk(
                error @ crate::chunks::ChunkError::Integrity,
            )) => {
                return self
                    .reject_large_value_upload(
                        upload_id,
                        crate::ivm::runtime::IvmRuntimeError::from(error).into(),
                    )
                    .await;
            }
            Err(crate::large_values::ReachabilityError::Chunk(error)) => {
                return Err(crate::ivm::runtime::IvmRuntimeError::from(error).into());
            }
        };
        if chunks.is_empty() && !requested.is_empty() {
            return self
                .reject_large_value_upload(
                    upload_id,
                    Error::InvalidLargeValueMetadata(
                        "upload supplied no requested nodes".to_owned(),
                    ),
                )
                .await;
        }
        let mut new_chunks = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            if requested.contains(&chunk.node_ref) {
                new_chunks.push(chunk);
                continue;
            }
            // Another connection may have satisfied the same authenticated
            // frontier after it was sent to this peer. Such a stale response
            // is idempotent only when it is byte-for-byte the stored node.
            let already_stored = self
                .local_chunk_reader()
                .get(chunk.node_ref.locator, chunk.node_ref.object_hash)
                .await
                .is_ok_and(|encoded| encoded.as_ref() == chunk.encoded.as_slice());
            if !already_stored {
                return self
                    .reject_large_value_upload(
                        upload_id,
                        Error::InvalidLargeValueMetadata(
                            "upload supplied a node outside the authenticated missing frontier"
                                .to_owned(),
                        ),
                    )
                    .await;
            }
        }
        if let Err(error) =
            crate::large_values::validate_staged_chunk_batch(value_ref.kind, &new_chunks)
        {
            return self
                .reject_large_value_upload(
                    upload_id,
                    crate::ivm::runtime::IvmRuntimeError::from(error).into(),
                )
                .await;
        }
        let staged = if require_existing {
            self.stage_large_value_chunk_batch_if_current(upload_id, value_ref.kind, new_chunks)
                .await
        } else {
            self.stage_large_value_chunk_batch(upload_id, value_ref.kind, new_chunks)
                .await
                .map(|()| true)
        };
        if let Err(error) = staged {
            if !is_retryable_upload_error(&error) {
                return self.reject_large_value_upload(upload_id, error).await;
            }
            return Err(error);
        }
        if !staged? {
            return Ok(None);
        }
        self.bind_pending_upload_descriptor(upload_id, &value_ref)
            .await?;
        self.large_value_upload_progress(upload_id, value_ref, require_existing)
            .await
    }

    async fn large_value_upload_progress(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        value_ref: crate::large_values::LargeValueRef,
        require_existing: bool,
    ) -> Result<Option<crate::large_values::LargeValueUploadProgress>, Error> {
        let missing = match crate::large_values::missing_upload_frontier(
            &value_ref,
            self.local_chunk_reader(),
            64,
        )
        .await
        {
            Ok(missing) => missing,
            Err(error) => {
                let error = match error {
                    crate::large_values::ReachabilityError::LargeValue(error) => {
                        crate::ivm::runtime::IvmRuntimeError::from(error)
                    }
                    crate::large_values::ReachabilityError::Chunk(error) => {
                        crate::ivm::runtime::IvmRuntimeError::from(error)
                    }
                };
                let error: Error = error.into();
                if !is_retryable_upload_error(&error) {
                    return self.reject_large_value_upload(upload_id, error).await;
                }
                return Err(error);
            }
        };
        if !missing.is_empty() {
            return Ok(Some(
                crate::large_values::LargeValueUploadProgress::Missing(missing),
            ));
        }
        if let Err(error) = self.validate_completed_large_value(&value_ref).await {
            if !is_retryable_upload_error(&error) {
                return self.reject_large_value_upload(upload_id, error).await;
            }
            return Err(error);
        }
        let finalized = if require_existing {
            self.finalize_large_value_upload_if_current(upload_id, value_ref)
                .await
        } else {
            self.finalize_large_value_upload(upload_id, value_ref)
                .await
                .map(Some)
        };
        let staged = match finalized {
            Ok(Some(staged)) => staged,
            Ok(None) => return Ok(None),
            Err(error) if is_retryable_upload_error(&error) => return Err(error),
            Err(error) => return self.reject_large_value_upload(upload_id, error).await,
        };
        Ok(Some(crate::large_values::LargeValueUploadProgress::Staged(
            staged,
        )))
    }

    async fn reject_large_value_upload<T>(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        error: Error,
    ) -> Result<T, Error> {
        self.evict_pending_large_value_upload(upload_id).await?;
        Err(error)
    }

    /// Recheck the complete final logical scalar immediately before a remote
    /// upload becomes publishable. Descriptor shape checks alone cannot prove
    /// that an untrusted edit tail preserves text or JSON validity.
    async fn validate_completed_large_value(
        &self,
        value: &crate::large_values::LargeValueRef,
    ) -> Result<(), Error> {
        let mut inputs = crate::ivm::runtime::evaluation_session::EvaluationInputs::default();
        let provider = self.ivm_runtime.chunk_provider();
        loop {
            match crate::large_values::validate_edit_tail_attempt(value, &mut inputs) {
                Ok(()) => break,
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
        let mut validator = crate::large_values::LogicalValueValidator::new(value)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        let mut offset = 0_u64;
        while offset < value.byte_length {
            let end = offset
                .saturating_add(crate::large_values::LEAF_MIN_BYTES as u64)
                .min(value.byte_length);
            let bytes = self.read_large_value_range(value, offset..end).await?;
            validator
                .push(&bytes)
                .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
            offset = end;
        }
        validator
            .finish(value)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        Ok(())
    }

    /// Finalize a streamed push upload into an opaque persisted staging
    /// receipt. The later physical-record batch still validates reachability
    /// and consumes this receipt atomically.
    pub async fn finalize_large_value_upload(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        value_ref: crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        self.finalize_large_value_upload_with_presence(upload_id, value_ref)
            .await?
            .ok_or_else(|| Error::InvalidLargeValueMetadata("pending upload is missing".to_owned()))
    }

    /// Finalize only while the exact pending journal remains present.
    /// Maintenance eviction and receipt registration are serialized by the
    /// same lifecycle lock.
    pub async fn finalize_large_value_upload_if_current(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        value_ref: crate::large_values::LargeValueRef,
    ) -> Result<Option<crate::large_values::StagedLargeValue>, Error> {
        self.finalize_large_value_upload_with_presence(upload_id, value_ref)
            .await
    }

    async fn finalize_large_value_upload_with_presence(
        &self,
        upload_id: crate::large_values::StagedLargeValueId,
        value_ref: crate::large_values::LargeValueRef,
    ) -> Result<Option<crate::large_values::StagedLargeValue>, Error> {
        let _lifecycle = self.large_value_lifecycle.lock().await;
        let key = pending_large_value_upload_key(upload_id);
        let Some(encoded) = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
            .await?
        else {
            return Ok(None);
        };
        let upload: crate::large_values::PendingLargeValueUpload = postcard::from_bytes(&encoded)
            .map_err(|error| {
            Error::InvalidLargeValueMetadata(format!(
                "cannot decode pending large-value upload: {error}"
            ))
        })?;
        if let Some(bound) = &upload.descriptor
            && bound != &value_ref
        {
            return Err(Error::InvalidLargeValueMetadata(
                "pending upload is bound to a different descriptor".to_owned(),
            ));
        }
        let uploaded_chunks = upload.chunks.iter().cloned().collect();
        crate::large_values::validate_finalized_upload(
            &value_ref,
            self.local_chunk_reader(),
            &uploaded_chunks,
            upload.descriptor.is_some(),
        )
        .await
        .map_err(|error| match error {
            crate::large_values::ReachabilityError::LargeValue(error) => {
                crate::ivm::runtime::IvmRuntimeError::from(error)
            }
            crate::large_values::ReachabilityError::Chunk(error) => {
                crate::ivm::runtime::IvmRuntimeError::from(error)
            }
        })?;
        self.validate_completed_large_value(&value_ref).await?;

        // Persist the exact descriptor before creating a receipt. A crash in
        // the following receipt write is retryable only with this descriptor,
        // never with another descriptor that happens to have reachable chunks.
        let receipt_id = upload.receipt_id.unwrap_or_else(|| {
            crate::large_values::StagedLargeValueId(*uuid::Uuid::new_v4().as_bytes())
        });
        if upload.descriptor.is_none() || upload.receipt_id.is_none() {
            let mut bound_upload = upload.clone();
            bound_upload.descriptor = Some(value_ref.clone());
            bound_upload.receipt_id = Some(receipt_id);
            self.storage
                .write_many(vec![OwnedWriteOperation::Set {
                    cf: LARGE_VALUE_METADATA_CF.to_owned(),
                    key: key.clone(),
                    value: postcard::to_allocvec(&bound_upload).map_err(|error| {
                        Error::InvalidLargeValueMetadata(format!(
                            "cannot encode bound pending large-value upload: {error}"
                        ))
                    })?,
                }])
                .await?;
        }
        let staged = self
            .register_staged_large_value_with_id(receipt_id, value_ref, upload.accounting)
            .await?;
        self.release_pending_large_value_upload(key, upload).await?;
        Ok(Some(staged))
    }

    async fn release_pending_large_value_upload(
        &self,
        key: Vec<u8>,
        upload: crate::large_values::PendingLargeValueUpload,
    ) -> Result<(), Error> {
        let mut operations = vec![OwnedWriteOperation::Delete {
            cf: LARGE_VALUE_METADATA_CF.to_owned(),
            key,
        }];
        for node_ref in upload.chunks {
            let node_key = large_value_node_key(&node_ref)?;
            let Some(encoded) = self
                .storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), node_key.clone())
                .await?
            else {
                continue;
            };
            let mut metadata: LargeValueNodeReferences =
                postcard::from_bytes(&encoded).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode upload node metadata: {error}"
                    ))
                })?;
            metadata.upload_references =
                metadata.upload_references.checked_sub(1).ok_or_else(|| {
                    Error::InvalidLargeValueMetadata("upload reference count underflow".to_owned())
                })?;
            operations.push(OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: node_key,
                value: postcard::to_allocvec(&metadata).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode upload node metadata: {error}"
                    ))
                })?,
            });
            if metadata.references == 0 && metadata.upload_references == 0 {
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
        Ok(())
    }

    /// Return restart-persistent incomplete push uploads for host expiry policy.
    pub async fn pending_large_value_uploads(
        &self,
    ) -> Result<Vec<crate::large_values::PendingLargeValueUpload>, Error> {
        let mut cursor = self
            .storage
            .scan(crate::storage::ScanRequest::prefix(
                LARGE_VALUE_METADATA_CF.to_owned(),
                b"upload/".to_vec(),
            ))
            .await?;
        let mut uploads = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            for (_, encoded) in batch {
                uploads.push(postcard::from_bytes(&encoded).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode pending large-value upload: {error}"
                    ))
                })?);
            }
        }
        Ok(uploads)
    }

    /// Idempotently expire one incomplete push upload and queue unreferenced
    /// chunks for the ordinary Groove reclaimer.
    pub async fn evict_pending_large_value_upload(
        &self,
        id: crate::large_values::StagedLargeValueId,
    ) -> Result<bool, Error> {
        let _lifecycle = self.large_value_lifecycle.lock().await;
        let key = pending_large_value_upload_key(id);
        let Some(encoded) = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
            .await?
        else {
            return Ok(false);
        };
        let upload = postcard::from_bytes(&encoded).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!(
                "cannot decode pending large-value upload: {error}"
            ))
        })?;
        self.release_pending_large_value_upload(key, upload).await?;
        Ok(true)
    }

    async fn register_staged_large_value_with_id(
        &self,
        id: crate::large_values::StagedLargeValueId,
        value_ref: crate::large_values::LargeValueRef,
        accounting: crate::large_values::StagedLargeValueAccounting,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let staged_key = staged_large_value_key(id);
        if let Some(encoded) = self
            .storage
            .get(LARGE_VALUE_METADATA_CF.to_owned(), staged_key.clone())
            .await?
        {
            let existing: crate::large_values::StagedLargeValue = postcard::from_bytes(&encoded)
                .map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode existing staged root: {error}"
                    ))
                })?;
            if existing.value_ref == value_ref && existing.accounting == accounting {
                return Ok(existing);
            }
            return Err(Error::InvalidLargeValueMetadata(
                "staged receipt id is already bound to a different descriptor".to_owned(),
            ));
        }
        let staged = crate::large_values::StagedLargeValue {
            id,
            value_ref,
            accounting,
            created_at_ms: web_time::SystemTime::now()
                .duration_since(web_time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
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
                key: staged_key,
                value: encoded,
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: root_key,
                value: references,
            },
        ];
        if activate_root {
            operations.extend(
                large_value_node_transition_operations(
                    &self.storage,
                    BTreeMap::new(),
                    vec![(staged.value_ref.root.clone(), 1)],
                    false,
                )
                .await?,
            );
        }
        self.storage.write_many(operations).await?;
        Ok(staged)
    }

    /// Return persisted opaque staging receipts for host rate/expiry policy.
    pub async fn staged_large_values(
        &self,
    ) -> Result<Vec<crate::large_values::StagedLargeValue>, Error> {
        let mut cursor = self
            .storage
            .scan(crate::storage::ScanRequest::prefix(
                LARGE_VALUE_METADATA_CF.to_owned(),
                b"staged/".to_vec(),
            ))
            .await?;
        let mut staged = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            for (_, encoded) in batch {
                staged.push(postcard::from_bytes(&encoded).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot decode staged root receipt: {error}"
                    ))
                })?);
            }
        }
        Ok(staged)
    }

    /// Idempotently evict one unaccepted staging root. Jazz uses this mechanism
    /// to enforce its own expiry/admission policy; Groove owns the persisted
    /// count transition and eventual orphan reclamation.
    pub async fn evict_staged_large_value(
        &self,
        id: crate::large_values::StagedLargeValueId,
    ) -> Result<bool, Error> {
        let _lifecycle = self.large_value_lifecycle.lock().await;
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
        if deactivate_root {
            operations.extend(
                large_value_node_transition_operations(
                    &self.storage,
                    BTreeMap::new(),
                    vec![(staged.value_ref.root.clone(), -1)],
                    false,
                )
                .await?,
            );
        }
        self.storage.write_many(operations).await?;
        Ok(true)
    }

    /// Drain persisted orphan work without walking row history. Each entry was
    /// produced by an atomic reference-count transition; deletion is exact and
    /// idempotent, so a crash leaves the queue entry available for retry.
    pub async fn reclaim_orphaned_large_value_chunks(&self, limit: usize) -> Result<usize, Error> {
        let _lifecycle = self.large_value_lifecycle.lock().await;
        // A request may have authenticated a branch but not yet fetched all of
        // its descendants. Treat the whole provider request/lease population
        // as one coarse ephemeral retainer: reclamation is maintenance work,
        // so deferring a pass is preferable to deleting a not-yet-requested
        // descendant beneath an active evaluation. The guard also prevents a
        // new request from racing the hash-guarded delete awaits below.
        let Some(_reclamation_guard) = self.ivm_runtime.chunk_provider().try_begin_reclamation()
        else {
            return Ok(0);
        };
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
                if metadata.references != 0 || metadata.upload_references != 0 {
                    self.storage
                        .delete(LARGE_VALUE_METADATA_CF.to_owned(), queue_key)
                        .await?;
                    continue;
                }
                self.chunk_storage
                    .delete(node_ref.locator, node_ref.object_hash)
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

    /// Consolidate a bounded edit tail inside one Groove-owned resumable
    /// preparation. Groove allocates fresh capabilities and retains completed
    /// local splices while it drives any missing-chunk retry loop.
    pub async fn consolidate_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::PreparedLargeValue, Error> {
        let mut continuation = crate::large_values::ConsolidationContinuation::new(value)
            .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
        let mut inputs = crate::ivm::runtime::evaluation_session::EvaluationInputs::default();
        let provider = self.ivm_runtime.chunk_provider();
        loop {
            match continuation.step(&mut inputs) {
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
    /// Groove, returning the opaque staging claim for later publication.
    pub async fn consolidate_and_stage_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let prepared = self.consolidate_large_value(value).await?;
        // Consolidation retains authenticated unchanged base nodes. Keep the
        // derived-receipt distinction here, where this local provenance is
        // still known, rather than weakening raw peer-upload admission.
        self.stage_derived_large_value_preparation(prepared).await
    }

    /// Prepare an append using the bounded edit tail, consolidating through a
    /// localized Groove continuation when the canonical tail limit is crossed.
    pub async fn append_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        bytes: Vec<u8>,
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
                self.consolidate_large_value(transient).await
            }
        }
    }

    /// Apply a bounded append and persist any consolidation nodes in Groove.
    pub async fn append_and_stage_large_value(
        &self,
        value: crate::large_values::LargeValueRef,
        bytes: Vec<u8>,
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let prepared = self.append_large_value(value, bytes).await?;
        self.stage_derived_large_value_preparation(prepared).await
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
                self.consolidate_large_value(transient).await
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
    ) -> Result<crate::large_values::StagedLargeValue, Error> {
        let prepared = self
            .edit_large_value(value, offset, delete_length, insert_bytes)
            .await?;
        self.stage_derived_large_value_preparation(prepared).await
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
        if !matches!(
            value.kind,
            crate::large_values::LargeValueKind::Json | crate::large_values::LargeValueKind::String
        ) {
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
