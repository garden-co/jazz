//! Native database startup and execution. The session owner receives an open
//! Db; only this adapter opens a StoreFile, blocks on futures, and owns the
//! background chunk-I/O threads. StoreLoop schedules calls to this adapter.

use super::*;

pub(super) struct NativeOwner {
    pub(super) core: Owner,
    chunk_io: Option<ChunkIoRunner>,
}

struct StoreWake(std_mpsc::Sender<Work>);
impl std::task::Wake for StoreWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(Work::Owner(Box::new(|_| {})));
    }
}

impl NativeOwner {
    /// Open the runtime on `file`. A reopened store admits every version
    /// it admitted before (the engine reads them from its catalogue) and
    /// makes its current permissions revision current again. `schema` is
    /// current instead when the store has no revision yet, or (unless
    /// `any_stored_catalogue`) none whose catalogue admits `schema`.
    pub(super) fn open(
        schema: JazzSchema,
        file: StoreFile,
        chunk_store: Option<ChunkStoreChoice>,
        wake: std_mpsc::Sender<Work>,
        any_stored_catalogue: bool,
        app: Option<String>,
    ) -> Result<Self, String> {
        let rows = file
            .catalogue()
            .map_err(|error| format!("read catalogue: {error}"))?;
        let stored = any_stored_catalogue
            || rows
                .schemas
                .iter()
                .any(|stored| stored.version == Some(schema.version_id()));
        let head = rows.head().filter(|_| stored);
        let compile = |hash: &jazz::tools::public_schema::SchemaHash| {
            let structural = rows
                .schema(hash)
                .ok_or("the current permissions revision's schema is missing")?;
            JazzSchema::new(&structural.schema)
                .map_err(|error| format!("catalogue schema: {error}"))
        };
        // Every activation records a revision: without one, the store's
        // genesis is current.
        let current = match (head, rows.genesis().filter(|_| any_stored_catalogue)) {
            (Some(head), _) => with_permissions(
                &compile(&head.schema)?,
                &head.permissions.clone().into_iter().collect(),
            )?,
            (None, Some(genesis)) => compile(&genesis.hash)?,
            (None, None) => schema,
        };
        let config = match &chunk_store {
            Some(choice) if choice.network => config(current)
                .with_chunk_store(Arc::clone(&choice.store))
                .with_host_chunk_io(),
            Some(choice) => config(current).with_chunk_store(Arc::clone(&choice.store)),
            None => config(current),
        };
        let chunk_io = match chunk_store {
            Some(choice) if choice.network => {
                Some(ChunkIoRunner::start(choice.store, wake.clone())?)
            }
            _ => None,
        };
        let db = block_on(Db::open_file(config, file))
            .map_err(|error| format!("open server store: {error}"))?;
        let mut core = Owner::from_db(db, app)?;
        core.wake = std::task::Waker::from(Arc::new(StoreWake(wake)));
        Ok(Self { core, chunk_io })
    }

    pub(super) fn tick(&mut self) {
        block_on(self.core.advance());
        if let Some(runner) = &self.chunk_io {
            runner.pump(&self.core);
        }
        self.core.flush();
    }

    pub(super) fn close(self) {
        self.core.close();
    }

    /// Fetches run beside a batch came back: install them. A failure
    /// refuses the reads waiting on them, and holds fetches back (batches
    /// go on without them) for the backoff.
    pub(super) fn finish_chunk_fetches(&mut self, done: jazz::chunk_store::ChunkIoDone) {
        let finished = self.core.db.finish_chunk_fetches(done);
        let log = match &self.chunk_io {
            Some(runner) => runner.fetched(finished.is_err()),
            None => finished.is_err(),
        };
        if let Err(error) = finished
            && log
        {
            tracing::warn!(%error, app = self.core.app.as_deref().unwrap_or("?"), "large-value store fetch failed; the reads waiting on it are refused, and fetches retry with backoff");
        }
    }

    pub(super) fn finish_chunk_io(&mut self, done: jazz::chunk_store::ChunkIoDone) {
        // Fetches that failed in a batch hold fetches back as the fetch
        // lane's do, so the next batch does not ask again at once.
        let fetches_failed = done.fetches_failed();
        let finished = self.core.db.finish_chunk_io(done);
        let log = match &self.chunk_io {
            Some(runner) => runner.returned_with(finished.is_err(), fetches_failed),
            None => finished.is_err(),
        };
        if let Err(error) = finished
            && log
        {
            tracing::warn!(%error, app = self.core.app.as_deref().unwrap_or("?"), "large-value store I/O failed; retrying with backoff");
        }
    }
}
