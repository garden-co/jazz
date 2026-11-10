//! Store files whose pages are not all in memory.
//!
//! SQLite reads synchronously. A VFS over storage that only reads
//! asynchronously (IndexedDB) can still page: a read of a page it does not
//! hold records the page and fails with [`PAGE_FAULT`]. That is an extended
//! `SQLITE_BUSY`, which SQLite treats as an ordinary statement error. It
//! aborts the statement, leaves the open transaction and its savepoints
//! standing, and leaves the pager usable. (An I/O error would roll the
//! whole transaction back.) The engine's savepoints then undo the
//! statement's partial writes, as for any failed statement.
//!
//! The engine runs each unit of work that can meet a fault ([`Db::open`],
//! [`Db::tick`]) under [`PageSource::begin_op`]. When a unit fails on a
//! fault, the engine awaits [`PageSource::load`] and runs the unit again. A
//! unit is restartable: on a fault it leaves the store and the engine's
//! state as they were before it ran. While an operation is open, the pages
//! it read stay resident, so every run gets further, and an operation
//! larger than the source's budget still ends.
//!
//! # The rule for engine code: a fault is a crash of one step
//!
//! A fault can surface from any statement. Code that runs inside a unit
//! keeps two promises, so that running the unit again is safe:
//!
//! 1. **A fault is never an answer.** It is not turned into a refusal, a
//!    rejected commit, a closed session, a "not found", a cached
//!    "unknown" or a default (`.ok()`, `.unwrap_or_default()`, `Err(_) =>
//!    refuse`). Wherever errors are classified, [`is_fault`] goes first and
//!    propagates.
//! 2. **Inputs are consumed with their effects.** A step that takes work
//!    off a queue (frames from a transport, a session's backlog, rows
//!    written since the last tick, parked pulls) puts back what it has not
//!    finished when it faults, in order, ahead of anything newer. Its
//!    partial effects are undone the way a failed step's are (its
//!    savepoint rolls back; in-memory state it changed is restored).
//!    Steps that finished stay finished: the next run resumes after them,
//!    so it does not repeat their side effects (frames sent, fates
//!    recorded).
//!
//! The fault suite (`JAZZ_PAGE_FAULTS=<seed>`, [`crate::fault_vfs`]) runs
//! every engine test with faults at random reads and checks both.
//!
//! A store without a page source never faults, and runs each unit once.
//!
//! [`Db::open`]: crate::db::Db::open_at
//! [`Db::tick`]: crate::db::Db::tick

use std::future::Future;
use std::pin::Pin;

use crate::options::{Error, ErrorCode};

/// The result code a paging VFS returns for a read of a page it does not
/// hold: `SQLITE_BUSY` with an extended code of its own, so it never
/// collides with a real busy file.
pub const PAGE_FAULT: i32 = rusqlite::ffi::SQLITE_BUSY | (0x4a << 8);

/// What a paging VFS needs from the engine: where operations start and
/// end, and when to load the pages the last run of one missed.
pub trait PageSource {
    /// An operation starts: pages it reads stay resident until its
    /// [`Self::end_op`]. Operations do not nest.
    fn begin_op(&self);
    /// The operation ended (done or failed); the source may evict down to
    /// its budget.
    fn end_op(&self);
    /// Load every page a read faulted on since the last load.
    fn load(&self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + '_>>;
}

/// Whether `error` is a read that faulted on a missing page.
pub fn is_fault(error: &Error) -> bool {
    error.code == ErrorCode::PageFault
}

pub(crate) fn fault() -> Error {
    Error::new(
        ErrorCode::PageFault,
        "a page this operation needs is not loaded yet",
    )
}

/// Whether SQLite returned [`PAGE_FAULT`].
pub(crate) fn is_sqlite_fault(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(rusqlite::ffi::Error { extended_code, .. }, _)
            if *extended_code == PAGE_FAULT
    )
}

/// A [`crate::options::DbConfig`] an operation may run more than once
/// with: each run gets a copy whose row-id source and clock forward to the
/// one the caller gave, so a run that faulted hands nothing over.
pub(crate) struct SharedConfig {
    config: crate::options::DbConfig,
    ids: Option<std::rc::Rc<std::cell::RefCell<Box<dyn crate::options::RowIdSource>>>>,
    clock: Option<std::rc::Rc<dyn Fn() -> u64>>,
}

struct SharedIds(std::rc::Rc<std::cell::RefCell<Box<dyn crate::options::RowIdSource>>>);

impl crate::options::RowIdSource for SharedIds {
    fn next_row_id(&mut self) -> crate::ids::RowUuid {
        self.0.borrow_mut().next_row_id()
    }
}

impl SharedConfig {
    pub(crate) fn new(mut config: crate::options::DbConfig) -> Self {
        let ids = config
            .id_source
            .take()
            .map(|ids| std::rc::Rc::new(std::cell::RefCell::new(ids)));
        let clock = config.clock.take().map(std::rc::Rc::from);
        Self { config, ids, clock }
    }

    pub(crate) fn attempt(&self) -> crate::options::DbConfig {
        let c = &self.config;
        crate::options::DbConfig {
            schema: c.schema.clone(),
            identity: c.identity,
            id_source: self
                .ids
                .clone()
                .map(|ids| Box::new(SharedIds(ids)) as Box<dyn crate::options::RowIdSource>),
            chunk_store: c.chunk_store.clone(),
            host_chunk_io: c.host_chunk_io,
            clock: self
                .clock
                .clone()
                .map(|clock| Box::new(move || clock()) as Box<dyn Fn() -> u64>),
            migrations: c.migrations.clone(),
            system_node: c.system_node,
            page_source: c.page_source.clone(),
        }
    }
}

/// A fault an engine step cannot return where it met it was noted for the
/// run's end ([`crate::fanout`]): the fault suite counts it as handled.
pub(crate) fn deferred(error: &Error) {
    debug_assert!(is_fault(error));
    #[cfg(feature = "testing")]
    crate::fault_vfs::acknowledge();
}
