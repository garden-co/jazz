//! Bounded native workers for committed subscription openings. SQL, policy
//! compilation and decoding are the ordinary engine path. The owner pins both
//! row and chunk snapshots before yielding; dependency invalidation cancels an
//! obsolete opening before the owner can publish it.

use super::*;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};
use std::thread::{self, JoinHandle};

type Rows = Result<Vec<CurrentRow>, Error>;
type Wake = Arc<dyn Fn() + Send + Sync>;

struct State {
    done: bool,
    canceled: bool,
    result: Option<Rows>,
}

struct Task {
    state: Arc<Mutex<State>>,
    interrupt: rusqlite::InterruptHandle,
}

impl Drop for Task {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock()
            && !state.done
        {
            state.canceled = true;
            self.interrupt.interrupt();
        }
    }
}

#[derive(Default)]
pub(super) struct Read {
    task: Option<Task>,
}

impl Read {
    pub(super) fn invalidate(&mut self) {
        // Dropping a running task interrupts its SQL. Its result has no
        // consumer; the worker still returns its connection to the pool.
        self.task.take();
    }
}

struct Reader {
    conn: Connection,
    chunks: Arc<crate::large::Chunks>,
}

impl Reader {
    fn open(path: &Path) -> Result<Self, Error> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(storage)?;
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        // This shadows the persistent scratch table on this connection.
        // A read never obtains the main file's writer lock for its preps.
        conn.execute_batch(
            "PRAGMA cache_size = -8192;
            CREATE TEMP TABLE _jazz_lv_scratch (slot INTEGER NOT NULL, cell BLOB NOT NULL,
            v BLOB, PRIMARY KEY (slot, cell)) WITHOUT ROWID;",
        )
        .map_err(storage)?;
        Ok(Self {
            conn,
            chunks: crate::large::Chunks::install(Some((path, None)))?,
        })
    }

    fn begin(&self) -> Result<(), Error> {
        self.conn.execute_batch("BEGIN").map_err(storage)?;
        let pinned = (|| {
            self.conn
                .query_row("SELECT version FROM _jazz_format LIMIT 1", [], |r| {
                    r.get::<_, i64>(0)
                })
                .map_err(storage)?;
            self.chunks.begin_read_snapshot()
        })();
        if pinned.is_err() {
            let _ = self.end();
        }
        pinned
    }

    fn end(&self) -> Result<(), Error> {
        let rows = self.conn.execute_batch("ROLLBACK").map_err(storage);
        let chunks = self.chunks.end_read_snapshot();
        rows.and(chunks)
    }
}

struct Job {
    reader: Reader,
    compiled: Compiled,
    table: String,
    state: Arc<Mutex<State>>,
    #[cfg(any(test, feature = "testing"))]
    before: Option<Wake>,
}

struct Slot {
    sender: Option<mpsc::SyncSender<Job>>,
    reader: Option<Reader>,
    interrupt: rusqlite::InterruptHandle,
    join: Option<JoinHandle<()>>,
}

pub(super) struct Pool {
    path: PathBuf,
    slots: Vec<Slot>,
    returned: mpsc::Receiver<(usize, Option<Reader>)>,
    fault: Option<String>,
    #[cfg(any(test, feature = "testing"))]
    before: Option<Wake>,
}

impl Pool {
    fn open(path: &Path, workers: usize, wake: Wake) -> Result<Self, Error> {
        let (returned, receiver) = mpsc::channel();
        let mut pool = Self {
            path: path.to_owned(),
            slots: Vec::new(),
            returned: receiver,
            fault: None,
            #[cfg(any(test, feature = "testing"))]
            before: None,
        };
        for index in 0..workers {
            let reader = Reader::open(path)?;
            let interrupt = reader.conn.get_interrupt_handle();
            let (sender, jobs) = mpsc::sync_channel::<Job>(1);
            let returned = returned.clone();
            let wake = wake.clone();
            let join = thread::Builder::new()
                .name(format!("jazz-read-{index}"))
                .spawn(move || {
                    while let Ok(job) = jobs.recv() {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            #[cfg(any(test, feature = "testing"))]
                            if let Some(before) = &job.before {
                                before();
                            }
                            if job.state.lock().map_or(true, |state| state.canceled) {
                                return Err(Error::new(ErrorCode::Storage, "SQLite read canceled"));
                            }
                            Inner::execute_on(
                                &job.reader.conn,
                                &job.reader.chunks,
                                &job.compiled,
                                &job.table,
                            )
                        }))
                        .unwrap_or_else(|_| {
                            Err(Error::new(
                                ErrorCode::Storage,
                                "SQLite read worker panicked",
                            ))
                        });
                        let ended = job.reader.end();
                        let reusable = ended.is_ok();
                        let result = match ended {
                            Ok(()) => result,
                            Err(error) => Err(error),
                        };
                        if let Ok(mut state) = job.state.lock() {
                            state.done = true;
                            state.result = Some(result);
                        }
                        let reader = reusable.then_some(job.reader);
                        if returned.send((index, reader)).is_err() {
                            break;
                        }
                        wake();
                    }
                })
                .map_err(|error| {
                    Error::new(
                        ErrorCode::Storage,
                        format!("start SQLite read worker: {error}"),
                    )
                })?;
            pool.slots.push(Slot {
                sender: Some(sender),
                reader: Some(reader),
                interrupt,
                join: Some(join),
            });
        }
        Ok(pool)
    }

    pub(super) fn poll(
        &mut self,
        read: &mut Read,
        compiled: &Compiled,
        table: &str,
    ) -> Result<Option<Vec<CurrentRow>>, Error> {
        if let Some(fault) = &self.fault {
            return Err(Error::new(ErrorCode::Storage, fault.clone()));
        }
        for (index, reader) in self.returned.try_iter() {
            let reader = match reader {
                Some(reader) => reader,
                None => match Reader::open(&self.path) {
                    Ok(reader) => reader,
                    Err(error) => {
                        self.fault = Some(error.to_string());
                        return Err(error);
                    }
                },
            };
            self.slots[index].interrupt = reader.conn.get_interrupt_handle();
            self.slots[index].reader = Some(reader);
        }
        if let Some(task) = &read.task {
            let result = task
                .state
                .lock()
                .map_err(|_| Error::new(ErrorCode::Storage, "SQLite read result poisoned"))?
                .result
                .take();
            if let Some(result) = result {
                read.task.take();
                return result.map(Some);
            }
            return Ok(None);
        }
        let Some(slot) = self.slots.iter_mut().find(|slot| slot.reader.is_some()) else {
            // Backpressure: leave the opening registered on the owner.
            // No additional jobs, threads or pinned snapshots are queued.
            return Ok(None);
        };
        let reader = slot.reader.take().expect("idle reader");
        if let Err(error) = reader.begin() {
            slot.reader = Some(reader);
            return Err(error);
        }
        let state = Arc::new(Mutex::new(State {
            done: false,
            canceled: false,
            result: None,
        }));
        let task = Task {
            state: state.clone(),
            interrupt: reader.conn.get_interrupt_handle(),
        };
        let job = Job {
            reader,
            compiled: compiled.clone(),
            table: table.to_owned(),
            state,
            #[cfg(any(test, feature = "testing"))]
            before: self.before.clone(),
        };
        if slot
            .sender
            .as_ref()
            .expect("open worker")
            .try_send(job)
            .is_err()
        {
            let message = "SQLite read worker closed";
            self.fault = Some(message.to_owned());
            return Err(Error::new(ErrorCode::Storage, message));
        }
        read.task = Some(task);
        Ok(None)
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            slot.sender.take();
            slot.interrupt.interrupt();
            slot.reader.take();
        }
        for slot in &mut self.slots {
            if let Some(join) = slot.join.take() {
                let _ = join.join();
            }
        }
        // Returned connections close here, while the writer is still open.
        for (_, reader) in self.returned.try_iter() {
            drop(reader);
        }
    }
}

impl Db {
    /// Whether this store can serve concurrent native WAL readers.
    pub fn supports_concurrent_reads(&self) -> bool {
        let inner = self.inner();
        inner.client.is_none()
            && inner.read_path.is_some()
            && inner
                .conn
                .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .is_ok_and(|mode| mode == "wal")
    }

    /// Offload committed subscription openings to bounded native readers.
    /// Call before opening subscriptions. Local/transaction reads and ongoing
    /// subscription maintenance retain their ordinary owner execution.
    /// `wake` asks the host to tick when a worker finishes.
    pub fn enable_concurrent_reads(&self, workers: usize, wake: Wake) -> Result<(), Error> {
        let mut inner = self.inner();
        if workers == 0 || workers > 64 || !inner.fan.live_members().is_empty() {
            return Err(unsupported(
                "configuring concurrent reads after subscription admission or outside 1..=64 workers",
            ));
        }
        let mode: String = inner
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(storage)?;
        let path = inner.read_path.as_deref();
        if inner.client.is_some() || mode != "wal" || path.is_none() {
            return Err(unsupported(
                "concurrent reads outside a native file-backed WAL authority",
            ));
        }
        let pool = Pool::open(path.expect("file path"), workers, wake)?;
        inner.read_pool = Some(RefCell::new(pool));
        Ok(())
    }

    /// Tests: pause a worker after both committed snapshots are pinned.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn set_read_worker_hook_for_test(&self, before: Wake) {
        self.inner()
            .read_pool
            .as_ref()
            .expect("read pool")
            .borrow_mut()
            .before = Some(before);
    }
}
