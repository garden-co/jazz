//! A durable server and clients on their own threads, for benchmarks that
//! time one client's requests while another client keeps the server busy.
//!
//! The server runs on a multi-threaded runtime of its own. Each client owns a
//! thread with a current-thread runtime, so its driver keeps ticking between
//! the requests a benchmark sends it, and no client's work runs on the
//! benchmark's thread or on another client's.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use jazz::tools::{AppContext, JazzClient, Schema};
use jazz_server::JazzServer;
use tokio::sync::mpsc;

use crate::{TestingClient, enroll_test_context};

/// The store file a persistent server opens in its data directory.
pub const SERVER_STORE_FILE: &str = "jazz.sqlite";

/// A request a client thread runs against its client.
pub type ClientFuture<T> = Pin<Box<dyn Future<Output = T>>>;
type Job = Box<dyn FnOnce(JazzClient) -> ClientFuture<()> + Send>;

/// One durable Jazz server over a pre-seeded store file.
pub struct ContentionServer {
    runtime: tokio::runtime::Runtime,
    server: Option<JazzServer>,
    schema: Schema,
    _dir: tempfile::TempDir,
}

impl ContentionServer {
    /// `seed` writes the fixture into the store file at the given path, and
    /// closes it, before the server opens it. Seeding is never measured.
    pub fn start(schema: Schema, seed: impl FnOnce(&Path)) -> Self {
        let dir = tempfile::tempdir().expect("create server data dir");
        seed(&dir.path().join(SERVER_STORE_FILE));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("contention-server")
            .build()
            .expect("server runtime");
        let server = runtime
            .block_on(
                JazzServer::builder()
                    .with_schema(schema.clone())
                    .with_persistent_storage()
                    .with_data_dir(dir.path())
                    .start(),
            )
            .expect("start contention server");
        Self {
            runtime,
            server: Some(server),
            schema,
            _dir: dir,
        }
    }

    /// Connect an ordinary JWT user (no backend or admin secret), so every
    /// request goes through the schema's row policies.
    pub fn connect_user(&self, user_id: &str) -> ClientThread {
        let server = self.server.as_ref().expect("server is running");
        let mut context = TestingClient::builder()
            .with_server(server)
            .with_schema(self.schema.clone())
            .with_user_id(user_id)
            .as_user()
            .build_context();
        self.runtime
            .block_on(enroll_test_context(&mut context))
            .expect("enroll contention client");
        ClientThread::connect(user_id, context)
    }
}

impl Drop for ContentionServer {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            let _ = self.runtime.block_on(server.shutdown());
        }
    }
}

/// A connected client on a thread of its own.
pub struct ClientThread {
    jobs: Option<mpsc::UnboundedSender<Job>>,
    thread: Option<JoinHandle<()>>,
}

impl ClientThread {
    fn connect(name: &str, context: AppContext) -> Self {
        let (jobs, mut queue) = mpsc::unbounded_channel::<Job>();
        let (ready, connected) = std_mpsc::channel();
        let thread = std::thread::Builder::new()
            .name(format!("contention-client-{name}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("client runtime");
                let local = tokio::task::LocalSet::new();
                local.block_on(&runtime, async move {
                    let client = crate::connect(context)
                        .await
                        .expect("connect contention client");
                    ready.send(()).expect("report connected client");
                    while let Some(job) = queue.recv().await {
                        job(client.clone()).await;
                    }
                    client
                        .shutdown()
                        .await
                        .expect("shut down contention client");
                });
            })
            .expect("spawn client thread");
        connected
            .recv()
            .expect("client thread connected before exiting");
        Self {
            jobs: Some(jobs),
            thread: Some(thread),
        }
    }

    /// Run `request` on the client's thread and wait for its result.
    pub fn run<T: Send + 'static>(
        &self,
        request: impl FnOnce(JazzClient) -> ClientFuture<T> + Send + 'static,
    ) -> T {
        let (reply, result) = std_mpsc::sync_channel(1);
        self.send(Box::new(move |client| {
            Box::pin(async move {
                let _ = reply.send(request(client).await);
            })
        }));
        result
            .recv()
            .expect("client thread panicked during a request")
    }

    /// Run `request` back to back, with an increasing counter, until the
    /// returned [`Background`] is stopped. Returns once the first request
    /// has finished, so warm-up is outside whatever is measured next.
    pub fn repeat(
        &self,
        mut request: impl FnMut(JazzClient, u64) -> ClientFuture<()> + Send + 'static,
    ) -> Background {
        let stop = Arc::new(AtomicBool::new(false));
        let (warm, warmed) = std_mpsc::sync_channel(1);
        let (done, samples) = std_mpsc::sync_channel(1);
        let running = Arc::clone(&stop);
        self.send(Box::new(move |client| {
            Box::pin(async move {
                request(client.clone(), 0).await;
                let _ = warm.send(());
                let mut durations = Vec::new();
                let mut n = 1;
                while !running.load(Ordering::Acquire) {
                    let started = Instant::now();
                    request(client.clone(), n).await;
                    durations.push(started.elapsed());
                    n += 1;
                }
                let _ = done.send(durations);
            })
        }));
        warmed
            .recv()
            .expect("client thread panicked during its first background request");
        Background { stop, samples }
    }

    fn send(&self, job: Job) {
        self.jobs
            .as_ref()
            .expect("client thread is running")
            .send(job)
            .expect("client thread has stopped");
    }
}

impl Drop for ClientThread {
    fn drop(&mut self) {
        drop(self.jobs.take());
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
            && !std::thread::panicking()
        {
            panic!("client thread panicked");
        }
    }
}

/// Requests a client keeps running until stopped.
pub struct Background {
    stop: Arc<AtomicBool>,
    samples: std_mpsc::Receiver<Vec<Duration>>,
}

impl Background {
    /// Stop after the request in flight and return the duration of every
    /// request after the first.
    pub fn stop(self) -> Vec<Duration> {
        self.stop.store(true, Ordering::Release);
        self.samples
            .recv()
            .expect("client thread panicked during a background request")
    }
}
