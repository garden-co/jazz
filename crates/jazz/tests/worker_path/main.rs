//! Regression tests for the asynchronous worker storage path (#5342 review).
//! `Db` runs over [`jazz_db::test_worker::SuspendingWorker`], whose replies
//! can be held, so one storage operation stays suspended while others run.
//! Its own target, in a directory, so it builds without the integration
//! binary; fold its modules into `tests/integration` when the fixes land.

#[allow(dead_code)]
#[path = "../common/mod.rs"]
mod common;

mod callback_transaction_cancellation;
mod concurrent_callers;
