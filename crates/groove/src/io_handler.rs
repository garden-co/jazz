//! IoHandler trait - Platform abstraction for I/O and scheduling.
//!
//! RuntimeCore is generic over this trait, allowing both native (tokio)
//! and WASM platforms to share the same core logic while handling
//! I/O and scheduling differently.

use crate::storage::{StorageRequest, StorageResponse};
use crate::sync_manager::OutboxEntry;

/// Platform abstraction for I/O operations and tick scheduling.
///
/// Implementations provide:
/// - Storage request dispatch (fire-and-forget, responses come back via parking)
/// - Sync message dispatch to the network
/// - Batched tick scheduling (debounced)
///
/// # Platform Implementations
///
/// - **TokioIoHandler** (groove-tokio): Uses tokio::spawn for scheduling,
///   Arc<Mutex> for shared state, AtomicBool for debouncing.
///
/// - **WasmIoHandler** (groove-wasm): Uses wasm_bindgen_futures::spawn_local,
///   Rc<RefCell> for shared state, JS callbacks for I/O.
pub trait IoHandler {
    /// Send a single storage request (fire-and-forget).
    ///
    /// The response will be delivered later via the runtime's
    /// `park_storage_response()` method. The IoHandler implementation
    /// is responsible for routing responses back to the core.
    fn send_storage_request(&mut self, request: StorageRequest);

    /// Send a sync message to the network.
    ///
    /// The message should be delivered to connected peers/servers
    /// according to the destination in the OutboxEntry.
    fn send_sync_message(&mut self, message: OutboxEntry);

    /// Schedule the next batched tick.
    ///
    /// This should be debounced - if a tick is already scheduled,
    /// this call should be a no-op. The scheduled tick should call
    /// `RuntimeCore::batched_tick()` when it fires.
    ///
    /// Platform implementations:
    /// - Tokio: `tokio::spawn` with `AtomicBool` debounce flag
    /// - WASM: `wasm_bindgen_futures::spawn_local` with `Rc<RefCell<bool>>` flag
    fn schedule_batched_tick(&self);

    /// Take any pending storage responses.
    ///
    /// For synchronous drivers (Tokio/RocksDB), storage responses are
    /// available immediately after `send_storage_request`. This method
    /// drains those responses so they can be processed.
    ///
    /// For async drivers (WASM), responses come back via callback and
    /// are parked directly on RuntimeCore, so this returns empty.
    fn take_pending_responses(&mut self) -> Vec<StorageResponse> {
        Vec::new()
    }
}

/// Null IoHandler for testing - does nothing.
///
/// Useful for unit tests that only exercise synchronous logic
/// and don't need actual I/O or scheduling.
#[derive(Default)]
pub struct NullIoHandler;

impl IoHandler for NullIoHandler {
    fn send_storage_request(&mut self, _request: StorageRequest) {
        // Drop the request - used for testing without storage
    }

    fn send_sync_message(&mut self, _message: OutboxEntry) {
        // Drop the message - used for testing without sync
    }

    fn schedule_batched_tick(&self) {
        // No-op - testing mode doesn't schedule ticks
    }
}

/// Test IoHandler with synchronous driver.
///
/// Processes storage requests immediately through the driver and stores
/// responses for retrieval via `take_pending_responses()`. This allows
/// tests to use real storage behavior without async scheduling.
///
/// # Example
///
/// ```ignore
/// let handler = TestIoHandler::new(TestDriver::new());
/// let mut core = RuntimeCore::new(schema_manager, handler);
/// core.insert("users", values)?;
/// core.immediate_tick();
/// core.batched_tick(); // Processes pending storage
/// ```
pub struct TestIoHandler<D: crate::driver::Driver> {
    driver: D,
    pending_responses: Vec<StorageResponse>,
}

impl<D: crate::driver::Driver> TestIoHandler<D> {
    /// Create a new TestIoHandler wrapping the given driver.
    pub fn new(driver: D) -> Self {
        Self {
            driver,
            pending_responses: Vec::new(),
        }
    }

    /// Get mutable access to the underlying driver.
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }
}

impl<D: crate::driver::Driver> IoHandler for TestIoHandler<D> {
    fn send_storage_request(&mut self, request: StorageRequest) {
        // Process synchronously through driver
        let responses = self.driver.process(vec![request]);
        self.pending_responses.extend(responses);
    }

    fn send_sync_message(&mut self, _message: OutboxEntry) {
        // Drop sync messages in tests (unless test needs them)
    }

    fn schedule_batched_tick(&self) {
        // No-op - tests call batched_tick explicitly
    }

    fn take_pending_responses(&mut self) -> Vec<StorageResponse> {
        std::mem::take(&mut self.pending_responses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_io_handler_implements_trait() {
        let mut handler = NullIoHandler;
        // These should not panic
        handler.send_storage_request(crate::storage::StorageRequest::CreateObject {
            id: crate::object::ObjectId::new(),
            metadata: std::collections::HashMap::new(),
        });
        handler.send_sync_message(crate::sync_manager::OutboxEntry {
            destination: crate::sync_manager::Destination::Server(
                crate::sync_manager::ServerId::new(),
            ),
            payload: crate::sync_manager::SyncPayload::ObjectUpdated {
                object_id: crate::object::ObjectId::new(),
                metadata: None,
                branch_name: crate::object::BranchName::new("main"),
                commits: vec![],
            },
        });
        handler.schedule_batched_tick();
    }

    #[test]
    fn test_io_handler_processes_through_driver() {
        use crate::driver::TestDriver;

        let driver = TestDriver::new();
        let mut handler = TestIoHandler::new(driver);

        // Send a storage request
        let object_id = crate::object::ObjectId::new();
        handler.send_storage_request(StorageRequest::CreateObject {
            id: object_id,
            metadata: std::collections::HashMap::new(),
        });

        // Responses should be available immediately
        let responses = handler.take_pending_responses();
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            StorageResponse::CreateObject { id, result } => {
                assert_eq!(*id, object_id);
                assert!(result.is_ok());
            }
            _ => panic!("Expected CreateObject response"),
        }

        // Second take should return empty
        let responses = handler.take_pending_responses();
        assert!(responses.is_empty());
    }
}
