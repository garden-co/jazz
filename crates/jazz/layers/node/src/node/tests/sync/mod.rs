// Synchronization behavior grouped by protocol phase and persisted state boundary.

include!("commit_causality.rs");
include!("receiver_batches.rs");
include!("convergence_and_fates.rs");
include!("repair.rs");
include!("performance_receipt.rs");
include!("known_state.rs");
include!("view_delivery.rs");
