/* jazz_node crate root (Rust: crates/jazz/layers/node/src/lib.rs).
 * Jazz's node layer: the storage-backed node state machine (`node`, see
 * node/mod.h) and the per-peer sync state that drives it (`peer`, see
 * peer/peer.h). It sits directly above jazz-engine. lib.rs holds only module
 * declarations and `use` re-exports (groove, jazz_model, jazz_protocol,
 * jazz_types), so there is no executable code to port; this unit checks that
 * the two public module headers compose. */
#include "jazz_node/node/mod.h"
#include "jazz_node/peer/peer.h"
