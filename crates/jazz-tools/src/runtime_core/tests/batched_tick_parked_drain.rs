//! Regression tests for the parked-message / deferred-subscription deadlock
//! between `batched_tick`'s early-return gates and `handle_sync_messages`.
//!
//! The bug surfaces in two places in `runtime_core::ticks::batched_tick`:
//!
//! * Line 718-725 returns early when `has_pending_query_subscriptions()` is
//!   true, *before* `handle_sync_messages()` at line 729 has had a chance to
//!   drain `parked_sync_messages`. The drain is the only path that empties the
//!   queue (`mem::take` at ticks.rs:881), so a parked
//!   `SyncPayload::CatalogueEntryUpdated` that would satisfy the deferred
//!   subscription is stranded.
//! * Both early-return gates (lines 724 and 741) unconditionally call
//!   `schedule_batched_tick()` before returning. With no JS-side debounce in
//!   `crates/jazz-rn`'s scheduler, that becomes a microtask hot-loop that pegs
//!   CPU and starves `setInterval`.
//!
//! The tests below are organised around the invariants the fix must restore:
//!
//! 1. [`batched_tick_strands_parked_messages_when_subscription_deferred`] —
//!    parked messages must be drained even when subs are pending.
//! 2. [`batched_tick_hot_spins_when_subscription_stays_deferred`] —
//!    `batched_tick` must not reschedule itself on a tick that made no
//!    forward progress.
//! 3. [`parked_catalogue_unblocks_deferred_subscription`] — end-to-end: a
//!    parked `CatalogueEntryUpdated` whose schema satisfies a deferred sub
//!    must, after a bounded number of ticks, leave the subscription compiled
//!    and the parked queue empty.
//! 4. [`batched_tick_drains_parked_messages_in_normal_path`] — control test:
//!    without any deferred subscription, `batched_tick` already drains parked
//!    messages; the fix must not regress this path.

use super::*;

use crate::catalogue::CatalogueEntry;
use crate::query_manager::query::Query;
use crate::query_manager::types::{ComposedBranchName, SchemaHash};
use crate::sync_manager::{QueryId, QueryPropagation};

type DeferredServerCore = RuntimeCore<MemoryStorage, CountingScheduler>;

/// Build a server-mode `RuntimeCore` with an empty schema and one downstream
/// client that has already sent a `QuerySubscription`. The subscription is
/// guaranteed to be deferred because `build_server_subscription_context`
/// (`query_manager/server_queries.rs:139`) returns `None` when `self.schema`
/// is empty and the query's branch does not parse as a `ComposedBranchName` —
/// the sub goes back into `pending_query_subscriptions` via `deferred.push`
/// at line 815.
///
/// Returns the core, the client id, and the branch string used by the
/// subscription (so callers can craft a matching `CatalogueEntryUpdated`).
/// App name used by all fixtures in this module. Tests that build a real
/// `CatalogueEntryUpdated` must use the same app name on the donor runtime,
/// otherwise `process_catalogue_schema` (schema_manager/manager.rs:1261)
/// rejects the entry with "different app, ignore" and the schema is never
/// added to `known_schemas`.
const FIXTURE_APP_NAME: &str = "deferred-sub-fixture";

fn server_with_deferred_subscription(
    scheduler: CountingScheduler,
    query_branch: String,
    query_id: QueryId,
) -> (DeferredServerCore, ClientId) {
    let sync_manager = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let manager =
        SchemaManager::new_server(sync_manager, AppId::from_name(FIXTURE_APP_NAME), "dev");
    let mut core = new_test_core(manager, MemoryStorage::new(), scheduler);

    let client_id = ClientId::new();
    core.add_client(client_id, None);

    let query = Query {
        branches: vec![query_branch],
        ..Query::new("users")
    };

    core.push_sync_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id,
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: Vec::new(),
        },
    });

    core.immediate_tick();
    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .has_pending_query_subscriptions(),
        "fixture precondition: QuerySubscription should be deferred while the \
         server has no compile-time schema and no matching catalogue entry"
    );

    (core, client_id)
}

/// Bug A — primary symptom.
///
/// A single `batched_tick` must drain `parked_sync_messages`, even when a
/// query subscription is pending. Today the early-return at ticks.rs:724
/// jumps past `handle_sync_messages`, so the parked message stays parked.
#[test]
fn batched_tick_strands_parked_messages_when_subscription_deferred() {
    let scheduler = CountingScheduler::default();
    let (mut core, _client_id) =
        server_with_deferred_subscription(scheduler.clone(), "main".to_string(), QueryId(1));

    // Park a message — payload doesn't matter for the drain invariant.
    core.park_sync_message(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::BatchFateNeeded {
            batch_ids: Vec::new(),
        },
    });
    assert_eq!(
        core.parked_sync_messages.len(),
        1,
        "setup precondition: one message is parked"
    );

    core.batched_tick();

    assert_eq!(
        core.parked_sync_messages.len(),
        0,
        "batched_tick must drain parked_sync_messages even when query \
         subscriptions are deferred — handle_sync_messages is the only path \
         that empties the queue (ticks.rs:881), and the parked message may \
         itself be the catalogue/schema that would unblock the deferred sub."
    );
}

/// Bug A — reschedule loop.
///
/// When no forward progress is possible (no parked messages, no outbox, no
/// pending storage flush) but a sub is deferred, `batched_tick` must not
/// reschedule itself *at all*. The pre-fix code unconditionally fired
/// `schedule_batched_tick()` from its early-return gates, so N ticks produced
/// ≥N reschedules — a hot loop. The fix gates the reschedule on `drained_any`
/// (ticks.rs:738) and on a pending/failed WAL flush, neither of which holds
/// here. A loose `reschedules < ticks` bound would still pass a 90%-hot loop
/// (9 reschedules over 10 ticks), so this pins the exact invariant: a
/// progressless tick re-arms the scheduler **zero** times.
#[test]
fn batched_tick_hot_spins_when_subscription_stays_deferred() {
    let scheduler = CountingScheduler::default();
    let (mut core, _client_id) =
        server_with_deferred_subscription(scheduler.clone(), "main".to_string(), QueryId(1));

    // No parked messages, no outbox, no storage flush — nothing that could
    // make progress this tick.
    assert_eq!(core.parked_sync_messages.len(), 0);

    let ticks = 10;
    let baseline = scheduler.schedule_count();
    for _ in 0..ticks {
        core.batched_tick();
    }
    let reschedules = scheduler.schedule_count() - baseline;

    assert_eq!(
        reschedules, 0,
        "batched_tick must not re-arm the scheduler on a progressless tick: \
         {ticks} ticks with no parked messages, no outbox, and no pending \
         storage flush produced {reschedules} reschedules. With nothing to \
         drain, `drained_any` is false so the reschedule gate at ticks.rs:738 \
         is skipped, and the `immediate_tick` at ticks.rs:713 only schedules \
         when it leaves outbound or a WAL-flush barrier behind (ticks.rs:685) \
         — a perpetually-deferred subscription creates neither. `reschedules < \
         ticks` would let a 90%-hot loop pass; the true invariant is that a \
         tick with no forward progress re-arms the scheduler zero times. The \
         next inbound message re-arms it via park_sync_message instead."
    );
}

/// Bug A — end-to-end recovery.
///
/// Park the `CatalogueEntryUpdated` that publishes the schema needed by a
/// deferred subscription, then run `batched_tick` until quiescent (bounded).
/// The drain → catalogue-apply → recompile path must complete: the parked
/// queue must be empty and `has_pending_query_subscriptions()` must be false.
///
/// Today this fails because `batched_tick` never reaches
/// `handle_sync_messages`, so the catalogue is never delivered.
#[test]
fn parked_catalogue_unblocks_deferred_subscription() {
    // 1. Build a "donor" runtime that publishes the schema we want to
    //    deliver. The donor's app name must match the server fixture's
    //    (see FIXTURE_APP_NAME), otherwise `process_catalogue_schema`
    //    (manager.rs:1261) silently drops the entry as "different app".
    let mut donor = create_runtime_with_schema(test_schema(), FIXTURE_APP_NAME);
    let catalogue_object_id = donor.publish_schema(test_schema());
    let catalogue_entry: CatalogueEntry = donor
        .storage()
        .load_catalogue_entry(catalogue_object_id)
        .expect("donor catalogue lookup must succeed")
        .expect("donor must persist the published schema as a catalogue entry");

    // 2. Build the composed branch name the client query will reference.
    //    After the catalogue arrives, `find_schema_by_short_hash` will match
    //    on the schema's short hash (subscriptions.rs:603) and
    //    `build_server_subscription_context` will succeed.
    let schema_hash = SchemaHash::compute(&test_schema());
    let composed_branch = ComposedBranchName::new("dev", schema_hash, "main").to_branch_name();

    // 3. Set up server with a deferred sub on that composed branch.
    let scheduler = CountingScheduler::default();
    let (mut core, _client_id) = server_with_deferred_subscription(
        scheduler.clone(),
        composed_branch.as_str().to_string(),
        QueryId(7),
    );

    // 4. Park the catalogue from an arbitrary upstream server.
    let upstream = ServerId::new();
    core.park_sync_message(InboxEntry {
        source: Source::Server(upstream),
        payload: SyncPayload::CatalogueEntryUpdated {
            entry: catalogue_entry,
        },
    });

    // 5. Pump batched_tick. Even after the fix, this needs more than one tick:
    //    the first tick drains parked into the inbox; the next tick's
    //    immediate_tick processes the catalogue and the second `process()`
    //    pass in immediate_tick (ticks.rs:495) retries the deferred sub. Cap
    //    at a small bound so we fail fast if no progress is made.
    let max_ticks = 8;
    let mut compiled_after = None;
    for tick_index in 0..max_ticks {
        core.batched_tick();
        let still_pending = core
            .schema_manager()
            .query_manager()
            .sync_manager()
            .has_pending_query_subscriptions();
        if !still_pending {
            compiled_after = Some(tick_index + 1);
            break;
        }
    }

    assert_eq!(
        core.parked_sync_messages.len(),
        0,
        "parked queue must be empty after the drain — the catalogue should \
         have been consumed by handle_sync_messages on the first tick."
    );
    assert!(
        compiled_after.is_some(),
        "deferred subscription was never compiled after {max_ticks} \
         batched_ticks; the parked CatalogueEntryUpdated never reached the \
         inbox so the server never learned the schema it needed."
    );
}

/// Control / regression guard.
///
/// In the absence of any deferred query subscription, `batched_tick` already
/// drains `parked_sync_messages`. The fix must keep this path working.
#[test]
fn batched_tick_drains_parked_messages_in_normal_path() {
    let mut core = create_test_runtime();

    // The default test runtime has no pending subs.
    assert!(
        !core
            .schema_manager()
            .query_manager()
            .sync_manager()
            .has_pending_query_subscriptions(),
        "normal-path precondition: no deferred subscriptions"
    );

    core.park_sync_message(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::BatchFateNeeded {
            batch_ids: Vec::new(),
        },
    });
    assert_eq!(core.parked_sync_messages.len(), 1);

    core.batched_tick();

    assert_eq!(
        core.parked_sync_messages.len(),
        0,
        "batched_tick must continue to drain parked_sync_messages on the \
         no-deferred-sub path — this is the existing behavior the fix must \
         preserve."
    );
}
