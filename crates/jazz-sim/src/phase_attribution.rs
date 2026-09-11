//! Opt-in phase timing for the single-threaded native benchmark driver.
//!
//! Async `instrument` spans enter only during polling. Nested time is subtracted
//! from parents, so exclusive phase times partition each outer node tick. Tick
//! remainder includes uninstrumented work, executor waiting, and timer overhead.
//! Snapshots are thread-local: this is not a cross-thread production profiler.

use std::{cell::RefCell, time::Instant};
use tracing::{
    Event, Metadata, Subscriber,
    span::{Attributes, Id, Record},
};

const PHASES: [&str; 20] = [
    "core",
    "relay",
    "client",
    "receive_updates",
    "ingest",
    "parent_completion",
    "storage_apply",
    "storage_persist",
    "ivm_update",
    "ivm_hydrate",
    "collect_results",
    "index_projection",
    "publish_supporting_rows",
    "query_setup",
    "decode_query_outputs",
    "deliver_query_outputs",
    "benchmark_transport",
    "decode_version_witness",
    "rebind_terminal_output",
    "canonical_supporting_version",
];
const ROLES: [&str; 4] = ["outside_node_ticks", "core", "relay", "client"];
#[derive(Clone, Copy, Default)]
struct Timing {
    inclusive_ns: u64,
    exclusive_ns: u64,
    entries: u64,
}
struct Frame {
    phase: usize,
    role: usize,
    start: u64,
    children: u64,
}
struct State {
    epoch: Instant,
    stack: Vec<Frame>,
    totals: [[Timing; PHASES.len()]; ROLES.len()],
}
impl Default for State {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            stack: Vec::new(),
            totals: [[Timing::default(); PHASES.len()]; ROLES.len()],
        }
    }
}
impl State {
    fn enter(&mut self, phase: usize, now: u64) {
        let role = if phase < 3 {
            phase + 1
        } else {
            self.stack.last().map_or(0, |f| f.role)
        };
        self.stack.push(Frame {
            phase,
            role,
            start: now,
            children: 0,
        });
    }
    fn exit(&mut self, phase: usize, now: u64) {
        let frame = self.stack.pop().expect("balanced phase spans");
        assert_eq!(frame.phase, phase, "phase span exit must match enter");
        let elapsed = now.saturating_sub(frame.start);
        assert!(
            frame.children <= elapsed,
            "nested phases exceed their parent"
        );
        let timing = &mut self.totals[frame.role][phase];
        timing.inclusive_ns += elapsed;
        timing.exclusive_ns += elapsed.saturating_sub(frame.children);
        timing.entries += 1;
        if let Some(parent) = self.stack.last_mut() {
            parent.children += elapsed;
        }
    }
}
thread_local! { static STATE: RefCell<State> = RefCell::new(State::default()); }
fn phase(metadata: &Metadata<'_>) -> Option<usize> {
    let name = metadata.name().strip_prefix("cold.phase.")?;
    PHASES.iter().position(|candidate| *candidate == name)
}
/// Minimal subscriber: unrelated spans and events stay disabled.
pub struct Collector;
impl Subscriber for Collector {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        phase(metadata).is_some()
    }
    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        Id::from_u64(phase(attrs.metadata()).expect("known phase") as u64 + 1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, id: &Id) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let now = s.epoch.elapsed().as_nanos() as u64;
            s.enter(id.into_u64() as usize - 1, now);
        });
    }
    fn exit(&self, id: &Id) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let now = s.epoch.elapsed().as_nanos() as u64;
            s.exit(id.into_u64() as usize - 1, now);
        });
    }
}
/// Reset outside any measured span, after seeding and before load setup.
pub fn reset() {
    STATE.with(|s| {
        assert!(s.borrow().stack.is_empty());
        *s.borrow_mut() = State::default();
    });
}
/// Capture before diagnostic queries. Inclusive times overlap; exclusive times
/// partition outer ticks. `entries` counts span entries (polls and scoped cleanup), not logical operations.
pub fn snapshot() -> serde_json::Value {
    STATE.with(|s| {
        let s = s.borrow();
        assert!(s.stack.is_empty());
        let mut roles = serde_json::Map::new();
        for (role, name) in ROLES.iter().enumerate() {
            if role > 0 {
                assert_eq!(s.totals[role].iter().map(|t| t.exclusive_ns).sum::<u64>(), s.totals[role][role - 1].inclusive_ns, "exclusive phases must partition node ticks");
            }
            let mut phases = serde_json::Map::new();
            for (phase, name) in PHASES.iter().enumerate() {
                let t = s.totals[role][phase];
                if t.entries != 0 { phases.insert((*name).into(), serde_json::json!({"inclusive_ns": t.inclusive_ns, "exclusive_ns": t.exclusive_ns, "entries": t.entries})); }
            }
            roles.insert((*name).into(), phases.into());
        }
        serde_json::json!({"roles":roles, "scope":"setup_and_settle_only", "units":"nanoseconds", "exclusive_times_are_additive":true, "inclusive_times_overlap":true})
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Internal instrumentation invariants need deterministic clock values rather
    // than flaky sleeps or application query assertions.
    #[test]
    fn nested_phases_partition_tick_and_do_not_leak_between_roles() {
        let mut state = State::default();
        state.enter(0, 0);
        state.enter(4, 10);
        state.enter(6, 20);
        state.exit(6, 50);
        state.exit(4, 70);
        state.exit(0, 100);
        state.enter(1, 110);
        state.enter(4, 120);
        state.exit(4, 140);
        state.exit(1, 150);
        assert_eq!(state.totals[1][0].inclusive_ns, 100);
        assert_eq!(state.totals[1][0].exclusive_ns, 40);
        assert_eq!(state.totals[1][4].exclusive_ns, 30);
        assert_eq!(state.totals[1][6].exclusive_ns, 30);
        assert_eq!(state.totals[2][4].exclusive_ns, 20);
        assert_eq!(
            state.totals[1].iter().map(|t| t.exclusive_ns).sum::<u64>(),
            100
        );
    }
    #[test]
    fn pending_future_releases_phase_and_resume_uses_current_role() {
        use std::{
            future::Future,
            task::{Context, Poll},
        };
        use tracing::Instrument;
        let _subscriber = tracing::subscriber::set_default(Collector);
        reset();
        let mut first = true;
        let future = std::future::poll_fn(move |_| {
            if first {
                first = false;
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .instrument(tracing::trace_span!("cold.phase.ingest"));
        let mut future = std::pin::pin!(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(
            tracing::trace_span!("cold.phase.core")
                .in_scope(|| future.as_mut().poll(&mut cx))
                .is_pending()
        );
        STATE.with(|s| assert!(s.borrow().stack.is_empty()));
        assert!(
            tracing::trace_span!("cold.phase.relay")
                .in_scope(|| future.as_mut().poll(&mut cx))
                .is_ready()
        );
        STATE.with(|s| {
            let s = s.borrow();
            assert!(s.stack.is_empty());
            assert_eq!(s.totals[1][4].entries, 1);
            assert_eq!(s.totals[2][4].entries, 1);
        });
    }
}
