//! Shared simulation and benchmark harness for Jazz scenarios.

pub mod distributions;
pub mod fixture;
pub mod mem;
pub mod profiling;

use hdrhistogram::Histogram;
use jazz::protocol::SyncMessage;
use jazz::protocol::{RegisterShapeOptions, ShapeAst};
use jazz::query::{QUERY_NAMESPACE, Query, ShapeId};
use jazz::schema::JazzSchema;
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireEnvelope, WireFrame, decode_frame,
    decode_sync_message, encode_frame, encode_sync_message,
};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// Named node in a simulation topology.
pub type NodeName = String;

/// Network profile for one directed peer-to-peer link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerProfile {
    /// Profile name reported in benchmark output.
    pub name: String,
    /// Base one-way latency in virtual/real milliseconds.
    pub one_way_latency_ms: u64,
    /// Maximum deterministic jitter added per message, in milliseconds.
    pub jitter_ms: u64,
    /// Fixed per-message delivery overhead in milliseconds.
    pub per_message_overhead_ms: u64,
}

/// Benchmark scale selected by `JAZZ_BENCH_PROFILE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchProfile {
    /// Fast, low-cardinality defaults for interactive iteration.
    Fast,
    /// Moderate defaults for profiler captures that need real work without full scale.
    Profile,
    /// Default product-scale-ish settings for retained snapshots and claims.
    Full,
}

impl BenchProfile {
    /// Read the current benchmark profile from the environment.
    pub fn from_env() -> Self {
        match std::env::var("JAZZ_BENCH_PROFILE").as_deref() {
            Ok("fast" | "FAST" | "interactive" | "INTERACTIVE") => Self::Fast,
            Ok("profile" | "PROFILE" | "profiling" | "PROFILING") => Self::Profile,
            _ => Self::Full,
        }
    }

    /// True when the benchmark should choose interactive defaults.
    pub fn is_fast(self) -> bool {
        self == Self::Fast
    }

    /// True when the benchmark should choose profiler-friendly defaults.
    pub fn is_profile(self) -> bool {
        self == Self::Profile
    }

    /// Select a default value for the active benchmark scale.
    pub fn select<T>(self, fast: T, profile: T, full: T) -> T {
        match self {
            Self::Fast => fast,
            Self::Profile => profile,
            Self::Full => full,
        }
    }
}

const GLOBAL_TRANSPORT_CODEC_ENV: &str = "JAZZ_TRANSPORT_CODEC";

/// True when benchmark configs should use fast interactive defaults.
pub fn fast_bench_profile() -> bool {
    BenchProfile::from_env().is_fast()
}

/// Active benchmark scale from `JAZZ_BENCH_PROFILE`.
pub fn bench_profile() -> BenchProfile {
    BenchProfile::from_env()
}

impl PeerProfile {
    /// Construct a peer profile.
    pub fn new(
        name: impl Into<String>,
        one_way_latency_ms: u64,
        jitter_ms: u64,
        per_message_overhead_ms: u64,
    ) -> Self {
        Self {
            name: name.into(),
            one_way_latency_ms,
            jitter_ms,
            per_message_overhead_ms,
        }
    }

    fn latency_ms(&self, rng: &mut Lcg) -> u64 {
        self.one_way_latency_ms
            + self.per_message_overhead_ms
            + if self.jitter_ms == 0 {
                0
            } else {
                rng.next_u64() % (self.jitter_ms + 1)
            }
    }
}

/// Role assigned to a declared node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeRole {
    /// Workload writer.
    Writer,
    /// Workload reader.
    Reader,
    /// Edge node that terminates a client identity and relays to core.
    Edge,
    /// Store-and-forward relay.
    Relay,
    /// Fate authority.
    Core,
}

/// Node declaration consumed by both drivers.
#[derive(Clone, Debug)]
pub struct NodeDecl {
    /// Stable node name.
    pub name: NodeName,
    /// Jazz schema used by the node.
    pub schema: JazzSchema,
    /// Scenario role.
    pub role: NodeRole,
}

/// Directed link declaration consumed by both drivers.
#[derive(Clone, Debug)]
pub struct LinkDecl {
    /// Source node name.
    pub from: NodeName,
    /// Destination node name.
    pub to: NodeName,
    /// Peer profile.
    pub profile: PeerProfile,
}

/// Topology declaration shared by deterministic and threaded drivers.
#[derive(Clone, Debug, Default)]
pub struct Topology {
    /// Declared nodes.
    pub nodes: BTreeMap<NodeName, NodeDecl>,
    /// Directed links.
    pub links: Vec<LinkDecl>,
}

impl Topology {
    /// Add a node declaration.
    pub fn node(mut self, name: impl Into<String>, schema: JazzSchema, role: NodeRole) -> Self {
        let name = name.into();
        self.nodes
            .insert(name.clone(), NodeDecl { name, schema, role });
        self
    }

    /// Add a directed link declaration.
    pub fn link(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        profile: PeerProfile,
    ) -> Self {
        self.links.push(LinkDecl {
            from: from.into(),
            to: to.into(),
            profile,
        });
        self
    }

    /// Add a bidirectional client↔edge↔core line.
    pub fn client_edge_core_line(
        self,
        client: impl Into<String>,
        edge: impl Into<String>,
        core: impl Into<String>,
        client_edge_profile: PeerProfile,
        edge_core_profile: PeerProfile,
    ) -> Self {
        let client = client.into();
        let edge = edge.into();
        let core = core.into();
        self.link(client.clone(), edge.clone(), client_edge_profile.clone())
            .link(edge.clone(), client, client_edge_profile)
            .link(edge.clone(), core.clone(), edge_core_profile.clone())
            .link(core, edge, edge_core_profile)
    }

    fn link_profile(&self, from: &str, to: &str) -> Option<&PeerProfile> {
        self.links
            .iter()
            .find(|link| link.from == from && link.to == to)
            .map(|link| &link.profile)
    }
}

/// Virtual millisecond clock for deterministic runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimClock {
    now_ms: u64,
}

impl SimClock {
    /// Current virtual time in milliseconds.
    pub fn now_ms(self) -> u64 {
        self.now_ms
    }

    fn advance_to(&mut self, now_ms: u64) {
        self.now_ms = self.now_ms.max(now_ms);
    }
}

/// Driver API used by scenario definitions.
pub trait DriverContext {
    /// Driver name.
    fn driver_name(&self) -> &'static str;
    /// Current driver time in milliseconds.
    fn now_ms(&self) -> u64;
    /// Send a sync message over a declared directed link.
    fn send(&mut self, from: &str, to: &str, message: SyncMessage);
    /// Receive the next message delivered to `node`, advancing/blocking as needed.
    fn recv(&mut self, node: &str) -> DeliveredMessage;
    /// Record a latency metric.
    fn record_latency(&mut self, metric: &str, micros: u64);
    /// Increment a deterministic counter.
    fn record_counter(&mut self, metric: &str, value: u64);
}

/// Optional codec used by simulator drivers before message delivery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SimulatorTransportCodec {
    /// Deliver in-memory semantic messages, matching the historical simulator behavior.
    #[default]
    Native,
    /// Round-trip messages through the canonical Jazz sync-message byte codec.
    WireBytes,
    /// Round-trip messages through canonical sync payload bytes wrapped in wire frames.
    WireFrames,
}

/// Parse a simulator transport codec name.
pub fn parse_transport_codec(name: &str, value: &str) -> SimulatorTransportCodec {
    match value.trim().to_ascii_lowercase().as_str() {
        "native" => SimulatorTransportCodec::Native,
        "wire_bytes" | "wire-bytes" | "wirebytes" | "bytes" => SimulatorTransportCodec::WireBytes,
        "wire_frames" | "wire-frames" | "wireframes" | "frames" => {
            SimulatorTransportCodec::WireFrames
        }
        other => panic!("{name} must be one of native, wire_bytes, or wire_frames; got {other:?}"),
    }
}

/// Read a simulator transport codec env var, falling back to the supplied default.
pub fn env_transport_codec(
    name: &str,
    default: SimulatorTransportCodec,
) -> SimulatorTransportCodec {
    std::env::var(name)
        .ok()
        .map(|value| parse_transport_codec(name, &value))
        .unwrap_or(default)
}

/// Read a scenario-specific transport codec with `JAZZ_TRANSPORT_CODEC` as a global default.
pub fn scenario_transport_codec_env(name: &str) -> SimulatorTransportCodec {
    match std::env::var(name) {
        Ok(value) => parse_transport_codec(name, &value),
        Err(_) => env_transport_codec(
            GLOBAL_TRANSPORT_CODEC_ENV,
            SimulatorTransportCodec::WireFrames,
        ),
    }
}

/// Behavior for messages sent over a paused deterministic link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseMode {
    /// Retain messages and schedule them with normal link latency when resumed.
    Queue,
    /// Drop messages until the link is resumed.
    Drop,
}

/// Message delivered by a driver.
#[derive(Clone, Debug, PartialEq)]
pub struct DeliveredMessage {
    /// Sending node.
    pub from: NodeName,
    /// Receiving node.
    pub to: NodeName,
    /// Delivered message.
    pub message: SyncMessage,
}

/// Driver-agnostic workload definition.
pub trait WorkloadDefinition {
    /// Scenario name.
    fn name(&self) -> &'static str;
    /// Scenario seed.
    fn seed(&self) -> u64;
    /// Topology consumed by both drivers.
    fn topology(&self) -> Topology;
    /// Fixture setup before steps begin.
    fn setup(&self, _ctx: &mut dyn DriverContext) {}
    /// Run the seeded step stream.
    fn run_steps(&self, ctx: &mut dyn DriverContext);
    /// End-of-run assertions. Deterministic drivers call this after `run_steps`.
    fn assert_deterministic(&self, _ctx: &dyn DriverContext) {}
}

/// Metric collection for one run.
#[derive(Debug, Default)]
pub struct Metrics {
    histograms: BTreeMap<String, Histogram<u64>>,
    counters: BTreeMap<String, u64>,
}

impl Metrics {
    /// Record a latency sample.
    pub fn record_latency(&mut self, name: &str, micros: u64) {
        self.histograms
            .entry(name.to_owned())
            .or_insert_with(|| Histogram::new(3).expect("valid histogram precision"))
            .record(micros)
            .expect("latency sample within histogram range");
    }

    /// Increment a deterministic counter.
    pub fn incr(&mut self, name: &str, value: u64) {
        *self.counters.entry(name.to_owned()).or_default() += value;
    }

    fn json_fields(&self) -> Map<String, Value> {
        let mut fields = Map::new();
        for (name, hist) in &self.histograms {
            fields.insert(
                format!("{name}_p50_us"),
                json!(hist.value_at_quantile(0.50)),
            );
            fields.insert(
                format!("{name}_p95_us"),
                json!(hist.value_at_quantile(0.95)),
            );
            fields.insert(
                format!("{name}_p99_us"),
                json!(hist.value_at_quantile(0.99)),
            );
            fields.insert(format!("{name}_max_us"), json!(hist.max()));
        }
        for (name, value) in &self.counters {
            fields.insert(name.clone(), json!(value));
        }
        self.insert_transport_codec_derived_fields(&mut fields);
        fields
    }

    fn insert_transport_codec_derived_fields(&self, fields: &mut Map<String, Value>) {
        let Some(encode_count) = self.counters.get("transport_codec_encode_count").copied() else {
            return;
        };
        if encode_count == 0 {
            return;
        }

        fields.insert(
            "transport_codec_messages_encoded".to_owned(),
            json!(encode_count),
        );

        if let Some(decode_count) = self.counters.get("transport_codec_decode_count").copied() {
            fields.insert(
                "transport_codec_messages_decoded".to_owned(),
                json!(decode_count),
            );
            if decode_count > 0
                && let Some(decode_total_us) = self
                    .counters
                    .get("transport_codec_decode_total_us")
                    .copied()
            {
                fields.insert(
                    "transport_codec_decode_avg_us_per_message".to_owned(),
                    json!(decode_total_us as f64 / decode_count as f64),
                );
            }
        }

        if let Some(encoded_bytes) = self.counters.get("transport_codec_encoded_bytes").copied() {
            fields.insert(
                "transport_codec_payload_bytes_per_message".to_owned(),
                json!(encoded_bytes as f64 / encode_count as f64),
            );

            if let Some(encoded_frame_bytes) = self
                .counters
                .get("transport_codec_encoded_frame_bytes")
                .copied()
            {
                fields.insert(
                    "transport_codec_frame_bytes_per_message".to_owned(),
                    json!(encoded_frame_bytes as f64 / encode_count as f64),
                );
                if encoded_frame_bytes >= encoded_bytes {
                    fields.insert(
                        "transport_codec_frame_overhead_bytes_per_message".to_owned(),
                        json!((encoded_frame_bytes - encoded_bytes) as f64 / encode_count as f64),
                    );
                }
            }
        }

        if let Some(encode_total_us) = self
            .counters
            .get("transport_codec_encode_total_us")
            .copied()
        {
            fields.insert(
                "transport_codec_encode_avg_us_per_message".to_owned(),
                json!(encode_total_us as f64 / encode_count as f64),
            );
        }
    }

    /// Return JSON fields for all collected counters and latency histograms.
    pub fn to_json_fields(&self) -> Map<String, Value> {
        self.json_fields()
    }
}

/// Completed benchmark run.
#[derive(Debug)]
pub struct RunReport {
    /// Scenario name.
    pub scenario: String,
    /// Driver name.
    pub driver: String,
    /// Seed.
    pub seed: u64,
    /// Profile name.
    pub profile: String,
    /// Metrics.
    pub metrics: Metrics,
}

impl RunReport {
    /// Return one JSONL object for stdout/retention.
    pub fn to_json_line(&self) -> String {
        let mut fields = Map::new();
        fields.insert("scenario".to_owned(), json!(self.scenario));
        fields.insert("driver".to_owned(), json!(self.driver));
        fields.insert("seed".to_owned(), json!(self.seed));
        fields.insert("profile".to_owned(), json!(self.profile));
        insert_process_metadata(&mut fields);
        fields.extend(self.metrics.json_fields());
        serde_json::to_string(&Value::Object(fields)).expect("json serialization succeeds")
    }

    /// Print JSONL and optionally retain under `benchmarks/results/jazz`.
    pub fn emit(&self) {
        let line = self.to_json_line();
        emit_json_line(&self.scenario, &line);
    }
}

/// Build the shared benchmark metadata fields required by `jazz/BENCHMARKS.md`.
pub fn metadata_fields(
    scenario: impl Into<String>,
    driver: impl Into<String>,
    seed: u64,
    profile: impl Into<String>,
) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("scenario".to_owned(), json!(scenario.into()));
    fields.insert("driver".to_owned(), json!(driver.into()));
    fields.insert("seed".to_owned(), json!(seed));
    fields.insert("profile".to_owned(), json!(profile.into()));
    insert_process_metadata(&mut fields);
    fields
}

/// Print a JSONL line and optionally retain it under `benchmarks/results/jazz`.
pub fn emit_json_line(scenario: &str, line: &str) {
    println!("{line}");
    if retain_enabled() {
        // Anchor at the workspace root: cargo bench sets the CWD to the
        // package directory, not the workspace.
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("benchmarks/results/jazz");
        std::fs::create_dir_all(&path).expect("create jazz results directory");
        path.push(format!("{scenario}.jsonl"));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open retained jazz result");
        writeln!(file, "{line}").expect("append retained jazz result");
    }
}

/// Deterministic simulation-first driver.
pub struct DeterministicDriver {
    topology: Topology,
    clock: SimClock,
    rng: Lcg,
    next_event_id: u64,
    queue: BinaryHeap<ScheduledEvent>,
    inboxes: BTreeMap<NodeName, VecDeque<DeliveredMessage>>,
    paused_links: BTreeMap<(NodeName, NodeName), PausedLink>,
    transport_codec: SimulatorTransportCodec,
    metrics: Metrics,
}

impl DeterministicDriver {
    /// Construct a deterministic driver.
    pub fn new(topology: Topology, seed: u64) -> Self {
        Self {
            topology,
            clock: SimClock::default(),
            rng: Lcg::new(seed),
            next_event_id: 0,
            queue: BinaryHeap::new(),
            inboxes: BTreeMap::new(),
            paused_links: BTreeMap::new(),
            transport_codec: SimulatorTransportCodec::default(),
            metrics: Metrics::default(),
        }
    }

    /// Return a driver configured with the supplied simulator transport codec.
    pub fn with_transport_codec(mut self, codec: SimulatorTransportCodec) -> Self {
        self.transport_codec = codec;
        self
    }

    /// Run a scenario and return its report.
    pub fn run<W: WorkloadDefinition>(mut self, workload: &W, profile: &str) -> RunReport {
        workload.setup(&mut self);
        workload.run_steps(&mut self);
        workload.assert_deterministic(&self);
        RunReport {
            scenario: workload.name().to_owned(),
            driver: "deterministic".to_owned(),
            seed: workload.seed(),
            profile: profile.to_owned(),
            metrics: self.metrics,
        }
    }

    /// Return the delivery schedule for all currently queued messages.
    pub fn queued_schedule(&self) -> Vec<(u64, u64, String, String)> {
        let mut events = self
            .queue
            .iter()
            .map(|event| {
                (
                    event.deliver_at_ms,
                    event.event_id,
                    event.message.from.clone(),
                    event.message.to.clone(),
                )
            })
            .collect::<Vec<_>>();
        events.sort();
        events
    }

    /// Pause a directed link, either queueing or dropping subsequent sends.
    pub fn pause_link(&mut self, from: &str, to: &str, mode: PauseMode) {
        self.topology
            .link_profile(from, to)
            .unwrap_or_else(|| panic!("missing link {from}->{to}"));
        let key = (from.to_owned(), to.to_owned());
        self.paused_links
            .entry(key)
            .and_modify(|paused| paused.mode = mode)
            .or_insert_with(|| PausedLink {
                mode,
                queued: VecDeque::new(),
            });
    }

    /// Resume a directed link and schedule any queued messages with normal latency.
    pub fn resume_link(&mut self, from: &str, to: &str) {
        let key = (from.to_owned(), to.to_owned());
        if let Some(mut paused) = self.paused_links.remove(&key) {
            while let Some(message) = paused.queued.pop_front() {
                self.schedule_delivered(from, to, message);
            }
        }
    }

    /// True when the directed link is currently paused.
    pub fn is_link_paused(&self, from: &str, to: &str) -> bool {
        self.paused_links
            .contains_key(&(from.to_owned(), to.to_owned()))
    }

    /// Return JSON fields for all metrics collected so far.
    pub fn metrics_json_fields(&self) -> Map<String, Value> {
        self.metrics.to_json_fields()
    }

    fn schedule(&mut self, from: &str, to: &str, message: SyncMessage) {
        let profile = self
            .topology
            .link_profile(from, to)
            .unwrap_or_else(|| panic!("missing link {from}->{to}"))
            .clone();
        let message = loopback_transport_message(self.transport_codec, message, &mut self.metrics);
        let delivered = DeliveredMessage {
            from: from.to_owned(),
            to: to.to_owned(),
            message,
        };
        if let Some(paused) = self.paused_links.get_mut(&(from.to_owned(), to.to_owned())) {
            match paused.mode {
                PauseMode::Queue => paused.queued.push_back(delivered),
                PauseMode::Drop => self.metrics.incr("messages_dropped", 1),
            }
            self.metrics.incr("messages_sent", 1);
            return;
        }
        self.schedule_delivered_with_profile(profile, delivered);
        self.metrics.incr("messages_sent", 1);
    }

    fn schedule_delivered(&mut self, from: &str, to: &str, message: DeliveredMessage) {
        let profile = self
            .topology
            .link_profile(from, to)
            .unwrap_or_else(|| panic!("missing link {from}->{to}"))
            .clone();
        self.schedule_delivered_with_profile(profile, message);
    }

    fn schedule_delivered_with_profile(&mut self, profile: PeerProfile, message: DeliveredMessage) {
        let deliver_at_ms = self.clock.now_ms() + profile.latency_ms(&mut self.rng);
        let event = ScheduledEvent {
            deliver_at_ms,
            event_id: self.next_event_id,
            message,
        };
        self.next_event_id += 1;
        self.queue.push(event);
    }

    fn pop_next(&mut self) -> DeliveredMessage {
        let event = self.queue.pop().expect("message delivery available");
        self.clock.advance_to(event.deliver_at_ms);
        event.message
    }
}

impl DriverContext for DeterministicDriver {
    fn driver_name(&self) -> &'static str {
        "deterministic"
    }

    fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    fn send(&mut self, from: &str, to: &str, message: SyncMessage) {
        self.schedule(from, to, message);
    }

    fn recv(&mut self, node: &str) -> DeliveredMessage {
        loop {
            if let Some(message) = self.inboxes.entry(node.to_owned()).or_default().pop_front() {
                self.metrics.incr("messages_delivered", 1);
                return message;
            }
            let message = self.pop_next();
            if message.to == node {
                self.metrics.incr("messages_delivered", 1);
                return message;
            }
            self.inboxes
                .entry(message.to.clone())
                .or_default()
                .push_back(message);
        }
    }

    fn record_latency(&mut self, metric: &str, micros: u64) {
        self.metrics.record_latency(metric, micros);
    }

    fn record_counter(&mut self, metric: &str, value: u64) {
        self.metrics.incr(metric, value);
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ScheduledEvent {
    deliver_at_ms: u64,
    event_id: u64,
    message: DeliveredMessage,
}

impl Eq for ScheduledEvent {}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deliver_at_ms
            .cmp(&self.deliver_at_ms)
            .then_with(|| other.event_id.cmp(&self.event_id))
    }
}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq)]
struct PausedLink {
    mode: PauseMode,
    queued: VecDeque<DeliveredMessage>,
}

/// Threaded channel driver.
pub struct ThreadedDriver {
    topology: Topology,
    rng: Lcg,
    start: Instant,
    receivers: BTreeMap<NodeName, mpsc::Receiver<DeliveredMessage>>,
    link_workers: BTreeMap<(NodeName, NodeName), mpsc::Sender<ScheduledDelivery>>,
    transport_codec: SimulatorTransportCodec,
    metrics: Metrics,
}

struct ScheduledDelivery {
    deadline: Instant,
    delivered: DeliveredMessage,
}

impl ThreadedDriver {
    /// Construct a threaded driver.
    pub fn new(topology: Topology, seed: u64) -> Self {
        let mut senders = BTreeMap::new();
        let mut receivers = BTreeMap::new();
        for name in topology.nodes.keys() {
            let (sender, receiver) = mpsc::channel();
            senders.insert(name.clone(), sender);
            receivers.insert(name.clone(), receiver);
        }
        let mut link_workers = BTreeMap::new();
        for link in &topology.links {
            let (scheduled_tx, scheduled_rx) = mpsc::channel::<ScheduledDelivery>();
            let delivered_tx = senders
                .get(&link.to)
                .unwrap_or_else(|| panic!("missing node {}", link.to))
                .clone();
            thread::spawn(move || {
                while let Ok(scheduled) = scheduled_rx.recv() {
                    let now = Instant::now();
                    if scheduled.deadline > now {
                        thread::sleep(scheduled.deadline - now);
                    }
                    let _ = delivered_tx.send(scheduled.delivered);
                }
            });
            link_workers.insert((link.from.clone(), link.to.clone()), scheduled_tx);
        }
        Self {
            topology,
            rng: Lcg::new(seed),
            start: Instant::now(),
            receivers,
            link_workers,
            transport_codec: SimulatorTransportCodec::default(),
            metrics: Metrics::default(),
        }
    }

    /// Return a driver configured with the supplied simulator transport codec.
    pub fn with_transport_codec(mut self, codec: SimulatorTransportCodec) -> Self {
        self.transport_codec = codec;
        self
    }

    /// Run a scenario and return its report.
    pub fn run<W: WorkloadDefinition>(mut self, workload: &W, profile: &str) -> RunReport {
        workload.setup(&mut self);
        workload.run_steps(&mut self);
        RunReport {
            scenario: workload.name().to_owned(),
            driver: "threaded".to_owned(),
            seed: workload.seed(),
            profile: profile.to_owned(),
            metrics: self.metrics,
        }
    }

    /// Return JSON fields for all metrics collected so far.
    pub fn metrics_json_fields(&self) -> Map<String, Value> {
        self.metrics.to_json_fields()
    }
}

impl DriverContext for ThreadedDriver {
    fn driver_name(&self) -> &'static str {
        "threaded"
    }

    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    fn send(&mut self, from: &str, to: &str, message: SyncMessage) {
        let profile = self
            .topology
            .link_profile(from, to)
            .unwrap_or_else(|| panic!("missing link {from}->{to}"))
            .clone();
        let latency = profile.latency_ms(&mut self.rng);
        let deadline = Instant::now() + Duration::from_millis(latency);
        let message = loopback_transport_message(self.transport_codec, message, &mut self.metrics);
        let delivered = DeliveredMessage {
            from: from.to_owned(),
            to: to.to_owned(),
            message,
        };
        self.link_workers
            .get(&(from.to_owned(), to.to_owned()))
            .unwrap_or_else(|| panic!("missing link worker {from}->{to}"))
            .send(ScheduledDelivery {
                deadline,
                delivered,
            })
            .expect("link worker open");
        self.metrics.incr("messages_sent", 1);
    }

    fn recv(&mut self, node: &str) -> DeliveredMessage {
        let message = self
            .receivers
            .get(node)
            .unwrap_or_else(|| panic!("missing receiver for node {node}"))
            .recv()
            .expect("driver channel open");
        self.metrics.incr("messages_delivered", 1);
        message
    }

    fn record_latency(&mut self, metric: &str, micros: u64) {
        self.metrics.record_latency(metric, micros);
    }

    fn record_counter(&mut self, metric: &str, value: u64) {
        self.metrics.incr(metric, value);
    }
}

/// Round-trip a sync message through the selected simulator transport codec.
///
/// This is the shared transport primitive used by simulator drivers and by
/// benchmark harnesses that own their own actor scheduling but still need to
/// exercise canonical wire encodings and collect comparable codec metrics.
pub fn loopback_transport_message(
    codec: SimulatorTransportCodec,
    message: SyncMessage,
    metrics: &mut Metrics,
) -> SyncMessage {
    match codec {
        SimulatorTransportCodec::Native => message,
        SimulatorTransportCodec::WireBytes | SimulatorTransportCodec::WireFrames => {
            let encode_start = Instant::now();
            let encoded_payload =
                encode_sync_message(&message).expect("simulator sync-message encode");
            let encoded_frame = match codec {
                SimulatorTransportCodec::WireFrames => {
                    let envelope = WireEnvelope::new(
                        WIRE_PROTOCOL_VERSION,
                        FEATURE_SYNC_MESSAGE_PAYLOAD,
                        encoded_payload.clone(),
                    );
                    Some(
                        encode_frame(&WireFrame::Message(envelope))
                            .expect("simulator frame encode"),
                    )
                }
                SimulatorTransportCodec::Native | SimulatorTransportCodec::WireBytes => None,
            };
            let encode_us = encode_start
                .elapsed()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX);
            metrics.incr("transport_codec_encode_count", 1);
            metrics.incr("transport_codec_encode_total_us", encode_us);
            metrics.incr(
                "transport_codec_encoded_bytes",
                encoded_payload.len() as u64,
            );
            if let Some(frame) = &encoded_frame {
                metrics.incr("transport_codec_encoded_frame_bytes", frame.len() as u64);
            }
            metrics.record_latency("transport_codec_encode", encode_us);

            let decode_start = Instant::now();
            let payload = match encoded_frame {
                Some(frame_bytes) => {
                    match decode_frame(&frame_bytes).expect("simulator frame decode") {
                        WireFrame::Message(envelope) => envelope.payload,
                        WireFrame::Hello(_) | WireFrame::Error(_) => {
                            panic!("simulator frame decode returned non-message frame")
                        }
                    }
                }
                None => encoded_payload,
            };
            let decoded = decode_sync_message(&payload).expect("simulator sync-message decode");
            let decode_us = decode_start
                .elapsed()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX);
            metrics.incr("transport_codec_decode_count", 1);
            metrics.incr("transport_codec_decode_total_us", decode_us);
            metrics.record_latency("transport_codec_decode", decode_us);
            decoded
        }
    }
}

/// Echo scenario used as the latency floor.
#[derive(Clone, Debug)]
pub struct EchoScenario {
    seed: u64,
    rounds: u64,
    profile: PeerProfile,
}

impl EchoScenario {
    /// Construct an echo scenario.
    pub fn new(seed: u64, rounds: u64, profile: PeerProfile) -> Self {
        Self {
            seed,
            rounds,
            profile,
        }
    }

    /// Expected deterministic round-trip time in microseconds when jitter is zero.
    pub fn expected_rtt_us(&self) -> Option<u64> {
        (self.profile.jitter_ms == 0).then_some(
            2 * (self.profile.one_way_latency_ms + self.profile.per_message_overhead_ms) * 1_000,
        )
    }
}

impl WorkloadDefinition for EchoScenario {
    fn name(&self) -> &'static str {
        "echo"
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn topology(&self) -> Topology {
        Topology::default()
            .node("a", empty_schema(), NodeRole::Writer)
            .node("b", empty_schema(), NodeRole::Reader)
            .link("a", "b", self.profile.clone())
            .link("b", "a", self.profile.clone())
    }

    fn run_steps(&self, ctx: &mut dyn DriverContext) {
        let schema_version = empty_schema().version_id();
        for round in 0..self.rounds {
            let start = ctx.now_ms();
            ctx.send(
                "a",
                "b",
                SyncMessage::RegisterShape {
                    shape_id: ShapeId(QUERY_NAMESPACE),
                    ast: ShapeAst::new(Query::from(format!("ping:{round}")), schema_version),
                    opts: RegisterShapeOptions::default(),
                },
            );
            let ping = ctx.recv("b");
            ctx.send("b", "a", ping.message);
            let _pong = ctx.recv("a");
            let elapsed_us = (ctx.now_ms() - start) * 1_000;
            ctx.record_latency("rtt", elapsed_us);
        }
    }

    fn assert_deterministic(&self, ctx: &dyn DriverContext) {
        let _ = ctx;
        // The deterministic echo bench validates histogram percentiles in
        // `run_echo_deterministic`; real scenarios run oracle checks here.
    }
}

/// Run echo in deterministic mode.
pub fn run_echo_deterministic(seed: u64, rounds: u64, profile: PeerProfile) -> RunReport {
    let scenario = EchoScenario::new(seed, rounds, profile.clone());
    let report = DeterministicDriver::new(scenario.topology(), seed).run(&scenario, &profile.name);
    if let Some(expected) = scenario.expected_rtt_us()
        && let Some(hist) = report.metrics.histograms.get("rtt")
    {
        assert_eq!(hist.value_at_quantile(0.50), expected);
        assert_eq!(hist.value_at_quantile(0.95), expected);
        assert_eq!(hist.value_at_quantile(0.99), expected);
    }
    report
}

/// Run echo in threaded mode.
pub fn run_echo_threaded(seed: u64, rounds: u64, profile: PeerProfile) -> RunReport {
    let scenario = EchoScenario::new(seed, rounds, profile.clone());
    ThreadedDriver::new(scenario.topology(), seed).run(&scenario, &profile.name)
}

fn empty_schema() -> JazzSchema {
    JazzSchema::new([])
}

#[derive(Clone, Debug)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
}

fn git_output<const N: usize>(args: [&str; N]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

#[derive(Clone, Debug)]
struct ProcessMetadata {
    git_sha: String,
    git_dirty: bool,
    hostname: String,
    knobs: BTreeMap<String, String>,
}

static PROCESS_METADATA: OnceLock<ProcessMetadata> = OnceLock::new();

fn process_metadata() -> &'static ProcessMetadata {
    PROCESS_METADATA.get_or_init(|| ProcessMetadata {
        git_sha: git_output(["rev-parse", "HEAD"]),
        git_dirty: git_dirty(),
        hostname: hostname(),
        knobs: knob_env(),
    })
}

fn git_dirty() -> bool {
    let status = git_output(["status", "--porcelain"]);
    if !retain_result_dirty_ignored() {
        return !status.is_empty();
    }
    status.lines().any(|line| {
        let path = line.get(3..).unwrap_or_default();
        !(path.starts_with("benchmarks/results/") || path.starts_with("\"benchmarks/results/"))
    })
}

fn retain_result_dirty_ignored() -> bool {
    matches!(
        std::env::var("JAZZ_BENCH_IGNORE_RESULT_DIRTY").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn insert_process_metadata(fields: &mut Map<String, Value>) {
    let metadata = process_metadata();
    fields.insert("git_sha".to_owned(), json!(metadata.git_sha));
    fields.insert("git_dirty".to_owned(), json!(metadata.git_dirty));
    fields.insert("hostname".to_owned(), json!(metadata.hostname));
    fields.insert("knobs".to_owned(), json!(metadata.knobs));
}

fn knob_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("JAZZ_") || key.starts_with("GROOVE_"))
        .collect()
}

fn retain_enabled() -> bool {
    matches!(
        std::env::var("JAZZ_RETAIN").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::abi::{
        ABI_FEATURE_NONE, ABI_VERSION, AbiResumeStatus, AbiTransportDiagnostics, DbIdentityPayload,
        Event, EventBudget, EventKind, FrameBudget, Handle, MemoryStorageConfig, OpenDbConfig,
        TransportDirection,
    };
    use jazz::abi_runtime::{AbiCallResult, AbiRuntime};
    use jazz::db::DbIdentity;
    use jazz::groove::schema::ColumnType;
    use jazz::ids::{AuthorId, NodeUuid};
    use jazz::schema::{ColumnSchema, TableSchema};
    use jazz::wire::WireErrorCode;

    fn register_shape_message(label: &str) -> SyncMessage {
        SyncMessage::RegisterShape {
            shape_id: ShapeId(QUERY_NAMESPACE),
            ast: ShapeAst::new(Query::from(label.to_owned()), empty_schema().version_id()),
            opts: RegisterShapeOptions::default(),
        }
    }

    fn two_node_topology() -> Topology {
        Topology::default()
            .node("a", empty_schema(), NodeRole::Writer)
            .node("b", empty_schema(), NodeRole::Reader)
            .link("a", "b", PeerProfile::new("test", 0, 0, 0))
    }

    fn two_node_topology_with_latency(latency_ms: u64) -> Topology {
        Topology::default()
            .node("a", empty_schema(), NodeRole::Writer)
            .node("b", empty_schema(), NodeRole::Reader)
            .link("a", "b", PeerProfile::new("test", latency_ms, 0, 0))
    }

    #[test]
    fn deterministic_queue_schedule_repeats_for_same_seed() {
        let profile = PeerProfile::new("test", 5, 3, 1);
        let topology = Topology::default()
            .node("a", empty_schema(), NodeRole::Writer)
            .node("b", empty_schema(), NodeRole::Reader)
            .link("a", "b", profile);

        fn schedule(topology: Topology) -> Vec<(u64, u64, String, String)> {
            let mut driver = DeterministicDriver::new(topology, 42);
            let schema_version = empty_schema().version_id();
            for idx in 0..8 {
                driver.send(
                    "a",
                    "b",
                    SyncMessage::RegisterShape {
                        shape_id: ShapeId(QUERY_NAMESPACE),
                        ast: ShapeAst::new(Query::from(format!("m{idx}")), schema_version),
                        opts: RegisterShapeOptions::default(),
                    },
                );
            }
            driver.queued_schedule()
        }

        assert_eq!(schedule(topology.clone()), schedule(topology));
    }

    #[test]
    fn link_default_delivery_is_unchanged() {
        let mut driver = DeterministicDriver::new(two_node_topology_with_latency(5), 42);
        let message = register_shape_message("default-link");

        driver.send("a", "b", message.clone());
        let delivered = driver.recv("b");

        assert_eq!(delivered.from, "a");
        assert_eq!(delivered.to, "b");
        assert_eq!(delivered.message, message);
        assert_eq!(driver.now_ms(), 5);
        assert_eq!(driver.metrics.counters["messages_sent"], 1);
        assert_eq!(driver.metrics.counters["messages_delivered"], 1);
    }

    #[test]
    fn link_pause_queue_retains_until_resume() {
        let mut driver = DeterministicDriver::new(two_node_topology_with_latency(7), 42);
        let message = register_shape_message("queued-link");

        driver.pause_link("a", "b", PauseMode::Queue);
        driver.send("a", "b", message.clone());

        assert!(driver.is_link_paused("a", "b"));
        assert!(driver.queued_schedule().is_empty());
        assert_eq!(driver.now_ms(), 0);

        driver.resume_link("a", "b");

        assert!(!driver.is_link_paused("a", "b"));
        assert_eq!(
            driver.queued_schedule(),
            vec![(7, 0, "a".into(), "b".into())]
        );
        let delivered = driver.recv("b");
        assert_eq!(delivered.message, message);
        assert_eq!(driver.now_ms(), 7);
    }

    #[test]
    fn link_pause_drop_discards_until_resume() {
        let mut driver = DeterministicDriver::new(two_node_topology(), 42);
        let dropped = register_shape_message("dropped-link");
        let delivered = register_shape_message("delivered-after-drop");

        driver.pause_link("a", "b", PauseMode::Drop);
        driver.send("a", "b", dropped);

        assert!(driver.queued_schedule().is_empty());
        assert_eq!(driver.metrics.counters["messages_sent"], 1);
        assert_eq!(driver.metrics.counters["messages_dropped"], 1);

        driver.resume_link("a", "b");
        driver.send("a", "b", delivered.clone());
        assert_eq!(driver.recv("b").message, delivered);
        assert_eq!(driver.metrics.counters["messages_sent"], 2);
        assert_eq!(driver.metrics.counters["messages_delivered"], 1);
    }

    #[test]
    fn link_resume_schedules_queued_messages_in_send_order() {
        let mut driver = DeterministicDriver::new(two_node_topology_with_latency(3), 42);
        let first = register_shape_message("resume-first");
        let second = register_shape_message("resume-second");

        driver.pause_link("a", "b", PauseMode::Queue);
        driver.send("a", "b", first.clone());
        driver.send("a", "b", second.clone());
        driver.resume_link("a", "b");

        assert_eq!(
            driver.queued_schedule(),
            vec![
                (3, 0, "a".into(), "b".into()),
                (3, 1, "a".into(), "b".into())
            ]
        );
        assert_eq!(driver.recv("b").message, first);
        assert_eq!(driver.recv("b").message, second);
    }

    #[test]
    fn native_transport_codec_preserves_existing_metrics() {
        let mut driver = DeterministicDriver::new(two_node_topology(), 42);

        driver.send("a", "b", register_shape_message("native"));

        assert!(
            !driver
                .metrics
                .counters
                .contains_key("transport_codec_encode_count")
        );
        assert!(
            !driver
                .metrics
                .counters
                .contains_key("transport_codec_decode_count")
        );
    }

    #[test]
    fn deterministic_wire_transport_codec_loopbacks_message() {
        let mut driver = DeterministicDriver::new(two_node_topology(), 42)
            .with_transport_codec(SimulatorTransportCodec::WireBytes);
        let message = register_shape_message("wire");

        driver.send("a", "b", message.clone());
        let delivered = driver.recv("b");

        assert_eq!(delivered.message, message);
        assert_eq!(driver.metrics.counters["transport_codec_encode_count"], 1);
        assert_eq!(driver.metrics.counters["transport_codec_decode_count"], 1);
        assert!(driver.metrics.counters["transport_codec_encoded_bytes"] > 0);
        assert!(
            driver
                .metrics
                .histograms
                .contains_key("transport_codec_encode")
        );
        assert!(
            driver
                .metrics
                .histograms
                .contains_key("transport_codec_decode")
        );
    }

    #[test]
    fn threaded_wire_transport_codec_loopbacks_message() {
        let mut driver = ThreadedDriver::new(two_node_topology(), 42)
            .with_transport_codec(SimulatorTransportCodec::WireBytes);
        let message = register_shape_message("wire-threaded");

        driver.send("a", "b", message.clone());
        let delivered = driver.recv("b");

        assert_eq!(delivered.message, message);
        assert_eq!(driver.metrics.counters["transport_codec_encode_count"], 1);
        assert_eq!(driver.metrics.counters["transport_codec_decode_count"], 1);
        assert!(driver.metrics.counters["transport_codec_encoded_bytes"] > 0);
    }

    #[test]
    fn deterministic_wire_frame_transport_codec_loopbacks_message() {
        let mut driver = DeterministicDriver::new(two_node_topology(), 42)
            .with_transport_codec(SimulatorTransportCodec::WireFrames);
        let message = register_shape_message("wire-frame");

        driver.send("a", "b", message.clone());
        let delivered = driver.recv("b");

        assert_eq!(delivered.message, message);
        assert_eq!(driver.metrics.counters["transport_codec_encode_count"], 1);
        assert_eq!(driver.metrics.counters["transport_codec_decode_count"], 1);
        assert!(driver.metrics.counters["transport_codec_encoded_bytes"] > 0);
        assert!(
            driver
                .metrics
                .counters
                .contains_key("transport_codec_encode_total_us")
        );
        assert!(
            driver
                .metrics
                .counters
                .contains_key("transport_codec_decode_total_us")
        );
        assert!(
            driver.metrics.counters["transport_codec_encoded_frame_bytes"]
                > driver.metrics.counters["transport_codec_encoded_bytes"]
        );
        assert!(
            driver
                .metrics
                .histograms
                .contains_key("transport_codec_encode")
        );
        assert!(
            driver
                .metrics
                .histograms
                .contains_key("transport_codec_decode")
        );

        let fields = driver.metrics.to_json_fields();
        assert_eq!(fields["transport_codec_messages_encoded"], json!(1));
        assert_eq!(fields["transport_codec_messages_decoded"], json!(1));
        assert!(fields.contains_key("transport_codec_payload_bytes_per_message"));
        assert!(fields.contains_key("transport_codec_frame_bytes_per_message"));
        assert!(fields.contains_key("transport_codec_frame_overhead_bytes_per_message"));
        assert!(fields.contains_key("transport_codec_encode_avg_us_per_message"));
        assert!(fields.contains_key("transport_codec_decode_avg_us_per_message"));
    }

    #[test]
    fn threaded_wire_frame_transport_codec_loopbacks_message() {
        let mut driver = ThreadedDriver::new(two_node_topology(), 42)
            .with_transport_codec(SimulatorTransportCodec::WireFrames);
        let message = register_shape_message("wire-frame-threaded");

        driver.send("a", "b", message.clone());
        let delivered = driver.recv("b");

        assert_eq!(delivered.message, message);
        assert_eq!(driver.metrics.counters["transport_codec_encode_count"], 1);
        assert_eq!(driver.metrics.counters["transport_codec_decode_count"], 1);
        assert!(driver.metrics.counters["transport_codec_encoded_bytes"] > 0);
        assert!(
            driver.metrics.counters["transport_codec_encoded_frame_bytes"]
                > driver.metrics.counters["transport_codec_encoded_bytes"]
        );
    }

    #[test]
    fn abi_byte_transport_attach_reports_session_and_adapter_errors() {
        let (mut runtime, runtime_handle, db) = open_abi_memory_db();

        assert_eq!(
            runtime.transport_attach(Some(40), db, TransportDirection::Upstream, Vec::new()),
            AbiCallResult::Ack
        );
        let transport = drain_events(&mut runtime, runtime_handle, 41)
            .into_iter()
            .find_map(|event| match event.kind {
                EventKind::TransportAttached { transport, .. } => Some(transport),
                _ => None,
            })
            .expect("transport attached");

        assert_eq!(
            runtime.transport_diagnostics(Some(42), transport),
            AbiCallResult::Ack
        );
        let diagnostics = drain_events(&mut runtime, runtime_handle, 43)
            .into_iter()
            .find_map(|event| match event.kind {
                EventKind::TransportDiagnostics { payload } => {
                    Some(postcard::from_bytes::<AbiTransportDiagnostics>(&payload).unwrap())
                }
                _ => None,
            })
            .expect("transport diagnostics");
        assert_eq!(diagnostics.transport, transport);
        assert_eq!(diagnostics.session_id, 1);
        assert_eq!(diagnostics.epoch, 1);
        assert_eq!(diagnostics.resume_status, AbiResumeStatus::Fresh);
        assert_eq!(diagnostics.inbound_queue_depth, 0);
        assert_eq!(diagnostics.outbound_queue_depth, 0);

        assert_eq!(
            runtime.transport_send_wire_frame(Some(44), transport, vec![0xff, 0x00, 0x7f]),
            AbiCallResult::Ack
        );
        assert_eq!(runtime.db_drive_tick(Some(45), db), AbiCallResult::Ack);
        drain_events(&mut runtime, runtime_handle, 46);
        assert_eq!(
            runtime.transport_recv_wire_frame(
                Some(47),
                transport,
                FrameBudget {
                    max_frames: 8,
                    max_bytes: u64::MAX,
                },
            ),
            AbiCallResult::Ack
        );

        let errors: Vec<_> = drain_events(&mut runtime, runtime_handle, 48)
            .into_iter()
            .flat_map(|event| match event.kind {
                EventKind::WireFrames { frames, .. } => frames,
                _ => Vec::new(),
            })
            .filter_map(|frame| match decode_frame(&frame).unwrap() {
                WireFrame::Error(error) => Some(error),
                WireFrame::Hello(_) | WireFrame::Message(_) => None,
            })
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, WireErrorCode::MalformedFrame);
    }

    fn open_abi_memory_db() -> (AbiRuntime, Handle, Handle) {
        let mut runtime = AbiRuntime::new();
        let AbiCallResult::RuntimeInitialized {
            runtime: runtime_handle,
        } = runtime.runtime_init(Some(10), ABI_VERSION, ABI_FEATURE_NONE)
        else {
            panic!("expected runtime init");
        };
        drain_events(&mut runtime, runtime_handle, 11);

        assert_eq!(
            runtime.storage_open_memory(
                Some(12),
                postcard::to_allocvec(&MemoryStorageConfig::default()).unwrap(),
            ),
            AbiCallResult::Ack
        );
        let storage = drain_events(&mut runtime, runtime_handle, 13)
            .into_iter()
            .find_map(|event| match event.kind {
                EventKind::StorageOpened { storage, .. } => Some(storage),
                _ => None,
            })
            .expect("storage opened");

        let schema = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let config = OpenDbConfig {
            identity: DbIdentityPayload::from(DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorId::from_bytes([2; 16]),
            }),
            row_id_seed: Some(0x51),
            history_complete: false,
        };
        assert_eq!(
            runtime.db_open_memory(
                Some(14),
                storage,
                postcard::to_allocvec(&schema).unwrap(),
                postcard::to_allocvec(&config).unwrap(),
            ),
            AbiCallResult::Ack
        );
        let db = drain_events(&mut runtime, runtime_handle, 15)
            .into_iter()
            .find_map(|event| match event.kind {
                EventKind::DbOpened { db, .. } => Some(db),
                _ => None,
            })
            .expect("db opened");

        (runtime, runtime_handle, db)
    }

    fn drain_events(
        runtime: &mut AbiRuntime,
        runtime_handle: Handle,
        request_id: u64,
    ) -> Vec<Event> {
        let AbiCallResult::Events { events } = runtime.events_poll(
            Some(request_id),
            runtime_handle,
            EventBudget {
                max_events: 128,
                max_bytes: u64::MAX,
            },
        ) else {
            panic!("expected event poll");
        };
        events
    }
}
