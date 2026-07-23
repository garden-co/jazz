//! Run configuration and the benchmarked topologies, resolved once from the
//! environment (`N` rows, `ONLY` topology, `JZ_PROGRESS`).

pub(crate) const BATCH: u64 = 1000;
pub(crate) const SAMPLE: usize = 500;
/// Point lookups on the (unindexed) Jazz path are full scans, so we time a small
/// sub-sample and report per-lookup latency rather than all `SAMPLE`.
pub(crate) const PROBE: usize = 20;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Topology {
    Raw,
    Local,
    Server,
}

impl Topology {
    pub(crate) const ALL: [Topology; 3] = [Topology::Raw, Topology::Local, Topology::Server];

    fn key(self) -> &'static str {
        match self {
            Topology::Raw => "raw",
            Topology::Local => "local",
            Topology::Server => "server",
        }
    }
}

pub(crate) struct Config {
    pub(crate) rows: usize,
    pub(crate) only: Option<Topology>,
    pub(crate) progress: bool,
}

impl Config {
    /// Read `N`, `ONLY`, and `JZ_PROGRESS` from the environment, rejecting
    /// malformed values loudly rather than silently falling back.
    pub(crate) fn from_env() -> Config {
        let rows = match std::env::var("N") {
            Ok(v) => v
                .parse()
                .unwrap_or_else(|_| panic!("N: expected a number, got {v:?}")),
            Err(_) => 15_000,
        };
        let only = std::env::var("ONLY").ok().map(|v| {
            Topology::ALL
                .into_iter()
                .find(|t| t.key() == v)
                .unwrap_or_else(|| panic!("ONLY: unknown topology {v:?} (raw|local|server)"))
        });
        Config {
            rows,
            only,
            progress: std::env::var("JZ_PROGRESS").is_ok(),
        }
    }

    pub(crate) fn runs(&self, topology: Topology) -> bool {
        self.only.is_none_or(|only| only == topology)
    }
}
