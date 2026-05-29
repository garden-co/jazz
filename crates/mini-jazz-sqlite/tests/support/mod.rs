use mini_jazz_sqlite::sync::{Bundle, QueryReadRecord};
use mini_jazz_sqlite::{BuiltQuery, Result, Runtime, SchemaDef, Storage};
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::TempDir;

pub mod todo_app;

pub struct Harness {
    dir: TempDir,
}

#[derive(Clone, Copy)]
pub enum NodeStorage {
    Memory,
    Durable(&'static str),
}

#[derive(Clone, Copy)]
pub enum NodeKind {
    Client { user: &'static str },
    TrustedPeer,
}

#[derive(Clone, Copy)]
pub struct NodeSpec {
    pub node_id: &'static str,
    pub kind: NodeKind,
    pub storage: NodeStorage,
}

impl NodeSpec {
    pub fn client_memory(node_id: &'static str, user: &'static str) -> Self {
        Self {
            node_id,
            kind: NodeKind::Client { user },
            storage: NodeStorage::Memory,
        }
    }

    pub fn trusted_peer_memory(node_id: &'static str) -> Self {
        Self {
            node_id,
            kind: NodeKind::TrustedPeer,
            storage: NodeStorage::Memory,
        }
    }

    pub fn trusted_peer_durable(file_name: &'static str, node_id: &'static str) -> Self {
        Self {
            node_id,
            kind: NodeKind::TrustedPeer,
            storage: NodeStorage::Durable(file_name),
        }
    }
}

pub struct Topology {
    nodes: BTreeMap<&'static str, Runtime>,
}

impl Topology {
    pub fn new(
        harness: &Harness,
        schema: SchemaDef,
        specs: &[(&'static str, NodeSpec)],
    ) -> Result<Self> {
        let mut nodes = BTreeMap::new();
        for (role, spec) in specs {
            nodes.insert(*role, harness.node_with_schema(*spec, schema.clone())?);
        }
        Ok(Self { nodes })
    }

    pub fn take(&mut self, role: &'static str) -> Runtime {
        self.nodes
            .remove(role)
            .unwrap_or_else(|| panic!("missing topology role {role}"))
    }
}

impl Harness {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("create test harness directory"),
        }
    }

    pub fn path(&self, file_name: &str) -> PathBuf {
        self.dir.path().join(file_name)
    }

    pub fn memory(&self, node_id: &str, user: &str) -> Result<Runtime> {
        open_todo_app(Storage::Memory, node_id, user)
    }

    pub fn durable(&self, file_name: &str, node_id: &str, user: &str) -> Result<Runtime> {
        open_todo_app(Storage::File(self.path(file_name)), node_id, user)
    }

    pub fn memory_with_schema(
        &self,
        node_id: &str,
        user: &str,
        schema: SchemaDef,
    ) -> Result<Runtime> {
        Runtime::open_with_schema(Storage::Memory, node_id, user, schema)
    }

    pub fn durable_with_schema(
        &self,
        file_name: &str,
        node_id: &str,
        user: &str,
        schema: SchemaDef,
    ) -> Result<Runtime> {
        Runtime::open_with_schema(Storage::File(self.path(file_name)), node_id, user, schema)
    }

    pub fn trusted_memory_with_schema(&self, node_id: &str, schema: SchemaDef) -> Result<Runtime> {
        Runtime::open_trusted_with_schema(Storage::Memory, node_id, schema)
    }

    pub fn trusted_durable_with_schema(
        &self,
        file_name: &str,
        node_id: &str,
        schema: SchemaDef,
    ) -> Result<Runtime> {
        Runtime::open_trusted_with_schema(Storage::File(self.path(file_name)), node_id, schema)
    }

    pub fn node_with_schema(&self, spec: NodeSpec, schema: SchemaDef) -> Result<Runtime> {
        let storage = match spec.storage {
            NodeStorage::Memory => Storage::Memory,
            NodeStorage::Durable(file_name) => Storage::File(self.path(file_name)),
        };
        match spec.kind {
            NodeKind::Client { user } => {
                Runtime::open_with_schema(storage, spec.node_id, user, schema)
            }
            NodeKind::TrustedPeer => {
                Runtime::open_trusted_with_schema(storage, spec.node_id, schema)
            }
        }
    }
}

pub fn open_todo_app(storage: Storage, node_id: &str, user: &str) -> Result<Runtime> {
    Runtime::open_with_schema(storage, node_id, user, todo_app_schema())
}

pub fn todo_app_schema() -> SchemaDef {
    SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.index("open_created", ["done", "$createdAt"]);
            table.index("created", ["$createdAt"]);
            table.index("by_title", ["title"]);
        })
        .table("labels", |table| {
            table.text("name");
            table.index("by_name", ["name"]);
        })
        .table("todo_labels", |table| {
            table.ref_("todo", "todos");
            table.ref_("label", "labels");
            table.index("by_todo", ["todo"]);
            table.index("by_label", ["label"]);
        })
}

pub fn run_as_user<T>(
    trusted_peer: &mut Runtime,
    user: &str,
    f: impl FnOnce(&mut Runtime) -> T,
) -> T {
    trusted_peer.run_as_user(user, f)
}

pub fn run_attributing_to_user<T>(
    trusted_peer: &mut Runtime,
    user: &str,
    f: impl FnOnce(&mut Runtime) -> T,
) -> T {
    trusted_peer.run_attributing_to_user(user, f)
}

pub fn apply(source_bundle: Bundle, target: &mut Runtime) -> Result<()> {
    target.apply_bundle(&source_bundle)
}

pub fn apply_untrusted(source_bundle: Bundle, target: &mut Runtime) -> Result<()> {
    target.apply_untrusted_bundle(&source_bundle)
}

pub fn apply_untrusted_as_user(
    source_bundle: Bundle,
    target: &mut Runtime,
    user: &str,
) -> Result<()> {
    target.apply_untrusted_bundle_as_user(&source_bundle, user)
}

pub fn sync_table(source: &Runtime, target: &mut Runtime, table_name: &str) -> Result<()> {
    target.apply_bundle(&source.export_table_history(table_name)?)
}

pub fn sync_table_untrusted(
    source: &Runtime,
    target: &mut Runtime,
    table_name: &str,
) -> Result<()> {
    target.apply_untrusted_bundle_as_user(
        &source.export_table_history(table_name)?,
        source.session_user(),
    )
}

pub fn forward_exclusive(
    source: &Runtime,
    target: &mut Runtime,
    table_name: &str,
    tx_id: &str,
    auth_user: &str,
) -> Result<()> {
    target.apply_untrusted_bundle(
        &source.export_exclusive_transaction_forwarding(table_name, tx_id, auth_user)?,
    )
}

pub fn refresh_observed_queries(source: &Runtime, target: &mut Runtime) -> Result<()> {
    let reads = target.observed_query_reads()?;
    refresh_query_reads(source, target, &reads)
}

pub fn refresh_query_reads(
    source: &Runtime,
    target: &mut Runtime,
    reads: &[QueryReadRecord],
) -> Result<()> {
    for refresh in source.export_query_read_refreshes(reads)? {
        target.apply_bundle(&refresh)?;
    }
    Ok(())
}

pub struct TrustedEdgeTopology {
    pub alice: Runtime,
    pub bob: Runtime,
    pub edge: Runtime,
}

impl TrustedEdgeTopology {
    pub fn memory(harness: &Harness, schema: SchemaDef) -> Result<Self> {
        let mut topology = Topology::new(
            harness,
            schema,
            &[
                ("alice", NodeSpec::client_memory("alice-node", "alice")),
                ("bob", NodeSpec::trusted_peer_memory("bob-node")),
                ("edge", NodeSpec::trusted_peer_memory("edge")),
            ],
        )?;
        Ok(Self {
            alice: topology.take("alice"),
            bob: topology.take("bob"),
            edge: topology.take("edge"),
        })
    }

    pub fn durable_edge(harness: &Harness, schema: SchemaDef) -> Result<Self> {
        let mut topology = Topology::new(
            harness,
            schema,
            &[
                ("alice", NodeSpec::client_memory("alice-node", "alice")),
                ("bob", NodeSpec::trusted_peer_memory("bob-node")),
                (
                    "edge",
                    NodeSpec::trusted_peer_durable("edge.sqlite", "edge"),
                ),
            ],
        )?;
        Ok(Self {
            alice: topology.take("alice"),
            bob: topology.take("bob"),
            edge: topology.take("edge"),
        })
    }
}

pub struct TrustedMeshTopology {
    pub alice_tab: Runtime,
    pub edge_a: Runtime,
    pub core: Runtime,
    pub edge_b: Runtime,
    pub alice_laptop: Runtime,
    pub bob_tab: Runtime,
}

impl TrustedMeshTopology {
    pub fn memory(harness: &Harness, schema: SchemaDef) -> Result<Self> {
        let mut topology = Topology::new(
            harness,
            schema,
            &[
                ("alice-tab", NodeSpec::client_memory("alice-tab", "alice")),
                ("edge-a", NodeSpec::trusted_peer_memory("edge-a")),
                ("core", NodeSpec::trusted_peer_memory("core")),
                ("edge-b", NodeSpec::trusted_peer_memory("edge-b")),
                (
                    "alice-laptop",
                    NodeSpec::client_memory("alice-laptop", "alice"),
                ),
                ("bob-tab", NodeSpec::client_memory("bob-tab", "bob")),
            ],
        )?;
        Ok(Self {
            alice_tab: topology.take("alice-tab"),
            edge_a: topology.take("edge-a"),
            core: topology.take("core"),
            edge_b: topology.take("edge-b"),
            alice_laptop: topology.take("alice-laptop"),
            bob_tab: topology.take("bob-tab"),
        })
    }
}

pub fn tasks_schema() -> SchemaDef {
    SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    })
}

pub fn notes_schema() -> SchemaDef {
    SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    })
}

pub fn eq_query(table: &str, field: &str, value: JsonValue) -> BuiltQuery {
    BuiltQuery::from_json_value(json!({
        "table": table,
        "conditions": [{"column": field, "op": "eq", "value": value}],
    }))
    .unwrap()
}

pub fn top_created_query(table: &str, field: &str, value: JsonValue, limit: usize) -> BuiltQuery {
    BuiltQuery::from_json_value(json!({
        "table": table,
        "conditions": [{"column": field, "op": "eq", "value": value}],
        "orderBy": [["$createdAt", "desc"]],
        "limit": limit,
    }))
    .unwrap()
}

pub fn folders_schema() -> SchemaDef {
    SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
        table.read_if_created_by_user();
    })
}
