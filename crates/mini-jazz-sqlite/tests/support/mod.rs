use mini_jazz_sqlite::sync::{Bundle, QueryReadRecord};
use mini_jazz_sqlite::{BuiltQuery, Result, RowView, Runtime, SchemaDef, Storage};
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TodoView {
    pub id: String,
    pub title: String,
    pub done: bool,
    pub project_id: String,
    pub project_title: Option<String>,
    pub created_by: String,
    pub tx_id: String,
}

pub trait FixtureRuntimeExt {
    fn create_project(&mut self, id: &str, title: &str) -> Result<String>;
    fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String>;
    fn delete_todo(&mut self, id: &str) -> Result<String>;
    fn open_todos(&self) -> Result<Vec<TodoView>>;
    fn open_todos_require_project(&self) -> Result<Vec<TodoView>>;
    fn newest_open_todos(&self, limit: usize) -> Result<Vec<TodoView>>;
    fn export_query_scope_open_todos(&self) -> Result<Bundle>;
    fn export_query_scope_newest_open_todos(&self, limit: usize) -> Result<Bundle>;
}

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
        Runtime::open(Storage::Memory, node_id, user)
    }

    pub fn durable(&self, file_name: &str, node_id: &str, user: &str) -> Result<Runtime> {
        Runtime::open(Storage::File(self.path(file_name)), node_id, user)
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
    for refresh in source.export_query_read_refreshes(&target.observed_query_reads()?)? {
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

impl FixtureRuntimeExt for Runtime {
    fn create_project(&mut self, id: &str, title: &str) -> Result<String> {
        self.insert_row(
            "projects",
            id,
            BTreeMap::from([("title".to_owned(), json!(title))]),
        )
    }

    fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String> {
        self.insert_row(
            "todos",
            id,
            BTreeMap::from([
                ("title".to_owned(), json!(title)),
                ("done".to_owned(), json!(done)),
                ("project".to_owned(), json!(project_id)),
            ]),
        )
    }

    fn delete_todo(&mut self, id: &str) -> Result<String> {
        self.delete_row("todos", id)
    }

    fn open_todos(&self) -> Result<Vec<TodoView>> {
        open_todo_rows(self.read_rows("todos")?, self.read_rows("projects")?)
    }

    fn open_todos_require_project(&self) -> Result<Vec<TodoView>> {
        Ok(self
            .open_todos()?
            .into_iter()
            .filter(|todo| todo.project_title.is_some())
            .collect())
    }

    fn newest_open_todos(&self, limit: usize) -> Result<Vec<TodoView>> {
        let todos = self.query(top_created_query(
            "todos",
            "done",
            JsonValue::Bool(false),
            limit,
        ))?;
        open_todo_rows_with_sort(todos, self.read_rows("projects")?, false)
    }

    fn export_query_scope_open_todos(&self) -> Result<Bundle> {
        let todos = self.open_todos()?;
        let mut bundle = self.export_query_where_eq_with_ref_include(
            "todos",
            "done",
            JsonValue::Bool(false),
            "project",
        )?;
        extend_with_project_scope(self, &mut bundle, &todos)?;
        Ok(bundle)
    }

    fn export_query_scope_newest_open_todos(&self, limit: usize) -> Result<Bundle> {
        let todos = self.newest_open_todos(limit)?;
        let mut bundle = self.export_query_with_ref_includes(
            top_created_query("todos", "done", JsonValue::Bool(false), limit),
            &["project"],
        )?;
        extend_with_project_scope(self, &mut bundle, &todos)?;
        Ok(bundle)
    }
}

fn open_todo_rows(todos: Vec<RowView>, projects: Vec<RowView>) -> Result<Vec<TodoView>> {
    open_todo_rows_with_sort(todos, projects, true)
}

fn open_todo_rows_with_sort(
    todos: Vec<RowView>,
    projects: Vec<RowView>,
    sort_by_id: bool,
) -> Result<Vec<TodoView>> {
    let projects = projects
        .into_iter()
        .map(|row| {
            (
                row.id,
                row.values
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut todos = todos
        .into_iter()
        .filter(|row| row.values.get("done") == Some(&JsonValue::Bool(false)))
        .map(|row| {
            let title = row
                .values
                .get("title")
                .and_then(JsonValue::as_str)
                .expect("todo missing title")
                .to_owned();
            let project_id = row
                .values
                .get("project")
                .and_then(JsonValue::as_str)
                .expect("todo missing project")
                .to_owned();
            let project_title = projects.get(&project_id).cloned().flatten();
            Ok(TodoView {
                id: row.id,
                title,
                done: false,
                project_id,
                project_title,
                created_by: row.created_by,
                tx_id: row.tx_id,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if sort_by_id {
        todos.sort_by(|left, right| left.id.cmp(&right.id));
    }
    Ok(todos)
}

fn extend_with_project_scope(
    _runtime: &Runtime,
    bundle: &mut Bundle,
    todos: &[TodoView],
) -> Result<()> {
    let Some(branch_id) = bundle
        .query_reads
        .first()
        .map(|read| read.branch_id.clone())
    else {
        return Ok(());
    };
    let mut added_absence = false;
    for todo in todos.iter().filter(|todo| todo.project_title.is_none()) {
        bundle.query_reads.push(QueryReadRecord {
            branch_id: branch_id.clone(),
            table: "projects".to_owned(),
            field: "id".to_owned(),
            op: "absent".to_owned(),
            value: JsonValue::String(todo.project_id.clone()),
        });
        added_absence = true;
    }
    if added_absence {
        bundle.policy_fingerprint = "legacy".to_owned();
    }
    Ok(())
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
