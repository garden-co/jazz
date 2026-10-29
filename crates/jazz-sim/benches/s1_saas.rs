//! S1 SaaS: relevance-scaled, query-driven partial sync.
//!
//! Orgs, teams, memberships, projects shared with teams, and issues with
//! tags. A user reads a project's issues only through a team they belong
//! to (an `INHERITS` chain the authority evaluates per subscriber). Every
//! client subscribes to its visible issues, a filtered project page and a
//! tag join; the oracle is a plain model of the same data and policies.
//!
//! Gates: cold load, live writes, reconnect catch-up and a subscriber
//! sweep each converge on the oracle, and no row a user may not read is
//! ever shipped to them (relevance, not workspace size, decides traffic).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use jazz::db::{Db, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{Query, col, eq, lit, ne};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use jazz_sim::distributions::Lcg;
use jazz_sim::oracle::{Live, Rows, cells as row_cells};
use jazz_sim::support::{
    allow_all, cells, compile, exists, insert, int, is_me, open, outer_eq, read_gated, row, settle,
    subject, text, user, uuid,
};
use jazz_sim::{
    PauseMode, PeerProfile, Sim, SimulatorTransportCodec, bench_profile, emit_summary, env_u64,
};
use serde_json::{Value as JsonValue, json};

const ORGS: &str = "orgs";
const TEAMS: &str = "teams";
const MEMBERSHIPS: &str = "memberships";
const PROJECTS: &str = "projects";
const PROJECT_TEAMS: &str = "projectTeams";
const TAGS: &str = "tags";
const ISSUES: &str = "issues";
const ISSUE_TAGS: &str = "issueTags";

const STATE_DONE: i64 = 3;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let profile = bench_profile();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x51aa_5001),
        profile: format!("{profile:?}").to_lowercase(),
        orgs: profile.select(1, 2, 4),
        teams_per_org: profile.select(4, 8, 16),
        users_per_org: profile.select(8, 32, 64),
        projects_per_org: profile.select(4, 12, 24),
        issues_per_org: profile.select(64, 400, 2_000),
        tags: profile.select(4, 8, 16),
        clients: profile.select(4, 8, 16),
        writes: profile.select(8, 32, 64),
        sweep: profile.select(vec![4], vec![8, 32], vec![8, 32, 128]),
        codec: SimulatorTransportCodec::from_env("JAZZ_S1_TRANSPORT_CODEC"),
        link: PeerProfile::new(
            "regional",
            env_u64("JAZZ_LINK_ONE_WAY_MS", 20),
            env_u64("JAZZ_LINK_JITTER_MS", 5),
            0,
        ),
    };
    run(&config, true);
}

#[allow(dead_code)]
pub fn smoke() {
    run(&smoke_config(), false);
}

fn smoke_config() -> Config {
    Config {
        seed: 0x51aa_5001,
        profile: "s1-smoke".to_owned(),
        orgs: 1,
        teams_per_org: 3,
        users_per_org: 4,
        projects_per_org: 3,
        issues_per_org: 16,
        tags: 3,
        clients: 3,
        writes: 3,
        sweep: vec![2],
        codec: SimulatorTransportCodec::WireFrames,
        link: PeerProfile::new("s1-smoke", 2, 1, 0),
    }
}

#[derive(Clone)]
struct Config {
    seed: u64,
    profile: String,
    orgs: usize,
    teams_per_org: usize,
    users_per_org: usize,
    projects_per_org: usize,
    issues_per_org: usize,
    tags: usize,
    clients: usize,
    writes: usize,
    sweep: Vec<usize>,
    codec: SimulatorTransportCodec,
    link: PeerProfile,
}

fn inherits(via: &str) -> PolicyExpr {
    PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: via.to_owned(),
        max_depth: None,
    }
}

fn schema() -> JazzSchema {
    let table = TableSchemaBuilder::new;
    compile(
        &SchemaBuilder::new()
            .table(
                table(ORGS)
                    .column("name", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                table(TEAMS)
                    .fk_column("org", ORGS)
                    .column("name", ColumnType::Text)
                    .policies(read_gated(exists(
                        MEMBERSHIPS,
                        vec![outer_eq("team", "id"), is_me("member")],
                    ))),
            )
            .table(
                table(MEMBERSHIPS)
                    .fk_column("team", TEAMS)
                    .column("member", ColumnType::Text)
                    .policies(read_gated(is_me("member"))),
            )
            .table(
                table(PROJECTS)
                    .fk_column("org", ORGS)
                    .column("name", ColumnType::Text)
                    .policies(read_gated(PolicyExpr::InheritsReferencing {
                        operation: Operation::Select,
                        source_table: PROJECT_TEAMS.to_owned(),
                        via_column: "project".to_owned(),
                        max_depth: None,
                    })),
            )
            .table(
                table(PROJECT_TEAMS)
                    .fk_column("project", PROJECTS)
                    .fk_column("team", TEAMS)
                    .policies(read_gated(inherits("team"))),
            )
            .table(
                table(TAGS)
                    .column("name", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                table(ISSUES)
                    .fk_column("project", PROJECTS)
                    .column("title", ColumnType::Text)
                    .column("state", ColumnType::BigInt)
                    .policies(read_gated(inherits("project"))),
            )
            .table(
                table(ISSUE_TAGS)
                    .fk_column("issue", ISSUES)
                    .fk_column("tag", TAGS)
                    .policies(read_gated(inherits("issue"))),
            )
            .build(),
    )
}

/// The oracle: the same rows, and the same read rules, as plain maps.
#[derive(Clone, Default)]
struct Model {
    orgs: Vec<RowUuid>,
    teams: BTreeMap<RowUuid, RowUuid>,
    /// Membership id -> (team, member).
    memberships: BTreeMap<RowUuid, (RowUuid, AuthorSubject)>,
    projects: BTreeMap<RowUuid, RowUuid>,
    /// Link id -> (project, team).
    project_teams: BTreeMap<RowUuid, (RowUuid, RowUuid)>,
    tags: Vec<RowUuid>,
    issues: BTreeMap<RowUuid, Issue>,
    /// Link id -> (issue, tag).
    issue_tags: BTreeMap<RowUuid, (RowUuid, RowUuid)>,
    /// Org of each user.
    users: Vec<(AuthorSubject, RowUuid)>,
}

#[derive(Clone, Debug, PartialEq)]
struct Issue {
    project: RowUuid,
    title: String,
    state: i64,
}

impl Issue {
    fn cells(&self) -> BTreeMap<String, Value> {
        cells([
            ("project", uuid(self.project)),
            ("title", text(self.title.clone())),
            ("state", int(self.state)),
        ])
    }
}

impl Model {
    fn generate(config: &Config) -> Self {
        let mut rng = Lcg::new(config.seed);
        let mut model = Model::default();
        for t in 0..config.tags {
            model.tags.push(row(0x16, t as u64));
        }
        for o in 0..config.orgs {
            let org = row(0x10, o as u64);
            model.orgs.push(org);
            let teams: Vec<_> = (0..config.teams_per_org)
                .map(|t| row(0x11, (o * config.teams_per_org + t) as u64))
                .collect();
            for team in &teams {
                model.teams.insert(*team, org);
            }
            for u in 0..config.users_per_org {
                let who = user((o * config.users_per_org + u) as u32);
                model.users.push((who, org));
                // Every user is in one or two of the org's teams.
                let first = rng.usize(teams.len());
                let mut joined = BTreeSet::from([teams[first]]);
                if rng.chance(1, 2) {
                    joined.insert(teams[rng.usize(teams.len())]);
                }
                for team in joined {
                    let id = row(0x12, model.memberships.len() as u64);
                    model.memberships.insert(id, (team, who));
                }
            }
            let projects: Vec<_> = (0..config.projects_per_org)
                .map(|p| row(0x13, (o * config.projects_per_org + p) as u64))
                .collect();
            for project in &projects {
                model.projects.insert(*project, org);
                // Shared with one team, sometimes two.
                let mut shared = BTreeSet::from([teams[rng.usize(teams.len())]]);
                if rng.chance(1, 3) {
                    shared.insert(teams[rng.usize(teams.len())]);
                }
                for team in shared {
                    let id = row(0x14, model.project_teams.len() as u64);
                    model.project_teams.insert(id, (*project, team));
                }
            }
            for i in 0..config.issues_per_org {
                let id = row(0x15, (o * config.issues_per_org + i) as u64);
                let project = projects[rng.usize(projects.len())];
                model.issues.insert(
                    id,
                    Issue {
                        project,
                        title: format!("issue-{o}-{i}"),
                        state: rng.usize(4) as i64,
                    },
                );
                for _ in 0..rng.usize(3) {
                    let link = row(0x17, model.issue_tags.len() as u64);
                    let tag = model.tags[rng.usize(model.tags.len())];
                    model.issue_tags.insert(link, (id, tag));
                }
            }
        }
        model
    }

    /// Every row as (table, id, cells), parents before children.
    fn rows(&self) -> Vec<(&'static str, RowUuid, BTreeMap<String, Value>)> {
        let mut rows = Vec::new();
        for (n, org) in self.orgs.iter().enumerate() {
            rows.push((ORGS, *org, cells([("name", text(format!("org-{n}")))])));
        }
        for (n, tag) in self.tags.iter().enumerate() {
            rows.push((TAGS, *tag, cells([("name", text(format!("tag-{n}")))])));
        }
        for (team, org) in &self.teams {
            rows.push((
                TEAMS,
                *team,
                cells([("org", uuid(*org)), ("name", text("team"))]),
            ));
        }
        for (id, (team, member)) in &self.memberships {
            rows.push((
                MEMBERSHIPS,
                *id,
                cells([("team", uuid(*team)), ("member", subject(*member))]),
            ));
        }
        for (project, org) in &self.projects {
            rows.push((
                PROJECTS,
                *project,
                cells([("org", uuid(*org)), ("name", text("project"))]),
            ));
        }
        for (id, (project, team)) in &self.project_teams {
            rows.push((
                PROJECT_TEAMS,
                *id,
                cells([("project", uuid(*project)), ("team", uuid(*team))]),
            ));
        }
        for (id, issue) in &self.issues {
            rows.push((ISSUES, *id, issue.cells()));
        }
        for (id, (issue, tag)) in &self.issue_tags {
            rows.push((
                ISSUE_TAGS,
                *id,
                cells([("issue", uuid(*issue)), ("tag", uuid(*tag))]),
            ));
        }
        rows
    }

    fn teams_of(&self, who: AuthorSubject) -> BTreeSet<RowUuid> {
        self.memberships
            .values()
            .filter(|(_, member)| *member == who)
            .map(|(team, _)| *team)
            .collect()
    }

    fn projects_of(&self, who: AuthorSubject) -> BTreeSet<RowUuid> {
        let teams = self.teams_of(who);
        self.project_teams
            .values()
            .filter(|(_, team)| teams.contains(team))
            .map(|(project, _)| *project)
            .collect()
    }

    fn visible_issues(&self, who: AuthorSubject) -> Rows {
        let projects = self.projects_of(who);
        self.issues
            .iter()
            .filter(|(_, issue)| projects.contains(&issue.project))
            .map(|(id, issue)| (*id, issue.cells()))
            .collect()
    }

    fn q1(&self, who: AuthorSubject, project: RowUuid) -> Rows {
        self.visible_issues(who)
            .into_iter()
            .filter(|(id, _)| {
                let issue = &self.issues[id];
                issue.project == project && issue.state != STATE_DONE
            })
            .collect()
    }

    fn q2(&self, who: AuthorSubject, tag: RowUuid) -> Rows {
        let tagged: BTreeSet<RowUuid> = self
            .issue_tags
            .values()
            .filter(|(_, t)| *t == tag)
            .map(|(issue, _)| *issue)
            .collect();
        self.visible_issues(who)
            .into_iter()
            .filter(|(id, _)| tagged.contains(id))
            .collect()
    }

    /// Whether `who` may read row `id` of `table`.
    fn readable(&self, who: AuthorSubject, table: &str, id: RowUuid) -> bool {
        let teams = self.teams_of(who);
        let projects = self.projects_of(who);
        match table {
            ORGS | TAGS => true,
            TEAMS => teams.contains(&id),
            MEMBERSHIPS => self.memberships.get(&id).is_some_and(|(_, m)| *m == who),
            PROJECTS => projects.contains(&id),
            PROJECT_TEAMS => self
                .project_teams
                .get(&id)
                .is_some_and(|(_, t)| teams.contains(t)),
            ISSUES => self
                .issues
                .get(&id)
                .is_some_and(|i| projects.contains(&i.project)),
            ISSUE_TAGS => self
                .issue_tags
                .get(&id)
                .is_some_and(|(issue, _)| self.readable(who, ISSUES, *issue)),
            other => panic!("unknown table {other}"),
        }
    }
}

fn q1_query(project: RowUuid) -> Query {
    Query::from(ISSUES)
        .filter(eq(col("project"), lit(uuid(project))))
        .filter(ne(col("state"), lit(int(STATE_DONE))))
}

fn q2_query(tag: RowUuid) -> Query {
    Query::from(ISSUES).join_via(ISSUE_TAGS, "issue", [eq(col("tag"), lit(uuid(tag)))])
}

/// One connected client and the oracle-checked subscriptions it holds.
struct Client {
    name: String,
    who: AuthorSubject,
    project: RowUuid,
    tag: RowUuid,
    scope: Live,
    q1: Live,
    q2: Live,
}

impl Client {
    fn poll(&mut self) {
        self.scope.poll();
        self.q1.poll();
        self.q2.poll();
    }

    fn assert_matches(&mut self, model: &Model, phase: &str) {
        self.poll();
        let who = self.who;
        assert_eq!(
            self.scope.rows,
            model.visible_issues(who),
            "{phase}: {} scope",
            self.name
        );
        assert_eq!(
            self.q1.rows,
            model.q1(who, self.project),
            "{phase}: {} q1",
            self.name
        );
        assert_eq!(
            self.q2.rows,
            model.q2(who, self.tag),
            "{phase}: {} q2",
            self.name
        );
    }
}

fn subscribe_client(
    sim: &mut Sim,
    model: &Model,
    config: &Config,
    n: usize,
    who: AuthorSubject,
) -> Client {
    let name = format!("client-{n}");
    sim.add(name.clone(), open(schema(), 100 + n as u32, who));
    sim.connect(&name, "core", who, config.link.clone());
    sim.record_downstream(&name);
    // A project the user can see, when there is one, and a tag.
    let project = model
        .projects_of(who)
        .into_iter()
        .next()
        .unwrap_or_else(|| *model.projects.keys().next().expect("a project"));
    let tag = model.tags[n % model.tags.len()];
    let db = sim.db(&name);
    Client {
        scope: Live::subscribe(db, &Query::from(ISSUES), ReadOpts::default()),
        q1: Live::subscribe(db, &q1_query(project), ReadOpts::default()),
        q2: Live::subscribe(db, &q2_query(tag), ReadOpts::default()),
        name,
        who,
        project,
        tag,
    }
}

/// No row the user may not read was ever shipped to them (`INV-BENCH-6`).
fn assert_relevant(sim: &Sim, model: &Model, client: &Client) -> usize {
    let mut shipped = 0;
    for message in sim.downstream_log(&client.name) {
        if let jazz::db::SyncMessage::Rows { images } = message {
            for image in images.iter().filter(|image| !image.deleted) {
                shipped += 1;
                let id = RowUuid::from_bytes(image.id);
                assert!(
                    model.readable(client.who, &image.table, id),
                    "{} was shipped {}/{id:?}, which it may not read",
                    client.name,
                    image.table
                );
            }
        }
    }
    shipped
}

fn run(config: &Config, emit: bool) {
    let model = Model::generate(config);
    let mut model = model;
    let mut sim = Sim::new(config.seed, config.codec);
    let core = sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    let seeding = Instant::now();
    for (table, id, values) in model.rows() {
        insert(core, table, id, values);
    }
    let seed_ms = seeding.elapsed().as_millis() as u64;

    // Cold load.
    let users: Vec<AuthorSubject> = model.users.iter().map(|(who, _)| *who).collect();
    let mut clients: Vec<Client> = (0..config.clients)
        .map(|n| subscribe_client(&mut sim, &model, config, n, users[n % users.len()]))
        .collect();
    let cold = Instant::now();
    sim.settle();
    let cold_us = cold.elapsed().as_micros() as u64;
    let cold_virtual_ms = sim.now_ms();
    for client in &mut clients {
        client.assert_matches(&model, "cold load");
        assert!(
            client.scope.settled,
            "{}: the cold load settled",
            client.name
        );
    }
    // The oracle is not vacuous: pages and joins have rows, and users are
    // denied issues the workspace holds.
    assert!(
        clients.iter().filter(|c| !c.scope.rows.is_empty()).count() >= 2,
        "users see issues"
    );
    assert!(
        clients.iter().any(|c| !c.q1.rows.is_empty()),
        "some project page has rows"
    );
    assert!(
        clients.iter().any(|c| !c.q2.rows.is_empty()),
        "some tag join has rows"
    );
    assert!(
        clients
            .iter()
            .any(|c| c.scope.rows.len() < model.issues.len()),
        "some user is denied issues"
    );
    let (_, cold_down) = sim.total_stats();

    // Live writes: each client edits issues it can see, and files one.
    let mut rng = Lcg::new(config.seed ^ 0x5157_1173);
    let writes = Instant::now();
    for round in 0..config.writes {
        for (n, client) in clients.iter().enumerate() {
            let visible: Vec<RowUuid> = client.scope.rows.keys().copied().collect();
            let db = sim.db(&client.name);
            let handle = if visible.is_empty() || round == 0 {
                let project = client.project;
                if !model.projects_of(client.who).contains(&project) {
                    continue;
                }
                let id = row(0x18, (round * config.clients + n) as u64);
                let issue = Issue {
                    project,
                    title: format!("filed-by-{}", client.name),
                    state: 0,
                };
                let handle = insert(db, ISSUES, id, issue.cells());
                model.issues.insert(id, issue);
                handle
            } else {
                let id = visible[rng.usize(visible.len())];
                let state = rng.usize(4) as i64;
                let title = format!("edit-{round}-{}", client.name);
                let handle = block_on(db.update(
                    ISSUES,
                    id,
                    cells([("state", int(state)), ("title", text(title.clone()))]),
                    Default::default(),
                ))
                .expect("issue update");
                let issue = model.issues.get_mut(&id).expect("modelled issue");
                issue.state = state;
                issue.title = title;
                handle
            };
            settle(&sim, db, &handle);
        }
    }
    sim.settle();
    let writes_us = writes.elapsed().as_micros() as u64;
    for client in &mut clients {
        client.assert_matches(&model, "after live writes");
    }
    assert_eq!(
        jazz_sim::oracle::read(sim.db("core"), &Query::from(ISSUES), ReadOpts::default()),
        model
            .issues
            .iter()
            .map(|(id, issue)| (*id, issue.cells()))
            .collect::<Rows>(),
        "the authority holds every accepted write"
    );

    // Reconnect: the last client drops off while the others write, then
    // catches up as a new session.
    let offline = clients.len() - 1;
    let offline_name = clients[offline].name.clone();
    sim.pause(&offline_name, PauseMode::Disconnect);
    for client in clients.iter().take(offline) {
        let Some(id) = client.scope.rows.keys().next().copied() else {
            continue;
        };
        let title = format!("while-{offline_name}-was-away");
        let handle = block_on(sim.db(&client.name).update(
            ISSUES,
            id,
            cells([("title", text(title.clone()))]),
            Default::default(),
        ))
        .expect("update while a client is away");
        model.issues.get_mut(&id).expect("modelled issue").title = title;
        settle(&sim, sim.db(&client.name), &handle);
    }
    sim.settle();
    let (before_up, before_down) = sim.link_stats(&offline_name);
    let reconnect = Instant::now();
    sim.resume(&offline_name);
    sim.settle();
    let reconnect_us = reconnect.elapsed().as_micros() as u64;
    let (after_up, after_down) = sim.link_stats(&offline_name);
    for client in &mut clients {
        client.assert_matches(&model, "after reconnect");
    }

    let mut shipped = 0;
    for client in &clients {
        shipped += assert_relevant(&sim, &model, client);
    }

    // Subscriber sweep: `count` more sessions of the first user watch its
    // scope; one write reaches every one of them exactly once.
    let mut sweeps = Vec::new();
    for (sweep_index, count) in config.sweep.iter().copied().enumerate() {
        let who = clients[0].who;
        let mut watchers: Vec<Client> = (0..count)
            .map(|n| {
                let index = 1_000 * (sweep_index + 1) + n;
                subscribe_client(&mut sim, &model, config, index, who)
            })
            .collect();
        sim.settle();
        for watcher in &mut watchers {
            watcher.assert_matches(&model, "sweep cold load");
        }
        let Some(id) = clients[0].scope.rows.keys().next().copied() else {
            continue;
        };
        sim.reset_stats();
        let title = format!("sweep-{count}");
        let handle = block_on(sim.db(&clients[0].name).update(
            ISSUES,
            id,
            cells([("title", text(title.clone()))]),
            Default::default(),
        ))
        .expect("sweep write");
        model.issues.get_mut(&id).expect("modelled issue").title = title;
        settle(&sim, sim.db(&clients[0].name), &handle);
        sim.settle();
        let mut images = 0;
        for watcher in &mut watchers {
            let updates_before = watcher.scope.updated;
            watcher.assert_matches(&model, "sweep write");
            assert_eq!(
                watcher.scope.updated - updates_before,
                1,
                "{}: one update for one write",
                watcher.name
            );
            images += sim.link_stats(&watcher.name).1.row_images;
            assert_relevant(&sim, &model, watcher);
        }
        let (_, down) = sim.total_stats();
        sweeps.push(json!({
            "subscribers": count,
            "notify_bytes": down.bytes,
            "notify_row_images": images,
        }));
    }

    if emit {
        let mut summary = sim.json_fields();
        summary.extend(
            json!({
                "phase": "summary",
                "rows": model.rows().len(),
                "clients": config.clients,
                "seed_ms": seed_ms,
                "cold_us": cold_us,
                "cold_virtual_ms": cold_virtual_ms,
                "cold_bytes": cold_down.bytes,
                "cold_row_images": cold_down.row_images,
                "writes": config.writes * config.clients,
                "writes_us": writes_us,
                "reconnect_us": reconnect_us,
                "reconnect_up_bytes": after_up.bytes - before_up.bytes,
                "reconnect_down_bytes": after_down.bytes - before_down.bytes,
                "shipped_row_images": shipped,
                "forbidden_row_images": 0,
                "sweep": sweeps,
            })
            .as_object()
            .expect("object")
            .clone(),
        );
        emit_summary(
            "s1_saas",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(summary),
        );
    }
}

/// The same data and subscriptions on the authority's own `Db`, read
/// under each user's identity: no sync, just the policy-filtered local
/// surface.
#[allow(dead_code)]
pub fn db_surface_smoke() {
    let config = smoke_config();
    let mut model = Model::generate(&config);
    let db = open(schema(), 0, AuthorSubject::SYSTEM);
    let users: Vec<AuthorSubject> = model.users.iter().map(|(who, _)| *who).collect();
    struct Watch {
        who: AuthorSubject,
        project: RowUuid,
        tag: RowUuid,
        q1: Live,
        q2: Live,
    }
    let subscribe = |db: &Db, query: &Query, who: AuthorSubject| {
        let prepared = db.prepare_query(query).expect("prepare");
        Live::from_stream(
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), who))
                .expect("subscribe for identity"),
        )
    };
    let mut watches: Vec<Watch> = users
        .iter()
        .enumerate()
        .map(|(n, who)| {
            let project = *model
                .projects
                .keys()
                .nth(n % model.projects.len())
                .expect("project");
            let tag = model.tags[n % model.tags.len()];
            Watch {
                who: *who,
                project,
                tag,
                q1: subscribe(&db, &q1_query(project), *who),
                q2: subscribe(&db, &q2_query(tag), *who),
            }
        })
        .collect();
    for watch in &mut watches {
        watch.q1.poll();
        watch.q2.poll();
        assert!(watch.q1.rows.is_empty() && watch.q2.rows.is_empty());
    }
    for (table, id, values) in model.rows() {
        insert(&db, table, id, values);
    }
    let check = |watches: &mut Vec<Watch>, model: &Model, phase: &str| {
        for watch in watches.iter_mut() {
            watch.q1.poll();
            watch.q2.poll();
            assert_eq!(
                watch.q1.rows,
                model.q1(watch.who, watch.project),
                "{phase}: q1"
            );
            assert_eq!(watch.q2.rows, model.q2(watch.who, watch.tag), "{phase}: q2");
            let prepared = db.prepare_query(&q1_query(watch.project)).expect("prepare");
            let read: Rows =
                block_on(db.all_for_identity(&prepared, ReadOpts::default(), watch.who))
                    .expect("read for identity")
                    .iter()
                    .map(|row| (row.row_uuid(), row_cells(row)))
                    .collect();
            assert_eq!(
                read, watch.q1.rows,
                "{phase}: one-shot read equals the subscription"
            );
        }
    };
    check(&mut watches, &model, "cold load");
    // Close every open issue of a project some user watches a non-empty
    // page of.
    let target = watches
        .iter()
        .find(|watch| !watch.q1.rows.is_empty())
        .expect("some user sees open issues")
        .project;
    let open_issues: Vec<RowUuid> = model
        .issues
        .iter()
        .filter(|(_, issue)| issue.project == target && issue.state != STATE_DONE)
        .map(|(id, _)| *id)
        .collect();
    for id in open_issues {
        block_on(db.update(
            ISSUES,
            id,
            cells([("state", int(STATE_DONE))]),
            Default::default(),
        ))
        .expect("close issue");
        model.issues.get_mut(&id).expect("issue").state = STATE_DONE;
    }
    check(&mut watches, &model, "after closing a project's issues");
    assert!(
        watches
            .iter()
            .filter(|watch| watch.project == target)
            .all(|watch| watch.q1.rows.is_empty()),
        "the closed project's open page is empty"
    );
    assert!(
        watches.iter().any(|watch| watch.q1.removed > 0),
        "some subscription saw issues leave"
    );
}
