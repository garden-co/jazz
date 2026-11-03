//! S3 recursive permission-filtered sync.
//!
//! Teams nest (a user's own team is a member of group teams, which are
//! members of the org team), and a resource is readable through an access
//! row naming any team the user's team reaches: a recursive gather the
//! authority evaluates per subscriber. Resources hold pages, pages hold
//! ordered blocks that inherit access.
//!
//! Gates: cold load by persona matches the oracle; a grant delivers the
//! resource and its block tree; revocations take them out of synced
//! subscriptions; a write through a resource the writer cannot read is
//! rejected; and no client is ever shipped a row it may not read
//! (`INV-BENCH-6`).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use jazz::db::{ErrorCode, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz_sim::oracle::{Live, Rows};
use jazz_sim::public_schema_fixture::seeded_recursive_access_policy;
use jazz_sim::support::{
    allow_all, cells, compile, insert, int, open, read_gated, row, settle, subject, text, user,
    uuid,
};
use jazz_sim::{
    Latencies, PeerProfile, Sim, SimulatorTransportCodec, bench_profile, emit_summary, env_u64,
};
use serde_json::{Value as JsonValue, json};

const TEAMS: &str = "teams";
const EDGES: &str = "teamEdges";
const RESOURCES: &str = "resources";
const ACCESS: &str = "resourceAccess";
const PAGES: &str = "pages";
const BLOCKS: &str = "blocks";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let profile = bench_profile();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x5300_0001),
        profile: format!("{profile:?}").to_lowercase(),
        groups: profile.select(4, 8, 16),
        users_per_group: profile.select(2, 4, 8),
        resources: profile.select(40, 200, 1_000),
        pages_per_resource: profile.select(1, 2, 3),
        blocks_per_page: profile.select(8, 24, 64),
        visible_blocks: 16,
        revocations: vec![1, 10],
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
    run(
        &Config {
            seed: 0x5300_0001,
            profile: "s3-smoke".to_owned(),
            groups: 3,
            users_per_group: 2,
            resources: 12,
            pages_per_resource: 1,
            blocks_per_page: 6,
            visible_blocks: 4,
            revocations: vec![1, 2],
            link: PeerProfile::new("s3-smoke", 2, 1, 0),
        },
        false,
    );
}

#[derive(Clone)]
struct Config {
    seed: u64,
    profile: String,
    groups: usize,
    users_per_group: usize,
    resources: usize,
    pages_per_resource: usize,
    blocks_per_page: usize,
    visible_blocks: usize,
    revocations: Vec<usize>,
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
    let access = seeded_recursive_access_policy(
        ACCESS,
        "resource",
        "team",
        &[],
        TEAMS,
        EDGES,
        "member",
        "parent",
        &[],
        TEAMS,
        "userTeamOf",
        &["user", "identity", "subject"],
        "id",
    );
    let table = TableSchemaBuilder::new;
    // Children are written only through a readable parent: the select
    // policy also gates inserts and updates.
    let through = |via: &str| {
        TablePolicies::new()
            .with_select(inherits(via))
            .with_insert(inherits(via))
            .with_update(Some(inherits(via)), inherits(via))
            .with_delete(inherits(via))
    };
    compile(
        &SchemaBuilder::new()
            .table(
                table(TEAMS)
                    .column("name", ColumnType::Text)
                    .column("userTeamOf", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                table(EDGES)
                    .fk_column("member", TEAMS)
                    .fk_column("parent", TEAMS)
                    .policies(allow_all()),
            )
            .table(
                table(RESOURCES)
                    .column("title", ColumnType::Text)
                    .policies(read_gated(access.clone()).with_update(Some(access.clone()), access)),
            )
            .table(
                table(ACCESS)
                    .fk_column("resource", RESOURCES)
                    .fk_column("team", TEAMS)
                    .policies(allow_all()),
            )
            .table(
                table(PAGES)
                    .fk_column("resource", RESOURCES)
                    .column("title", ColumnType::Text)
                    .policies(through("resource")),
            )
            .table(
                table(BLOCKS)
                    .fk_column("page", PAGES)
                    .column("position", ColumnType::BigInt)
                    .column("body", ColumnType::Text)
                    .composite_index(["page", "position"])
                    .policies(through("page")),
            )
            .build(),
    )
}

fn org_team() -> RowUuid {
    row(0x30, 0)
}

fn group_team(g: usize) -> RowUuid {
    row(0x31, g as u64)
}

fn user_team(u: usize) -> RowUuid {
    row(0x32, u as u64)
}

/// The oracle.
#[derive(Clone, Default)]
struct Model {
    /// Team -> parents.
    parents: BTreeMap<RowUuid, BTreeSet<RowUuid>>,
    /// Access row -> (resource, team).
    access: BTreeMap<RowUuid, (RowUuid, RowUuid)>,
    resources: BTreeSet<RowUuid>,
    /// Page -> resource.
    pages: BTreeMap<RowUuid, RowUuid>,
    /// Block -> (page, position, body).
    blocks: BTreeMap<RowUuid, (RowUuid, i64, String)>,
}

impl Model {
    fn reachable(&self, from: RowUuid) -> BTreeSet<RowUuid> {
        let mut seen = BTreeSet::from([from]);
        let mut frontier = vec![from];
        while let Some(team) = frontier.pop() {
            for parent in self.parents.get(&team).into_iter().flatten() {
                if seen.insert(*parent) {
                    frontier.push(*parent);
                }
            }
        }
        seen
    }

    fn resources_of(&self, u: usize) -> BTreeSet<RowUuid> {
        let teams = self.reachable(user_team(u));
        self.access
            .values()
            .filter(|(_, team)| teams.contains(team))
            .map(|(resource, _)| *resource)
            .collect()
    }

    fn readable(&self, u: usize, table: &str, id: RowUuid) -> bool {
        let resources = self.resources_of(u);
        match table {
            TEAMS | EDGES | ACCESS => true,
            RESOURCES => resources.contains(&id),
            PAGES => self.pages.get(&id).is_some_and(|r| resources.contains(r)),
            BLOCKS => self
                .blocks
                .get(&id)
                .is_some_and(|(page, _, _)| self.readable(u, PAGES, *page)),
            other => panic!("unknown table {other}"),
        }
    }

    fn resource_rows(&self, u: usize) -> Rows {
        self.resources_of(u)
            .into_iter()
            .map(|id| (id, resource_cells(id)))
            .collect()
    }

    /// The first `limit` blocks of `page` by position, if readable.
    fn page_blocks(&self, u: usize, page: RowUuid, limit: usize) -> Rows {
        if !self.readable(u, PAGES, page) {
            return Rows::new();
        }
        let mut blocks: Vec<_> = self
            .blocks
            .iter()
            .filter(|(_, (p, _, _))| *p == page)
            .collect();
        blocks.sort_by_key(|(id, (_, position, _))| (*position, **id));
        blocks
            .into_iter()
            .take(limit)
            .map(|(id, (page, position, body))| (*id, block_cells(*page, *position, body)))
            .collect()
    }
}

fn resource_cells(id: RowUuid) -> BTreeMap<String, Value> {
    cells([("title", text(format!("resource-{}", id.0.as_u128() as u16)))])
}

fn block_cells(page: RowUuid, position: i64, body: &str) -> BTreeMap<String, Value> {
    cells([
        ("page", uuid(page)),
        ("position", int(position)),
        ("body", text(body)),
    ])
}

fn blocks_query(page: RowUuid, limit: usize) -> Query {
    Query::from(BLOCKS)
        .filter(eq(col("page"), lit(uuid(page))))
        .order_by("position", OrderDirection::Asc)
        .limit(limit)
}

struct Persona {
    name: String,
    u: usize,
    page: RowUuid,
    resources: Live,
    blocks: Live,
}

impl Persona {
    fn assert_matches(&mut self, model: &Model, config: &Config, phase: &str) {
        self.resources.poll();
        self.blocks.poll();
        assert_eq!(
            self.resources.rows,
            model.resource_rows(self.u),
            "{phase}: {} resources",
            self.name
        );
        assert_eq!(
            self.blocks.rows,
            model.page_blocks(self.u, self.page, config.visible_blocks),
            "{phase}: {} blocks",
            self.name
        );
    }
}

/// Row images shipped to `persona` since message `from` of its log, and how
/// many of them it may not read. The caller checks after every phase, so
/// each image is judged against the permissions of the phase that sent it.
fn forbidden_images(sim: &Sim, model: &Model, persona: &Persona, from: usize) -> (usize, usize) {
    let mut shipped = 0;
    let mut forbidden = 0;
    for message in sim.downstream_log(&persona.name).into_iter().skip(from) {
        if let jazz::db::SyncMessage::Rows { images, .. } = message {
            for image in images.iter().filter(|image| !image.deleted) {
                shipped += 1;
                if !model.readable(persona.u, &image.table, RowUuid::from_bytes(image.id)) {
                    forbidden += 1;
                    eprintln!("forbidden: {} {}/{:?}", persona.name, image.table, image.id);
                }
            }
        }
    }
    (shipped, forbidden)
}

fn run(config: &Config, emit: bool) {
    let mut model = Model::default();
    let mut sim = Sim::new(config.seed, SimulatorTransportCodec::WireFrames);
    let core = sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    let users = config.groups * config.users_per_group;
    // Teams: org <- groups <- user teams.
    insert(
        core,
        TEAMS,
        org_team(),
        cells([("name", text("org")), ("userTeamOf", text(""))]),
    );
    let mut edge = 0_u64;
    let mut add_edge =
        |core: &jazz::db::Db, model: &mut Model, member: RowUuid, parent: RowUuid| {
            insert(
                core,
                EDGES,
                row(0x33, edge),
                cells([("member", uuid(member)), ("parent", uuid(parent))]),
            );
            model.parents.entry(member).or_default().insert(parent);
            edge += 1;
        };
    for g in 0..config.groups {
        insert(
            core,
            TEAMS,
            group_team(g),
            cells([
                ("name", text(format!("group-{g}"))),
                ("userTeamOf", text("")),
            ]),
        );
        add_edge(core, &mut model, group_team(g), org_team());
    }
    for u in 0..users {
        insert(
            core,
            TEAMS,
            user_team(u),
            cells([
                ("name", text(format!("user-{u}"))),
                ("userTeamOf", subject(user(u as u32))),
            ]),
        );
        add_edge(
            core,
            &mut model,
            user_team(u),
            group_team(u / config.users_per_group),
        );
    }
    // Resources: every 8th is org-wide, the rest belong to one group, and
    // some are also shared with one user directly.
    let mut access = 0_u64;
    for r in 0..config.resources {
        let id = row(0x34, r as u64);
        insert(core, RESOURCES, id, resource_cells(id));
        model.resources.insert(id);
        let team = if r % 8 == 0 {
            org_team()
        } else {
            group_team(r % config.groups)
        };
        let mut grant = |team: RowUuid| {
            let link = row(0x35, access);
            insert(
                core,
                ACCESS,
                link,
                cells([("resource", uuid(id)), ("team", uuid(team))]),
            );
            model.access.insert(link, (id, team));
            access += 1;
        };
        grant(team);
        if r % 5 == 1 {
            grant(user_team(r % users));
        }
        for p in 0..config.pages_per_resource {
            let page = row(0x36, (r * config.pages_per_resource + p) as u64);
            insert(
                core,
                PAGES,
                page,
                cells([("resource", uuid(id)), ("title", text("page"))]),
            );
            model.pages.insert(page, id);
            for b in 0..config.blocks_per_page {
                let block = row(0x37, (page.0.as_u128() as u64 & 0xffff) << 16 | b as u64);
                let body = format!("block-{b}");
                // Positions descend so the first page of blocks is not
                // insertion order.
                let position = (config.blocks_per_page - b) as i64 * 10;
                insert(core, BLOCKS, block, block_cells(page, position, &body));
                model.blocks.insert(block, (page, position, body));
            }
        }
    }

    // Personas: one user per group, plus the first group's second user.
    let mut personas: Vec<Persona> = (0..config.groups)
        .map(|g| g * config.users_per_group)
        .chain([1])
        .map(|u| {
            let name = format!("user-{u}");
            let who = user(u as u32);
            sim.add(name.clone(), open(schema(), 10 + u as u32, who));
            sim.connect(&name, "core", who, config.link.clone());
            sim.record_downstream(&name);
            let page = model
                .resources_of(u)
                .into_iter()
                .next()
                .and_then(|r| {
                    model
                        .pages
                        .iter()
                        .find(|(_, res)| **res == r)
                        .map(|(p, _)| *p)
                })
                .expect("every persona reads a resource");
            let db = sim.db(&name);
            Persona {
                resources: Live::subscribe(db, &Query::from(RESOURCES), ReadOpts::default()),
                blocks: Live::subscribe(
                    db,
                    &blocks_query(page, config.visible_blocks),
                    ReadOpts::default(),
                ),
                name,
                u,
                page,
            }
        })
        .collect();
    let cold = Instant::now();
    sim.settle();
    let cold_us = cold.elapsed().as_micros() as u64;
    let mut forbidden_deliveries = 0;
    let mut shipped = 0;
    let mut log_marks = vec![0; personas.len()];
    // Judge what each persona was shipped since the last check.
    let mut check_traffic = |sim: &Sim, model: &Model, personas: &[Persona]| {
        for (persona, mark) in personas.iter().zip(log_marks.iter_mut()) {
            let (s, f) = forbidden_images(sim, model, persona, *mark);
            shipped += s;
            forbidden_deliveries += f;
            *mark = sim.downstream_log(&persona.name).len();
        }
    };
    for persona in &mut personas {
        persona.assert_matches(&model, config, "cold load");
        assert!(
            persona.resources.rows.len() < config.resources,
            "{} is denied resources",
            persona.name
        );
    }
    check_traffic(&sim, &model, &personas);

    // Grant: share a resource the first persona cannot read with its team.
    let mut grant_latency = Latencies::default();
    let target = personas[0].u;
    let hidden: Vec<RowUuid> = model
        .resources
        .iter()
        .filter(|r| !model.resources_of(target).contains(r))
        .copied()
        .take(2)
        .collect();
    assert!(
        !hidden.is_empty(),
        "the first persona is denied some resource"
    );
    let admin = "admin";
    sim.add(admin, open(schema(), 999, user(999)));
    sim.connect(admin, "core", user(999), config.link.clone());
    for resource in &hidden {
        let link = row(0x38, access);
        access += 1;
        let at = sim.now_ms();
        let handle = insert(
            sim.db(admin),
            ACCESS,
            link,
            cells([
                ("resource", uuid(*resource)),
                ("team", uuid(user_team(target))),
            ]),
        );
        model.access.insert(link, (*resource, user_team(target)));
        settle(&sim, sim.db(admin), &handle);
        loop {
            personas[0].resources.poll();
            if personas[0].resources.ids().contains(resource) {
                break;
            }
            // The last tick can deliver it with nothing left in flight.
            if !sim.step() {
                personas[0].resources.poll();
                assert!(
                    personas[0].resources.ids().contains(resource),
                    "the grant must reach the persona"
                );
            }
        }
        grant_latency.record(sim.now_ms() - at);
    }
    sim.settle();
    // The granted resource's page, seen through a fresh subscription.
    let granted_page = *model
        .pages
        .iter()
        .find(|(_, r)| **r == hidden[0])
        .expect("page")
        .0;
    let mut granted_blocks = Live::subscribe(
        sim.db(&personas[0].name),
        &blocks_query(granted_page, config.visible_blocks),
        ReadOpts::default(),
    );
    sim.settle();
    granted_blocks.poll();
    assert_eq!(
        granted_blocks.rows,
        model.page_blocks(target, granted_page, config.visible_blocks),
        "the granted resource's blocks arrive"
    );
    for persona in &mut personas {
        persona.assert_matches(&model, config, "after grants");
    }
    check_traffic(&sim, &model, &personas);

    // Revocations: drop `size` access rows that grant the first persona a
    // resource; the resources leave its synced subscription.
    let mut revocations = Vec::new();
    for size in config.revocations.clone() {
        let teams = model.reachable(user_team(target));
        let doomed: Vec<RowUuid> = model
            .access
            .iter()
            .filter(|(_, (_, team))| teams.contains(team) && *team != org_team())
            .map(|(link, _)| *link)
            .take(size)
            .collect();
        let before = model.resources_of(target);
        let at = sim.now_ms();
        for link in &doomed {
            let handle = block_on(sim.db(admin).delete(ACCESS, *link, Default::default()))
                .unwrap_or_else(|error| panic!("revoke: {error}"));
            model.access.remove(link);
            settle(&sim, sim.db(admin), &handle);
        }
        sim.settle();
        check_traffic(&sim, &model, &personas);
        let lost = before.difference(&model.resources_of(target)).count();
        assert!(lost > 0, "revoking {size} access rows takes resources away");
        for persona in &mut personas {
            persona.assert_matches(&model, config, &format!("after revoking {size}"));
        }
        revocations.push(json!({
            "size": size,
            "resources_lost": lost,
            "virtual_ms": sim.now_ms() - at,
        }));
    }

    // Forbidden write: a block written into a page the writer cannot read
    // is rejected, and no one ever sees it.
    let outsider = personas[0].name.clone();
    let forbidden_page = *model
        .pages
        .iter()
        .find(|(_, r)| !model.resources_of(target).contains(r))
        .expect("a page the persona cannot read")
        .0;
    let sneaky = row(0x39, 0);
    let handle = insert(
        sim.db(&outsider),
        BLOCKS,
        sneaky,
        block_cells(forbidden_page, 0, "sneaky"),
    );
    let fate = sim.drive(
        sim.db(&outsider)
            .wait_for_transaction(handle.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
    );
    let error = fate.expect_err("the authority rejects a write through an unreadable page");
    assert_eq!(error.code, ErrorCode::WriteRejected, "{error}");
    sim.settle();
    let core_blocks =
        jazz_sim::oracle::ids(sim.db("core"), &Query::from(BLOCKS), ReadOpts::default());
    assert!(
        !core_blocks.contains(&sneaky),
        "the rejected block never landed"
    );

    for persona in &mut personas {
        persona.assert_matches(&model, config, "after the forbidden write");
    }
    check_traffic(&sim, &model, &personas);
    assert_eq!(
        forbidden_deliveries, 0,
        "no client was shipped a row it may not read"
    );

    if emit {
        let mut fields = sim.json_fields();
        fields.extend(
            json!({
                "phase": "summary",
                "resources": config.resources,
                "blocks": model.blocks.len(),
                "personas": personas.len(),
                "cold_us": cold_us,
                "shipped_row_images": shipped,
                "forbidden_deliveries": forbidden_deliveries,
                "revocation": revocations,
            })
            .as_object()
            .expect("object")
            .clone(),
        );
        fields.extend(grant_latency.json_fields("grant_virtual_ms"));
        emit_summary(
            "s3_permissions",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
}
