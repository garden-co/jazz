//! Deterministic fixture and seeding support for the SaaS permission fan-out benchmark.
//!
//! The fixture keeps the benchmark runner small: configuration, tenant/document
//! distribution, access-path identities, direct ACL grants, and expected initial
//! pages all come from one model. Seeding uses only the public [`jazz::db::Db`]
//! transaction API.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use jazz::db::{Db, RowCells};
use jazz::groove::records::Value;
use jazz::groove::storage::{OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorId, RowUuid};
use jazz::schema::JazzSchema;
use serde::Serialize;

use super::saas_permission_support as support;

pub const TOP_PAGE_SIZE: usize = 100;
pub const REQUESTED_MIN_DOCUMENTS_PER_TEAM: usize = 100;
pub const REQUESTED_MAX_DOCUMENTS_PER_TEAM: usize = 30_000;
pub const MAX_LOCAL_SEED_BATCH: usize = 2_048;

const USER_NAMESPACE_WIDTH: u64 = 1 << 60;
const ORGANIZATION_USER_BASE: u64 = USER_NAMESPACE_WIDTH;
const DIRECT_ACL_USER_BASE: u64 = USER_NAMESPACE_WIDTH * 2;
const PUBLIC_USER_BASE: u64 = USER_NAMESPACE_WIDTH * 3;
const ADMIN_USER_BASE: u64 = USER_NAMESPACE_WIDTH * 4;
const BACKGROUND_ACL_USER_BASE: u64 = USER_NAMESPACE_WIDTH * 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Baseline,
    RealWorld,
}

impl Profile {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "real_world" => Ok(Self::RealWorld),
            other => Err(format!(
                "JAZZ_SAAS_PROFILE must be baseline or real_world, got {other:?}"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    DistinctTeams,
    HotTeam,
}

impl Topology {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "distinct_teams" => Ok(Self::DistinctTeams),
            "hot_team" => Ok(Self::HotTeam),
            other => Err(format!(
                "JAZZ_SAAS_TOPOLOGY must be distinct_teams or hot_team, got {other:?}"
            )),
        }
    }
}

/// Environment-driven benchmark scale.
///
/// `baseline` stays near the original 500k-row scenario with one binding.
/// `real_world` is the heavier opt-in profile: 2m documents, 15k teams, 900k
/// memberships, 100k ACL rows, and the three authorization branches whose
/// Top-100 correctness canary currently passes.
#[derive(Clone, Debug, Serialize)]
pub struct Config {
    pub profile: Profile,
    pub topology: Topology,
    pub documents: usize,
    pub organizations: usize,
    pub teams: usize,
    pub hot_team_documents: usize,
    pub team_memberships_per_team: usize,
    pub organization_memberships_per_organization: usize,
    pub direct_acl_documents_per_subscriber: usize,
    pub direct_acl_rows: usize,
    pub permission_branches: usize,
    pub active_subscriptions: usize,
    pub drop_subscriptions: usize,
    pub matching_writes: usize,
    pub unrelated_writes: usize,
    pub batched_write_rows: usize,
    pub local_seed_batch: usize,
}

impl Config {
    pub fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Baseline => Self {
                profile,
                topology: Topology::DistinctTeams,
                documents: 529_900,
                organizations: 5_000,
                teams: 5_000,
                hot_team_documents: 30_000,
                team_memberships_per_team: 10,
                organization_memberships_per_organization: 0,
                direct_acl_documents_per_subscriber: 0,
                direct_acl_rows: 0,
                permission_branches: 1,
                active_subscriptions: 1,
                drop_subscriptions: 0,
                matching_writes: 5,
                unrelated_writes: 5,
                batched_write_rows: 100,
                local_seed_batch: MAX_LOCAL_SEED_BATCH,
            },
            Profile::RealWorld => Self {
                profile,
                topology: Topology::DistinctTeams,
                documents: 2_000_000,
                organizations: 5_000,
                teams: 15_000,
                hot_team_documents: 30_000,
                team_memberships_per_team: 40,
                organization_memberships_per_organization: 60,
                direct_acl_documents_per_subscriber: TOP_PAGE_SIZE,
                direct_acl_rows: 100_000,
                permission_branches: 3,
                active_subscriptions: 3,
                drop_subscriptions: 0,
                matching_writes: 3,
                unrelated_writes: 3,
                batched_write_rows: 100,
                local_seed_batch: MAX_LOCAL_SEED_BATCH,
            },
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let profile = Profile::parse(
            &std::env::var("JAZZ_SAAS_PROFILE").unwrap_or_else(|_| "baseline".to_owned()),
        )?;
        let topology = Topology::parse(
            &std::env::var("JAZZ_SAAS_TOPOLOGY").unwrap_or_else(|_| "distinct_teams".to_owned()),
        )?;
        let mut config = Self::for_profile(profile);
        config.topology = topology;
        config.documents = env_usize("JAZZ_SAAS_DOCUMENTS", config.documents)?;
        config.organizations = env_usize("JAZZ_SAAS_ORGANIZATIONS", config.organizations)?;
        config.teams = env_usize("JAZZ_SAAS_TEAMS", config.teams)?;
        config.hot_team_documents =
            env_usize("JAZZ_SAAS_HOT_DOCUMENTS", config.hot_team_documents)?;
        config.team_memberships_per_team = env_usize(
            "JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM",
            config.team_memberships_per_team,
        )?;
        config.organization_memberships_per_organization = env_usize(
            "JAZZ_SAAS_ORG_MEMBERS_PER_ORG",
            config.organization_memberships_per_organization,
        )?;
        config.direct_acl_documents_per_subscriber = env_usize(
            "JAZZ_SAAS_DIRECT_ACL_DOCS_PER_SUBSCRIBER",
            config.direct_acl_documents_per_subscriber,
        )?;
        config.direct_acl_rows = env_usize("JAZZ_SAAS_DIRECT_ACL_ROWS", config.direct_acl_rows)?;
        config.permission_branches =
            env_usize("JAZZ_SAAS_PERMISSION_BRANCHES", config.permission_branches)?;
        config.active_subscriptions = env_usize(
            "JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS",
            config.active_subscriptions,
        )?;
        config.drop_subscriptions =
            env_usize("JAZZ_SAAS_DROP_SUBSCRIPTIONS", config.drop_subscriptions)?;
        config.matching_writes = env_usize("JAZZ_SAAS_MATCHING_WRITES", config.matching_writes)?;
        config.unrelated_writes = env_usize("JAZZ_SAAS_UNRELATED_WRITES", config.unrelated_writes)?;
        config.batched_write_rows =
            env_usize("JAZZ_SAAS_BATCHED_WRITE_ROWS", config.batched_write_rows)?;
        config.local_seed_batch = env_usize("JAZZ_SAAS_SEED_BATCH", config.local_seed_batch)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        require_nonzero("JAZZ_SAAS_DOCUMENTS", self.documents)?;
        require_nonzero("JAZZ_SAAS_ORGANIZATIONS", self.organizations)?;
        require_nonzero("JAZZ_SAAS_TEAMS", self.teams)?;
        require_nonzero("JAZZ_SAAS_HOT_DOCUMENTS", self.hot_team_documents)?;
        require_nonzero(
            "JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM",
            self.team_memberships_per_team,
        )?;
        require_nonzero("JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS", self.active_subscriptions)?;
        require_nonzero("JAZZ_SAAS_SEED_BATCH", self.local_seed_batch)?;
        if self.profile == Profile::RealWorld {
            require_nonzero(
                "JAZZ_SAAS_ORG_MEMBERS_PER_ORG",
                self.organization_memberships_per_organization,
            )?;
            require_nonzero(
                "JAZZ_SAAS_DIRECT_ACL_DOCS_PER_SUBSCRIBER",
                self.direct_acl_documents_per_subscriber,
            )?;
            require_nonzero("JAZZ_SAAS_DIRECT_ACL_ROWS", self.direct_acl_rows)?;
        }
        if !(1..=5).contains(&self.permission_branches) {
            return Err("JAZZ_SAAS_PERMISSION_BRANCHES must be between 1 and 5".to_owned());
        }
        if self.local_seed_batch > MAX_LOCAL_SEED_BATCH {
            return Err(format!(
                "JAZZ_SAAS_SEED_BATCH must not exceed {MAX_LOCAL_SEED_BATCH}"
            ));
        }
        if self.topology == Topology::DistinctTeams && self.active_subscriptions > self.teams {
            return Err(format!(
                "distinct_teams needs at least one team per active subscription ({} subscriptions, {} teams)",
                self.active_subscriptions, self.teams
            ));
        }
        if self.unrelated_writes > 0
            && ((self.topology == Topology::DistinctTeams
                && self.active_subscriptions == self.teams)
                || (self.topology == Topology::HotTeam && self.teams == 1))
        {
            return Err(
                "JAZZ_SAAS_UNRELATED_WRITES requires at least one team without an active subscription"
                    .to_owned(),
            );
        }
        if self.drop_subscriptions >= self.active_subscriptions && self.drop_subscriptions != 0 {
            return Err(format!(
                "JAZZ_SAAS_DROP_SUBSCRIPTIONS must leave at least one live subscription ({} requested drops, {} active subscriptions)",
                self.drop_subscriptions, self.active_subscriptions
            ));
        }
        if self.drop_subscriptions > 0 && self.topology != Topology::DistinctTeams {
            return Err(
                "JAZZ_SAAS_DROP_SUBSCRIPTIONS currently requires distinct_teams so a dropped route is unbound from every survivor"
                    .to_owned(),
            );
        }
        if self.drop_subscriptions > 0 && self.organizations <= self.active_subscriptions {
            return Err(
                "subscription churn needs more organizations than active subscriptions so its organization update is unrelated to every route"
                    .to_owned(),
            );
        }
        if self.topology == Topology::HotTeam
            && self.profile == Profile::Baseline
            && self.active_subscriptions > self.team_memberships_per_team
        {
            return Err(format!(
                "hot_team baseline needs one seeded member per viewer ({} subscriptions, {} members/team)",
                self.active_subscriptions, self.team_memberships_per_team
            ));
        }

        let team_memberships = checked_product(
            self.teams,
            self.team_memberships_per_team,
            "teams * team_memberships_per_team",
        )?;
        let organization_memberships = checked_product(
            self.organizations,
            self.organization_memberships_per_organization,
            "organizations * organization_memberships_per_organization",
        )?;
        ensure_user_namespace(team_memberships, "team membership users")?;
        ensure_user_namespace(organization_memberships, "organization membership users")?;
        ensure_user_namespace(self.active_subscriptions, "subscriber identities")?;
        checked_product(
            self.active_subscriptions,
            self.direct_acl_documents_per_subscriber,
            "active_subscriptions * direct_acl_documents_per_subscriber",
        )?;
        if self.profile == Profile::RealWorld && self.permission_branches >= 3 {
            let direct_acl_subscriptions = (0..self.active_subscriptions)
                .filter(|index| {
                    AccessPath::REAL_WORLD_CYCLE[index % self.permission_branches]
                        == AccessPath::DirectAcl
                })
                .count();
            let required_acl_rows = checked_product(
                direct_acl_subscriptions,
                self.direct_acl_documents_per_subscriber,
                "direct ACL subscriptions * direct ACL documents per subscriber",
            )?;
            if required_acl_rows > self.direct_acl_rows {
                return Err(format!(
                    "JAZZ_SAAS_DIRECT_ACL_ROWS={} is smaller than the maximum {} subscriber grants",
                    self.direct_acl_rows, required_acl_rows
                ));
            }
        }
        ensure_user_namespace(self.direct_acl_rows, "background ACL users")?;
        self.hot_team_documents
            .checked_add(
                self.teams
                    .saturating_sub(1)
                    .checked_mul(REQUESTED_MIN_DOCUMENTS_PER_TEAM)
                    .ok_or_else(|| "requested document distribution overflows usize".to_owned())?,
            )
            .ok_or_else(|| "requested document distribution overflows usize".to_owned())?;

        for (label, value) in [
            ("documents", self.documents),
            ("organizations", self.organizations),
            ("teams", self.teams),
            ("team memberships", team_memberships),
            ("organization memberships", organization_memberships),
        ] {
            u64::try_from(value).map_err(|_| format!("{label} does not fit in u64 fixture ids"))?;
        }
        Ok(())
    }

    pub fn schema(&self) -> JazzSchema {
        support::permission_schema_with_branches(self.permission_branches)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::for_profile(Profile::Baseline)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DocumentDistribution {
    #[serde(skip_serializing)]
    team_document_counts: Vec<usize>,
    #[serde(skip_serializing)]
    team_document_starts: Vec<usize>,
    pub total_documents: usize,
    pub total_teams: usize,
    pub hot_team_index: usize,
    pub hot_team_documents: usize,
    pub requested_min_documents_per_team: usize,
    pub requested_max_documents_per_team: usize,
    pub actual_min_documents_per_team: usize,
    pub actual_max_documents_per_team: usize,
    pub teams_below_requested_min: usize,
    pub teams_above_requested_max: usize,
    pub minimum_total_for_requested_hot_and_bounds: usize,
    pub arithmetic_shortfall: usize,
    pub exact_total_preserved: bool,
}

impl DocumentDistribution {
    pub fn build(config: &Config) -> Result<Self, String> {
        config.validate()?;
        let mut counts = vec![0; config.teams];
        let hot = config.hot_team_documents.min(config.documents);
        counts[0] = hot;
        if config.teams > 1 {
            let remaining = config.documents - hot;
            let other_teams = config.teams - 1;
            let per_team = remaining / other_teams;
            let remainder = remaining % other_teams;
            for (offset, count) in counts[1..].iter_mut().enumerate() {
                *count = per_team + usize::from(offset < remainder);
            }
        }

        let mut starts = Vec::with_capacity(config.teams);
        let mut next_start = 0usize;
        for &count in &counts {
            starts.push(next_start);
            next_start = next_start
                .checked_add(count)
                .ok_or_else(|| "document distribution prefix sum overflows usize".to_owned())?;
        }
        let requested_total = config
            .hot_team_documents
            .checked_add(
                config
                    .teams
                    .saturating_sub(1)
                    .checked_mul(REQUESTED_MIN_DOCUMENTS_PER_TEAM)
                    .ok_or_else(|| "requested document distribution overflows usize".to_owned())?,
            )
            .ok_or_else(|| "requested document distribution overflows usize".to_owned())?;
        let actual_min = counts.iter().copied().min().unwrap_or(0);
        let actual_max = counts.iter().copied().max().unwrap_or(0);
        Ok(Self {
            total_documents: config.documents,
            total_teams: config.teams,
            hot_team_index: 0,
            hot_team_documents: counts[0],
            requested_min_documents_per_team: REQUESTED_MIN_DOCUMENTS_PER_TEAM,
            requested_max_documents_per_team: REQUESTED_MAX_DOCUMENTS_PER_TEAM,
            actual_min_documents_per_team: actual_min,
            actual_max_documents_per_team: actual_max,
            teams_below_requested_min: counts
                .iter()
                .filter(|&&count| count < REQUESTED_MIN_DOCUMENTS_PER_TEAM)
                .count(),
            teams_above_requested_max: counts
                .iter()
                .filter(|&&count| count > REQUESTED_MAX_DOCUMENTS_PER_TEAM)
                .count(),
            minimum_total_for_requested_hot_and_bounds: requested_total,
            arithmetic_shortfall: requested_total.saturating_sub(config.documents),
            exact_total_preserved: next_start == config.documents,
            team_document_counts: counts,
            team_document_starts: starts,
        })
    }

    pub fn count(&self, team_index: usize) -> usize {
        self.team_document_counts[team_index]
    }

    pub fn start(&self, team_index: usize) -> usize {
        self.team_document_starts[team_index]
    }

    pub fn counts(&self) -> &[usize] {
        &self.team_document_counts
    }

    pub fn starts(&self) -> &[usize] {
        &self.team_document_starts
    }

    pub fn team_for_document(&self, document_index: usize) -> Option<usize> {
        if document_index >= self.total_documents {
            return None;
        }
        let insertion = self
            .team_document_starts
            .partition_point(|&start| start <= document_index);
        Some(insertion.saturating_sub(1))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentStatus {
    Draft,
    Closed,
    Active,
}

impl DocumentStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Closed => "closed",
            Self::Active => "active",
        }
    }

    pub fn appears_in_list(self) -> bool {
        matches!(self, Self::Draft | Self::Active)
    }
}

/// Compact model of a row generated by [`support::document_cells`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DocumentSpec {
    pub index: u64,
    pub row: RowUuid,
    pub organization_index: usize,
    pub organization: RowUuid,
    pub team_index: usize,
    pub team: RowUuid,
    pub owner: AuthorId,
    pub updated_at: u64,
    pub status: DocumentStatus,
    pub archived: bool,
    pub public: bool,
    pub published: bool,
}

impl DocumentSpec {
    pub fn appears_in_list(self) -> bool {
        !self.archived && self.status.appears_in_list()
    }

    pub fn cells(self) -> RowCells {
        support::document_cells(
            self.index,
            self.organization,
            self.team,
            self.owner,
            self.updated_at,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessPath {
    TeamMember,
    OrganizationAdmin,
    DirectAcl,
    Public,
    AdminClaim,
}

impl AccessPath {
    const REAL_WORLD_CYCLE: [Self; 5] = [
        Self::TeamMember,
        Self::OrganizationAdmin,
        Self::DirectAcl,
        Self::Public,
        Self::AdminClaim,
    ];
}

#[derive(Clone, Debug, Serialize)]
pub struct SubscriberPlan {
    pub index: usize,
    pub access_path: AccessPath,
    pub identity: AuthorId,
    pub organization_index: usize,
    pub organization: RowUuid,
    pub team_index: usize,
    pub team: RowUuid,
    pub claims: BTreeMap<String, Value>,
    #[serde(skip_serializing)]
    pub expected_page: Vec<RowUuid>,
}

impl SubscriberPlan {
    pub fn expected_page(&self) -> &[RowUuid] {
        &self.expected_page
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DirectAclGrant {
    pub row: RowUuid,
    pub document: RowUuid,
    pub user: AuthorId,
    pub permission: &'static str,
    pub active: bool,
}

/// Complete deterministic benchmark model.
#[derive(Debug)]
pub struct Fixture {
    config: Config,
    distribution: DocumentDistribution,
    subscribers: Vec<SubscriberPlan>,
    direct_acl_grants: Vec<DirectAclGrant>,
    direct_documents_by_user: BTreeMap<AuthorId, BTreeSet<RowUuid>>,
}

impl Fixture {
    pub fn build(config: Config) -> Result<Self, String> {
        config.validate()?;
        let distribution = DocumentDistribution::build(&config)?;
        let subscribers = build_subscriber_plans(&config)?;
        let mut fixture = Self {
            config,
            distribution,
            subscribers,
            direct_acl_grants: Vec::new(),
            direct_documents_by_user: BTreeMap::new(),
        };
        fixture.build_direct_acl_grants()?;
        let expected_pages = fixture
            .subscribers
            .iter()
            .map(|subscriber| fixture.derive_expected_page(subscriber))
            .collect::<Vec<_>>();
        for (subscriber, expected_page) in fixture.subscribers.iter_mut().zip(expected_pages) {
            subscriber.expected_page = expected_page;
        }
        Ok(fixture)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn distribution(&self) -> &DocumentDistribution {
        &self.distribution
    }

    pub fn subscribers(&self) -> &[SubscriberPlan] {
        &self.subscribers
    }

    pub fn direct_acl_grants(&self) -> &[DirectAclGrant] {
        &self.direct_acl_grants
    }

    pub fn schema(&self) -> JazzSchema {
        self.config.schema()
    }

    pub fn organization_for_team(&self, team_index: usize) -> usize {
        team_index % self.config.organizations
    }

    pub fn team_member_identity(&self, team_index: usize, slot: usize) -> AuthorId {
        debug_assert!(team_index < self.config.teams);
        debug_assert!(slot < self.config.team_memberships_per_team);
        support::user_identity((team_index * self.config.team_memberships_per_team + slot) as u64)
    }

    pub fn organization_member_identity(&self, organization_index: usize, slot: usize) -> AuthorId {
        debug_assert!(organization_index < self.config.organizations);
        debug_assert!(slot < self.config.organization_memberships_per_organization);
        support::user_identity(
            ORGANIZATION_USER_BASE
                + (organization_index * self.config.organization_memberships_per_organization
                    + slot) as u64,
        )
    }

    pub fn document(&self, team_index: usize, local_index: usize) -> Option<DocumentSpec> {
        if team_index >= self.config.teams || local_index >= self.distribution.count(team_index) {
            return None;
        }
        let index = self.distribution.start(team_index) + local_index;
        Some(self.document_with_team(index as u64, team_index))
    }

    pub fn seeded_document(&self, index: usize) -> Option<DocumentSpec> {
        let team_index = self.distribution.team_for_document(index)?;
        Some(self.document_with_team(index as u64, team_index))
    }

    /// Build cells for a post-seed document assigned to an explicit team.
    pub fn synthetic_document(&self, index: u64, team_index: usize) -> DocumentSpec {
        assert!(team_index < self.config.teams, "team index out of range");
        self.document_with_team(index, team_index)
    }

    pub fn next_document_index(&self) -> u64 {
        self.config.documents as u64
    }

    pub fn expected_page(&self, subscriber_index: usize) -> Option<&[RowUuid]> {
        self.subscribers
            .get(subscriber_index)
            .map(SubscriberPlan::expected_page)
    }

    pub fn derive_expected_page(&self, subscriber: &SubscriberPlan) -> Vec<RowUuid> {
        let direct_documents = self.direct_documents_by_user.get(&subscriber.identity);
        let start = self.distribution.start(subscriber.team_index);
        let count = self.distribution.count(subscriber.team_index);
        (start..start + count)
            .rev()
            .map(|index| self.document_with_team(index as u64, subscriber.team_index))
            .filter(|document| document.appears_in_list())
            .filter(|document| self.document_visible_to(subscriber, *document, direct_documents))
            .take(TOP_PAGE_SIZE)
            .map(|document| document.row)
            .collect()
    }

    pub fn seed_local<S>(&self, db: &Db<S>) -> Result<SeedReport, String>
    where
        S: OrderedKvStorage + ReopenableStorage + 'static,
    {
        let total_started = Instant::now();
        let organizations = seed_rows(
            db,
            support::ORGANIZATIONS,
            self.config.organizations,
            self.config.local_seed_batch,
            |index| {
                (
                    support::organization_row(index as u64),
                    support::organization_cells(index as u64, false),
                )
            },
        )?;
        let teams = seed_rows(
            db,
            support::TEAMS,
            self.config.teams,
            self.config.local_seed_batch,
            |index| {
                let organization_index = self.organization_for_team(index);
                (
                    support::team_row(index as u64),
                    support::team_cells(
                        index as u64,
                        support::organization_row(organization_index as u64),
                        false,
                    ),
                )
            },
        )?;
        let team_membership_rows = checked_product(
            self.config.teams,
            self.config.team_memberships_per_team,
            "team membership seed rows",
        )?;
        let team_memberships = seed_rows(
            db,
            support::TEAM_MEMBERSHIPS,
            team_membership_rows,
            self.config.local_seed_batch,
            |index| {
                let team_index = index / self.config.team_memberships_per_team;
                let slot = index % self.config.team_memberships_per_team;
                let role = match slot {
                    0 => "admin",
                    1 => "editor",
                    _ => "viewer",
                };
                (
                    support::team_membership_row(index as u64),
                    support::team_membership_cells(
                        support::team_row(team_index as u64),
                        self.team_member_identity(team_index, slot),
                        role,
                        true,
                    ),
                )
            },
        )?;
        let organization_membership_rows = checked_product(
            self.config.organizations,
            self.config.organization_memberships_per_organization,
            "organization membership seed rows",
        )?;
        let organization_memberships = seed_rows(
            db,
            support::ORGANIZATION_MEMBERSHIPS,
            organization_membership_rows,
            self.config.local_seed_batch,
            |index| {
                let organization_index =
                    index / self.config.organization_memberships_per_organization;
                let slot = index % self.config.organization_memberships_per_organization;
                let role = if slot == 0 { "owner" } else { "admin" };
                (
                    support::organization_membership_row(index as u64),
                    support::organization_membership_cells(
                        support::organization_row(organization_index as u64),
                        self.organization_member_identity(organization_index, slot),
                        role,
                        true,
                        true,
                    ),
                )
            },
        )?;
        let documents = seed_rows(
            db,
            support::DOCUMENTS,
            self.config.documents,
            self.config.local_seed_batch,
            |index| {
                let document = self
                    .seeded_document(index)
                    .expect("seeding index must exist in the distribution");
                (document.row, document.cells())
            },
        )?;
        let document_acl = seed_rows(
            db,
            support::DOCUMENT_ACL,
            self.direct_acl_grants.len(),
            self.config.local_seed_batch,
            |index| {
                let grant = self.direct_acl_grants[index];
                (
                    grant.row,
                    support::document_acl_cells(
                        grant.document,
                        grant.user,
                        grant.permission,
                        grant.active,
                    ),
                )
            },
        )?;
        Ok(SeedReport {
            organizations,
            teams,
            team_memberships,
            organization_memberships,
            documents,
            document_acl,
            total_ms: millis(total_started.elapsed()),
        })
    }

    fn document_with_team(&self, index: u64, team_index: usize) -> DocumentSpec {
        let organization_index = self.organization_for_team(team_index);
        let owner_slot = index as usize % self.config.team_memberships_per_team;
        let archived = index.is_multiple_of(20);
        let public = index % 100 == 7;
        DocumentSpec {
            index,
            row: support::document_row(index),
            organization_index,
            organization: support::organization_row(organization_index as u64),
            team_index,
            team: support::team_row(team_index as u64),
            owner: self.team_member_identity(team_index, owner_slot),
            updated_at: index,
            status: match index % 5 {
                0 => DocumentStatus::Draft,
                1 => DocumentStatus::Closed,
                _ => DocumentStatus::Active,
            },
            archived,
            public,
            // Keep this exactly aligned with support::document_cells.
            published: public && !archived,
        }
    }

    fn build_direct_acl_grants(&mut self) -> Result<(), String> {
        if self.config.profile != Profile::RealWorld || self.config.permission_branches < 3 {
            return Ok(());
        }
        let mut grants = Vec::new();
        let mut by_user = BTreeMap::<AuthorId, BTreeSet<RowUuid>>::new();
        for subscriber in self
            .subscribers
            .iter()
            .filter(|subscriber| subscriber.access_path == AccessPath::DirectAcl)
        {
            let start = self.distribution.start(subscriber.team_index);
            let count = self.distribution.count(subscriber.team_index);
            for document in (start..start + count)
                .rev()
                .map(|index| self.document_with_team(index as u64, subscriber.team_index))
                .filter(|document| document.appears_in_list())
                .take(self.config.direct_acl_documents_per_subscriber)
            {
                let grant_index = u64::try_from(grants.len())
                    .map_err(|_| "direct ACL grant count does not fit in u64".to_owned())?;
                by_user
                    .entry(subscriber.identity)
                    .or_default()
                    .insert(document.row);
                grants.push(DirectAclGrant {
                    row: support::document_acl_row(grant_index),
                    document: document.row,
                    user: subscriber.identity,
                    permission: if grant_index.is_multiple_of(2) {
                        "view"
                    } else {
                        "edit"
                    },
                    active: true,
                });
            }
        }
        while grants.len() < self.config.direct_acl_rows {
            let grant_index = u64::try_from(grants.len())
                .map_err(|_| "direct ACL grant count does not fit in u64".to_owned())?;
            let document_index = grant_index % self.config.documents as u64;
            grants.push(DirectAclGrant {
                row: support::document_acl_row(grant_index),
                document: support::document_row(document_index),
                user: support::user_identity(BACKGROUND_ACL_USER_BASE + grant_index),
                permission: if grant_index.is_multiple_of(2) {
                    "view"
                } else {
                    "edit"
                },
                active: true,
            });
        }
        self.direct_acl_grants = grants;
        self.direct_documents_by_user = by_user;
        Ok(())
    }

    fn document_visible_to(
        &self,
        subscriber: &SubscriberPlan,
        document: DocumentSpec,
        direct_documents: Option<&BTreeSet<RowUuid>>,
    ) -> bool {
        let team_member = subscriber.access_path == AccessPath::TeamMember
            && subscriber.team_index == document.team_index;
        if self.config.profile == Profile::Baseline {
            return team_member;
        }
        let organization_admin = subscriber.access_path == AccessPath::OrganizationAdmin
            && subscriber.organization_index == document.organization_index;
        let direct_acl =
            direct_documents.is_some_and(|documents| documents.contains(&document.row));
        let public = self.config.permission_branches >= 4 && document.public && document.published;
        let admin = self.config.permission_branches >= 5
            && subscriber
                .claims
                .get("isAdmin")
                .is_some_and(|value| value == &Value::Bool(true));
        team_member || organization_admin || direct_acl || public || admin
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SeedPhaseReport {
    pub rows: usize,
    pub batches: usize,
    pub duration_ms: f64,
    pub rows_per_second: f64,
    pub write_api: &'static str,
}

impl SeedPhaseReport {
    fn new(rows: usize, batches: usize, elapsed: Duration) -> Self {
        let seconds = elapsed.as_secs_f64();
        Self {
            rows,
            batches,
            duration_ms: millis(elapsed),
            rows_per_second: if seconds == 0.0 {
                f64::INFINITY
            } else {
                rows as f64 / seconds
            },
            write_api: "mergeable_tx.insert_with_id",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SeedReport {
    pub organizations: SeedPhaseReport,
    pub teams: SeedPhaseReport,
    pub team_memberships: SeedPhaseReport,
    pub organization_memberships: SeedPhaseReport,
    pub documents: SeedPhaseReport,
    pub document_acl: SeedPhaseReport,
    pub total_ms: f64,
}

fn build_subscriber_plans(config: &Config) -> Result<Vec<SubscriberPlan>, String> {
    let mut team_member_slots = BTreeMap::<usize, usize>::new();
    let mut organization_member_slots = BTreeMap::<usize, usize>::new();
    let mut subscribers = Vec::with_capacity(config.active_subscriptions);
    for index in 0..config.active_subscriptions {
        let access_path = match config.profile {
            Profile::Baseline => AccessPath::TeamMember,
            Profile::RealWorld => AccessPath::REAL_WORLD_CYCLE[index % config.permission_branches],
        };
        let team_index = match config.topology {
            Topology::DistinctTeams => index,
            Topology::HotTeam => 0,
        };
        let organization_index = team_index % config.organizations;
        let identity = match access_path {
            AccessPath::TeamMember => {
                let slot = team_member_slots.entry(team_index).or_default();
                if *slot >= config.team_memberships_per_team {
                    return Err(format!(
                        "team {team_index} needs more than {} distinct member identities for active subscriptions",
                        config.team_memberships_per_team
                    ));
                }
                let identity = support::user_identity(
                    (team_index * config.team_memberships_per_team + *slot) as u64,
                );
                *slot += 1;
                identity
            }
            AccessPath::OrganizationAdmin => {
                let slot = organization_member_slots
                    .entry(organization_index)
                    .or_default();
                if *slot >= config.organization_memberships_per_organization {
                    return Err(format!(
                        "organization {organization_index} needs more than {} distinct admin identities for active subscriptions",
                        config.organization_memberships_per_organization
                    ));
                }
                let identity = support::user_identity(
                    ORGANIZATION_USER_BASE
                        + (organization_index * config.organization_memberships_per_organization
                            + *slot) as u64,
                );
                *slot += 1;
                identity
            }
            AccessPath::DirectAcl => support::user_identity(DIRECT_ACL_USER_BASE + index as u64),
            AccessPath::Public => support::user_identity(PUBLIC_USER_BASE + index as u64),
            AccessPath::AdminClaim => support::user_identity(ADMIN_USER_BASE + index as u64),
        };
        let claims = if access_path == AccessPath::AdminClaim {
            BTreeMap::from([("isAdmin".to_owned(), Value::Bool(true))])
        } else {
            BTreeMap::new()
        };
        subscribers.push(SubscriberPlan {
            index,
            access_path,
            identity,
            organization_index,
            organization: support::organization_row(organization_index as u64),
            team_index,
            team: support::team_row(team_index as u64),
            claims,
            expected_page: Vec::new(),
        });
    }
    Ok(subscribers)
}

fn seed_rows<S>(
    db: &Db<S>,
    table: &'static str,
    rows: usize,
    batch_size: usize,
    mut row: impl FnMut(usize) -> (RowUuid, RowCells),
) -> Result<SeedPhaseReport, String>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let started = Instant::now();
    let mut batches = 0;
    for batch_start in (0..rows).step_by(batch_size) {
        let batch_end = rows.min(batch_start + batch_size);
        let mut tx = db.mergeable_tx();
        for index in batch_start..batch_end {
            let (row_id, cells) = row(index);
            tx.insert_with_id(table, row_id, cells)
                .map_err(|error| format!("seed {table} row {index}: {error}"))?;
        }
        tx.commit()
            .map_err(|error| format!("commit {table} batch {batches}: {error}"))?;
        batches += 1;
    }
    Ok(SeedPhaseReport::new(rows, batches, started.elapsed()))
}

fn env_usize(key: &str, default: usize) -> Result<usize, String> {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("{key} must be a non-negative integer: {error}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("failed to read {key}: {error}")),
    }
}

fn require_nonzero(key: &str, value: usize) -> Result<(), String> {
    if value == 0 {
        Err(format!("{key} must be at least 1"))
    } else {
        Ok(())
    }
}

fn checked_product(left: usize, right: usize, label: &str) -> Result<usize, String> {
    left.checked_mul(right)
        .ok_or_else(|| format!("{label} overflows usize"))
}

fn ensure_user_namespace(value: usize, label: &str) -> Result<(), String> {
    let value = u64::try_from(value).map_err(|_| format!("{label} does not fit in u64"))?;
    if value >= USER_NAMESPACE_WIDTH {
        Err(format!(
            "{label} exceeds the deterministic identity namespace ({USER_NAMESPACE_WIDTH})"
        ))
    } else {
        Ok(())
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
