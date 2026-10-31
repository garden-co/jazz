//! Jazz queries, predicates, policies and relation IR compiled to SQL.
//!
//! Every boolean this module emits is two-valued (0 or 1, never NULL), so
//! `NOT` behaves like Jazz's `Not` and a missing session value can never
//! match a NULL cell by accident.

use std::collections::{BTreeMap, BTreeSet};

use crate::db::Pos;
use crate::ids::{AuthorSubject, RowUuid};
use crate::options::Error;
use crate::query::{
    ArraySubquery, ArraySubqueryRequirement, JoinTarget, Operand, OrderDirection, Predicate, Query,
    RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr, RelationKeyRef,
    RelationPredicate, RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
use crate::records::{Value, ValueType};
use crate::tools::{CmpOp, Operation, PolicyExpr, PolicyValue};

use crate::values::{
    STATE_AS_SENT, STATE_DELETED, STATE_NO_CONTENT, Schema, Sql, Table, query_error, quote, to_sql,
    unsupported,
};

mod history;

/// Inherits chains with no explicit bound unroll this deep.
pub(crate) const DEFAULT_INHERITS_DEPTH: usize = 10;
/// Fixpoint gathers carry a depth column only as a runaway guard.
const FIXPOINT_GUARD: usize = 1_000;

/// The identity a read or write is authorized as.
#[derive(Clone, Debug, Default)]
pub struct Session {
    /// The internal authority capability: no policies apply.
    pub system: bool,
    pub issuer: Option<String>,
    pub subject: Option<String>,
    pub account: Option<uuid::Uuid>,
    /// Post-admission claims, keyed as `Db::set_identity_claims` receives them.
    pub claims: BTreeMap<String, Value>,
    /// Set when compilation read any session value: the SQL is then
    /// specific to this identity and cannot be shared across identities.
    pub used: std::cell::Cell<bool>,
}

impl Session {
    pub fn system() -> Self {
        Self {
            system: true,
            ..Self::default()
        }
    }

    pub fn for_author(author: AuthorSubject, claims: BTreeMap<String, Value>) -> Self {
        if author == AuthorSubject::SYSTEM {
            return Self {
                claims,
                ..Self::system()
            };
        }
        let (issuer, subject) = author.principal_parts();
        Self {
            system: false,
            issuer: Some(issuer),
            subject: Some(subject),
            account: author.account_id().map(|account| account.0),
            claims,
            used: Default::default(),
        }
    }

    /// `session.<path>` as a value, or `None` when the session has none
    /// (or holds an explicit null there).
    pub fn resolve(&self, path: &[String]) -> Option<Value> {
        self.resolve_raw(path).and_then(unwrap_nullable)
    }

    /// `session.<path>` as held: an explicit null claim is
    /// `Some(Nullable(None))`, a missing path `None`. Claim paths walk
    /// nested claim objects by literal segments.
    fn resolve_raw(&self, path: &[String]) -> Option<Value> {
        self.used.set(true);
        let parts: Vec<&str> = path.iter().map(String::as_str).collect();
        Some(match parts.as_slice() {
            ["user", "identity", "subject"] => Value::String(self.subject.clone()?),
            ["user", "identity", "issuer"] => Value::String(self.issuer.clone()?),
            ["user", "account"] => Value::Uuid(self.account?),
            // How the identity signed in, from its issuer (as Jazz derives
            // `session.authMode`).
            ["authMode"] => Value::String(
                match self.issuer.as_deref()? {
                    AuthorSubject::LOCAL_FIRST_ISSUER => "local-first",
                    AuthorSubject::ANONYMOUS_ISSUER => "anonymous",
                    _ => "external",
                }
                .to_owned(),
            ),
            ["claims", _, ..] => {
                jazz_model::model::policy_claims::policy_claim_at_path(&self.claims, path)?
            }
            _ => return None,
        })
    }

    /// `session.<path> IS NULL`: an explicit null. A missing claim path is
    /// not null (it is absent); a missing user part is.
    pub fn is_null(&self, path: &[String]) -> bool {
        match self.resolve_raw(path) {
            Some(value) => unwrap_nullable(value).is_none(),
            None => path.first().map(String::as_str) != Some("claims"),
        }
    }

    pub fn claim(&self, key: &str) -> Option<Value> {
        self.used.set(true);
        unwrap_nullable(self.claims.get(key)?.clone())
    }
}

fn unwrap_nullable(value: Value) -> Option<Value> {
    match value {
        Value::Nullable(None) => None,
        Value::Nullable(Some(inner)) => unwrap_nullable(*inner),
        value => Some(value),
    }
}

/// An operand ready for SQL.
enum Term {
    Sql(String),
    /// A large-capable column's cell ([`LargeColumn`]).
    Large(LargeColumn),
    /// An enum column's cell ([`EnumColumn`]).
    Enum(EnumColumn),
    /// A bound value (literal, parameter, session or claim value): its
    /// placeholder and the value itself.
    Bound(String, Sql),
    /// A session or claim value the identity does not have.
    Missing,
    /// NaN: unordered against everything, SQLite cannot store it.
    NaN,
}

impl Term {
    /// The SQL of a present operand.
    fn sql(&self) -> Option<&str> {
        match self {
            Term::Sql(sql) | Term::Bound(sql, _) => Some(sql),
            Term::Large(column) => Some(&column.cell),
            Term::Enum(column) => Some(&column.cell),
            Term::Missing | Term::NaN => None,
        }
    }
}

/// An enum column's cell: a variant's tag (see
/// [`crate::values::EnumTags`]). A variant name it is compared with is
/// bound as its tag, so the comparison keeps the column's index and order;
/// anything else reads the cell as its name.
#[derive(Clone)]
struct EnumColumn {
    cell: String,
    tags: crate::values::ColumnTags,
    /// The column as the query's schema declares it.
    column: crate::values::Column,
}

impl EnumColumn {
    /// The cell read as its variant name.
    fn names(&self) -> Term {
        Term::Sql(self.tags.borrow().name_sql(&self.cell))
    }

    /// The cell as it orders: its rank in the query schema's declaration
    /// order, or the cell itself where that is tag order.
    fn ordered(&self) -> Option<String> {
        self.column.rank_sql(&self.cell)
    }
}

/// A large-capable column's cell. An inline cell compares as its value;
/// a chunked one (over the inline threshold, SPEC 19.5) is an INTEGER
/// that equals only itself and takes part in no range.
#[derive(Clone)]
struct LargeColumn {
    cell: String,
    table: String,
    column: String,
}

/// SQL text plus its positional parameters.
pub struct Ctx<'a> {
    pub schema: &'a Schema,
    /// `Some` applies read policies as this session (unless it is system).
    pub policy: Option<&'a Session>,
    /// Supplies `Operand::Claim` values.
    pub claims: &'a Session,
    pub binding: &'a BTreeMap<String, Value>,
    pub params: Vec<Sql>,
    /// Every table the compiled SQL reads: the invalidation dependencies.
    pub tables: BTreeSet<String>,
    /// Whether `_jazz_withheld` exists: a branch view over a base masks
    /// base rows its markers name too.
    pub withheld: bool,
    /// The branch view every branched table is read through.
    pub view: Option<View>,
    /// An as-of cut every table is read through.
    pub cut: Option<Cut>,
    /// The cut is a replica's last server seq: the read leaves out its
    /// pending local versions (which history reads do anyway).
    pub settled_only: bool,
    /// The authority's node, whose own commits history reads name by their
    /// seq; `None` on a replica, which lists only versions whose
    /// transaction it knows.
    pub history_origin: Option<[u8; 16]>,
    /// How many of a write check's grounded rows (see [`Cut::overlay`])
    /// the compiled SQL reads, summed over every table reference.
    pub overlaid_rows: usize,
    /// The stored tables a write check reads through its cut's overlay.
    pub overlay_tables: BTreeSet<String>,
    /// A relation policy keyed only by the row id runs once for the whole
    /// statement, as an `IN` list (`exists_rel`): set for reads not
    /// narrowed to listed rows. A per-row `EXISTS` costs every row its own
    /// subquery but seeks by the key, so a write check or a recheck of a few
    /// rows keeps it rather than listing every row the policy grants.
    lift_row_keys: bool,
    next_alias: usize,
}

/// A frozen snapshot: every version at or before position `seq`, plus the
/// open transaction's own staged writes (written at `own`), plus the
/// versions written by the individual commits in `extra` (a replica's own
/// writes that were not yet in `seq` when the snapshot was taken).
#[derive(Clone, Debug, Default)]
pub struct Cut {
    pub seq: Pos,
    pub own: Pos,
    pub extra: Vec<Pos>,
    /// Every unsettled version is in the cut too: a live query reading as
    /// of its subscription's settled position still shows this replica's
    /// own pending writes, which no server position covers.
    pub unsettled: bool,
    /// A write check's evidence (SPEC 7, WITH CHECK): which of the
    /// versions at `own` are overlaid. `None` overlays them all.
    pub overlay: Option<Overlay>,
}

/// The versions at a write check's `own` that its policy sees (SPEC 7);
/// every other row the unit wrote reads as committed.
#[derive(Clone, Debug)]
pub enum Overlay {
    /// The live rows grounded so far, by stored table.
    Grounded(std::sync::Arc<BTreeMap<String, BTreeSet<[u8; 16]>>>),
    /// Every live row but `row` in branch `key` of stored table `table`:
    /// the unit's post-state, without the row under check.
    Except {
        table: String,
        row: [u8; 16],
        key: Vec<u8>,
    },
}

impl Cut {
    pub fn at(seq: Pos) -> Self {
        Self {
            seq,
            ..Self::default()
        }
    }

    /// Whether a version at `at` is in the cut.
    pub fn sees(&self, at: Pos) -> bool {
        at <= self.seq
            || at == self.own
            || self.extra.contains(&at)
            || (self.unsettled && !at.accepted())
    }
}

/// A head branch, optionally composed over a base branch that is live or
/// frozen at `base_cut`.
#[derive(Clone, Debug)]
pub struct View {
    pub head: crate::protocol::BranchSelector,
    pub base: Option<crate::protocol::BranchSelector>,
    pub base_cut: Option<Cut>,
}

/// A compiled root query.
#[derive(Clone)]
pub struct Compiled {
    pub sql: String,
    pub params: Vec<Sql>,
    /// Each projected cell: its name, its type and, for an enum column, the
    /// tags it holds.
    pub projection: Vec<Projected>,
    pub tables: BTreeSet<String>,
    /// Rows are aggregate groups: column 0 is the group key, hashed to an id.
    pub aggregate: bool,
    /// Joined source row ids each row carries after its root's id, seq and
    /// deletion flag (a flat join's occurrence; 0 otherwise).
    pub joined: usize,
    /// Each row carries its root's version (a history read's
    /// `$transactionId`) after its id, seq and deletion flag, as a flat
    /// join does: a history row's occurrence is its version.
    pub versioned: bool,
    /// Rows are union rows: column 3 is the arm's label.
    pub union: bool,
    /// The part each projected column is read in, if only a part
    /// (SPEC 19.12); empty when none is.
    pub partial: Vec<Option<crate::query::Part>>,
}

impl<'a> Ctx<'a> {
    pub fn new(
        schema: &'a Schema,
        policy: Option<&'a Session>,
        claims: &'a Session,
        binding: &'a BTreeMap<String, Value>,
    ) -> Self {
        Self {
            schema,
            policy: policy.filter(|session| !session.system),
            claims,
            binding,
            params: Vec::new(),
            tables: BTreeSet::new(),
            withheld: false,
            view: None,
            cut: None,
            settled_only: false,
            history_origin: None,
            overlaid_rows: 0,
            overlay_tables: BTreeSet::new(),
            lift_row_keys: false,
            next_alias: 0,
        }
    }

    pub fn with_history_origin(mut self, origin: Option<[u8; 16]>) -> Self {
        self.history_origin = origin;
        self
    }

    pub fn with_view(mut self, view: Option<View>) -> Self {
        self.view = view;
        self
    }

    pub fn with_withheld(mut self, withheld: bool) -> Self {
        self.withheld = withheld;
        self
    }

    /// Read as of `cut`. Without one, `settled_only` leaves out a replica's
    /// pending local versions, reading as of its last server seq.
    pub fn with_cut(mut self, cut: Option<Cut>, settled_only: bool) -> Self {
        match cut {
            Some(cut) => self.cut = Some(cut),
            None if settled_only => {
                self.cut = Some(Cut::at(Pos::SETTLED));
                self.settled_only = true;
            }
            None => {}
        }
        self
    }

    /// A table's rows, as of the cut when there is one. Current rows written
    /// at or before the cut (or by the transaction itself) are visible as
    /// they are; for rows changed since, the newest superseded image at or
    /// before the cut stands in. Rows created after the cut have neither.
    fn rows(&mut self, table: &Table, columns: &str) -> String {
        self.rows_at(table, columns, self.cut.clone())
    }

    fn rows_at(&mut self, table: &Table, columns: &str, cut: Option<Cut>) -> String {
        let name = quote(&table.stored);
        let Some(cut) = cut else {
            return name;
        };
        let seq = self.bind(Sql::Integer(cut.seq.t()));
        let own_t = self.bind(Sql::Integer(cut.own.t()));
        let own = format!("_t = {own_t}");
        let c_own = format!("c._t = {own_t}");
        // Positions are integers, so the extra commits are inlined.
        let extra = cut
            .extra
            .iter()
            .map(|at| at.t().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        // The versions at `own` that are overlaid: all of them, or for a
        // write check the unit's grounded live rows.
        if cut.overlay.is_some() {
            self.overlay_tables.insert(table.stored.clone());
        }
        let (overlaid_here, overlaid_c) = match &cut.overlay {
            None => (format!("({own})"), format!("({c_own})")),
            Some(Overlay::Grounded(grounded)) => {
                match grounded.get(&table.stored).filter(|ids| !ids.is_empty()) {
                    None => ("0".to_owned(), "0".to_owned()),
                    Some(ids) => {
                        self.overlaid_rows += ids.len();
                        let ids = ids
                            .iter()
                            .map(|id| self.bind(Sql::Blob(id.to_vec())))
                            .collect::<Vec<_>>()
                            .join(", ");
                        (
                            format!("({own} AND (_st & {STATE_DELETED}) = 0 AND id IN ({ids}))"),
                            format!(
                                "({c_own} AND (c._st & {STATE_DELETED}) = 0 AND c.id IN ({ids}))"
                            ),
                        )
                    }
                }
            }
            // No id list: SQLite reaches the overlaid rows through the
            // policy's correlated keys, as it does committed ones.
            Some(Overlay::Except {
                table: except,
                row,
                key,
            }) if *except == table.stored => {
                let row = self.bind(Sql::Blob(row.to_vec()));
                let key = self.bind(Sql::Blob(key.clone()));
                (
                    format!(
                        "({own} AND (_st & {STATE_DELETED}) = 0 AND (id <> {row} OR _b <> {key}))"
                    ),
                    format!(
                        "({c_own} AND (c._st & {STATE_DELETED}) = 0 AND (c.id <> {row} OR c._b <> {key}))"
                    ),
                )
            }
            Some(Overlay::Except { .. }) => (
                format!("({own} AND (_st & {STATE_DELETED}) = 0)"),
                format!("({c_own} AND (c._st & {STATE_DELETED}) = 0)"),
            ),
        };
        let (mut visible, h_visible, mut newer) = if extra.is_empty() {
            (
                format!("_t <= {seq} OR {overlaid_here}"),
                format!("h2._t <= {seq}"),
                format!("c._t > {seq} AND NOT {overlaid_c}"),
            )
        } else {
            (
                format!("_t <= {seq} OR {overlaid_here} OR _t IN ({extra})"),
                format!("(h2._t <= {seq} OR h2._t IN ({extra}))"),
                format!("c._t > {seq} AND NOT {overlaid_c} AND c._t NOT IN ({extra})"),
            )
        };
        // An unsettled current version is newer than every accepted one:
        // it shows as it is, and only accepted rows past the cut fall back
        // to their history.
        if cut.unsettled {
            let unsettled = crate::db::sync::band_start(crate::db::sync::UPSTREAM);
            visible.push_str(&format!(" OR _t >= {unsettled}"));
            newer.push_str(&format!(" AND c._t < {unsettled}"));
        }
        let history = quote(&format!("{}__h", table.stored));
        // Both arms must let SQLite search by a correlated predicate (a
        // policy EXISTS's `m.band = a.band`), or every read through a cut
        // scans the whole table (#4298). History rows have no `rid`, and its
        // stand-in needs `rid`'s INTEGER affinity: a bare NULL has none, and
        // arms that differ in a column's affinity are not flattened, so the
        // predicate stays outside them. The history arm starts from the
        // current rows changed since the cut (`_t` is indexed; CROSS JOIN
        // keeps that order) rather than scanning every history row for a
        // column the history table has no index on.
        let h_columns = columns
            .replace("rid, ", "")
            .split(", ")
            .map(|c| format!("h.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "(SELECT {columns} FROM {name} WHERE {visible} \
             UNION ALL SELECT CAST(NULL AS INTEGER), {h_columns} FROM {name} AS c \
             CROSS JOIN {history} AS h ON c.id = h.id AND c._b = h._b WHERE {newer} \
             AND h._t = (SELECT h2._t FROM {history} AS h2 \
             WHERE h2.id = h.id AND h2._b = h._b AND (h2._st & {STATE_AS_SENT}) = 0 AND {h_visible} \
             ORDER BY h2._t DESC LIMIT 1))"
        )
    }

    /// The SQL source a table is read from. Under a branch view a branched
    /// table is its effective rows: head rows (tombstones included, so they
    /// mask), then base rows the head does not override. Filters, policies,
    /// ordering and windows all apply after this masking.
    pub(crate) fn source(&mut self, table: &Table) -> Result<String, Error> {
        // Explicit columns: a schema view reads its own columns of the
        // physical (unioned) table.
        let mut columns = "rid, id, _b, _t, _st".to_owned();
        for column in table.stored() {
            columns.push_str(&format!(", {}", quote(&column.stored)));
        }
        let view = self.view.clone().filter(|_| !table.branch_by.is_empty());
        let Some(view) = view else {
            if self.cut.is_none() {
                return Ok(quote(&table.stored));
            }
            return Ok(self.rows(table, &columns));
        };
        let head = self.bind(Sql::Blob(table.selector_key(&view.head)?));
        let rows = self.rows(table, &columns);
        Ok(match view.base {
            None => format!("(SELECT {columns} FROM {rows} WHERE _b = {head})"),
            Some(base) => {
                // Inherited rows are read as head rows: their branch key and
                // branch columns are the head's, so predicates, policies and
                // projections see one effective branch.
                let head_cells = table.selector_cells(&view.head)?;
                let mut inherited = format!("m.rid, m.id, {head}, m._t, m._st");
                for column in table.stored() {
                    match head_cells.get(&column.name) {
                        Some(value) => {
                            let term = match column.cell_sql(value)? {
                                Some(sql) => self.bind(sql),
                                None => "NULL".to_owned(),
                            };
                            inherited.push_str(&format!(", {term}"));
                        }
                        None => inherited.push_str(&format!(", m.{}", quote(&column.stored))),
                    }
                }
                let base = self.bind(Sql::Blob(table.selector_key(&base)?));
                let base_cut = view.base_cut.clone().or_else(|| self.cut.clone());
                let base_rows = self.rows_at(table, &columns, base_cut);
                let head_rows = self.rows(table, &columns);
                // A head version this reader may not read still overrides
                // the base's row: the replica holds a marker of it.
                let withheld = if self.withheld {
                    let name = self.bind(Sql::Text(table.stored.clone().into_bytes()));
                    format!(
                        " AND NOT EXISTS (SELECT 1 FROM _jazz_withheld AS w \
                         WHERE w.tbl = {name} AND w.id = m.id AND w._b = {head})"
                    )
                } else {
                    String::new()
                };
                format!(
                    "(SELECT {columns} FROM {rows} WHERE _b = {head} UNION ALL SELECT {inherited} \
                     FROM {base_rows} AS m WHERE m._b = {base} AND NOT EXISTS (SELECT 1 FROM {head_rows} AS x \
                     WHERE x.id = m.id AND x._b = {head}){withheld})"
                )
            }
        })
    }

    fn alias(&mut self, prefix: &str) -> String {
        self.next_alias += 1;
        format!("{prefix}{}", self.next_alias)
    }

    fn bind(&mut self, value: Sql) -> String {
        self.params.push(value);
        format!("?{}", self.params.len())
    }

    fn value_term(&mut self, value: &Value) -> Result<Term, Error> {
        Ok(match to_sql(value)? {
            None => Term::NaN,
            Some(sql) => Term::Bound(self.bind(sql.clone()), sql),
        })
    }

    fn present(&mut self, value: Option<Value>) -> Result<Term, Error> {
        match value {
            None => Ok(Term::Missing),
            Some(value) => self.value_term(&value),
        }
    }

    /// A table by this schema's name for it; its stored name joins the
    /// dependencies.
    fn table(&mut self, name: &str) -> Result<&'a Table, Error> {
        let table = self.schema.table(name)?;
        self.tables.insert(table.stored.clone());
        Ok(table)
    }

    // ---- root queries ---------------------------------------------------

    /// `GROUP BY` over the effective, filtered, policy-checked rows. Each
    /// group is a synthetic row whose id is a hash of its group key (column
    /// 0 carries the raw key; the executor hashes it), so subscriptions diff
    /// groups like rows. Output cells: the group column, then aggregates.
    fn compile_aggregate(
        mut self,
        query: &Query,
        aggregate: &crate::query::AggregateQuery,
        table: &Table,
        from: &str,
        alias: &str,
        wheres: &[String],
    ) -> Result<Compiled, Error> {
        use crate::query::AggregateFunction as F;
        let mut projection = Vec::new();
        let mut outputs = Vec::new();
        // The group cell. A chunked one groups only the rows naming that
        // cell (SPEC 19.5).
        // Each output an `ORDER BY` names, as it orders (an enum's rank in
        // the query schema's declaration order).
        let mut ordered: BTreeMap<String, String> = BTreeMap::new();
        // A group's identity is its key's value: an enum variant by name,
        // so a renumbering keeps it.
        let mut group_key = None;
        let group = match &aggregate.group_by {
            Some(column) => {
                let sql = column_sql(table, alias, column)?;
                let column = table.column(column).expect("checked");
                projection.push((column.name.clone(), column.ty.clone(), column.tags.clone()));
                outputs.push(sql.clone());
                if let Some(rank) = column.rank_sql(&sql) {
                    ordered.insert(column.name.clone(), rank);
                }
                group_key = column
                    .tags
                    .as_ref()
                    .map(|tags| tags.borrow().name_sql(&sql));
                Some(sql)
            }
            None => None,
        };
        for output in &aggregate.aggregates {
            let input = |column: &Option<String>| -> Result<(String, ValueType), Error> {
                let column = column
                    .as_deref()
                    .ok_or_else(|| query_error(format!("`{}` needs a column", output.alias)))?;
                let ty = table
                    .column(column)
                    .ok_or_else(|| {
                        query_error(format!("unknown column `{}.{column}`", table.name))
                    })?
                    .ty
                    .clone();
                let ty = match ty {
                    ValueType::Nullable(inner) => *inner,
                    ty => ty,
                };
                Ok((column_sql(table, alias, column)?, ty))
            };
            // MIN and MAX of an enum column are variants, in the query
            // schema's declaration order.
            let enum_column = output
                .column
                .as_deref()
                .and_then(|column| table.column(column))
                .filter(|column| column.tags.is_some())
                .filter(|_| matches!(output.function, F::Min | F::Max));
            let tags = enum_column.and_then(|column| column.tags.clone());
            let (sql, ty) = match output.function {
                F::Count => match &output.column {
                    None => ("COUNT(*)".to_owned(), ValueType::U64),
                    Some(_) => (
                        format!("COUNT({})", input(&output.column)?.0),
                        ValueType::U64,
                    ),
                },
                F::Avg => {
                    // SQLite's AVG divides a sum, which overflows to
                    // infinity for large finite inputs; then the inputs,
                    // scaled exactly by a power of two, average instead.
                    let column = input(&output.column)?.0;
                    (
                        format!(
                            "(CASE WHEN COUNT({column}) = 0 THEN NULL \
                             WHEN abs(AVG({column})) <= {max:e} THEN AVG({column}) \
                             ELSE AVG({column} * {down:e}) * {up:e} END)",
                            max = f64::MAX,
                            down = 2f64.powi(-128),
                            up = 2f64.powi(128),
                        ),
                        ValueType::Nullable(Box::new(ValueType::F64)),
                    )
                }
                F::Sum | F::Min | F::Max => {
                    let (column, ty) = input(&output.column)?;
                    let name = match output.function {
                        F::Sum => "SUM",
                        F::Min => "MIN",
                        _ => "MAX",
                    };
                    let rank = enum_column.and_then(|enum_column| {
                        let rank = enum_column.rank_sql(&column)?;
                        Some((enum_column, format!("{name}({rank})")))
                    });
                    match rank {
                        Some((enum_column, rank)) => {
                            ordered.insert(output.alias.clone(), rank.clone());
                            (
                                enum_column.unrank_sql(&rank),
                                ValueType::Nullable(Box::new(ty)),
                            )
                        }
                        None => (
                            format!("{name}({column})"),
                            ValueType::Nullable(Box::new(ty)),
                        ),
                    }
                }
            };
            outputs.push(format!("{sql} AS {}", quote(&output.alias)));
            projection.push((output.alias.clone(), ty, tags));
        }
        let mut keys = Vec::new();
        for key in &query.order_by {
            let direction = match key.direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            };
            let term = if let Some(rank) = ordered.get(&key.column) {
                rank.clone()
            } else if aggregate.group_by.as_deref() == Some(key.column.as_str()) {
                group.clone().expect("grouped")
            } else if aggregate.aggregates.iter().any(|a| a.alias == key.column) {
                quote(&key.column)
            } else {
                return Err(query_error(format!(
                    "aggregate queries order by a group column or an aggregate alias, not `{}`",
                    key.column
                )));
            };
            keys.push(format!("{term} {direction}"));
        }
        let (key, group_by) = match &group {
            Some(sql) => {
                keys.push(format!("{sql} ASC"));
                (
                    group_key.unwrap_or_else(|| sql.clone()),
                    format!(" GROUP BY {sql}"),
                )
            }
            None => ("NULL".to_owned(), String::new()),
        };
        let order = if keys.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", keys.join(", "))
        };
        let limit = self.bind(Sql::Integer(query.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(query.offset as i64));
        let sql = format!(
            "SELECT {key}, 0, 0, {} FROM {from} AS {alias} WHERE {}{group_by}{order} \
             LIMIT {limit} OFFSET {offset}",
            outputs.join(", "),
            and(wheres),
        );
        Ok(Compiled {
            sql,
            partial: Vec::new(),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: true,
            joined: 0,
            versioned: false,
            union: false,
        })
    }

    pub fn compile_query(
        mut self,
        query: &Query,
        include_deleted: bool,
    ) -> Result<Compiled, Error> {
        self.lift_row_keys = !narrowed_to_ids(query);
        let schema = self.schema;
        let mut compiled = self.compile_shape(query, include_deleted)?;
        if !query.partial.is_empty() {
            compiled.partial = partial_projection(schema, &query.table, &query.partial, &compiled)?;
        }
        Ok(compiled)
    }

    fn compile_shape(mut self, query: &Query, include_deleted: bool) -> Result<Compiled, Error> {
        if let Some(column) = deletion_column(query) {
            if !include_deleted {
                return Err(query_error(format!(
                    "Reading {column} requires includeDeleted"
                )));
            }
            // includeDeleted widens only the root: joined rows are live.
            if !query.joins.is_empty() || query.flat_join.is_some() {
                return Err(unsupported(format!("reading {column} in joins")));
            }
        }
        if query.flat_join.is_some() {
            return self.compile_flat_join(query);
        }
        if let Some(source) = crate::schema::history::history_source_table(&query.table) {
            if !query.policy_branches.is_empty()
                || !query.inherits.is_empty()
                || query.relation.is_some()
            {
                return Err(unsupported("joins or relationships in history queries"));
            }
            return self.compile_history(query, source, include_deleted);
        }
        if query.flat_join.is_some() {
            return self.compile_flat_join(query);
        }
        if let Some(plan) = crate::db::union_plan(query, self.schema)? {
            return self.compile_union(plan, include_deleted);
        }
        if !query.policy_branches.is_empty()
            || !query.inherits.is_empty()
            || query.relation.is_some()
        {
            return Err(unsupported(
                "this query form (policy branches, inherits, relation)",
            ));
        }
        let (table, projection, wheres) = self.root_rows(query, include_deleted)?;
        let alias = "t";
        if let Some(aggregate) = &query.aggregate {
            if !query.array_subqueries.is_empty() || query.select.is_some() {
                return Err(unsupported(
                    "aggregates with selections or array subqueries",
                ));
            }
            let from = self.source(table)?;
            return self.compile_aggregate(query, aggregate, table, &from, alias, &wheres);
        }
        let order = self.order_sql(table, alias, &query.order_by)?;
        let columns = projection
            .iter()
            .map(|(name, ..)| Ok(format!(", {}", column_sql(table, alias, name)?)))
            .collect::<Result<String, Error>>()?;
        let limit = self.bind(Sql::Integer(query.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(query.offset as i64));
        let source = self.source(table)?;
        let statement = |order: &str| {
            format!(
                "SELECT {alias}.id, {alias}._t, {alias}._st{columns} FROM {source} AS {alias} \
                 WHERE {} ORDER BY {order} LIMIT {limit} OFFSET {offset}",
                and(&wheres),
            )
        };
        Ok(Compiled {
            sql: statement(&order),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            joined: 0,
            versioned: false,
            union: false,
            partial: Vec::new(),
        })
    }

    /// A plain query's root table (read as `t`), its projection, and the
    /// conditions its rows meet: filters, traversals, required includes and
    /// array requirements, the read policy, and liveness.
    fn root_rows(
        &mut self,
        query: &Query,
        include_deleted: bool,
    ) -> Result<(&'a Table, Vec<Projected>, Vec<String>), Error> {
        let table = self.table(&query.table)?;
        let projection = projection(table, query.select.as_deref())?;
        let alias = "t";
        let mut wheres = Vec::new();
        if !include_deleted {
            wheres.push(format!("({alias}._st & {STATE_DELETED}) = 0"));
        }
        for filter in &query.filters {
            wheres.push(self.predicate(filter, table, alias)?);
        }
        for reachable in &query.reachable {
            wheres.push(self.reachable(reachable, table, alias)?);
        }
        for join in &query.joins {
            wheres.push(self.join_exists(join, table, alias)?);
        }
        for include in &query.includes {
            let column = include.path.split('.').next().unwrap_or_default();
            let target_name = table
                .references
                .get(column)
                .ok_or_else(|| query_error(format!("`{}` is not a reference", include.path)))?;
            let target = self.table(target_name)?;
            if include.path.contains('.') {
                return Err(unsupported("multi-hop include paths"));
            }
            if include.require {
                let i = self.alias("i");
                wheres.push(format!(
                    "EXISTS (SELECT 1 FROM {} AS {i} WHERE {i}.id = {alias}.{} AND ({i}._st & {STATE_DELETED}) = 0)",
                    self.source(target)?,
                    quote(table.stored_name(column))
                ));
            }
        }
        for array in &query.array_subqueries {
            self.validate_array(array)?;
        }
        self.array_requirements(table, alias, &query.array_subqueries, &mut wheres)?;
        if let Some(session) = self.policy {
            wheres.push(self.read_policy(table, alias, session)?);
        }
        Ok((table, projection, wheres))
    }

    /// A union: each arm's rows under its own conditions and read policy,
    /// labelled with the arm (column 3), `UNION ALL`, then ordered and
    /// windowed as a whole. Ties break by row id, then by arm label, in
    /// the last key's direction, so a row matched by several arms has a
    /// stable place for each.
    fn compile_union(
        mut self,
        plan: crate::db::UnionPlan,
        include_deleted: bool,
    ) -> Result<Compiled, Error> {
        let alias = "t";
        let mut projection = Vec::new();
        let mut arms = Vec::with_capacity(plan.arms.len());
        let mut keys = Vec::new();
        for (label, arm) in &plan.arms {
            let unsupported_arm = [
                (arm.flat_join.is_some(), "flat joins"),
                (
                    crate::schema::history::history_source_table(&arm.table).is_some(),
                    "history reads",
                ),
                (!arm.policy_branches.is_empty(), "policy branches"),
                (!arm.inherits.is_empty(), "inherited-policy traversals"),
                (arm.aggregate.is_some(), "aggregates"),
                (!arm.includes.is_empty(), "includes"),
                (!arm.array_subqueries.is_empty(), "array subqueries"),
                (arm.select.is_some(), "select projections"),
            ];
            if let Some((_, feature)) = unsupported_arm.iter().find(|(present, _)| *present) {
                return Err(unsupported(format!("union arms with {feature}")));
            }
            if let Some(column) = deletion_column(arm) {
                if !include_deleted {
                    return Err(query_error(format!(
                        "Reading {column} requires includeDeleted"
                    )));
                }
                if !arm.joins.is_empty() {
                    return Err(unsupported(format!("reading {column} in joins")));
                }
            }
            let (table, arm_projection, wheres) = self.root_rows(arm, include_deleted)?;
            let columns = arm_projection
                .iter()
                .enumerate()
                .map(|(i, (name, ..))| Ok(format!(", {} AS c{i}", column_sql(table, alias, name)?)))
                .collect::<Result<String, Error>>()?;
            let label = self.bind(Sql::Text(label.as_bytes().to_vec()));
            keys = self.order_terms(table, alias, &plan.order_by)?;
            let keys_sql: String = keys
                .iter()
                .enumerate()
                .map(|(i, key)| format!(", {} AS k{i}", key.sql))
                .collect();
            let source = self.source(table)?;
            let mut body = format!(
                "SELECT {alias}.id AS id, {alias}._t AS _t, {alias}._st AS _st, \
                 {label} AS _arm{columns}{keys_sql} FROM {source} AS {alias} WHERE {}",
                and(&wheres)
            );
            // An arm's own window applies before the union.
            if arm.limit.is_some() || arm.offset != 0 {
                let order = self.order_sql(table, alias, &arm.order_by)?;
                let limit = self.bind(Sql::Integer(arm.limit.map_or(-1, |l| l as i64)));
                let offset = self.bind(Sql::Integer(arm.offset as i64));
                body = format!(
                    "SELECT * FROM ({body} ORDER BY {order} LIMIT {limit} OFFSET {offset})"
                );
            }
            projection = arm_projection;
            arms.push(body);
        }
        let tie = match plan.order_by.last().map(|key| key.direction) {
            Some(OrderDirection::Desc) => "DESC",
            _ => "ASC",
        };
        let mut order: Vec<String> = keys
            .iter()
            .enumerate()
            .map(|(i, key)| format!("k{i} {}", key.direction))
            .collect();
        order.push(format!("id {tie}"));
        order.push(format!("_arm {tie}"));
        let limit = self.bind(Sql::Integer(plan.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(plan.offset as i64));
        let columns: String = (0..projection.len()).map(|i| format!(", c{i}")).collect();
        let sql = format!(
            "SELECT id, _t, _st, _arm{columns} FROM ({}) ORDER BY {} \
             LIMIT {limit} OFFSET {offset}",
            arms.join(" UNION ALL "),
            order.join(", ")
        );
        Ok(Compiled {
            sql,
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            joined: 0,
            versioned: false,
            union: true,
            partial: Vec::new(),
        })
    }

    /// A flat join: one row per matching tuple of the root and each joined
    /// source, every source live and read-policy filtered. Cells are each
    /// source's columns qualified as `<source>.<column>`, root first; a
    /// row's id is its root's, and its joined source ids follow it, so the
    /// tuple is its occurrence. Filters name qualified columns, or
    /// unqualified ones only one source has, and each conjunct reads one
    /// source. Ordered by the occurrence.
    ///
    /// A `history.<table>` source lists its rows' versions (gated by the
    /// current row, as a history read is), so each version is its own
    /// occurrence. It must be bounded by row id: an id filter, or a join
    /// key on its `id` whose other side reads a bounded source.
    fn compile_flat_join(mut self, query: &Query) -> Result<Compiled, Error> {
        let Some(flat_join) = &query.flat_join else {
            unreachable!("compile_flat_join on a query without one")
        };
        let combined = [
            (flat_join.sources.is_empty(), "zero joined sources"),
            (!query.joins.is_empty(), "existential joins"),
            (!query.policy_branches.is_empty(), "policy branches"),
            (!query.reachable.is_empty(), "reachability"),
            (!query.inherits.is_empty(), "inherited-policy traversals"),
            (!query.includes.is_empty(), "includes"),
            (!query.array_subqueries.is_empty(), "array subqueries"),
            (query.relation.is_some(), "relations"),
            (query.select.is_some(), "select projections"),
            (!query.order_by.is_empty(), "ordering"),
            (query.aggregate.is_some(), "aggregates"),
            (query.limit.is_some(), "limits"),
            (query.offset != 0, "offsets"),
        ];
        if let Some((_, feature)) = combined.iter().find(|(present, _)| *present) {
            return Err(unsupported(format!("flat joins with {feature}")));
        }
        let mut declared = vec![(
            flat_join
                .root_alias
                .clone()
                .unwrap_or_else(|| query.table.clone()),
            query.table.clone(),
            "t".to_owned(),
        )];
        for source in &flat_join.sources {
            let name = source.alias.clone().unwrap_or_else(|| source.table.clone());
            if declared.iter().any(|(seen, ..)| *seen == name) {
                return Err(query_error(format!("flat join duplicate source `{name}`")));
            }
            let alias = self.alias("f");
            declared.push((name, source.table.clone(), alias));
        }
        // Each source as it reads: a history source as its versions (whose
        // SQL waits for its bound), others as their table.
        let mut sources: Vec<FlatSource<'a>> = Vec::with_capacity(declared.len());
        for (name, table, alias) in declared {
            let history = crate::schema::history::history_source_table(&table);
            let base = self.table(history.unwrap_or(&table))?;
            let (table, sql) = match history {
                Some(_) => (history::versions_table(base), None),
                None => (base.clone(), Some(self.source(base)?)),
            };
            sources.push(FlatSource {
                name,
                base,
                table,
                alias,
                history: history.is_some(),
                sql,
            });
        }
        let resolve = |sources: &[FlatSource], column: &str| -> Result<(usize, String), Error> {
            let (scope, field) = match column.find(".$") {
                Some(at) => (&column[..at], &column[at + 1..]),
                None => column.rsplit_once('.').unwrap_or(("", column)),
            };
            let has = |table: &Table, field: &str| {
                field == "id" || field == "_id" || column_sql(table, "t", field).is_ok()
            };
            let public = |field: &str| if field == "_id" { "id" } else { field }.to_owned();
            if let Some(at) = sources.iter().position(|source| source.name == scope) {
                if !has(&sources[at].table, field) {
                    return Err(query_error(format!("unknown flat join column `{column}`")));
                }
                return Ok((at, public(field)));
            }
            let mut found = (0..sources.len()).filter(|&at| has(&sources[at].table, column));
            match (found.next(), found.next()) {
                (Some(at), None) => Ok((at, public(column))),
                (Some(_), Some(_)) => Err(query_error(format!(
                    "ambiguous flat join column `{column}`; qualify its source"
                ))),
                (None, _) => Err(query_error(format!("unknown flat join column `{column}`"))),
            }
        };
        let mut keys = Vec::with_capacity(flat_join.sources.len());
        for (at, source) in flat_join.sources.iter().enumerate() {
            let left = resolve(&sources[..=at], &source.on.left)?;
            let right = resolve(&sources, &source.on.right)?;
            if right.0 != at + 1 {
                return Err(query_error(format!(
                    "flat join condition `{}` must name the joined source `{}`",
                    source.on.right,
                    sources[at + 1].name
                )));
            }
            keys.push((left, right));
        }
        // Filters, each conjunct routed to the one source it reads.
        let mut routed: Vec<(usize, Predicate)> = Vec::new();
        for filter in &query.filters {
            let mut conjuncts = Vec::new();
            flatten_all(filter, &mut conjuncts);
            for conjunct in conjuncts {
                if matches!(conjunct, Predicate::EnumMatch { .. }) {
                    return Err(unsupported("enum-match filters in flat joins"));
                }
                let mut predicate = conjunct.clone();
                let mut reads = BTreeSet::new();
                let mut failed = None;
                predicate.for_each_operand_mut(&mut |operand| {
                    if let Operand::Column(column) = operand {
                        match resolve(&sources, column) {
                            Ok((at, field)) => {
                                reads.insert(at);
                                *column = field;
                            }
                            Err(error) => failed = failed.take().or(Some(error)),
                        }
                    }
                });
                if let Some(error) = failed {
                    return Err(error);
                }
                if reads.len() > 1 {
                    return Err(unsupported("flat join filters spanning multiple sources"));
                }
                routed.push((reads.first().copied().unwrap_or(0), predicate));
            }
        }
        // Bound history sources until none is left: by an id filter, else
        // by a key on their id whose other side is bound.
        while let Some(at) = sources.iter().position(|source| source.sql.is_none()) {
            let mut bound = routed
                .iter()
                .filter(|(source, _)| *source == at)
                .find_map(|(_, predicate)| history::bounding_id(predicate))
                .map(history::HistoryBound::Filter);
            if bound.is_none() {
                for ((left, left_column), (right, right_column)) in &keys {
                    let other = match (*left == at, *right == at) {
                        (true, false) if left_column == "id" => (*right, right_column),
                        (false, true) if right_column == "id" => (*left, left_column),
                        _ => continue,
                    };
                    let source = &sources[other.0];
                    let Some(sql) = &source.sql else { continue };
                    let b = self.alias("b");
                    bound = Some(history::HistoryBound::Ids(format!(
                        "SELECT {} FROM {sql} AS {b} WHERE ({b}._st & {STATE_DELETED}) = 0",
                        column_sql(&source.table, &b, other.1)?
                    )));
                    break;
                }
            }
            let Some(bound) = bound else {
                return Err(query_error(
                    "unsupported relation query: Reading history without a bounded row access \
                     path is not supported in history queries yet",
                ));
            };
            let base = sources[at].base;
            let (sql, _) = self.history_source(&base.name, bound, false)?;
            sources[at].sql = Some(sql);
        }
        let sql_of = |source: &FlatSource| source.sql.clone().expect("every source is bound");
        let mut joins = String::new();
        for ((left, left_column), (right, right_column)) in &keys {
            let (left, right) = (&sources[*left], &sources[*right]);
            if (left.history || right.history)
                && (is_array(&left.table, left_column) || is_array(&right.table, right_column))
            {
                return Err(unsupported("flat joins of history on array columns"));
            }
            let on = self.join_key(
                (&left.table, &left.alias, left_column),
                (&right.table, &right.alias, right_column),
            )?;
            joins.push_str(&format!(
                " JOIN {} AS {alias} ON ({alias}._st & {STATE_DELETED}) = 0 AND {on}",
                sql_of(right),
                alias = right.alias
            ));
        }
        let mut wheres = vec![format!("(t._st & {STATE_DELETED}) = 0")];
        for (at, predicate) in &routed {
            let source = &sources[*at];
            wheres.push(self.predicate(predicate, &source.table, &source.alias)?);
        }
        if let Some(session) = self.policy {
            // History sources are gated by their current rows already.
            for source in sources.iter().filter(|source| !source.history) {
                wheres.push(self.read_policy(&source.table, &source.alias, session)?);
            }
        }
        let version = |source: &FlatSource| match source.history {
            true => format!(
                "{}.{}",
                source.alias,
                quote(crate::schema::history::TRANSACTION_ID_COLUMN)
            ),
            false => "NULL".to_owned(),
        };
        let mut ids = format!(", {}", version(&sources[0]));
        let mut order = vec!["t.id".to_owned()];
        if sources[0].history {
            order.push("t._t".to_owned());
        }
        let mut projection = Vec::new();
        let mut columns = String::new();
        for (at, source) in sources.iter().enumerate() {
            if at > 0 {
                ids.push_str(&format!(", {}.id, {}", source.alias, version(source)));
                order.push(format!("{}.id", source.alias));
                if source.history {
                    order.push(format!("{}._t", source.alias));
                }
            }
            for (column, ty, tags) in crate::compile::projection(&source.table, None)? {
                columns.push_str(&format!(
                    ", {}",
                    column_sql(&source.table, &source.alias, &column)?
                ));
                projection.push((format!("{}.{column}", source.name), ty, tags));
            }
        }
        let sql = format!(
            "SELECT t.id, t._t, t._st{ids}{columns} FROM {} AS t{joins} \
             WHERE {} ORDER BY {}",
            sql_of(&sources[0]),
            and(&wheres),
            order.join(", ")
        );
        Ok(Compiled {
            sql,
            partial: Vec::new(),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            joined: sources.len() - 1,
            versioned: true,
            union: false,
        })
    }

    /// A flat join's key equality, `left = right`; when one side is an
    /// array, any of its elements equal to the other side.
    fn join_key(
        &mut self,
        left: (&Table, &str, &str),
        right: (&Table, &str, &str),
    ) -> Result<String, Error> {
        let element = |table: &Table, column: &str| {
            let ty = table.column(column).map(|column| match &column.ty {
                ValueType::Nullable(inner) => inner.as_ref().clone(),
                ty => ty.clone(),
            });
            match ty {
                Some(ValueType::Array(element)) => Some(*element),
                _ => None,
            }
        };
        let side = |(table, alias, column): (&Table, &str, &str)| column_sql(table, alias, column);
        let (array, scalar) = match (element(left.0, left.2), element(right.0, right.2)) {
            (None, None) => return Ok(format!("{} = {}", side(left)?, side(right)?)),
            (Some(element), None) => ((left, element), right),
            (None, Some(element)) => ((right, element), left),
            (Some(_), Some(_)) => return Err(unsupported("flat joins on two array columns")),
        };
        let ((array, element), scalar) = (array, scalar);
        let r = self.alias("r");
        let member = match element {
            ValueType::Uuid => array_element_id(&r),
            _ => format!("{r}.value"),
        };
        Ok(format!(
            "EXISTS (SELECT 1 FROM json_each({}) AS {r} WHERE {member} = {})",
            side(array)?,
            side(scalar)?
        ))
    }

    /// The conditions a row of `table` (as `alias`) meets for its required
    /// array relations: a parent whose required relation is missing rows is
    /// left out, at the root or nested.
    fn array_requirements(
        &mut self,
        table: &'a Table,
        alias: &str,
        arrays: &[ArraySubquery],
        wheres: &mut Vec<String>,
    ) -> Result<(), Error> {
        for array in arrays {
            if array.requirement == ArraySubqueryRequirement::Optional {
                continue;
            }
            let child = self.table(&array.table)?;
            let c = self.alias("c");
            let outer = column_sql(table, alias, &array.outer_column)?;
            let mut conds = vec![format!("({c}._st & {STATE_DELETED}) = 0")];
            for filter in &array.filters {
                conds.push(self.predicate(filter, child, &c)?);
            }
            let source = self.source(child)?;
            if is_array(table, &array.outer_column) {
                // A forward array relation: one child per element, in the
                // array's order and with its duplicates.
                let r = self.alias("r");
                conds.push(format!(
                    "{} = {}",
                    column_sql(child, &c, &array.inner_column)?,
                    array_element_id(&r)
                ));
                let matched = format!(
                    "SELECT 1 FROM json_each({outer}) AS {r} JOIN {source} AS {c} ON {}",
                    conds.join(" AND ")
                );
                wheres.push(match array.requirement {
                    ArraySubqueryRequirement::AtLeastOne => format!("EXISTS ({matched})"),
                    _ => {
                        format!("((SELECT count(*) FROM ({matched})) = json_array_length({outer}))")
                    }
                });
                continue;
            }
            if array.requirement == ArraySubqueryRequirement::MatchCorrelationCardinality {
                return Err(unsupported(
                    "cardinality-matching requirements on scalar relations",
                ));
            }
            conds.push(correlate(child, &c, &array.inner_column, &outer)?);
            wheres.push(format!(
                "EXISTS (SELECT 1 FROM {source} AS {c} WHERE {})",
                conds.join(" AND ")
            ));
        }
        Ok(())
    }

    fn validate_array(&mut self, array: &ArraySubquery) -> Result<(), Error> {
        let child = self.table(&array.table)?;
        column_sql(child, "c", &array.inner_column)?;
        projection(child, array.select.as_deref())?;
        for nested in &array.nested_arrays {
            self.validate_array(nested)?;
        }
        Ok(())
    }

    /// One array relation for one parent row.
    pub fn compile_children(
        mut self,
        array: &ArraySubquery,
        parent: &Value,
    ) -> Result<Compiled, Error> {
        let child = self.table(&array.table)?;
        let projection = projection(child, array.select.as_deref())?;
        let alias = "t";
        let elements = match parent {
            Value::Nullable(Some(inner)) => matches!(**inner, Value::Array(_)),
            value => matches!(value, Value::Array(_)),
        };
        let parent = match self.value_term(parent)? {
            Term::Bound(sql, _) => sql,
            _ => "NULL".to_owned(),
        };
        // A forward array relation reads one child per array element, in
        // the array's order and with its duplicates.
        let (from, correlated) = if elements {
            ("JOIN", array_element_id("r"))
        } else {
            ("", parent.clone())
        };
        let mut wheres = vec![
            format!("({alias}._st & {STATE_DELETED}) = 0"),
            correlate(child, alias, &array.inner_column, &correlated)?,
        ];
        for filter in &array.filters {
            wheres.push(self.predicate(filter, child, alias)?);
        }
        self.array_requirements(child, alias, &array.nested_arrays, &mut wheres)?;
        if let Some(session) = self.policy {
            wheres.push(self.read_policy(child, alias, session)?);
        }
        let order = self.order_sql(child, alias, &array.order_by)?;
        let columns = projection
            .iter()
            .map(|(name, ..)| Ok(format!(", {}", column_sql(child, alias, name)?)))
            .collect::<Result<String, Error>>()?;
        let limit = self.bind(Sql::Integer(array.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(array.offset as i64));
        let source = self.source(child)?;
        let source = if elements {
            format!("json_each({parent}) AS r {from} {source}")
        } else {
            source
        };
        let order = if elements && array.order_by.is_empty() {
            "r.key".to_owned()
        } else {
            order
        };
        let statement = |order: &str| {
            format!(
                "SELECT {alias}.id, {alias}._t, {alias}._st{columns} FROM {source} AS {alias} \
                 WHERE {} ORDER BY {order} LIMIT {limit} OFFSET {offset}",
                and(&wheres),
            )
        };
        let schema = self.schema;
        let mut compiled = Compiled {
            sql: statement(&order),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            joined: 0,
            versioned: false,
            union: false,
            partial: Vec::new(),
        };
        if !array.partial.is_empty() {
            compiled.partial = partial_projection(schema, &array.table, &array.partial, &compiled)?;
        }
        Ok(compiled)
    }

    /// One cell of row `id` of `table`, read through the same view and cut
    /// as the rows it came from: a relation's parent value. The SQL, its
    /// params and the cell's type.
    pub fn compile_cell(
        mut self,
        table: &str,
        column: &str,
        id: RowUuid,
    ) -> Result<
        (
            String,
            Vec<Sql>,
            ValueType,
            Option<crate::values::ColumnTags>,
        ),
        Error,
    > {
        let table = self.table(table)?;
        let column = table
            .column(column)
            .ok_or_else(|| unsupported("unknown correlation column"))?;
        let source = self.source(table)?;
        let id = self.bind(Sql::Blob(id.0.as_bytes().to_vec()));
        let sql = format!(
            "SELECT t.{} FROM {source} AS t WHERE t.id = {id}",
            quote(&column.stored)
        );
        Ok((sql, self.params, column.ty.clone(), column.tags.clone()))
    }

    /// A junction traversal from the row of `table` at `alias`: some live
    /// row of the joined table matches its correlations and filters, is
    /// readable by the session, and satisfies its nested joins in turn.
    fn join_exists(
        &mut self,
        join: &crate::query::JoinVia,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        let target = self.table(&join.table)?;
        let j = self.alias("j");
        let mut conds = vec![format!("({j}._st & {STATE_DELETED}) = 0")];
        if let Some(session) = self.policy {
            conds.push(self.read_policy(target, &j, session)?);
        }
        match join.target {
            JoinTarget::Uncorrelated => {
                if join.source_lookup.is_some() || !join.correlated_filters.is_empty() {
                    return Err(query_error(
                        "an uncorrelated join takes no source lookup or correlations",
                    ));
                }
            }
            JoinTarget::Column | JoinTarget::RowId => {
                let source = match &join.source_lookup {
                    // The value the join matches is a column of the row
                    // the source row references.
                    Some(lookup) => Some({
                        let referenced = self.table(&lookup.table)?;
                        let l = self.alias("l");
                        let mut found = vec![
                            format!("({l}._st & {STATE_DELETED}) = 0"),
                            format!(
                                "{l}.id = {}",
                                column_sql(table, alias, &lookup.row_id_source_column)?
                            ),
                        ];
                        if let Some(session) = self.policy {
                            found.push(self.read_policy(referenced, &l, session)?);
                        }
                        format!(
                            "(SELECT {} FROM {} AS {l} WHERE {})",
                            column_sql(referenced, &l, &lookup.value_column)?,
                            self.source(referenced)?,
                            found.join(" AND ")
                        )
                    }),
                    None => {
                        let column = join.source_column.as_deref().unwrap_or("id");
                        // A forward hop through an array of references: the
                        // joined row is one the array holds.
                        if is_array(table, column) {
                            let id = column_sql(target, &j, &join.on_column)?;
                            conds.push(correlate(table, alias, column, &id)?);
                            None
                        } else {
                            Some(column_sql(table, alias, column)?)
                        }
                    }
                };
                // A reverse hop through an array of references: the joined
                // row's array holds the source row.
                if let Some(source) = source {
                    conds.push(correlate(target, &j, &join.on_column, &source)?);
                }
            }
        }
        for correlation in &join.correlated_filters {
            conds.push(format!(
                "{} = {}",
                column_sql(target, &j, &correlation.join_column)?,
                column_sql(table, alias, &correlation.source_column)?
            ));
        }
        for filter in &join.filters {
            conds.push(self.predicate(filter, target, &j)?);
        }
        for nested in &join.nested_joins {
            conds.push(self.join_exists(nested, target, &j)?);
        }
        Ok(semi_join(format!("{} AS {j}", self.source(target)?), conds))
    }

    /// Root rows an access edge grants to a team reachable from the seed.
    /// The closure is `WITH RECURSIVE` over effective edge rows: `UNION`
    /// dedupes, so a fixpoint terminates on cycles; a depth bound carries
    /// the depth and stops expanding at it.
    fn reachable(
        &mut self,
        via: &crate::query::ReachableVia,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        if via.access_team_target == JoinTarget::Uncorrelated {
            return Err(unsupported("uncorrelated reachability"));
        }
        let access = self.table(&via.access_table)?;
        let edges = self.table(&via.edge_table)?;
        let r = self.alias("reach");
        // The initial frontier: the seed rows' team column, or one value.
        let seed = match &via.seed {
            Some(seed) => {
                let seeds = self.table(&seed.table)?;
                let s = self.alias("s");
                let mut conds = vec![format!("({s}._st & {STATE_DELETED}) = 0")];
                if let Some(session) = self.policy {
                    conds.push(self.read_policy(seeds, &s, session)?);
                }
                match (&seed.user_column, &seed.user_claim) {
                    (None, None) => {}
                    (Some(column), Some(claim)) => {
                        let value = self.claims.claim(claim);
                        let Some(value) = self.present(value)?.sql().map(str::to_owned) else {
                            return Ok("0".to_owned());
                        };
                        conds.push(format!("{} = {value}", column_sql(seeds, &s, column)?));
                    }
                    _ => {
                        return Err(query_error(
                            "a reachability seed names a user column without a claim",
                        ));
                    }
                }
                for filter in &seed.filters {
                    conds.push(self.predicate(filter, seeds, &s)?);
                }
                format!(
                    "SELECT {} AS team FROM {} AS {s} WHERE {}",
                    column_sql(seeds, &s, &seed.team_column)?,
                    self.source(seeds)?,
                    conds.join(" AND ")
                )
            }
            None => match self.operand(&via.from, table, alias)? {
                Term::Missing | Term::NaN => return Ok("0".to_owned()),
                term => format!("SELECT {} AS team", term.sql().expect("present")),
            },
        };
        let e = self.alias("e");
        let mut step = vec![format!("({e}._st & {STATE_DELETED}) = 0")];
        if let Some(session) = self.policy {
            step.push(self.read_policy(edges, &e, session)?);
        }
        for filter in &via.edge_filters {
            step.push(self.predicate(filter, edges, &e)?);
        }
        let member = column_sql(edges, &e, &via.edge_member_column)?;
        let parent = column_sql(edges, &e, &via.edge_parent_column)?;
        let edge_source = self.source(edges)?;
        let cte = match via.bound {
            crate::query::RecursionBound::Fixpoint => format!(
                "{r}(team) AS ({seed} UNION SELECT {parent} FROM {edge_source} AS {e} \
                 JOIN {r} ON {member} = {r}.team WHERE {})",
                and(&step)
            ),
            crate::query::RecursionBound::MaxDepth(depth) => {
                step.push(format!("{r}.d < {depth}"));
                format!(
                    "{r}(team, d) AS (SELECT team, 0 FROM ({seed}) UNION \
                     SELECT {parent}, {r}.d + 1 \
                     FROM {edge_source} AS {e} JOIN {r} ON {member} = {r}.team WHERE {})",
                    and(&step)
                )
            }
        };
        let a = self.alias("a");
        let mut conds = vec![
            format!("({a}._st & {STATE_DELETED}) = 0"),
            format!(
                "{} = {alias}.id",
                column_sql(access, &a, &via.access_row_column)?
            ),
            format!(
                "{} IN (SELECT team FROM {r})",
                column_sql(access, &a, &via.access_team_column)?
            ),
        ];
        if let Some(session) = self.policy {
            conds.push(self.read_policy(access, &a, session)?);
        }
        for filter in &via.access_filters {
            conds.push(self.predicate(filter, access, &a)?);
        }
        Ok(format!(
            "EXISTS (WITH RECURSIVE {cte} SELECT 1 FROM {} AS {a} WHERE {})",
            self.source(access)?,
            conds.join(" AND ")
        ))
    }

    // ---- query predicates ----------------------------------------------

    fn operand(&mut self, operand: &Operand, table: &Table, alias: &str) -> Result<Term, Error> {
        match operand {
            Operand::Column(column) => self.column_term(table, alias, column),
            Operand::Param(name) => {
                let value = self
                    .binding
                    .get(name)
                    .ok_or_else(|| query_error(format!("parameter `{name}` is not bound")))?
                    .clone();
                self.value_term(&value)
            }
            Operand::Claim(key) => {
                let value = self.claims.claim(key);
                self.present(value)
            }
            Operand::Literal(value) => self.value_term(value),
        }
    }

    pub fn predicate(
        &mut self,
        predicate: &Predicate,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        Ok(match predicate {
            Predicate::All(children) => {
                let parts = children
                    .iter()
                    .map(|child| self.predicate(child, table, alias))
                    .collect::<Result<Vec<_>, _>>()?;
                and(&parts)
            }
            Predicate::Any(children) => {
                let parts = children
                    .iter()
                    .map(|child| self.predicate(child, table, alias))
                    .collect::<Result<Vec<_>, _>>()?;
                or(&parts)
            }
            Predicate::Not(child) => format!(
                "(NOT COALESCE({}, 0))",
                self.predicate(child, table, alias)?
            ),
            Predicate::Eq(l, r) => {
                let sql = self.cmp2(CmpOp::Eq, l, r, table, alias)?;
                if is_id(l) || is_id(r) {
                    whole_key(table, alias, sql)
                } else {
                    sql
                }
            }
            Predicate::Ne(l, r) => self.cmp2(CmpOp::Ne, l, r, table, alias)?,
            Predicate::Lt(l, r) => self.cmp2(CmpOp::Lt, l, r, table, alias)?,
            Predicate::Lte(l, r) => self.cmp2(CmpOp::Le, l, r, table, alias)?,
            Predicate::Gt(l, r) => self.cmp2(CmpOp::Gt, l, r, table, alias)?,
            Predicate::Gte(l, r) => self.cmp2(CmpOp::Ge, l, r, table, alias)?,
            Predicate::In(left, options) => {
                let id = is_id(left);
                if let Operand::Column(column) = left {
                    for option in options {
                        check_candidate(table, column, option)?;
                    }
                }
                let left = self.operand(left, table, alias)?;
                let options = options
                    .iter()
                    .map(|option| self.operand(option, table, alias))
                    .collect::<Result<Vec<_>, _>>()?;
                let sql = self.in_list(left, options)?;
                if id {
                    whole_key(table, alias, sql)
                } else {
                    sql
                }
            }
            Predicate::Contains(l, r) => {
                if let Operand::Column(column) = l {
                    check_haystack(table, column)?;
                }
                let array = matches!(l, Operand::Column(column) if is_array(table, column));
                let l = self.operand(l, table, alias)?;
                let r = self.operand(r, table, alias)?;
                if array {
                    self.array_contains(l, r)
                } else {
                    self.contains(l, r)
                }
            }
            Predicate::IsNull(operand) => match self.operand(operand, table, alias)? {
                Term::Missing => "1".to_owned(),
                Term::NaN => "0".to_owned(),
                term => format!("({} IS NULL)", term.sql().expect("present")),
            },
            Predicate::EnumMatch { .. } => {
                return Err(unsupported("enum payload predicates"));
            }
        })
    }

    fn cmp2(
        &mut self,
        op: CmpOp,
        left: &Operand,
        right: &Operand,
        table: &Table,
        alias: &str,
    ) -> Result<String, Error> {
        let left = self.operand(left, table, alias)?;
        let right = self.operand(right, table, alias)?;
        self.cmp(op, left, right)
    }

    // ---- policies -------------------------------------------------------

    /// The SELECT policy of `table` at `alias`, as `session`.
    pub fn read_policy(
        &mut self,
        table: &Table,
        alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        self.operation_policy(table, Operation::Select, alias, session)
    }

    pub fn operation_policy(
        &mut self,
        table: &Table,
        operation: Operation,
        alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        if session.system {
            return Ok("1".to_owned());
        }
        self.under_policy_schema(table, |ctx, table| match table.policy(operation) {
            None => Ok("0".to_owned()),
            Some(policy) => {
                ctx.policy_expr(policy, table, alias, None, session, DEFAULT_INHERITS_DEPTH)
            }
        })
    }

    /// Compile a policy of `table` under the schema whose names its policies
    /// use (see [`Schema::policies_from`]); its SQL reads the same stored
    /// rows either way.
    fn under_policy_schema(
        &mut self,
        table: &Table,
        body: impl FnOnce(&mut Self, &Table) -> Result<String, Error>,
    ) -> Result<String, Error> {
        let Some(current) = self.schema.policies_from.as_deref() else {
            return body(self, table);
        };
        let Some(governing) = current.by_stored(&table.stored) else {
            return Ok("0".to_owned());
        };
        let own = std::mem::replace(&mut self.schema, current);
        let result = body(self, governing);
        self.schema = own;
        result
    }

    /// A specific policy clause (e.g. UPDATE's WITH CHECK) at `alias`.
    pub fn policy_clause(
        &mut self,
        policy: Option<&PolicyExpr>,
        table: &Table,
        alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        if session.system {
            return Ok("1".to_owned());
        }
        self.under_policy_schema(table, |ctx, table| match policy {
            None => Ok("0".to_owned()),
            Some(policy) => {
                ctx.policy_expr(policy, table, alias, None, session, DEFAULT_INHERITS_DEPTH)
            }
        })
    }

    fn policy_value(
        &mut self,
        value: &PolicyValue,
        outer: Option<(&Table, &str)>,
        session: &Session,
    ) -> Result<Term, Error> {
        match value {
            PolicyValue::Literal(value) => self.value_term(&public_value(value)?),
            PolicyValue::SessionRef(path)
                if path.first().map(String::as_str) == Some("__jazz_outer_row") =>
            {
                let (table, alias) =
                    outer.ok_or_else(|| query_error("outer row reference outside EXISTS"))?;
                let column = path
                    .get(1)
                    .ok_or_else(|| query_error("outer row reference without a column"))?;
                self.column_term(table, alias, column)
            }
            PolicyValue::SessionRef(path) => {
                let value = session.resolve(path);
                self.present(value)
            }
        }
    }

    fn policy_expr(
        &mut self,
        policy: &PolicyExpr,
        table: &Table,
        alias: &str,
        outer: Option<(&Table, &str)>,
        session: &Session,
        depth: usize,
    ) -> Result<String, Error> {
        let recurse = |ctx: &mut Self, policy: &PolicyExpr| {
            ctx.policy_expr(policy, table, alias, outer, session, depth)
        };
        Ok(match policy {
            PolicyExpr::True => "1".to_owned(),
            PolicyExpr::False => "0".to_owned(),
            PolicyExpr::And(parts) => {
                let parts = parts
                    .iter()
                    .map(|p| recurse(self, p))
                    .collect::<Result<Vec<_>, _>>()?;
                and(&parts)
            }
            PolicyExpr::Or(parts) => {
                let parts = parts
                    .iter()
                    .map(|p| recurse(self, p))
                    .collect::<Result<Vec<_>, _>>()?;
                or(&parts)
            }
            PolicyExpr::Not(inner) => format!("(NOT COALESCE({}, 0))", recurse(self, inner)?),
            PolicyExpr::Cmp {
                column,
                op,
                value: PolicyValue::SessionRef(path),
            } if crate::values::author_record(column).is_some()
                && path.first().map(String::as_str) == Some("user") =>
            {
                self.author_cmp(table, alias, column, op, path, session)?
            }
            PolicyExpr::Cmp {
                column,
                op,
                value: PolicyValue::SessionRef(path),
            } if is_uuid(table, column)
                && path.first().map(String::as_str) != Some("__jazz_outer_row") =>
            {
                // A claim is JSON, so a UUID claim arrives as its string:
                // compare it with a UUID column as the UUID it spells.
                let value = match session.resolve(path) {
                    Some(Value::String(text)) => text.parse().map(Value::Uuid).ok(),
                    value => value,
                };
                let left = self.column_term(table, alias, column)?;
                let right = self.present(value)?;
                self.cmp(op.clone(), left, right)?
            }
            PolicyExpr::Cmp { column, op, value } => {
                let left = self.column_term(table, alias, column)?;
                let right = self.policy_value(value, outer, session)?;
                self.cmp(op.clone(), left, right)?
            }
            PolicyExpr::SessionCmp { path, op, value } => {
                let left = session.resolve(path);
                let left = self.present(left)?;
                let right = self.value_term(&public_value(value)?)?;
                self.cmp(op.clone(), left, right)?
            }
            PolicyExpr::IsNull { column } => {
                format!("({} IS NULL)", column_sql(table, alias, column)?)
            }
            PolicyExpr::IsNotNull { column } => {
                format!("({} IS NOT NULL)", column_sql(table, alias, column)?)
            }
            PolicyExpr::SessionIsNull { path } => bool_sql(session.is_null(path)),
            PolicyExpr::SessionIsNotNull { path } => bool_sql(session.resolve(path).is_some()),
            PolicyExpr::Contains { column, value } => {
                let left = self.column_term(table, alias, column)?;
                let right = self.policy_value(value, outer, session)?;
                if is_array(table, column) {
                    self.array_contains(left, right)
                } else {
                    self.contains(left, right)
                }
            }
            PolicyExpr::In {
                column,
                session_path,
            } => {
                let left = self.column_term(table, alias, column)?;
                let options = match session.resolve(session_path) {
                    Some(Value::Array(values)) => values
                        .iter()
                        .map(|value| self.value_term(value))
                        .collect::<Result<Vec<_>, _>>()?,
                    Some(value) => vec![self.value_term(&value)?],
                    None => Vec::new(),
                };
                self.in_list(left, options)?
            }
            PolicyExpr::InList { column, values } => {
                let left = self.column_term(table, alias, column)?;
                let options = values
                    .iter()
                    .map(|value| self.policy_value(value, outer, session))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            PolicyExpr::SessionInList { path, values } => {
                let left = session.resolve(path);
                let left = self.present(left)?;
                let options = values
                    .iter()
                    .map(|value| self.value_term(&public_value(value)?))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            // Both sides are known at compile time: an array claim holds the
            // value, or a text claim contains it.
            PolicyExpr::SessionContains { path, value } => {
                let needle = public_value(value)?;
                let same = |a: &Value, b: &Value| match (a, b) {
                    (Value::Uuid(u), Value::String(s)) | (Value::String(s), Value::Uuid(u)) => {
                        u.to_string() == *s
                    }
                    (a, b) => a == b,
                };
                bool_sql(match session.resolve(path) {
                    Some(Value::Array(items)) => items.iter().any(|item| {
                        unwrap_nullable(item.clone()).is_some_and(|i| same(&i, &needle))
                    }),
                    Some(Value::String(text)) => match &needle {
                        Value::String(part) => text.contains(part.as_str()),
                        _ => false,
                    },
                    _ => false,
                })
            }
            // Policy EXISTS reads its table as raw evidence: the dependency
            // table's own policy is not composed in (INV-RLS-21).
            PolicyExpr::Exists {
                table: target,
                condition,
            } => {
                let target = self.table(target)?;
                let e = self.alias("e");
                let inner =
                    self.policy_expr(condition, target, &e, Some((table, alias)), session, depth)?;
                semi_join(
                    format!("{} AS {e}", self.source(target)?),
                    vec![format!("({e}._st & {STATE_DELETED}) = 0"), inner],
                )
            }
            // The relation's outer row is the row under check, also inside
            // an EXISTS over another table (as `__jazz_outer_row` is).
            PolicyExpr::ExistsRel { rel } => {
                let (table, alias) = outer.unwrap_or((table, alias));
                self.exists_rel(rel, table, alias, session)?
            }
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok("0".to_owned());
                }
                let target_name = table.references.get(via_column).ok_or_else(|| {
                    query_error(format!(
                        "INHERITS via `{via_column}`, which is not a reference"
                    ))
                })?;
                let target = self.table(target_name)?;
                if let Some(chain) = recursive_base(table, target, *operation, via_column) {
                    // Distance n is checked with depth - n left, so the
                    // hop below the first stops at its own bound.
                    let bound = depth.min(chain.depth.map_or(usize::MAX, |d| d.saturating_add(1)));
                    return self
                        .ancestor_closure(target, alias, via_column, &chain, bound, session);
                }
                let p = self.alias("p");
                let inner = match target.policy(*operation) {
                    None => "0".to_owned(),
                    Some(policy) => {
                        self.policy_expr(policy, target, &p, None, session, depth - 1)?
                    }
                };
                // An array reference inherits from any row it names.
                let on = self.join_key((target, &p, "id"), (table, alias, via_column))?;
                semi_join(
                    format!("{} AS {p}", self.source(target)?),
                    vec![on, format!("({p}._st & {STATE_DELETED}) = 0"), inner],
                )
            }
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok("0".to_owned());
                }
                let source = self.table(source_table)?;
                let s = self.alias("s");
                let inner = match source.policy(*operation) {
                    None => "0".to_owned(),
                    Some(policy) => {
                        self.policy_expr(policy, source, &s, None, session, depth - 1)?
                    }
                };
                // An array reference grants when it lists the row's id.
                semi_join(
                    format!("{} AS {s}", self.source(source)?),
                    vec![
                        correlate(source, &s, via_column, &format!("{alias}.id"))?,
                        format!("({s}._st & {STATE_DELETED}) = 0"),
                        inner,
                    ],
                )
            }
        })
    }

    /// A row's ancestors through its own reference `via`, up to `bound`
    /// hops (each taken from an alive row meeting the chain's guards), any
    /// of which (alive) meets one of its base disjuncts: a parent chain's
    /// policy as one `WITH RECURSIVE` closure, not one nested EXISTS per
    /// level (Durable Object SQLite caps expression depth at 100). The
    /// caller checked the first hop's guards. `UNION` dedupes, so a cycle
    /// terminates.
    fn ancestor_closure(
        &mut self,
        table: &Table,
        alias: &str,
        via: &str,
        chain: &ParentChain<'_>,
        bound: usize,
        session: &Session,
    ) -> Result<String, Error> {
        let c = self.alias("anc");
        let p = self.alias("p");
        let a = self.alias("a");
        let seed = column_sql(table, alias, via)?;
        let parent = column_sql(table, &p, via)?;
        let steps = self.source(table)?;
        let mut step = vec![
            format!("({p}._st & {STATE_DELETED}) = 0"),
            format!("{c}.d < {bound}"),
        ];
        for guard in &chain.guards {
            step.push(self.policy_expr(guard, table, &p, None, session, 0)?);
        }
        let cte = format!(
            "{c}(id, d) AS (SELECT {seed}, 1 UNION SELECT {parent}, {c}.d + 1 FROM {steps} AS {p} \
             JOIN {c} ON {p}.id = {c}.id WHERE {})",
            and(&step)
        );
        let base = chain
            .base
            .iter()
            .map(|part| self.policy_expr(part, table, &a, None, session, 0))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!(
            "EXISTS (WITH RECURSIVE {cte} SELECT 1 FROM {} AS {a} JOIN {c} ON {a}.id = {c}.id \
             WHERE ({a}._st & {STATE_DELETED}) = 0 AND {})",
            self.source(table)?,
            or(&base)
        ))
    }

    // ---- relation IR ----------------------------------------------------

    fn exists_rel(
        &mut self,
        rel: &RelationExpr,
        outer: &Table,
        outer_alias: &str,
        session: &Session,
    ) -> Result<String, Error> {
        // Correlated only by `key = outer row id`, the relation runs once
        // as `outer.id IN (SELECT key ...)`: SQLite materializes an
        // uncorrelated IN list, where an EXISTS re-runs its body (a
        // recursive closure included) for every outer row.
        let key = std::cell::RefCell::new(None);
        let keyed = self.lift_row_keys && outer_key_only(rel);
        let scope = RelScope {
            outer: Some((outer, outer_alias)),
            frontier: None,
            session,
            key: keyed.then_some(&key),
        };
        let mut rel = self.relation(rel, &scope)?;
        let with = if rel.ctes.is_empty() {
            String::new()
        } else {
            format!("WITH RECURSIVE {} ", rel.ctes.join(", "))
        };
        if let Some(key) = key.into_inner() {
            // A NULL key matches no row id, and keeps `IN` two-valued. `IN`
            // compares with the left side's collation, as `=` on the key
            // did: row ids are blobs, which have none.
            rel.wheres.push(format!("{key} IS NOT NULL"));
            return Ok(format!(
                "{outer_alias}.id IN ({with}SELECT {key} FROM {} WHERE {})",
                rel.sql_from_clause(),
                and(&rel.wheres)
            ));
        }
        Ok(format!(
            "EXISTS ({with}SELECT 1 FROM {} WHERE {})",
            rel.sql_from_clause(),
            and(&rel.wheres)
        ))
    }

    fn relation(&mut self, expr: &RelationExpr, scope: &RelScope<'_>) -> Result<Rel, Error> {
        match expr {
            RelationExpr::TableScan { table, alias } => {
                let table = self.table(table)?;
                let r = self.alias("r");
                Ok(Rel {
                    ctes: Vec::new(),
                    wheres: vec![format!("({r}._st & {STATE_DELETED}) = 0")],
                    from: vec![Src {
                        sql: self.source(table)?,
                        alias: r,
                        scope: alias.clone().unwrap_or_else(|| table.name.clone()),
                        table: Some(table.name.clone()),
                    }],
                    id: None,
                })
            }
            RelationExpr::Filter { input, predicate } => {
                let mut rel = self.relation(input, scope)?;
                let condition = self.rel_predicate(predicate, &rel, scope)?;
                rel.wheres.push(condition);
                Ok(rel)
            }
            RelationExpr::Join {
                left,
                right,
                on,
                join_kind,
            } => {
                if *join_kind != crate::query::RelationJoinKind::Inner {
                    return Err(unsupported("outer relation joins"));
                }
                let mut left = self.relation(left, scope)?;
                let right = self.relation(right, scope)?;
                let mut conditions = Vec::new();
                for condition in on {
                    conditions.push(format!(
                        "{} = {}",
                        self.resolve(&left, &condition.left)?,
                        self.resolve(&right, &condition.right)?
                    ));
                }
                left.ctes.extend(right.ctes);
                left.from.extend(right.from);
                left.wheres.extend(right.wheres);
                left.wheres.extend(conditions);
                left.id = None;
                Ok(left)
            }
            RelationExpr::Project { input, columns } => {
                let mut rel = self.relation(input, scope)?;
                if let Some(column) = columns.iter().find(|column| column.alias == "id") {
                    rel.id = Some(match &column.expr {
                        RelationProjectExpr::Column(column) => self.resolve(&rel, column)?,
                        RelationProjectExpr::RowId(row) => self.row_id(&rel, *row, scope)?,
                    });
                }
                Ok(rel)
            }
            RelationExpr::Gather {
                seed,
                step,
                frontier_key,
                bound,
                dedupe_key,
            } => {
                let current = RelationKeyRef::RowId(RelationRowIdRef::Current);
                if *frontier_key != current || dedupe_key.iter().any(|key| *key != current) {
                    return Err(unsupported("gather frontiers other than the row id"));
                }
                let g = self.alias("g");
                let seed = self.relation(seed, scope)?;
                let step_scope = RelScope {
                    frontier: Some(format!("{g}.id")),
                    ..*scope
                };
                let mut step = self.relation(step, &step_scope)?;
                // A step reaches rows of the seed's table: when it projects
                // no id, the reached row is the last such source (a hop
                // joined after the edge it walks).
                if step.id.is_none()
                    && let Some(table) = seed.from.first().and_then(|src| src.table.as_ref())
                    && let Some(reached) = step
                        .from
                        .iter()
                        .rev()
                        .find(|src| src.table.as_ref() == Some(table))
                {
                    step.id = Some(format!("{}.id", reached.alias));
                }
                let (max, guard) = match bound {
                    RecursionBound::MaxDepth(depth) => (*depth, false),
                    RecursionBound::Fixpoint => (FIXPOINT_GUARD, true),
                };
                let _ = guard;
                let cte = format!(
                    "{g}(id, d) AS (SELECT {}, 0 FROM {} WHERE {} \
                     UNION SELECT {}, {g}.d + 1 FROM {g}, {} WHERE {g}.d < {max} AND {})",
                    seed.output_id(),
                    seed.sql_from_clause(),
                    and(&seed.wheres),
                    step.output_id(),
                    step.sql_from_clause(),
                    and(&step.wheres),
                );
                let mut ctes = seed.ctes;
                ctes.extend(step.ctes);
                ctes.push(cte);
                let a = self.alias("ga");
                Ok(Rel {
                    ctes,
                    wheres: Vec::new(),
                    from: vec![Src {
                        sql: g,
                        alias: a,
                        scope: String::new(),
                        table: None,
                    }],
                    id: None,
                })
            }
            other => Err(unsupported(format!(
                "relation `{}`",
                format!("{other:?}")
                    .split([' ', '{', '('])
                    .next()
                    .unwrap_or("?")
            ))),
        }
    }

    fn resolve(&self, rel: &Rel, column: &RelationColumnRef) -> Result<String, Error> {
        let has = |src: &Src| match &src.table {
            Some(table) => self.schema.tables[table].has_column(&column.column),
            None => column.column == "id",
        };
        let src = match &column.scope {
            Some(scope) => rel
                .from
                .iter()
                .find(|src| &src.scope == scope)
                .or_else(|| rel.from.iter().find(|src| src.table.is_none() && has(src))),
            None => rel.from.iter().find(|src| has(src)),
        }
        .ok_or_else(|| query_error(format!("relation column {column:?} does not resolve")))?;
        if !has(src) {
            return Err(query_error(format!(
                "relation column {column:?} does not exist"
            )));
        }
        Ok(match &src.table {
            Some(table) => column_sql(&self.schema.tables[table], &src.alias, &column.column)?,
            None if column.column == "id" => format!("{}.id", src.alias),
            None => format!("{}.{}", src.alias, quote(&column.column)),
        })
    }

    /// [`Self::resolve`] as an operand.
    fn resolve_term(&self, rel: &Rel, column: &RelationColumnRef) -> Result<Term, Error> {
        let sql = self.resolve(rel, column)?;
        let src = rel.from.iter().find(|src| {
            src.table.as_ref().is_some_and(|table| {
                sql.starts_with(&format!("{}.", src.alias))
                    && self.schema.tables[table].has_column(&column.column)
            })
        });
        Ok(match src.and_then(|src| src.table.as_ref()) {
            Some(table) => self.large_term(&self.schema.tables[table], sql, &column.column),
            None => Term::Sql(sql),
        })
    }

    fn row_id(
        &self,
        rel: &Rel,
        row: RelationRowIdRef,
        scope: &RelScope<'_>,
    ) -> Result<String, Error> {
        Ok(match row {
            RelationRowIdRef::Current => rel
                .from
                .first()
                .map(|src| format!("{}.id", src.alias))
                .ok_or_else(|| query_error("current row id of an empty relation"))?,
            RelationRowIdRef::Outer => scope
                .outer
                .map(|(_, alias)| format!("{alias}.id"))
                .ok_or_else(|| query_error("outer row id outside a policy"))?,
            RelationRowIdRef::Frontier => scope
                .frontier
                .clone()
                .ok_or_else(|| query_error("frontier row id outside a gather step"))?,
        })
    }

    fn rel_value(
        &mut self,
        value: &RelationValueRef,
        rel: &Rel,
        scope: &RelScope<'_>,
    ) -> Result<Term, Error> {
        match value {
            RelationValueRef::Literal(json) => self.value_term(&json_value(json)?),
            RelationValueRef::Param(name) => {
                let value = self
                    .binding
                    .get(name)
                    .ok_or_else(|| query_error(format!("parameter `{name}` is not bound")))?
                    .clone();
                self.value_term(&value)
            }
            RelationValueRef::SessionRef(path) => {
                let value = scope.session.resolve(path);
                self.present(value)
            }
            RelationValueRef::OuterColumn(column) => {
                let (table, alias) = scope
                    .outer
                    .ok_or_else(|| query_error("outer column outside a policy"))?;
                self.column_term(table, alias, &column.column)
            }
            RelationValueRef::FrontierColumn(_) => Err(unsupported("frontier columns")),
            RelationValueRef::RowId(row) => Ok(Term::Sql(self.row_id(rel, *row, scope)?)),
        }
    }

    fn rel_predicate(
        &mut self,
        predicate: &RelationPredicate,
        rel: &Rel,
        scope: &RelScope<'_>,
    ) -> Result<String, Error> {
        Ok(match predicate {
            RelationPredicate::True => "1".to_owned(),
            RelationPredicate::False => "0".to_owned(),
            RelationPredicate::And(parts) => {
                let parts = parts
                    .iter()
                    .map(|part| self.rel_predicate(part, rel, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                and(&parts)
            }
            RelationPredicate::Or(parts) => {
                let parts = parts
                    .iter()
                    .map(|part| self.rel_predicate(part, rel, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                or(&parts)
            }
            RelationPredicate::Not(part) => format!(
                "(NOT COALESCE({}, 0))",
                self.rel_predicate(part, rel, scope)?
            ),
            RelationPredicate::Cmp {
                left,
                op: RelationCmpOp::Eq,
                right,
            } if scope.key.is_some() && is_outer_id(right) => {
                // The correlation [`Self::exists_rel`] lifts out.
                let key = scope.key.expect("key");
                *key.borrow_mut() = Some(self.resolve(rel, left)?);
                "1".to_owned()
            }
            RelationPredicate::Cmp { left, op, right } => {
                let left = self.resolve_term(rel, left)?;
                let right = self.rel_value(right, rel, scope)?;
                let op = match op {
                    RelationCmpOp::Eq => CmpOp::Eq,
                    RelationCmpOp::Ne => CmpOp::Ne,
                    RelationCmpOp::Lt => CmpOp::Lt,
                    RelationCmpOp::Le => CmpOp::Le,
                    RelationCmpOp::Gt => CmpOp::Gt,
                    RelationCmpOp::Ge => CmpOp::Ge,
                };
                self.cmp(op, left, right)?
            }
            RelationPredicate::IsNull { column } => {
                format!("({} IS NULL)", self.resolve(rel, column)?)
            }
            RelationPredicate::IsNotNull { column } => {
                format!("({} IS NOT NULL)", self.resolve(rel, column)?)
            }
            RelationPredicate::In { left, values } => {
                let left = self.resolve_term(rel, left)?;
                let options = values
                    .iter()
                    .map(|value| self.rel_value(value, rel, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                self.in_list(left, options)?
            }
            RelationPredicate::Contains { left, right } => {
                let left = self.resolve_term(rel, left)?;
                let right = self.rel_value(right, rel, scope)?;
                self.contains(left, right)
            }
            RelationPredicate::EnumMatch { .. } => {
                return Err(unsupported("enum payload relation predicates"));
            }
        })
    }
}

struct RelScope<'s> {
    outer: Option<(&'s Table, &'s str)>,
    frontier: Option<String>,
    session: &'s Session,
    /// Set when the relation's one outer reference is a conjoined
    /// `column = outer row id` ([`outer_key_only`]): that comparison
    /// compiles to true and leaves its column's SQL here.
    key: Option<&'s std::cell::RefCell<Option<String>>>,
}

impl Clone for RelScope<'_> {
    fn clone(&self) -> Self {
        Self {
            outer: self.outer,
            frontier: self.frontier.clone(),
            session: self.session,
            key: self.key,
        }
    }
}

/// Whether `query`'s root rows are picked by id (`id = x`, `id IN (..)`), as
/// a recheck of changed rows picks them.
fn narrowed_to_ids(query: &Query) -> bool {
    let is_id = |operand: &Operand| matches!(operand, Operand::Column(column) if column == "id");
    query.filters.iter().any(|filter| match filter {
        Predicate::Eq(left, right) => is_id(left) || is_id(right),
        Predicate::In(left, _) => is_id(left),
        _ => false,
    })
}

/// A value naming the outer row's id, which is never NULL.
fn is_outer_id(value: &RelationValueRef) -> bool {
    match value {
        RelationValueRef::RowId(RelationRowIdRef::Outer) => true,
        RelationValueRef::OuterColumn(column) => column.column == "id",
        _ => false,
    }
}

/// Whether `rel` refers to its outer row exactly once, as `column = outer
/// row id` in a conjunction of a filter outside any gather: then it holds
/// for an outer row exactly when that row's id is among the column's values
/// in the relation without the comparison.
fn outer_key_only(rel: &RelationExpr) -> bool {
    fn refs(value: &RelationValueRef) -> usize {
        matches!(
            value,
            RelationValueRef::OuterColumn(_) | RelationValueRef::RowId(RelationRowIdRef::Outer)
        ) as usize
    }
    fn key_refs(key: &RelationKeyRef) -> usize {
        matches!(key, RelationKeyRef::RowId(RelationRowIdRef::Outer)) as usize
    }
    fn predicate_refs(predicate: &RelationPredicate) -> usize {
        match predicate {
            RelationPredicate::Cmp { right, .. } | RelationPredicate::Contains { right, .. } => {
                refs(right)
            }
            RelationPredicate::In { values, .. } => values.iter().map(refs).sum(),
            RelationPredicate::EnumMatch { payload, .. } | RelationPredicate::Not(payload) => {
                predicate_refs(payload)
            }
            RelationPredicate::And(parts) | RelationPredicate::Or(parts) => {
                parts.iter().map(predicate_refs).sum()
            }
            RelationPredicate::IsNull { .. }
            | RelationPredicate::IsNotNull { .. }
            | RelationPredicate::True
            | RelationPredicate::False => 0,
        }
    }
    fn expr_refs(rel: &RelationExpr) -> usize {
        match rel {
            RelationExpr::TableScan { .. } => 0,
            RelationExpr::Filter { input, predicate } => {
                expr_refs(input) + predicate_refs(predicate)
            }
            RelationExpr::Join { left, right, .. } => expr_refs(left) + expr_refs(right),
            RelationExpr::Project { input, columns } => {
                expr_refs(input)
                    + columns
                        .iter()
                        .filter(|column| {
                            column.expr == RelationProjectExpr::RowId(RelationRowIdRef::Outer)
                        })
                        .count()
            }
            RelationExpr::Gather {
                seed,
                step,
                frontier_key,
                dedupe_key,
                ..
            } => {
                expr_refs(seed)
                    + expr_refs(step)
                    + key_refs(frontier_key)
                    + dedupe_key.iter().map(key_refs).sum::<usize>()
            }
            // Not compiled ([`Compiler::relation`] refuses them): count
            // them as correlated so nothing is lifted.
            _ => 2,
        }
    }
    /// A conjoined `column = outer row id` among `predicate`'s terms.
    fn keyed(predicate: &RelationPredicate) -> bool {
        match predicate {
            RelationPredicate::Cmp {
                op: RelationCmpOp::Eq,
                right,
                ..
            } => is_outer_id(right),
            RelationPredicate::And(parts) => parts.iter().any(keyed),
            _ => false,
        }
    }
    /// A filter keyed so, outside any gather.
    fn has_key(rel: &RelationExpr) -> bool {
        match rel {
            RelationExpr::Filter { input, predicate } => keyed(predicate) || has_key(input),
            RelationExpr::Join { left, right, .. } => has_key(left) || has_key(right),
            RelationExpr::Project { input, .. } => has_key(input),
            _ => false,
        }
    }
    expr_refs(rel) == 1 && has_key(rel)
}

struct Src {
    sql: String,
    alias: String,
    scope: String,
    /// `None` for a gather CTE, whose only column is `id`.
    table: Option<String>,
}

struct Rel {
    ctes: Vec<String>,
    from: Vec<Src>,
    wheres: Vec<String>,
    /// The projected `id`, when a projection named one.
    id: Option<String>,
}

impl Rel {
    fn sql_from_clause(&self) -> String {
        self.from
            .iter()
            .map(|src| format!("{} AS {}", src.sql, src.alias))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn output_id(&self) -> String {
        self.id
            .clone()
            .unwrap_or_else(|| format!("{}.id", self.from[0].alias))
    }
}

/// A projected cell: its name, its type and an enum column's tags.
pub type Projected = (String, ValueType, Option<crate::values::ColumnTags>);

fn projected(column: &crate::values::Column) -> Projected {
    (column.name.clone(), column.ty.clone(), column.tags.clone())
}

fn projection(table: &Table, select: Option<&[String]>) -> Result<Vec<Projected>, Error> {
    match select {
        None => Ok(table.columns.iter().map(projected).collect()),
        Some(select) => select
            .iter()
            .filter(|name| name.as_str() != "id")
            .map(|name| {
                table
                    .column(name)
                    .map(projected)
                    .ok_or_else(|| query_error(format!("unknown column `{}.{name}`", table.name)))
            })
            .collect(),
    }
}

/// One source of a flat join.
struct FlatSource<'a> {
    /// Its effective name: alias or table.
    name: String,
    /// The schema table it reads (a history source's table).
    base: &'a Table,
    /// What it reads as: the table, or a history source's versions.
    table: Table,
    alias: String,
    history: bool,
    /// Its SQL source; a history source's waits for its bound.
    sql: Option<String>,
}

/// The conjuncts of a filter: an `All` contributes each of its parts.
fn flatten_all<'p>(predicate: &'p Predicate, out: &mut Vec<&'p Predicate>) {
    match predicate {
        Predicate::All(parts) => parts.iter().for_each(|part| flatten_all(part, out)),
        predicate => out.push(predicate),
    }
}

/// The row id an array element names: array cells hold uuids as their
/// hyphenated text (see [`crate::values::to_sql`]).
fn array_element_id(element: &str) -> String {
    format!("unhex(replace({element}.value, '-', ''))")
}

/// What a value or column type is, for operand type checks: numbers of
/// any width compare with each other, a UUID with its string spelling.
#[derive(PartialEq, Debug)]
enum Kind {
    Number,
    Text,
    Bool,
    Uuid,
    Array,
    Bytes,
    Other,
}

fn type_kind(ty: &ValueType) -> Kind {
    match ty {
        ValueType::Nullable(inner) => type_kind(inner),
        ValueType::U8
        | ValueType::U16
        | ValueType::U32
        | ValueType::U64
        | ValueType::I32
        | ValueType::I64
        | ValueType::F64 => Kind::Number,
        ValueType::String => Kind::Text,
        ValueType::Bool => Kind::Bool,
        ValueType::Uuid => Kind::Uuid,
        ValueType::Array(_) => Kind::Array,
        ValueType::Bytes => Kind::Bytes,
        _ => Kind::Other,
    }
}

fn value_kind(value: &Value) -> Option<Kind> {
    Some(match value {
        Value::Nullable(None) => return None,
        Value::Nullable(Some(inner)) => return value_kind(inner),
        Value::U8(_)
        | Value::U16(_)
        | Value::U32(_)
        | Value::U64(_)
        | Value::U48(_)
        | Value::I32(_)
        | Value::I64(_)
        | Value::F64(_) => Kind::Number,
        Value::String(_) => Kind::Text,
        Value::Bool(_) => Kind::Bool,
        Value::Uuid(_) => Kind::Uuid,
        Value::Array(_) => Kind::Array,
        Value::Bytes(_) => Kind::Bytes,
        _ => Kind::Other,
    })
}

fn value_type_name(value: &Value) -> String {
    match value {
        Value::Nullable(Some(inner)) => value_type_name(inner),
        value => format!("{value:?}")
            .split(['(', ' ', '{'])
            .next()
            .unwrap_or_default()
            .to_owned(),
    }
}

/// An `in` literal must be a whole value of the column's type: a scalar
/// is no candidate for an array column (that is `contains`).
fn check_candidate(table: &Table, column: &str, option: &Operand) -> Result<(), Error> {
    let (Operand::Literal(value), Some(declared)) = (option, table.column(column)) else {
        return Ok(());
    };
    let (want, Some(got)) = (type_kind(&declared.ty), value_kind(value)) else {
        return Ok(());
    };
    let compatible = want == got
        || want == Kind::Other
        || got == Kind::Other
        || (want == Kind::Uuid && got == Kind::Text);
    if compatible {
        return Ok(());
    }
    Err(query_error(format!(
        "in candidate for column {column} has type {}, but the column has type {:?}",
        value_type_name(value),
        declared.ty
    )))
}

/// `contains` searches text, bytes or an array.
fn check_haystack(table: &Table, column: &str) -> Result<(), Error> {
    let Some(declared) = table.column(column) else {
        return Ok(());
    };
    match type_kind(&declared.ty) {
        Kind::Text | Kind::Bytes | Kind::Array | Kind::Other => Ok(()),
        _ => Err(query_error(format!(
            "operand type mismatch: contains on {column}, which has type {:?}",
            declared.ty
        ))),
    }
}

/// Whether `column` is a UUID column (nullable or not).
fn is_uuid(table: &Table, column: &str) -> bool {
    table.column(column).is_some_and(|column| match &column.ty {
        ValueType::Nullable(inner) => matches!(**inner, ValueType::Uuid),
        ty => matches!(ty, ValueType::Uuid),
    })
}

/// Whether `column` is an array column (nullable or not).
/// A child row of `table` at `alias` correlates with `value` by `column`:
/// equal to it, or for an array column (a reverse array relation),
/// holding it.
fn correlate(table: &Table, alias: &str, column: &str, value: &str) -> Result<String, Error> {
    let sql = column_sql(table, alias, column)?;
    if is_array(table, column) {
        let e = format!("{alias}_e");
        return Ok(format!(
            "EXISTS (SELECT 1 FROM json_each({sql}) AS {e} WHERE {} = {value})",
            array_element_id(&e)
        ));
    }
    Ok(format!("{sql} = {value}"))
}

pub(crate) fn is_array(table: &Table, column: &str) -> bool {
    let Some(column) = table.column(column) else {
        return false;
    };
    let ty = match &column.ty {
        ValueType::Nullable(inner) => inner.as_ref(),
        ty => ty,
    };
    matches!(ty, ValueType::Array(_))
}

pub(crate) fn column_sql(table: &Table, alias: &str, column: &str) -> Result<String, Error> {
    if column == "id" {
        return Ok(format!("{alias}.id"));
    }
    if let Some((stored, whole)) = crate::values::author_record(column) {
        // A JSON array of the record's fields, read back by
        // `values::from_sql`.
        let identity = "json_array(issuer, subject)";
        let fields = if whole {
            format!("json_array(lower(hex(account)), {identity})")
        } else {
            identity.to_owned()
        };
        let code = match stored {
            "$deletedBy" => deletion_sql(table, alias, stored),
            _ => stored_sql(alias, stored),
        };
        return Ok(format!(
            "(SELECT {fields} FROM _jazz_authors WHERE n = {code})"
        ));
    }
    if let Some((stored, part)) = crate::values::author_part(column) {
        let code = match stored {
            "$deletedBy" => deletion_sql(table, alias, stored),
            _ => stored_sql(alias, stored),
        };
        return Ok(format!(
            "(SELECT {part} FROM _jazz_authors WHERE n = {code})"
        ));
    }
    if column == "$deletedAt" {
        return Ok(deletion_sql(table, alias, column));
    }
    if crate::values::elided_as(column).is_some() {
        return Ok(stored_sql(alias, column));
    }
    if table.column(column).is_none() {
        return Err(query_error(format!(
            "unknown column `{}.{column}`",
            table.name
        )));
    }
    Ok(format!("{alias}.{}", quote(table.stored_name(column))))
}

/// The first deletion column a query's root selects, filters or orders by.
fn deletion_column(query: &Query) -> Option<String> {
    let mut found = query
        .select
        .iter()
        .flatten()
        .chain(query.order_by.iter().map(|order| &order.column))
        .find(|column| crate::values::is_deletion(column))
        .cloned();
    for filter in &query.filters {
        if found.is_some() {
            break;
        }
        filter.any_operand(&mut |operand| match operand {
            Operand::Column(column) if crate::values::is_deletion(column) => {
                found = Some(column.clone());
                true
            }
            _ => false,
        });
    }
    found
}

/// `$deletedAt` or the `$deletedBy` author code: the update stamp of a
/// deleted current version, which the deletion wrote, else NULL. A source
/// that carries them as columns (history versions, which take them from
/// their current row) is read directly.
fn deletion_sql(table: &Table, alias: &str, column: &str) -> String {
    if table.stored_provenance.iter().any(|c| c.stored == column) {
        return format!("{alias}.{}", quote(column));
    }
    let stamp = match column {
        "$deletedAt" => "$updatedAt",
        _ => "$updatedBy",
    };
    format!(
        "CASE WHEN ({alias}._st & {STATE_DELETED}) <> 0 THEN {} END",
        stored_sql(alias, stamp)
    )
}

/// A stored provenance column; an update stamp left NULL reads as the
/// create stamp it equals.
fn stored_sql(alias: &str, stored: &str) -> String {
    match crate::values::elided_as(stored) {
        Some(created) => format!(
            "COALESCE({alias}.{}, {alias}.{})",
            quote(stored),
            quote(created)
        ),
        None => format!("{alias}.{}", quote(stored)),
    }
}

fn and(parts: &[String]) -> String {
    if parts.is_empty() {
        "1".to_owned()
    } else {
        chain(parts, " AND ")
    }
}

fn or(parts: &[String]) -> String {
    if parts.is_empty() {
        "0".to_owned()
    } else {
        chain(parts, " OR ")
    }
}

/// Terms flatter than this join as one chain. SQLite parses a chain of n
/// terms n deep, and Durable Object SQLite caps expression depth at 100.
const CHAIN_MAX: usize = 32;

/// `EXISTS (SELECT 1 FROM {from} WHERE {conds})`, with any condition that
/// is itself such an `EXISTS` joined in: `EXISTS (A WHERE c AND EXISTS (B
/// WHERE d))` is `EXISTS (A, B WHERE c AND d)`. SQLite counts a nested
/// subquery's depth on top of its enclosing expressions', so an inheritance
/// chain nested level by level outgrows Durable Object SQLite's expression
/// depth cap of 100 within a few levels; joined, it stays one level deep.
fn semi_join(from: String, conds: Vec<String>) -> String {
    let mut sources = vec![from];
    let mut terms = Vec::with_capacity(conds.len());
    for cond in conds {
        match plain_exists(&cond) {
            Some((source, term)) => {
                sources.push(source.to_owned());
                terms.push(term.to_owned());
            }
            None => terms.push(cond),
        }
    }
    format!(
        "EXISTS (SELECT 1 FROM {} WHERE {})",
        sources.join(", "),
        and(&terms)
    )
}

/// The source list and condition of `sql` when it is exactly one
/// `EXISTS (SELECT 1 FROM {from} WHERE {cond})`, as [`semi_join`] writes.
fn plain_exists(sql: &str) -> Option<(&str, &str)> {
    const HEAD: &str = "EXISTS (SELECT 1 FROM ";
    let body = sql.strip_prefix(HEAD)?.strip_suffix(')')?;
    // Scan the body at nesting depth zero, outside quotes: it must not
    // close the EXISTS early, and its first top-level WHERE splits it.
    let bytes = body.as_bytes();
    let (mut depth, mut quote, mut split) = (0usize, None, None);
    for (i, &b) in bytes.iter().enumerate() {
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' if depth == 0 => return None,
                b')' => depth -= 1,
                b' ' if depth == 0 && split.is_none() && body[i..].starts_with(" WHERE ") => {
                    split = Some(i)
                }
                _ => {}
            },
        }
    }
    let split = split?;
    let (from, cond) = (&body[..split], &body[split + " WHERE ".len()..]);
    // Every source must be aliased: an unaliased one (`json_each(..)`)
    // names its columns bare, which another joined source could shadow.
    if !top_level(from)
        .split(',')
        .all(|source| source.contains(" AS "))
    {
        return None;
    }
    // Only a bare condition: no top-level clause after it.
    let top = top_level(cond);
    if [" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "]
        .iter()
        .any(|clause| top.contains(clause))
    {
        return None;
    }
    Some((from, cond))
}

/// `sql` with every parenthesised or quoted span blanked out.
fn top_level(sql: &str) -> String {
    let (mut depth, mut quote) = (0usize, None);
    sql.bytes()
        .map(|b| {
            let keep = match quote {
                Some(q) => {
                    if b == q {
                        quote = None;
                    }
                    false
                }
                None => match b {
                    b'\'' | b'"' => {
                        quote = Some(b);
                        false
                    }
                    b'(' => {
                        depth += 1;
                        false
                    }
                    b')' => {
                        depth = depth.saturating_sub(1);
                        false
                    }
                    _ => depth == 0,
                },
            };
            if keep { b as char } else { ' ' }
        })
        .collect()
}

/// `parts` joined by `op`, nested in halves past [`CHAIN_MAX`] so the depth
/// grows with the log of the count. SQLite splits nested AND and OR terms
/// the same as a flat chain.
fn chain(parts: &[String], op: &str) -> String {
    if parts.len() <= CHAIN_MAX {
        return format!("({})", parts.join(op));
    }
    let (left, right) = parts.split_at(parts.len() / 2);
    format!("({}{op}{})", chain(left, op), chain(right, op))
}

/// Lists longer than this bind as one JSON array read by `json_each`, not
/// a parameter per value: Durable Object SQLite caps a statement at 100
/// bound parameters.
pub(crate) const BOUND_LIST_MAX: usize = 16;

/// A `SELECT` of `values` from the JSON array bound at `mark`, and that
/// array; `None` when the values have no exact shared JSON form (reals, or
/// blobs mixed with other types).
pub(crate) fn json_list(values: &[&Sql], mark: &str) -> Option<(String, Sql)> {
    let blobs = values.iter().any(|value| matches!(value, Sql::Blob(_)));
    let mut items = Vec::with_capacity(values.len());
    for value in values {
        items.push(match value {
            Sql::Null => serde_json::Value::Null,
            Sql::Blob(bytes) => serde_json::Value::String(hex(bytes)),
            _ if blobs => return None,
            Sql::Integer(n) => serde_json::Value::from(*n),
            Sql::Text(text) => serde_json::Value::String(String::from_utf8(text.clone()).ok()?),
            Sql::Real(_) => return None,
        });
    }
    let column = if blobs { "unhex(value)" } else { "value" };
    Some((
        format!("SELECT {column} FROM json_each({mark})"),
        Sql::Text(serde_json::Value::Array(items).to_string().into_bytes()),
    ))
}

/// Ids as a JSON array for [`json_list`]'s blob form.
pub(crate) fn json_ids<'a>(ids: impl IntoIterator<Item = &'a [u8]>) -> Sql {
    let items: Vec<serde_json::Value> = ids
        .into_iter()
        .map(|id| serde_json::Value::String(hex(id)))
        .collect();
    Sql::Text(serde_json::Value::Array(items).to_string().into_bytes())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

fn bool_sql(value: bool) -> String {
    if value { "1" } else { "0" }.to_owned()
}

/// Jazz comparison semantics: equality treats null as a value (null = null),
/// orderings are false against null, and everything is false against NaN.
/// Orderings and `IN` stay bare so SQLite can use an index for them: their
/// SQL null means false, which AND, OR and WHERE already treat as false;
/// only NOT (and a condition read as a value) coalesces it.
fn is_range(op: &CmpOp) -> bool {
    matches!(op, CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge)
}

fn cmp_plain(op: &CmpOp, l: &str, r: &str) -> String {
    match op {
        CmpOp::Eq => format!("({l} IS {r})"),
        CmpOp::Ne => format!("({l} IS NOT {r})"),
        CmpOp::Lt => format!("({l} < {r})"),
        CmpOp::Le => format!("({l} <= {r})"),
        CmpOp::Gt => format!("({l} > {r})"),
        CmpOp::Ge => format!("({l} >= {r})"),
    }
}

/// A large-capable cell `c` against any other SQL term `r`. Equality is by
/// cell: a chunked cell is an INTEGER no inline value equals. A chunked cell
/// takes part in no range, so ranges match inline cells only (SPEC 19.5).
fn large_against(op: &CmpOp, c: &str, r: &str) -> String {
    match op {
        CmpOp::Eq => format!("({c} IS {r})"),
        CmpOp::Ne => format!("(NOT ({c} IS {r}))"),
        op => format!(
            "(NOT {} AND {})",
            crate::large::chunked_sql(c),
            cmp_plain(op, c, r)
        ),
    }
}

/// `a op b` as `b flip(op) a`.
fn flip(op: &CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        op => op.clone(),
    }
}

/// Comparing a large-capable column with a literal over the inline
/// threshold: the literal could only equal a chunked value, which compares
/// by identity (SPEC 19.5).
fn long_literal(column: &LargeColumn) -> Error {
    unsupported(format!(
        "comparing `{}.{}` with a literal over {} bytes",
        column.table,
        column.column,
        jazz_types::large_values::INLINE_VALUE_MAX_BYTES
    ))
}

/// The logical bytes of a bound string or bytes literal.
fn literal_of(value: &Sql) -> Option<&[u8]> {
    match value {
        Sql::Text(bytes) | Sql::Blob(bytes) => Some(bytes),
        _ => None,
    }
}

impl Ctx<'_> {
    /// A column as an operand: a large-capable column compares by its cell.
    fn column_term(&self, table: &Table, alias: &str, column: &str) -> Result<Term, Error> {
        let sql = column_sql(table, alias, column)?;
        Ok(self.large_term(table, sql, column))
    }

    fn large_term(&self, table: &Table, sql: String, column: &str) -> Term {
        if let Some(col) = table.column(column)
            && let Some(tags) = col.tags.clone()
        {
            return Term::Enum(EnumColumn {
                cell: sql,
                tags,
                column: col.clone(),
            });
        }
        match table.column(column) {
            Some(col)
                if column != "id"
                    && crate::values::elided_as(column).is_none()
                    && crate::values::author_part(column).is_none()
                    && crate::values::author_record(column).is_none()
                    && crate::large::large_capable(col) =>
            {
                Term::Large(LargeColumn {
                    cell: sql,
                    table: table.stored.clone(),
                    column: col.stored.clone(),
                })
            }
            _ => Term::Sql(sql),
        }
    }

    /// An author record (`$createdBy`, `$updatedBy.identity`, ...) against
    /// the session's user (or its identity): equal when every part is, with
    /// an accountless session standing for the system account as
    /// [`crate::values::author_key`] stores it.
    fn author_cmp(
        &mut self,
        table: &Table,
        alias: &str,
        column: &str,
        op: &CmpOp,
        path: &[String],
        session: &Session,
    ) -> Result<String, Error> {
        let (_, whole) = crate::values::author_record(column).expect("author record");
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        let parts: &[&str] = match (whole, path.as_slice()) {
            (true, ["user"]) => &["account", "identity.issuer", "identity.subject"],
            (false, ["user", "identity"]) => &["issuer", "subject"],
            _ => {
                return Err(query_error(format!(
                    "comparing `{column}` with `session.{}`: compare an author record \
                     with `session.user` and its identity with `session.user.identity`",
                    path.join(".")
                )));
            }
        };
        let negate = match op {
            CmpOp::Eq => false,
            CmpOp::Ne => true,
            op => {
                return Err(unsupported(format!(
                    "comparing the author record `{column}` with {op:?}"
                )));
            }
        };
        if session.system || session.subject.is_none() {
            return Ok(bool_sql(negate));
        }
        let record = if whole {
            column.to_owned()
        } else {
            column.trim_end_matches(".identity").to_owned()
        };
        let mut clauses = Vec::new();
        for part in parts {
            let part = part.trim_start_matches("identity.");
            let field = match part {
                "account" => format!("{record}.account"),
                part => format!("{record}.identity.{part}"),
            };
            let left = self.column_term(table, alias, &field)?;
            let value = match part {
                "account" => {
                    Some(Value::Uuid(session.account.unwrap_or(
                        jazz_types::account_registry::SYSTEM_ACCOUNT_ID.0,
                    )))
                }
                part => session.resolve(&["user".into(), "identity".into(), part.into()]),
            };
            let right = self.present(value)?;
            clauses.push(self.cmp(CmpOp::Eq, left, right)?);
        }
        let all = and(&clauses);
        Ok(if negate {
            format!("(NOT COALESCE({all}, 0))")
        } else {
            all
        })
    }

    /// A bound value an enum column's rank is compared with: a variant name
    /// with a tag here, rebound as its rank (see [`EnumColumn::ordered`]).
    fn ranked(&mut self, column: &EnumColumn, term: Term) -> Term {
        let Term::Bound(placeholder, Sql::Text(name)) = &term else {
            return term;
        };
        let rank = std::str::from_utf8(name)
            .ok()
            .and_then(|name| column.column.rank(name));
        let Some(rank) = rank else {
            return term;
        };
        let slot = placeholder[1..]
            .parse::<usize>()
            .expect("a numbered placeholder")
            - 1;
        self.params[slot] = Sql::Integer(rank);
        Term::Bound(placeholder.clone(), Sql::Integer(rank))
    }

    /// A bound value an enum column is compared with: a variant name with a
    /// tag here, rebound as the tag.
    fn tagged(&mut self, column: &EnumColumn, term: Term) -> Term {
        let Term::Bound(placeholder, Sql::Text(name)) = &term else {
            return term;
        };
        let tag = std::str::from_utf8(name)
            .ok()
            .and_then(|name| column.tags.borrow().tag(name));
        let Some(tag) = tag else {
            return term;
        };
        let slot = placeholder[1..]
            .parse::<usize>()
            .expect("a numbered placeholder")
            - 1;
        self.params[slot] = Sql::Integer(tag);
        Term::Bound(placeholder.clone(), Sql::Integer(tag))
    }

    fn cmp(&mut self, op: CmpOp, left: Term, right: Term) -> Result<String, Error> {
        // A range needs the literal's place in the order: a name the
        // column never held has none.
        if is_range(&op) {
            for (column, other) in [(&left, &right), (&right, &left)] {
                if let (Term::Enum(column), Term::Bound(_, Sql::Text(name))) = (column, other) {
                    let name = String::from_utf8_lossy(name);
                    if column.tags.borrow().tag(&name).is_none() {
                        return Err(query_error(format!(
                            "comparing `{}` with `{name}`, which is not one of its variants",
                            column.column.name
                        )));
                    }
                }
            }
        }
        Ok(match (left, right) {
            (Term::Missing | Term::NaN, _) | (_, Term::Missing | Term::NaN) => "0".to_owned(),
            // A range in a schema whose order is not tag order compares
            // ranks.
            (Term::Enum(column), right @ Term::Bound(..))
                if is_range(&op) && column.ordered().is_some() =>
            {
                let right = self.ranked(&column, right);
                let left = column.ordered().expect("checked");
                cmp_plain(&op, &left, right.sql().expect("present"))
            }
            (left @ Term::Bound(..), Term::Enum(column))
                if is_range(&op) && column.ordered().is_some() =>
            {
                let left = self.ranked(&column, left);
                let right = column.ordered().expect("checked");
                cmp_plain(&op, left.sql().expect("present"), &right)
            }
            (Term::Enum(column), right @ Term::Bound(..)) => {
                let right = self.tagged(&column, right);
                cmp_plain(&op, &column.cell, right.sql().expect("present"))
            }
            (left @ Term::Bound(..), Term::Enum(column)) => {
                let left = self.tagged(&column, left);
                cmp_plain(&op, left.sql().expect("present"), &column.cell)
            }
            (Term::Enum(a), Term::Enum(b)) if std::rc::Rc::ptr_eq(&a.tags, &b.tags) => {
                match (is_range(&op), a.ordered(), b.ordered()) {
                    (true, Some(l), Some(r)) => cmp_plain(&op, &l, &r),
                    _ => cmp_plain(&op, &a.cell, &b.cell),
                }
            }
            // Two enum columns' tags differ: they compare by name, as
            // anything else does with one.
            (Term::Enum(column), other) => return self.cmp(op, column.names(), other),
            (other, Term::Enum(column)) => return self.cmp(op, other, column.names()),
            (Term::Large(column), Term::Bound(sql, value)) => {
                self.cmp_large(&op, &column, &sql, &value)?
            }
            (Term::Bound(sql, value), Term::Large(column)) => {
                self.cmp_large(&flip(&op), &column, &sql, &value)?
            }
            (Term::Large(a), Term::Large(b)) => self.cmp_large_pair(&op, &a, &b)?,
            (Term::Large(column), other) => {
                large_against(&op, &column.cell, other.sql().expect("present"))
            }
            (other, Term::Large(column)) => {
                large_against(&flip(&op), &column.cell, other.sql().expect("present"))
            }
            (left, right) => cmp_plain(
                &op,
                left.sql().expect("present"),
                right.sql().expect("present"),
            ),
        })
    }

    /// A large-capable cell against a bound value. Every value up to the
    /// inline threshold has one inline cell, which compares as SQLite
    /// compares the value; a chunked cell equals no such literal and takes
    /// part in no range (SPEC 19.5).
    fn cmp_large(
        &mut self,
        op: &CmpOp,
        column: &LargeColumn,
        p: &str,
        value: &Sql,
    ) -> Result<String, Error> {
        let c = &column.cell;
        if literal_of(value)
            .is_some_and(|literal| literal.len() > jazz_types::large_values::INLINE_VALUE_MAX_BYTES)
            && matches!(op, CmpOp::Eq | CmpOp::Ne)
        {
            return Err(long_literal(column));
        }
        Ok(large_against(op, c, p))
    }

    /// Two large-capable cells: equal when both are inline and equal, or
    /// both name the same chunked cell (SPEC 19.5).
    fn cmp_large_pair(
        &mut self,
        op: &CmpOp,
        a: &LargeColumn,
        b: &LargeColumn,
    ) -> Result<String, Error> {
        let eq = format!("({} IS {})", a.cell, b.cell);
        match op {
            CmpOp::Eq => Ok(eq),
            CmpOp::Ne => Ok(format!("(NOT {eq})")),
            _ => Err(unsupported(format!(
                "ordering comparisons between two string or bytes columns (`{}.{}`, `{}.{}`)",
                a.table, a.column, b.table, b.column
            ))),
        }
    }

    fn in_list(&mut self, left: Term, options: Vec<Term>) -> Result<String, Error> {
        let options: Vec<Term> = options
            .into_iter()
            .filter(|option| !matches!(option, Term::Missing | Term::NaN))
            .collect();
        if options.is_empty() || matches!(left, Term::Missing | Term::NaN) {
            return Ok("0".to_owned());
        }
        // An enum column in a list of names: in their tags, else by name.
        let (left, options) = match left {
            Term::Enum(column) if options.iter().all(|o| matches!(o, Term::Bound(..))) => {
                let options = options
                    .into_iter()
                    .map(|option| self.tagged(&column, option))
                    .collect();
                (Term::Enum(column), options)
            }
            Term::Enum(column) => (column.names(), options),
            left => (left, options),
        };
        let options: Vec<Term> = options
            .into_iter()
            .map(|option| match option {
                Term::Enum(column) => column.names(),
                option => option,
            })
            .collect();
        if let Term::Large(column) = &left {
            // A chunked cell is in no list of inline values; a longer
            // literal is refused as `=` refuses it.
            let long = |option: &Term| match option {
                Term::Bound(_, value) => literal_of(value).is_some_and(|literal| {
                    literal.len() > jazz_types::large_values::INLINE_VALUE_MAX_BYTES
                }),
                _ => false,
            };
            if options.iter().any(long) {
                return Err(long_literal(column));
            }
        }
        let list = self.bound_list(&options);
        let list = list.unwrap_or_else(|| {
            let list: Vec<&str> = options.iter().filter_map(Term::sql).collect();
            list.join(", ")
        });
        Ok(format!("({} IN ({list}))", left.sql().expect("present")))
    }

    /// A long list of values bound last, rebound as one JSON array
    /// ([`json_list`]): its `SELECT`, or `None` to keep one placeholder per
    /// value.
    fn bound_list(&mut self, options: &[Term]) -> Option<String> {
        if options.len() <= BOUND_LIST_MAX || options.len() > self.params.len() {
            return None;
        }
        let first = self.params.len() - options.len();
        let mut values = Vec::with_capacity(options.len());
        for (i, option) in options.iter().enumerate() {
            match option {
                Term::Bound(mark, value) if *mark == format!("?{}", first + i + 1) => {
                    values.push(value)
                }
                _ => return None,
            }
        }
        let (select, array) = json_list(&values, &format!("?{}", first + 1))?;
        self.params.truncate(first);
        self.params.push(array);
        Some(select)
    }

    /// An array cell (JSON text, see [`crate::values::to_sql`]) holding an
    /// element equal to `right`.
    fn array_contains(&mut self, left: Term, right: Term) -> String {
        // An enum array holds its variants' tags.
        let right = match &left {
            Term::Enum(column) => match self.tagged(column, right) {
                Term::Bound(placeholder, Sql::Integer(_)) => {
                    let array = left.sql().expect("present");
                    return format!(
                        "EXISTS (SELECT 1 FROM json_each({array}) WHERE value = {placeholder})"
                    );
                }
                right => right,
            },
            _ => right,
        };
        let (Some(array), Term::Bound(placeholder, needle)) = (left.sql(), right) else {
            return "0".to_owned();
        };
        // Rebind the needle in its own slot, as `json_each` reads elements.
        let slot = placeholder[1..]
            .parse::<usize>()
            .expect("a numbered placeholder")
            - 1;
        self.params[slot] = crate::values::array_needle(needle);
        let needle = placeholder;
        format!("EXISTS (SELECT 1 FROM json_each({array}) WHERE value = {needle})")
    }

    fn contains(&mut self, left: Term, right: Term) -> String {
        // Text containment reads an enum cell as its name.
        let names = |term: Term| match term {
            Term::Enum(column) => column.names(),
            term => term,
        };
        let (left, right) = (names(left), names(right));
        let (Some(l), Some(r)) = (left.sql(), right.sql()) else {
            return "0".to_owned();
        };
        // Bytewise: TEXT `instr` steps by code point.
        let instr = |l: &str, r: &str| {
            format!("COALESCE(instr(CAST({l} AS BLOB), CAST({r} AS BLOB)) > 0, 0)")
        };
        // A chunked cell is an INTEGER, not its value: it contains nothing and
        // is contained in nothing (SPEC 19.5).
        let mut guards = Vec::new();
        if matches!(left, Term::Large(_)) {
            guards.push(format!("NOT {}", crate::large::chunked_sql(l)));
        }
        if matches!(right, Term::Large(_)) {
            guards.push(format!("NOT {}", crate::large::chunked_sql(r)));
        }
        if guards.is_empty() {
            return instr(l, r);
        }
        format!("({} AND {})", guards.join(" AND "), instr(l, r))
    }

    /// `ORDER BY` keys. Ties break by row id in the last key's direction,
    /// so a declared index (which ends in `id`) supplies the whole order in
    /// one scan, in either direction, with no sort step.
    fn order_sql(
        &mut self,
        table: &Table,
        alias: &str,
        order: &[crate::query::OrderBy],
    ) -> Result<String, Error> {
        let mut keys: Vec<String> = self
            .order_terms(table, alias, order)?
            .into_iter()
            .map(|key| format!("{} {}", key.sql, key.direction))
            .collect();
        let tie = match order.last().map(|key| key.direction) {
            Some(OrderDirection::Desc) => "DESC",
            _ => "ASC",
        };
        keys.push(format!("{alias}.id {tie}"));
        Ok(keys.join(", "))
    }

    /// Each order key's SQL and direction. A large-capable column orders by
    /// its cell: SQLite puts a chunked cell (an INTEGER) after NULL and
    /// before every inline value, so values over the inline threshold sort
    /// first ascending, among themselves in no meaningful order
    /// (SPEC 19.5).
    fn order_terms(
        &mut self,
        table: &Table,
        alias: &str,
        order: &[crate::query::OrderBy],
    ) -> Result<Vec<OrderTerm>, Error> {
        let mut terms = Vec::with_capacity(order.len());
        for key in order {
            let direction = match key.direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            };
            if crate::values::author_record(&key.column).is_some() {
                return Err(unsupported(format!(
                    "ordering by the author record `{}` (order by its parts)",
                    key.column
                )));
            }
            let mut sql = column_sql(table, alias, &key.column)?;
            // An enum column orders by the query schema's declaration order.
            if let Some(rank) = table.column(&key.column).and_then(|c| c.rank_sql(&sql)) {
                sql = rank;
            }
            terms.push(OrderTerm { sql, direction });
        }
        Ok(terms)
    }
}

/// One order key: its SQL and its direction.
struct OrderTerm {
    sql: String,
    direction: &'static str,
}

fn is_id(operand: &Operand) -> bool {
    matches!(operand, Operand::Column(column) if column == "id")
}

/// A row-id predicate on an unbranched table, with the branch key every
/// such row has (`''`): together they are the whole `UNIQUE (id, _b)` key,
/// so SQLite seeks the ids instead of preferring a declared index that
/// also covers the order or another filter.
fn whole_key(table: &Table, alias: &str, sql: String) -> String {
    if table.branch_by.is_empty() {
        format!("({sql} AND {alias}._b = X'')")
    } else {
        sql
    }
}

/// A public schema value (policy literals, defaults) as a storage value.
pub(crate) fn public_value(value: &crate::tools::Value) -> Result<Value, Error> {
    use crate::tools::Value as P;
    Ok(match value {
        P::Integer(v) => Value::I32(*v),
        P::BigInt(v) => Value::I64(*v),
        P::Double(v) => Value::F64(*v),
        P::Boolean(v) => Value::Bool(*v),
        P::Text(v) => Value::String(v.clone()),
        P::Timestamp(v) => Value::U64(*v),
        P::Uuid(v) => Value::Uuid(*v.uuid()),
        P::Bytea(v) => Value::Bytes(v.clone()),
        P::Null => Value::Nullable(None),
        other => return Err(unsupported(format!("policy literal {other:?}"))),
    })
}

/// A relation-IR literal: a bare JSON value or a typed public-value envelope.
fn json_value(json: &serde_json::Value) -> Result<Value, Error> {
    use serde_json::Value as J;
    if let Ok(value) = serde_json::from_value::<crate::tools::Value>(json.clone()) {
        return public_value(&value);
    }
    Ok(match json {
        J::Null => Value::Nullable(None),
        J::Bool(v) => Value::Bool(*v),
        J::Number(n) if n.is_i64() => Value::I64(n.as_i64().unwrap_or_default()),
        J::Number(n) => Value::F64(n.as_f64().unwrap_or(f64::NAN)),
        J::String(s) => match uuid::Uuid::parse_str(s) {
            Ok(uuid) if s.len() == 36 => Value::Uuid(uuid),
            _ => Value::String(s.clone()),
        },
        other => return Err(unsupported(format!("relation literal {other}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::{outer_key_only, plain_exists, semi_join};
    use crate::query::{
        RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinCondition,
        RelationJoinKind, RelationKeyRef, RelationPredicate, RelationRowIdRef, RelationValueRef,
    };

    fn scan(table: &str) -> RelationExpr {
        RelationExpr::TableScan {
            table: table.to_owned(),
            alias: None,
        }
    }

    fn filter(input: RelationExpr, predicate: RelationPredicate) -> RelationExpr {
        RelationExpr::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    fn eq(column: &str, right: RelationValueRef) -> RelationPredicate {
        RelationPredicate::Cmp {
            left: RelationColumnRef::unscoped(column),
            op: RelationCmpOp::Eq,
            right,
        }
    }

    fn outer_id() -> RelationValueRef {
        RelationValueRef::RowId(RelationRowIdRef::Outer)
    }

    fn literal() -> RelationValueRef {
        RelationValueRef::Literal(serde_json::json!("viewer"))
    }

    fn gather(seed: RelationExpr) -> RelationExpr {
        RelationExpr::Gather {
            seed: Box::new(seed),
            step: Box::new(filter(
                scan("edges"),
                eq("child", RelationValueRef::RowId(RelationRowIdRef::Frontier)),
            )),
            frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
            bound: RecursionBound::MaxDepth(8),
            dedupe_key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)],
        }
    }

    fn joined(left: RelationExpr, right: RelationExpr) -> RelationExpr {
        RelationExpr::Join {
            left: Box::new(left),
            right: Box::new(right),
            on: vec![RelationJoinCondition {
                left: RelationColumnRef::unscoped("id"),
                right: RelationColumnRef::unscoped("team"),
            }],
            join_kind: RelationJoinKind::Inner,
        }
    }

    /// White-box: the schema refuses most correlations the lift must not
    /// take (under OR, in a gather seed), so only the precondition itself
    /// shows that the compiler would keep them per-row EXISTS too.
    #[test]
    fn only_a_lone_conjoined_row_id_key_is_lifted() {
        let keyed = filter(
            scan("grants"),
            RelationPredicate::And(vec![eq("resource", outer_id()), eq("role", literal())]),
        );
        assert!(outer_key_only(&keyed));
        // Below a join with a gather of the session's teams.
        assert!(outer_key_only(&filter(
            joined(
                gather(filter(scan("teams"), eq("member", literal()))),
                scan("grants")
            ),
            eq("resource", outer_id()),
        )));

        let under_or = filter(
            scan("grants"),
            RelationPredicate::Or(vec![eq("resource", outer_id()), eq("role", literal())]),
        );
        let under_not = filter(
            scan("grants"),
            RelationPredicate::Not(Box::new(eq("resource", outer_id()))),
        );
        let second_outer_column = filter(
            scan("grants"),
            RelationPredicate::And(vec![
                eq("resource", outer_id()),
                eq(
                    "role",
                    RelationValueRef::OuterColumn(RelationColumnRef::unscoped("label")),
                ),
            ]),
        );
        let in_gather_seed = joined(
            gather(filter(scan("teams"), eq("id", outer_id()))),
            scan("grants"),
        );
        let not_equal = filter(
            scan("grants"),
            RelationPredicate::Cmp {
                left: RelationColumnRef::unscoped("resource"),
                op: RelationCmpOp::Ne,
                right: outer_id(),
            },
        );
        let other_outer_column = filter(
            scan("grants"),
            eq(
                "resource",
                RelationValueRef::OuterColumn(RelationColumnRef::unscoped("parent")),
            ),
        );
        for (name, rel) in [
            ("under OR", under_or),
            ("under NOT", under_not),
            ("with a second outer column", second_outer_column),
            ("in a gather seed", in_gather_seed),
            ("not equal", not_equal),
            ("on another outer column", other_outer_column),
        ] {
            assert!(!outer_key_only(&rel), "{name}");
        }
    }

    #[test]
    fn nested_exists_conditions_join_into_one_level() {
        let inner = semi_join(
            r#""b" AS b"#.to_owned(),
            vec!["b.x = a.id".to_owned(), "b.s IS 'W) WHERE ('".to_owned()],
        );
        let outer = semi_join(
            r#""a" AS a"#.to_owned(),
            vec!["(a._st & 2) = 0".to_owned(), inner],
        );
        assert_eq!(
            outer,
            r#"EXISTS (SELECT 1 FROM "a" AS a, "b" AS b WHERE ((a._st & 2) = 0 AND (b.x = a.id AND b.s IS 'W) WHERE (')))"#
        );
        // A subquery source keeps its own WHERE.
        let sourced =
            r#"EXISTS (SELECT 1 FROM (SELECT * FROM "c" WHERE k = 1) AS c WHERE c.v = 2)"#;
        assert_eq!(
            plain_exists(sourced),
            Some((r#"(SELECT * FROM "c" WHERE k = 1) AS c"#, "c.v = 2"))
        );
    }

    #[test]
    fn only_a_single_bare_exists_is_joined_in() {
        for sql in [
            "EXISTS (SELECT 1 FROM a WHERE x) OR EXISTS (SELECT 1 FROM b WHERE y)",
            "NOT EXISTS (SELECT 1 FROM a WHERE x)",
            "EXISTS (SELECT 1 FROM a WHERE x LIMIT 1)",
            "EXISTS (SELECT 1 FROM json_each(?1) WHERE value = 3) AND 1",
            "EXISTS (SELECT 1 FROM json_each(t.a) WHERE value = 3)",
        ] {
            assert_eq!(plain_exists(sql), None, "{sql}");
        }
    }
}

/// `target`'s `operation` policy split as a parent chain: when `table`
/// inherits from its own rows through the scalar reference `via` and that
/// policy is `base OR (guards AND INHERITS(operation, via))`, the other
/// disjuncts, the guards a row meets to take the hop, and the hop's own
/// depth bound. None when any of them inherits further: that case unrolls
/// level by level.
fn recursive_base<'p>(
    table: &Table,
    target: &'p Table,
    operation: Operation,
    via: &str,
) -> Option<ParentChain<'p>> {
    let array = |ty: &ValueType| match ty {
        ValueType::Nullable(inner) => matches!(**inner, ValueType::Array(_)),
        ty => matches!(ty, ValueType::Array(_)),
    };
    if target.stored != table.stored || target.column(via).is_none_or(|c| array(&c.ty)) {
        return None;
    }
    let PolicyExpr::Or(parts) = target.policy(operation)? else {
        return None;
    };
    let is_hop = |part: &PolicyExpr| {
        matches!(part, PolicyExpr::Inherits { operation: op, via_column, .. }
            if *op == operation && via_column == via)
    };
    let hop = |part: &'p PolicyExpr| -> Option<(Option<usize>, Vec<&'p PolicyExpr>)> {
        let conds: Vec<&PolicyExpr> = match part {
            PolicyExpr::And(conds) => conds.iter().collect(),
            part => vec![part],
        };
        let mut hops = conds.iter().filter(|c| is_hop(c));
        let Some(PolicyExpr::Inherits { max_depth, .. }) = hops.next() else {
            return None;
        };
        if hops.next().is_some() {
            return None;
        }
        let guards = conds.into_iter().filter(|c| !is_hop(c)).collect();
        Some((*max_depth, guards))
    };
    let mut hops = parts.iter().filter_map(|part| hop(part).map(|h| (part, h)));
    let (hop_part, (depth, guards)) = hops.next()?;
    if hops.next().is_some() {
        return None;
    }
    let base: Vec<&PolicyExpr> = parts
        .iter()
        .filter(|part| !std::ptr::eq(*part, hop_part))
        .collect();
    base.iter()
        .chain(&guards)
        .all(|part| !inherits(part))
        .then_some(ParentChain {
            base,
            guards,
            depth,
        })
}

struct ParentChain<'p> {
    base: Vec<&'p PolicyExpr>,
    guards: Vec<&'p PolicyExpr>,
    depth: Option<usize>,
}

fn inherits(policy: &PolicyExpr) -> bool {
    match policy {
        PolicyExpr::Inherits { .. } | PolicyExpr::InheritsReferencing { .. } => true,
        PolicyExpr::And(parts) | PolicyExpr::Or(parts) => parts.iter().any(inherits),
        PolicyExpr::Not(inner) => inherits(inner),
        PolicyExpr::Exists { condition, .. } => inherits(condition),
        _ => false,
    }
}

/// The part each of `compiled`'s projected columns is read in: a partial
/// selection names a large-capable column of the root table of a plain row
/// query, read in a part its kind has.
fn partial_projection(
    schema: &Schema,
    table: &str,
    partial: &[crate::query::PartialSelect],
    compiled: &Compiled,
) -> Result<Vec<Option<crate::query::Part>>, Error> {
    use crate::query::Part;
    use jazz_types::large_values::LargeValueKind as Kind;
    if compiled.aggregate || compiled.union || compiled.joined > 0 {
        return Err(unsupported(
            "partial large-value selections in aggregate, union or joined queries",
        ));
    }
    let table = schema.table(table)?;
    let mut out = vec![None; compiled.projection.len()];
    for selected in partial {
        let column = table
            .column(&selected.column)
            .filter(|column| crate::large::large_capable(column))
            .ok_or_else(|| {
                query_error(format!(
                    "partial selection of {:?} needs a text, bytes or JSON column",
                    selected.column
                ))
            })?;
        let kind = crate::large::kind(column);
        let fits = matches!(
            (&selected.part, kind),
            (Part::Bytes { .. }, Kind::Bytes)
                | (Part::TextUtf16 { .. } | Part::TextUtf8 { .. }, Kind::String)
                | (Part::JsonPointer(_), Kind::Json)
        );
        if !fits {
            return Err(query_error(format!(
                "partial selection of {:?} does not fit its column's kind",
                selected.column
            )));
        }
        if let Part::Bytes { from, to } | Part::TextUtf16 { from, to } | Part::TextUtf8 { from, to } =
            &selected.part
            && from > to
        {
            return Err(query_error(format!(
                "partial selection of {:?} ends before it starts",
                selected.column
            )));
        }
        // A compilation that projects no such column (one reading only the
        // rows' ids) reads none of it.
        let Some(index) = compiled
            .projection
            .iter()
            .position(|(name, ..)| *name == column.name)
        else {
            continue;
        };
        out[index] = Some(selected.part.clone());
    }
    Ok(out)
}
