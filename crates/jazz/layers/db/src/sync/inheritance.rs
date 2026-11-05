//! Bounded readable witnesses for explicit inherited query capabilities.
use super::*;
use crate::compile::DEFAULT_INHERITS_DEPTH;
use crate::query::{InheritsOperation, InheritsVia, col, eq, in_list, lit};
use crate::records::ValueType;
use crate::values::{Schema, Table};

fn predicate(via: &InheritsVia) -> PolicyExpr {
    PolicyExpr::Inherits {
        operation: match via.operation {
            InheritsOperation::Select => Operation::Select,
            InheritsOperation::Insert => Operation::Insert,
            InheritsOperation::Update => Operation::Update,
            InheritsOperation::Delete => Operation::Delete,
        },
        via_column: via.parent_column.clone(),
        max_depth: via.max_depth,
    }
}
fn charge(budget: &mut usize, count: usize) -> Result<(), Error> {
    *budget = budget.checked_sub(count).ok_or_else(|| {
        unsupported("remote inheritance witnesses exceeding their bounded work limit")
    })?;
    Ok(())
}
fn reference<'a>(schema: &'a Schema, table: &Table, column: &str) -> Result<&'a Table, Error> {
    let scalar = table.column(column).is_some_and(|c| match &c.ty {
        ValueType::Uuid => true,
        ValueType::Nullable(inner) => matches!(inner.as_ref(), ValueType::Uuid),
        _ => false,
    });
    if !scalar && !crate::array_index::uuid_array(table, column) {
        return Err(unsupported(
            "remote inheritance without scalar or array UUID references",
        ));
    }
    let target = schema.table(
        table
            .references
            .get(column)
            .ok_or_else(|| unsupported("remote inheritance without a declared reference"))?,
    )?;
    if !table.branch_by.is_empty() || !target.branch_by.is_empty() {
        return Err(unsupported("remote inheritance over branched rows"));
    }
    Ok(target)
}
fn validate<'a>(
    schema: &'a Schema,
    table: &'a Table,
    policy: &'a PolicyExpr,
    depth: usize,
    seen: &mut HashSet<(String, Operation, usize)>,
    budget: &mut usize,
) -> Result<(), Error> {
    charge(budget, 1)?;
    if row_local_policy(policy) {
        return Ok(());
    }
    match policy {
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            for item in items {
                validate(schema, table, item, depth, seen, budget)?;
            }
        }
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => {
            let depth = max_depth.unwrap_or(depth).min(depth);
            if depth == 0 {
                return Ok(());
            }
            let target = reference(schema, table, via_column)?;
            let governing = schema.policy_schema();
            let target = governing.by_stored(&target.stored).ok_or_else(|| {
                unsupported("remote inheritance with an unknown governing parent")
            })?;
            for (operation, depth) in [
                (*operation, depth - 1),
                (Operation::Select, DEFAULT_INHERITS_DEPTH),
            ] {
                if seen.insert((target.stored.clone(), operation, depth))
                    && let Some(policy) = target.inherited_policy(operation)
                {
                    validate(governing, target, policy, depth, seen, budget)?;
                }
            }
        }
        _ => {
            return Err(unsupported(
                "remote inheritance with negated, reverse or relational policy dependencies",
            ));
        }
    }
    Ok(())
}

struct Walk<'a> {
    inner: &'a Inner,
    schema: &'a Schema,
    prepared: &'a PreparedQuery,
    identity: Option<AuthorSubject>,
    read: &'a ReadSpec,
    budget: usize,
    cache: BTreeMap<(String, RowUuid), Option<Image>>,
    visited: HashSet<(String, [u8; 16], u64, Operation, usize)>,
    out: Vec<Image>,
}
impl Walk<'_> {
    fn lookup(&mut self, target: &Table, id: RowUuid) -> Result<Option<Image>, Error> {
        let key = (target.stored.clone(), id);
        if let Some(found) = self.cache.get(&key) {
            return Ok(found.clone());
        }
        charge(&mut self.budget, 1)?;
        let visible = self
            .schema
            .by_stored(&target.stored)
            .ok_or_else(|| unsupported("remote inheritance without a local parent schema"))?;
        let mut lookup = self.prepared.clone();
        lookup.query =
            Query::from(visible.name.clone()).filter(eq(col("id"), lit(Value::Uuid(id.0))));
        let read = ReadSpec {
            include_deleted: false,
            members_only: self.inner.client.is_some() || self.read.members_only,
            ..self.read.clone()
        };
        let rows = self.inner.with_candidate_sql_budget(&mut self.budget, || {
            self.inner.run(&lookup, self.identity, &read)
        })?;
        let found = if let Some(row) = rows.first() {
            let readable = ReadSpec {
                readable_inheritance: true,
                ..read
            };
            let checked = self.inner.with_candidate_sql_budget(&mut self.budget, || {
                self.inner.run(&lookup, self.identity, &readable)
            })?;
            if checked.is_empty() {
                return Err(unsupported(
                    "remote inheritance whose parent lacks readable permission witnesses",
                ));
            }
            let image = self
                .inner
                .scoped_evidence_image(&ScopedReadRowV1 {
                    table: target.stored.clone(),
                    id: *row.id.as_bytes(),
                    key: Vec::new(),
                    seq: row.seq,
                })?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    Error::new(ErrorCode::NotObserved, "inherited parent image missing")
                })?;
            charge(&mut self.budget, 1 + image.cells.len())?;
            self.out.push(image.clone());
            Some(image)
        } else {
            None
        };
        self.cache.insert(key, found.clone());
        Ok(found)
    }

    fn collect(
        &mut self,
        schema: &Schema,
        table: &Table,
        image: &Image,
        policy: &PolicyExpr,
        depth: usize,
    ) -> Result<(), Error> {
        charge(&mut self.budget, 1)?;
        match policy {
            PolicyExpr::And(items) | PolicyExpr::Or(items) => {
                for policy in items {
                    self.collect(schema, table, image, policy, depth)?;
                }
            }
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => {
                let depth = max_depth.unwrap_or(depth).min(depth);
                if depth == 0 {
                    return Ok(());
                }
                let target = reference(schema, table, via_column)?;
                let value = image_cell(image, table.stored_name(via_column)).unwrap_or(Sql::Null);
                let values = if crate::compile::is_array(table, via_column) {
                    crate::array_index::members(&value)?
                } else {
                    vec![value]
                };
                charge(&mut self.budget, values.len())?;
                let governing = schema.policy_schema();
                let target = governing
                    .by_stored(&target.stored)
                    .ok_or_else(|| unsupported("unknown inherited policy table"))?;
                for value in values {
                    let Sql::Blob(id) = value else {
                        continue;
                    };
                    let id =
                        RowUuid::from_bytes(id.try_into().map_err(|_| {
                            Error::new(ErrorCode::Storage, "invalid inheritance UUID")
                        })?);
                    let Some(found) = self.lookup(target, id)? else {
                        continue;
                    };
                    for (operation, depth) in [
                        (*operation, depth - 1),
                        (Operation::Select, DEFAULT_INHERITS_DEPTH),
                    ] {
                        if self.visited.insert((
                            target.stored.clone(),
                            found.id,
                            found.seq,
                            operation,
                            depth,
                        )) && let Some(policy) = target.inherited_policy(operation)
                        {
                            self.collect(governing, target, &found, policy, depth)?;
                        }
                    }
                }
            }
            _ if row_local_policy(policy) => {}
            _ => {
                return Err(unsupported(
                    "remote inheritance with negated, reverse or relational policy dependencies",
                ));
            }
        }
        Ok(())
    }
}

impl Inner {
    pub(super) fn check_inheritance_shape(&self, prepared: &PreparedQuery) -> Result<(), Error> {
        if prepared.query.inherits.is_empty() {
            return Ok(());
        }
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let table = schema.table(&prepared.query.table)?;
        if !table.branch_by.is_empty() || is_history_table(&prepared.query.table) {
            return Err(unsupported("remote inheritance over branches or history"));
        }
        let mut seen = HashSet::new();
        let mut budget = scoped_reads::MAX_EVIDENCE;
        for via in &prepared.query.inherits {
            validate(
                &schema,
                table,
                &predicate(via),
                DEFAULT_INHERITS_DEPTH,
                &mut seen,
                &mut budget,
            )?;
        }
        let governing = schema.policy_schema();
        if let Some(table) = governing.by_stored(&table.stored)
            && let Some(policy) = table.policy(Operation::Select)
        {
            validate(
                governing,
                table,
                policy,
                DEFAULT_INHERITS_DEPTH,
                &mut seen,
                &mut budget,
            )?;
        }
        Ok(())
    }

    pub(super) fn inheritance_inputs(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        roots: &[CurrentRow],
        read: &ReadSpec,
    ) -> Result<Vec<Image>, Error> {
        if prepared.query.inherits.is_empty() {
            return Ok(Vec::new());
        }
        self.check_inheritance_shape(prepared)?;
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let table = schema.table(&prepared.query.table)?;
        let mut budget = scoped_reads::MAX_EVIDENCE;
        charge(&mut budget, roots.len())?;
        let mut checked = prepared.clone();
        checked.query.filters.push(in_list(
            col("id"),
            roots.iter().map(|row| lit(Value::Uuid(row.id.0))),
        ));
        let visible = ReadSpec {
            readable_inheritance: true,
            members_only: self.client.is_some() || read.members_only,
            ..read.clone()
        };
        let reproduced =
            self.with_candidate_sql_budget(&mut budget, || self.run(&checked, identity, &visible))?;
        if roots.iter().map(|row| row.id).collect::<BTreeSet<_>>()
            != reproduced.iter().map(|row| row.id).collect::<BTreeSet<_>>()
        {
            return Err(unsupported(
                "remote inheritance whose capability requires a parent that is not readable",
            ));
        }
        let mut walk = Walk {
            inner: self,
            schema: &schema,
            prepared,
            identity,
            read,
            budget,
            cache: BTreeMap::new(),
            visited: HashSet::new(),
            out: Vec::new(),
        };
        let governing = schema.policy_schema();
        for row in roots {
            let images = self.scoped_evidence_image(&ScopedReadRowV1 {
                table: table.stored.clone(),
                id: *row.id.as_bytes(),
                key: Vec::new(),
                seq: row.seq,
            })?;
            for image in images {
                for via in &prepared.query.inherits {
                    walk.collect(
                        &schema,
                        table,
                        &image,
                        &predicate(via),
                        DEFAULT_INHERITS_DEPTH,
                    )?;
                }
                if let Some(table) = governing.by_stored(&table.stored)
                    && let Some(policy) = table.policy(Operation::Select)
                {
                    walk.collect(governing, table, &image, policy, DEFAULT_INHERITS_DEPTH)?;
                }
            }
        }
        Ok(walk.out)
    }
}
