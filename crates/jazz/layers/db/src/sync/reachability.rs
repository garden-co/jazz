//! Readable witnesses for synced recursive queries. Work begins at returned
//! roots and follows indexed reverse edges, never loading a whole input table.
use super::*;
use crate::query::{JoinTarget, Predicate, RecursionBound, col, eq, lit};
use crate::records::ValueType;
use crate::values::Table;

pub(super) fn needed(query: &Query) -> bool {
    query.reachable.iter().any(|via| !gather_shaped(query, via))
}

fn uuid_column(table: &Table, name: &str) -> bool {
    name == "id"
        || table.column(name).is_some_and(|column| match &column.ty {
            ValueType::Uuid => true,
            ValueType::Nullable(inner) => matches!(inner.as_ref(), ValueType::Uuid),
            _ => false,
        })
}

fn indexed(table: &Table, name: &str) -> bool {
    name == "id"
        || table
            .indexes
            .iter()
            .any(|index| index.first().is_some_and(|first| first == name))
}

fn charge(budget: &mut usize, count: usize) -> Result<(), Error> {
    *budget = budget.checked_sub(count).ok_or_else(|| {
        unsupported("remote reachability witnesses exceeding their bounded work limit")
    })?;
    Ok(())
}

fn uuid_value(image: &Image, column: &str) -> Result<Option<RowUuid>, Error> {
    match image_cell(image, column) {
        None | Some(Sql::Null) => Ok(None),
        Some(Sql::Blob(value)) => {
            Ok(Some(RowUuid::from_bytes(value.try_into().map_err(
                |_| Error::new(ErrorCode::Storage, "invalid remote reachability UUID"),
            )?)))
        }
        _ => Err(Error::new(
            ErrorCode::Storage,
            "invalid remote reachability UUID",
        )),
    }
}

impl Inner {
    /// Validate every source even if the query currently returns no rows.
    pub(super) fn check_reachability_shape(&self, prepared: &PreparedQuery) -> Result<(), Error> {
        if !needed(&prepared.query) {
            return Ok(());
        }
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let root = schema.table(&prepared.query.table)?;
        if !root.branch_by.is_empty() || is_history_table(&prepared.query.table) {
            return Err(unsupported("remote reachability over branches or history"));
        }
        for via in prepared
            .query
            .reachable
            .iter()
            .filter(|via| !gather_shaped(&prepared.query, via))
        {
            let access = schema.table(&via.access_table)?;
            let edges = schema.table(&via.edge_table)?;
            let mut sources = vec![access, edges];
            let mut lookups = vec![
                (access, via.access_row_column.as_str()),
                (edges, via.edge_parent_column.as_str()),
            ];
            let mut columns = vec![
                (access, via.access_team_column.as_str()),
                (edges, via.edge_member_column.as_str()),
            ];
            if let Some(seed) = &via.seed {
                let seeds = schema.table(&seed.table)?;
                sources.push(seeds);
                lookups.push((seeds, seed.team_column.as_str()));
            }
            columns.extend(lookups.iter().copied());
            if via.access_team_target == JoinTarget::Uncorrelated
                || sources.iter().any(|source| !source.branch_by.is_empty())
                || columns
                    .iter()
                    .any(|(source, column)| !uuid_column(source, column))
                || lookups
                    .iter()
                    .any(|(source, column)| !indexed(source, column))
            {
                return Err(unsupported(
                    "remote reachability without unbranched indexed scalar UUID correlations",
                ));
            }
            // Cross-row support policies need their own witness closure. Until
            // that port exists, refuse rather than ship an incomplete graph.
            if sources.iter().any(|source| {
                schema
                    .policy_schema()
                    .by_stored(&source.stored)
                    .and_then(|source| source.policy(Operation::Select))
                    .is_some_and(|policy| !row_local_policy(policy))
            }) {
                return Err(unsupported(
                    "remote reachability whose supporting rows have cross-row read policies",
                ));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn reachability_lookup(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        table: &Table,
        column: &str,
        value: RowUuid,
        filters: &[Predicate],
        budget: &mut usize,
    ) -> Result<Vec<Image>, Error> {
        charge(budget, 1)?;
        let mut lookup = prepared.clone();
        lookup.query =
            Query::from(table.name.clone()).filter(eq(col(column), lit(Value::Uuid(value.0))));
        lookup.query.filters.extend_from_slice(filters);
        lookup.query.limit = Some(*budget + 1);
        let read = ReadSpec {
            include_deleted: false,
            members_only: read.members_only || self.client.is_some(),
            ..read.clone()
        };
        let rows = self.with_candidate_sql_budget(budget, || self.run(&lookup, identity, &read))?;
        charge(budget, rows.len())?;
        let mut images = Vec::new();
        for row in rows {
            images.extend(self.scoped_evidence_image(&ScopedReadRowV1 {
                table: table.stored.clone(),
                id: *row.id.as_bytes(),
                key: Vec::new(),
                seq: row.seq,
            })?);
        }
        Ok(images)
    }

    /// Collect before publishing: budget exhaustion never sends half a path.
    /// Breadth-first traversal bounds depth and visits each frontier once, even
    /// in cycles. Filters and session policies apply to every indexed lookup.
    pub(super) fn reachability_inputs(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        roots: &[RowUuid],
        read: &ReadSpec,
    ) -> Result<Vec<Image>, Error> {
        if !needed(&prepared.query) {
            return Ok(Vec::new());
        }
        self.check_reachability_shape(prepared)?;
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let mut budget = scoped_reads::MAX_EVIDENCE;
        charge(&mut budget, roots.len())?;
        let mut images = Vec::new();
        for via in prepared
            .query
            .reachable
            .iter()
            .filter(|via| !gather_shaped(&prepared.query, via))
        {
            let access = schema.table(&via.access_table)?;
            let edges = schema.table(&via.edge_table)?;
            let seed = via
                .seed
                .as_ref()
                .map(|seed| schema.table(&seed.table))
                .transpose()?;
            let mut seed_filters = via
                .seed
                .as_ref()
                .map(|seed| seed.filters.clone())
                .unwrap_or_default();
            if let Some(seed) = &via.seed
                && let (Some(column), Some(claim)) = (&seed.user_column, &seed.user_claim)
            {
                seed_filters.push(eq(col(column), crate::query::claim(claim)));
            }
            let mut frontier = VecDeque::new();
            for root in roots {
                let found = self.reachability_lookup(
                    prepared,
                    identity,
                    read,
                    access,
                    &via.access_row_column,
                    *root,
                    &via.access_filters,
                    &mut budget,
                )?;
                for image in &found {
                    if let Some(team) =
                        uuid_value(image, access.stored_name(&via.access_team_column))?
                    {
                        frontier.push_back((team, 0));
                    }
                }
                images.extend(found);
            }
            let mut visited = BTreeSet::new();
            while let Some((team, depth)) = frontier.pop_front() {
                if !visited.insert(team) {
                    continue;
                }
                if let (Some(seed), Some(source)) = (&via.seed, seed) {
                    images.extend(self.reachability_lookup(
                        prepared,
                        identity,
                        read,
                        source,
                        &seed.team_column,
                        team,
                        &seed_filters,
                        &mut budget,
                    )?);
                }
                if matches!(via.bound, RecursionBound::MaxDepth(max) if depth >= max) {
                    continue;
                }
                let found = self.reachability_lookup(
                    prepared,
                    identity,
                    read,
                    edges,
                    &via.edge_parent_column,
                    team,
                    &via.edge_filters,
                    &mut budget,
                )?;
                for image in &found {
                    if let Some(member) =
                        uuid_value(image, edges.stored_name(&via.edge_member_column))?
                    {
                        frontier.push_back((member, depth + 1));
                    }
                }
                images.extend(found);
            }
        }
        let mut sent = BTreeSet::new();
        images.retain(|image| sent.insert((image.table.clone(), image.id, image.seq)));
        Ok(images)
    }
}
