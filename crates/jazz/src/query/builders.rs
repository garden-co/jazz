/// A table participating in a query, optionally under a public alias.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef {
    table: String,
    alias: Option<String>,
}

impl TableRef {
    /// Assign the name used to qualify this table's fields.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
}

impl<T> From<T> for TableRef
where
    T: Into<String>,
{
    fn from(table: T) -> Self {
        Self {
            table: table.into(),
            alias: None,
        }
    }
}

/// Refer to a table that may be assigned an alias.
pub fn table(name: impl Into<String>) -> TableRef {
    TableRef::from(name.into())
}

/// Configuration for recursively gathering rows through an edge relation.
#[derive(Clone, Debug, PartialEq)]
pub struct Gather {
    step_table: String,
    current_column: Option<String>,
    hop_column: Option<String>,
    frontier_column: Option<String>,
    filters: Vec<Predicate>,
    bound: RecursionBound,
}

impl Gather {
    /// Start a recursive step from `table`.
    pub fn from(table: impl Into<String>) -> Self {
        Self {
            step_table: table.into(),
            current_column: None,
            hop_column: None,
            frontier_column: None,
            filters: Vec::new(),
            bound: RecursionBound::default_max_depth(),
        }
    }

    /// Match this step-table column against the current recursive frontier.
    pub fn where_current(mut self, column: impl Into<String>) -> Self {
        self.current_column = Some(column.into());
        self
    }

    /// Follow this step-table reference to the next gathered row.
    pub fn hop_to(mut self, column: impl Into<String>) -> Self {
        self.hop_column = Some(column.into());
        self
    }

    /// Use an application column as the recursive frontier instead of row ids.
    pub fn frontier_column(mut self, column: impl Into<String>) -> Self {
        self.frontier_column = Some(column.into());
        self
    }

    /// Restrict the step rows traversed during recursion.
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Stop after at most `max_depth` recursive steps. Zero evaluates only the seed.
    pub fn max_depth(mut self, max_depth: usize) -> Self {
        self.bound = RecursionBound::MaxDepth(max_depth);
        self
    }

    /// Continue until the recursive frontier reaches a fixpoint.
    pub fn until_fixpoint(mut self) -> Self {
        self.bound = RecursionBound::Fixpoint;
        self
    }
}

impl Query {
    /// Construct a query rooted at `table`.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues");
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn from(source: impl Into<TableRef>) -> Self {
        let source = source.into();
        let flat_join = source.alias.map(|root_alias| FlatJoin {
            root_alias: Some(root_alias),
            sources: Vec::new(),
        });
        Self {
            table: source.table,
            filters: Vec::new(),
            joins: Vec::new(),
            flat_join,
            policy_branches: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
            includes: Vec::new(),
            array_subqueries: Vec::new(),
            select: None,
            order_by: Vec::new(),
            aggregate: None,
            limit: None,
            offset: 0,
        }
    }

    /// Add a policy-only OR branch. Runtime query evaluation ignores these;
    /// row policy checks treat the base query and every branch as alternatives.
    pub fn policy_branch(mut self, branch: PolicyBranch) -> Self {
        self.policy_branches.push(branch);
        self
    }

    /// Add a filter.
    ///
    /// ```rust
    /// # use jazz::query::{col, doctest_support, eq, param, Query};
    /// let query = Query::from("issues").filter(eq(col("assignee"), param("user")));
    ///
    /// let validated = query.validate(&doctest_support::schema())?;
    /// assert!(validated.params().contains_key("user"));
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Add a source to an output-changing flat join.
    ///
    /// Unlike [`Query::join_via`], a flat join emits fields from every source
    /// and may produce multiple result occurrences for one root row.
    pub fn flat_join(
        mut self,
        source: impl Into<TableRef>,
        left: impl Into<String>,
        right: impl Into<String>,
    ) -> Self {
        let source = source.into();
        let flat_join = self.flat_join.get_or_insert_with(|| FlatJoin {
            root_alias: None,
            sources: Vec::new(),
        });
        flat_join.sources.push(FlatJoinSource {
            table: source.table,
            alias: source.alias,
            on: FlatJoinOn {
                left: left.into(),
                right: right.into(),
            },
        });
        self
    }

    /// Gather root-table rows recursively through a relation step.
    ///
    /// Filters already present on the query select the seed rows. Filters
    /// added after `gather` apply to the gathered output. Row ids form the
    /// frontier by default; use [`Gather::frontier_column`] for scalar values.
    pub fn gather(mut self, gather: Gather) -> Self {
        let root_table = self.table.clone();
        let (frontier_column, access_team_target) = gather
            .frontier_column
            .map(|column| (column, JoinTarget::Column))
            .unwrap_or_else(|| ("id".to_owned(), JoinTarget::RowId));
        let edge_member_column = gather
            .current_column
            .expect("Gather::where_current must be called before Query::gather");
        let edge_parent_column = gather
            .hop_column
            .expect("Gather::hop_to must be called before Query::gather");
        let seed_filters = std::mem::take(&mut self.filters);

        self.reachable.push(ReachableVia {
            access_table: root_table.clone(),
            access_row_column: frontier_column.clone(),
            access_team_column: frontier_column.clone(),
            access_team_target,
            from: Operand::Literal(Value::Uuid(uuid::Uuid::nil())),
            access_filters: Vec::new(),
            edge_table: gather.step_table,
            edge_member_column,
            edge_parent_column,
            edge_filters: gather.filters,
            bound: gather.bound,
            seed: Some(ReachableSeed {
                table: root_table,
                user_column: None,
                user_claim: None,
                team_column: frontier_column,
                filters: seed_filters,
            }),
        });
        self
    }

    /// Add a junction traversal.
    ///
    /// ```rust
    /// # use jazz::query::{col, doctest_support, eq, param, Query};
    /// let query = Query::from("issues")
    ///     .join_via("issue_tags", "issue", [eq(col("tag"), param("tag"))]);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn join_via(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: None,
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a junction traversal correlated through a root-table reference column.
    ///
    /// This expresses `exists table where table.on_column = root.source_column`.
    pub fn join_via_column(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        source_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a junction traversal with extra source-row equality correlations.
    pub fn join_via_column_with_correlations(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        source_column: impl Into<String>,
        correlated_filters: impl IntoIterator<Item = JoinCorrelation>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: correlated_filters.into_iter().collect(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a traversal correlated through a referenced source row.
    ///
    /// This expresses `exists table where table.on_column = source.value_column`,
    /// with `source.id = root.row_id_source_column`.
    pub fn join_via_source_lookup(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        source_lookup: JoinSourceLookup,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self = self.join_via_source_lookup_with_target(
            table,
            on_column,
            JoinTarget::Column,
            source_lookup,
            filters,
        );
        self
    }

    /// Add a traversal correlated through a referenced source row with an explicit target.
    pub fn join_via_source_lookup_with_target(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        target: JoinTarget,
        source_lookup: JoinSourceLookup,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target,
            source_column: Some(source_lookup.value_column.clone()),
            source_lookup: Some(source_lookup),
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a junction traversal whose matched row must satisfy nested policy joins.
    pub fn join_via_with_nested_joins(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
        nested_joins: impl IntoIterator<Item = JoinVia>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: None,
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: nested_joins.into_iter().collect(),
        });
        self
    }

    /// Add a row-correlated traversal to rows whose id is referenced by a root-table column.
    ///
    /// This expresses `exists table where table.id = root.source_column`.
    pub fn join_via_row_id(
        mut self,
        table: impl Into<String>,
        source_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: "id".to_owned(),
            target: JoinTarget::RowId,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a row-id traversal with extra source-row equality correlations.
    pub fn join_via_row_id_with_correlations(
        mut self,
        table: impl Into<String>,
        source_column: impl Into<String>,
        correlated_filters: impl IntoIterator<Item = JoinCorrelation>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: "id".to_owned(),
            target: JoinTarget::RowId,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: correlated_filters.into_iter().collect(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a recursive reachability traversal through an access table and edge table.
    #[allow(clippy::too_many_arguments)]
    pub fn reachable_via(
        mut self,
        access_table: impl Into<String>,
        access_row_column: impl Into<String>,
        access_team_column: impl Into<String>,
        from: Operand,
        edge_table: impl Into<String>,
        edge_member_column: impl Into<String>,
        edge_parent_column: impl Into<String>,
        edge_filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self = self.reachable_via_with_access_filters(
            access_table,
            access_row_column,
            access_team_column,
            from,
            [],
            edge_table,
            edge_member_column,
            edge_parent_column,
            edge_filters,
        );
        self
    }

    /// Add a recursive reachability traversal with predicates on both the
    /// access edge and recursive edge tables.
    #[allow(clippy::too_many_arguments)]
    pub fn reachable_via_with_access_filters(
        mut self,
        access_table: impl Into<String>,
        access_row_column: impl Into<String>,
        access_team_column: impl Into<String>,
        from: Operand,
        access_filters: impl IntoIterator<Item = Predicate>,
        edge_table: impl Into<String>,
        edge_member_column: impl Into<String>,
        edge_parent_column: impl Into<String>,
        edge_filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.reachable.push(ReachableVia {
            access_table: access_table.into(),
            access_row_column: access_row_column.into(),
            access_team_column: access_team_column.into(),
            access_team_target: JoinTarget::Column,
            from,
            access_filters: access_filters.into_iter().collect(),
            edge_table: edge_table.into(),
            edge_member_column: edge_member_column.into(),
            edge_parent_column: edge_parent_column.into(),
            edge_filters: edge_filters.into_iter().collect(),
            bound: RecursionBound::default_max_depth(),
            seed: None,
        });
        self
    }

    /// Use a seed relation for the most recently added reachable traversal.
    ///
    /// The seed relation contributes initial teams by filtering `seed_table`
    /// rows where `user_column == claim(claim_path)`, then projecting
    /// `team_column` into the recursive frontier.
    pub fn seeded_by(
        mut self,
        seed_table: impl Into<String>,
        user_column: impl Into<String>,
        claim_path: impl Into<String>,
        team_column: impl Into<String>,
    ) -> Self {
        let Some(reachable) = self.reachable.last_mut() else {
            panic!("seeded_by requires a preceding reachable_via traversal");
        };
        let user_column = user_column.into();
        let claim_path = claim_path.into();
        reachable.seed = Some(ReachableSeed {
            table: seed_table.into(),
            user_column: Some(user_column.clone()),
            user_claim: Some(claim_path.clone()),
            team_column: team_column.into(),
            filters: Vec::new(),
        });
        self
    }

    /// Require the row referenced by `parent_column` to be readable under the
    /// parent table's composed read policy.
    pub fn inherits(mut self, parent_column: impl Into<String>) -> Self {
        self.inherits.push(InheritsVia {
            parent_column: parent_column.into(),
            operation: InheritsOperation::Select,
            max_depth: None,
        });
        self
    }

    /// Require the row referenced by `parent_column` to be readable under the
    /// parent table's composed read policy, with a bound for recursion through
    /// the same inheritance atom. Zero performs no inheritance hop and denies.
    pub fn inherits_with_depth(
        mut self,
        parent_column: impl Into<String>,
        max_depth: usize,
    ) -> Self {
        self.inherits.push(InheritsVia {
            parent_column: parent_column.into(),
            operation: InheritsOperation::Select,
            max_depth: Some(max_depth),
        });
        self
    }

    /// Require the row referenced by `parent_column` to satisfy the parent
    /// table policy for `operation`.
    pub fn inherits_operation(
        mut self,
        parent_column: impl Into<String>,
        operation: InheritsOperation,
    ) -> Self {
        self.inherits.push(InheritsVia {
            parent_column: parent_column.into(),
            operation,
            max_depth: None,
        });
        self
    }

    /// Require the row referenced by `parent_column` to satisfy the parent
    /// policy for `operation`, with a bound for recursive inheritance. Zero
    /// performs no inheritance hop and denies.
    pub fn inherits_operation_with_depth(
        mut self,
        parent_column: impl Into<String>,
        operation: InheritsOperation,
        max_depth: usize,
    ) -> Self {
        self.inherits.push(InheritsVia {
            parent_column: parent_column.into(),
            operation,
            max_depth: Some(max_depth),
        });
        self
    }

    /// Add an include path such as `project.org`.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").include("project.org");
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn include(mut self, path: impl Into<String>) -> Self {
        self.includes.push(Include::new(path));
        self
    }

    /// Add an include path with options.
    pub fn include_with(mut self, include: Include) -> Self {
        self.includes.push(include);
        self
    }

    /// Add a correlated relation array subquery.
    pub fn array_subquery(mut self, subquery: ArraySubquery) -> Self {
        self.array_subqueries.push(subquery);
        self
    }

    /// Select application columns. The row id is always included.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").select(["title", "state"]);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn select(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.select = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Add a result-level ordering key.
    ///
    /// Multiple calls preserve precedence: earlier keys are compared first.
    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDirection) -> Self {
        self.order_by.push(OrderBy {
            column: column.into(),
            direction,
        });
        self
    }

    /// Count result rows.
    pub fn count(mut self) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::count()])));
        self
    }

    /// Sum a numeric result column.
    pub fn sum(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::sum(column)])));
        self
    }

    /// Average a numeric result column.
    pub fn avg(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::avg(column)])));
        self
    }

    /// Find the minimum value for an orderable result column.
    pub fn min(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::min(column)])));
        self
    }

    /// Find the maximum value for an orderable result column.
    pub fn max(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::max(column)])));
        self
    }

    /// Replace the aggregate list for this query.
    pub fn aggregate(mut self, aggregates: impl IntoIterator<Item = Aggregate>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new(aggregates)));
        self
    }

    /// Group aggregate output by a root-table column.
    pub fn group_by(mut self, column: impl Into<String>) -> Self {
        let aggregate = self
            .aggregate
            .get_or_insert_with(|| Box::new(AggregateQuery::new([Aggregate::count()])));
        aggregate.group_by = Some(column.into());
        self
    }

    /// Limit result rows after filtering.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").limit(25);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip result rows after filtering.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").offset(50);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Validate and canonicalize this query against a Jazz schema.
    pub fn validate(&self, schema: &JazzSchema) -> Result<ValidatedQuery, QueryError> {
        self.validate_runtime(schema.runtime())
    }

    pub(crate) fn validate_runtime(
        &self,
        schema: &RuntimeSchema,
    ) -> Result<ValidatedQuery, QueryError> {
        validate_query(self, schema)
    }

    pub(crate) fn validate_with_schema_version(
        &self,
        schema: &RuntimeSchema,
        schema_version: SchemaVersionId,
    ) -> Result<ValidatedQuery, QueryError> {
        validate_query_with_schema_version(self, schema, schema_version)
    }
}
