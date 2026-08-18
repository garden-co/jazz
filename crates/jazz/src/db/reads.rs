//! Prepared, one-shot, relation, and result-tree read APIs.

use super::*;

pub(super) fn prepare_query_loaded<S>(
    node: &Node<S>,
    schema: &JazzSchema,
    schema_version: SchemaVersionId,
    query: &Query,
) -> Result<PreparedQuery, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    prepare_query_bound_loaded(node, schema, schema_version, query, BTreeMap::new())
}

pub(super) fn prepare_query_bound_loaded<S>(
    node: &Node<S>,
    schema: &JazzSchema,
    schema_version: SchemaVersionId,
    query: &Query,
    params: BTreeMap<String, Value>,
) -> Result<PreparedQuery, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let shape = query.validate_with_schema_version(schema, schema_version)?;
    let binding = shape.bind(params)?;
    let (local_plan, global_plan) = if should_install_prepared_plan(&shape)
        && !node.node.borrow().uses_schema_projected_read(&shape)
    {
        let mut state = node.node.borrow_mut();
        (
            Some(state.prepared_query_plan(
                &shape,
                &binding,
                DurabilityTier::Local,
                AuthorId::SYSTEM,
            )?),
            Some(state.prepared_query_plan(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorId::SYSTEM,
            )?),
        )
    } else {
        (None, None)
    };
    let groove_runtime_token = node.node.borrow().groove_runtime_token();
    Ok(PreparedQuery {
        shape,
        binding,
        local_plan,
        global_plan,
        groove_runtime_token,
    })
}

pub(super) fn all_loaded<S>(
    node: &Node<S>,
    prepared: &PreparedQuery,
    opts: &ReadOpts,
    author: AuthorId,
    authorization_mode: QueryAuthorizationMode,
) -> Result<Vec<CurrentRow>, crate::node::Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let tier = effective_read_tier(opts);
    let upstream_tier = node
        .upstream_register_shape_options(
            tier,
            opts.read_view.clone(),
            opts.propagation == Propagation::Full,
        )
        .tier;
    let mut state = node.node.borrow_mut();
    match &opts.read_view.source {
        ReadViewSourceSpec::Current => {}
        ReadViewSourceSpec::Branch { branch } if !opts.include_deleted => {
            return match authorization_mode {
                QueryAuthorizationMode::TrustedServing => state.query_rows_on_branch_for_link(
                    crate::ids::BranchId(*branch),
                    &prepared.shape,
                    &prepared.binding,
                    author,
                ),
                QueryAuthorizationMode::ClientLocal if tier < DurabilityTier::Edge => state
                    .query_rows_on_branch_for_client(
                        crate::ids::BranchId(*branch),
                        &prepared.shape,
                        &prepared.binding,
                        author,
                    ),
                QueryAuthorizationMode::ClientLocal => state.query_rows_for_client_read_view(
                    &prepared.shape,
                    &prepared.binding,
                    upstream_tier,
                    &opts.read_view,
                ),
            };
        }
        _ => {}
    }
    match (opts.include_deleted, authorization_mode) {
        (true, mode) => state.query_rows_including_deleted_in_authorization_mode(
            &prepared.shape,
            &prepared.binding,
            tier,
            None,
            author,
            mode,
        ),
        (false, QueryAuthorizationMode::TrustedServing) => state
            .query_rows_with_prepared_plan_for_identity(
                &prepared.shape,
                &prepared.binding,
                tier,
                None,
                author,
            ),
        (false, QueryAuthorizationMode::ClientLocal) => {
            state.query_rows_for_client(&prepared.shape, &prepared.binding, tier, author)
        }
    }
}

pub(super) fn relation_snapshot_loaded<S>(
    node: &Node<S>,
    prepared: &PreparedQuery,
    opts: &ReadOpts,
    author: AuthorId,
    authorization_mode: QueryAuthorizationMode,
) -> Result<RelationSnapshot, crate::node::Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let tier = effective_read_tier(opts);
    let mut state = node.node.borrow_mut();
    match authorization_mode {
        QueryAuthorizationMode::ClientLocal => state.query_relation_snapshot_for_client(
            &prepared.shape,
            &prepared.binding,
            tier,
            author,
            &opts.read_view,
        ),
        QueryAuthorizationMode::TrustedServing => state
            .query_relation_snapshot_for_serving_in_read_view(
                &prepared.shape,
                &prepared.binding,
                tier,
                author,
                &opts.read_view,
            ),
    }
}
