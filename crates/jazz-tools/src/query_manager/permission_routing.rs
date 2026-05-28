use std::collections::HashMap;

use crate::metadata::RowProvenance;
use crate::object::{BranchName, ObjectId};
use crate::storage::Storage;

use super::graph_nodes::policy_eval::PolicyContextEvaluator;
use super::manager::QueryManager;
use super::policy::{BranchPolicyContext, Operation, PolicyExpr};
use super::session::Session;
use super::settlement_eval_cache::SettlementEvalCache;
use super::types::{
    ComposedBranchName, PermissionPhase, Row, RowDescriptor, RowPolicyMode, Schema, SchemaHash,
    TableName, TablePolicies, TableSchema,
};

/// Inputs for evaluating one already-selected policy expression.
///
/// This is the low-level evaluation shape: the caller has already decided which
/// policy applies. `branch_context` is present only when a `forBranch` policy is
/// being evaluated and `$branch.*` references need backing-row values.
pub(crate) struct AuthorizationPolicyRequest<'a> {
    pub(crate) object_id: ObjectId,
    pub(crate) branch_name: BranchName,
    pub(crate) table_name: TableName,
    pub(crate) policy: &'a PolicyExpr,
    pub(crate) content: &'a [u8],
    pub(crate) provenance: &'a RowProvenance,
    pub(crate) session: &'a Session,
    pub(crate) auth_schema: &'a Schema,
    pub(crate) auth_context: &'a crate::schema_manager::SchemaContext,
    pub(crate) source_branch_schema_map: &'a HashMap<String, SchemaHash>,
    pub(crate) operation: Operation,
    pub(crate) settlement_eval_cache: Option<&'a mut SettlementEvalCache>,
    pub(crate) branch_context: Option<&'a BranchPolicyContext<'a>>,
}

/// Inputs for evaluating a permission route.
///
/// This is the preferred shape for permission checks. The caller supplies the
/// row and operation, while the route decides which policy expression applies
/// and whether a missing policy should allow or deny.
pub(crate) struct PermissionEvaluationRequest<'a> {
    pub(crate) object_id: ObjectId,
    pub(crate) branch_name: BranchName,
    pub(crate) table_name: TableName,
    pub(crate) content: &'a [u8],
    pub(crate) provenance: &'a RowProvenance,
    pub(crate) session: &'a Session,
    pub(crate) auth_schema: &'a Schema,
    pub(crate) auth_context: &'a crate::schema_manager::SchemaContext,
    pub(crate) source_branch_schema_map: &'a HashMap<String, SchemaHash>,
    pub(crate) operation: Operation,
    pub(crate) phase: super::types::PermissionPhase,
    pub(crate) settlement_eval_cache: Option<&'a mut SettlementEvalCache>,
}

/// Backing row resolved from a composed branch id for `forBranch` policies.
///
/// For a branch like `<env>/<schema>/<row-id>`, the user branch segment points
/// at a row on the composed main branch. That row becomes the `$branch` context
/// exposed to branch policies.
pub(crate) struct ResolvedBranchPolicyBacking {
    pub(crate) backing_table: TableName,
    pub(crate) row_id: ObjectId,
    pub(crate) descriptor: RowDescriptor,
    pub(crate) content: Vec<u8>,
    pub(crate) provenance: RowProvenance,
}

/// Permission route for a row.
///
/// Normal rows use table policies directly. Composed non-main branches never
/// fall back to normal policies; they either use a resolved `forBranch` policy,
/// carry no branch policy so missing-policy rules apply, or deny immediately.
pub(crate) enum PermissionRoute<'a> {
    Normal {
        policies: &'a TablePolicies,
    },
    Branch {
        policies: Option<&'a TablePolicies>,
        backing: Option<ResolvedBranchPolicyBacking>,
    },
    Denied,
}

/// Result of evaluating a route.
///
/// Callers use this when they need different error messages for missing policy,
/// route resolution failure, and policy expression failure.
pub(crate) enum PermissionDecision {
    Allowed,
    DeniedRoute,
    DeniedMissingPolicy,
    DeniedPolicy,
}

/// Result of trying one `forBranch` backing table.
///
/// `NotFound` means this backing table did not contain the branch row, so the
/// router may try another `forBranch` entry. `Denied` means a row existed or was
/// otherwise resolved far enough to fail hard, so resolution must stop.
pub(crate) enum BranchBackingResolution {
    Found(ResolvedBranchPolicyBacking),
    NotFound,
    Denied,
}

pub(crate) fn branch_policy_scope(branch_name: &BranchName) -> Option<ComposedBranchName> {
    ComposedBranchName::parse_non_main(branch_name)
}

pub(crate) fn branch_main_name(composed: &ComposedBranchName) -> BranchName {
    ComposedBranchName::new(&composed.env, composed.schema_hash, "main").to_branch_name()
}

impl PermissionRoute<'_> {
    pub(crate) fn policies(&self) -> Option<&TablePolicies> {
        match self {
            Self::Normal { policies } => Some(policies),
            Self::Branch { policies, .. } => *policies,
            Self::Denied => None,
        }
    }

    pub(crate) fn is_branch(&self) -> bool {
        matches!(self, Self::Branch { .. })
    }

    pub(crate) fn is_denied(&self) -> bool {
        matches!(self, Self::Denied)
    }

    pub(crate) fn policy_for_operation(
        &self,
        operation: Operation,
        phase: super::types::PermissionPhase,
    ) -> Option<&PolicyExpr> {
        self.policies()
            .and_then(|policies| policies.policy_for_operation(operation, phase))
    }

    pub(crate) fn branch_context(&self) -> Option<BranchPolicyContext<'_>> {
        match self {
            Self::Branch {
                backing: Some(backing),
                ..
            } => Some(BranchPolicyContext {
                table_name: &backing.backing_table,
                row_id: backing.row_id,
                descriptor: &backing.descriptor,
                content: &backing.content,
                provenance: &backing.provenance,
            }),
            _ => None,
        }
    }

    pub(crate) fn allows_missing_policy(
        &self,
        operation: Operation,
        row_policy_mode: RowPolicyMode,
    ) -> bool {
        let Some(policies) = self.policies() else {
            return !row_policy_mode.denies_missing_explicit_policy();
        };

        if operation == Operation::Update && policies.has_explicit_update_policy() {
            return true;
        }

        !row_policy_mode.denies_missing_explicit_policy()
    }

    pub(crate) fn missing_policy_decision(
        &self,
        operation: Operation,
        row_policy_mode: RowPolicyMode,
    ) -> PermissionDecision {
        if self.allows_missing_policy(operation, row_policy_mode) {
            PermissionDecision::Allowed
        } else {
            PermissionDecision::DeniedMissingPolicy
        }
    }

    pub(crate) fn has_policy_for_operation(
        &self,
        operation: Operation,
        phase: PermissionPhase,
    ) -> bool {
        self.policy_for_operation(operation, phase).is_some()
    }
}

pub(crate) fn resolve_permission_route_with_backing_loader<'a, F>(
    branch_name: BranchName,
    table_name: TableName,
    auth_schema: &'a Schema,
    row_policy_mode: RowPolicyMode,
    mut resolve_backing: F,
) -> PermissionRoute<'a>
where
    F: FnMut(&TableName, &TableSchema, ObjectId, BranchName) -> BranchBackingResolution,
{
    // Routing owns the branch-shape rules. The caller-owned callback owns the
    // storage/evaluation details, which differ between QueryManager and graph
    // policy filters.
    let Some(target_schema) = auth_schema.get(&table_name) else {
        return PermissionRoute::Denied;
    };
    let Some(composed) = branch_policy_scope(&branch_name) else {
        return PermissionRoute::Normal {
            policies: &target_schema.policies,
        };
    };

    if target_schema.policies.for_branch.is_empty() {
        return PermissionRoute::Branch {
            policies: None,
            backing: None,
        };
    }

    let Ok(branch_uuid) = uuid::Uuid::parse_str(&composed.user_branch) else {
        return PermissionRoute::Denied;
    };
    let branch_object_id = ObjectId::from_uuid(branch_uuid);
    let main_branch = branch_main_name(&composed);

    for (backing_table, branch_policies) in &target_schema.policies.for_branch {
        let Some(backing_schema) = auth_schema.get(backing_table) else {
            return PermissionRoute::Denied;
        };
        let backing =
            match resolve_backing(backing_table, backing_schema, branch_object_id, main_branch) {
                BranchBackingResolution::Found(backing) => backing,
                BranchBackingResolution::NotFound => continue,
                BranchBackingResolution::Denied => return PermissionRoute::Denied,
            };

        return PermissionRoute::Branch {
            policies: Some(branch_policies),
            backing: Some(backing),
        };
    }

    let _ = row_policy_mode;
    PermissionRoute::Denied
}

impl QueryManager {
    pub(crate) fn evaluate_authorization_policy(
        &mut self,
        storage: &dyn Storage,
        request: AuthorizationPolicyRequest<'_>,
    ) -> bool {
        let AuthorizationPolicyRequest {
            object_id,
            branch_name,
            table_name,
            policy,
            content,
            provenance,
            session,
            auth_schema,
            auth_context,
            source_branch_schema_map,
            operation,
            settlement_eval_cache,
            branch_context,
        } = request;

        let Some(table_schema) = auth_schema.get(&table_name) else {
            return false;
        };
        let Some(transformed) = self.transform_content_to_authorization_schema(
            table_name.as_str(),
            content,
            crate::row_histories::BatchId([0; 16]),
            branch_name,
            source_branch_schema_map,
            auth_context,
        ) else {
            return false;
        };

        let mut evaluator = PolicyContextEvaluator::new(
            auth_schema,
            session,
            branch_name.as_str(),
            self.row_policy_mode,
        )
        .with_settlement_eval_cache(settlement_eval_cache);
        if let Some(branch_context) = branch_context {
            evaluator = evaluator.with_branch_context(branch_context);
        }
        let row = Row::new(
            object_id,
            transformed,
            crate::row_histories::BatchId([0; 16]),
            provenance.clone(),
        );
        let mut visited = std::collections::HashSet::new();
        let mut row_loader = |related_id: ObjectId, _table_hint: Option<TableName>| {
            self.load_row_for_authorization_context(
                storage,
                related_id,
                branch_name,
                source_branch_schema_map,
                auth_context,
            )
        };

        evaluator.evaluate_row_access(
            operation,
            &row,
            &table_schema.columns,
            table_name.as_str(),
            Some(policy),
            storage,
            &mut row_loader,
            0,
            &mut visited,
        )
    }

    pub(crate) fn evaluate_permission_route(
        &mut self,
        storage: &dyn Storage,
        route: &PermissionRoute<'_>,
        request: PermissionEvaluationRequest<'_>,
    ) -> bool {
        matches!(
            self.evaluate_permission_route_decision(storage, route, request),
            PermissionDecision::Allowed
        )
    }

    pub(crate) fn evaluate_permission_route_decision(
        &mut self,
        storage: &dyn Storage,
        route: &PermissionRoute<'_>,
        request: PermissionEvaluationRequest<'_>,
    ) -> PermissionDecision {
        if route.is_denied() {
            return PermissionDecision::DeniedRoute;
        }
        let Some(policy) = route
            .policies()
            .and_then(|policies| policies.policy_for_operation(request.operation, request.phase))
        else {
            return route.missing_policy_decision(request.operation, self.row_policy_mode);
        };

        let branch_context = route.branch_context();
        if self.evaluate_authorization_policy(
            storage,
            AuthorizationPolicyRequest {
                object_id: request.object_id,
                branch_name: request.branch_name,
                table_name: request.table_name,
                policy,
                content: request.content,
                provenance: request.provenance,
                session: request.session,
                auth_schema: request.auth_schema,
                auth_context: request.auth_context,
                source_branch_schema_map: request.source_branch_schema_map,
                operation: request.operation,
                settlement_eval_cache: request.settlement_eval_cache,
                branch_context: branch_context.as_ref(),
            },
        ) {
            PermissionDecision::Allowed
        } else {
            PermissionDecision::DeniedPolicy
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_permission_route<'a>(
        &mut self,
        storage: &dyn Storage,
        branch_name: BranchName,
        table_name: TableName,
        session: &Session,
        auth_schema: &'a Schema,
        auth_context: &crate::schema_manager::SchemaContext,
        source_branch_schema_map: &HashMap<String, SchemaHash>,
    ) -> PermissionRoute<'a> {
        resolve_permission_route_with_backing_loader(
            branch_name,
            table_name,
            auth_schema,
            self.row_policy_mode,
            |backing_table, backing_schema, branch_object_id, main_branch| {
                let Ok(Some(backing_row)) = storage.load_visible_region_row(
                    backing_table.as_str(),
                    main_branch.as_str(),
                    branch_object_id,
                ) else {
                    return BranchBackingResolution::NotFound;
                };
                if backing_row.is_hard_deleted() {
                    return BranchBackingResolution::Denied;
                };
                let backing_provenance = backing_row.row_provenance();
                let Some(transformed_backing_content) = self
                    .transform_content_to_authorization_schema(
                        backing_table.as_str(),
                        &backing_row.data,
                        backing_row.batch_id,
                        main_branch,
                        source_branch_schema_map,
                        auth_context,
                    )
                else {
                    return BranchBackingResolution::Denied;
                };

                if let Some(backing_select) = backing_schema.policies.select_policy() {
                    if !self.evaluate_authorization_policy(
                        storage,
                        AuthorizationPolicyRequest {
                            object_id: branch_object_id,
                            branch_name: main_branch,
                            table_name: *backing_table,
                            policy: backing_select,
                            content: &backing_row.data,
                            provenance: &backing_provenance,
                            session,
                            auth_schema,
                            auth_context,
                            source_branch_schema_map,
                            operation: Operation::Select,
                            settlement_eval_cache: None,
                            branch_context: None,
                        },
                    ) {
                        return BranchBackingResolution::Denied;
                    }
                } else if self.row_policy_mode.denies_missing_explicit_policy() {
                    return BranchBackingResolution::Denied;
                }

                BranchBackingResolution::Found(ResolvedBranchPolicyBacking {
                    backing_table: *backing_table,
                    row_id: branch_object_id,
                    descriptor: backing_schema.columns.clone(),
                    content: transformed_backing_content,
                    provenance: backing_provenance,
                })
            },
        )
    }
}
