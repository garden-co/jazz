//! Context-scoped mutable inputs for locally unavailable current rows.
//!
//! This module installs source exclusions; local_availability_receipts owns
//! durable ordering and recovery. The network owner verifies receipts before
//! applying them. The low-level boolean setter remains internal test setup.
//! Stored content remains intact and authorization proofs never read this input.
//!
//! Stable global table identities preserve exclusions across schema projection.
//! Historical and non-default views remain outside this current-read contract.

use super::*;
use crate::protocol::{CanonicalPolicyClaims, PolicyBindingKey};

#[derive(Clone, Debug, Default)]
pub(crate) struct LocalUnavailableInput {
    id: Option<InputSourceId>,
    runtime_token: u64,
    rows: BTreeSet<RowUuid>,
}

fn descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("row_uuid", ValueType::Uuid)])
}

pub(super) fn local_unavailable_policy_binding(
    request: &QueryProgramRequest,
) -> Option<PolicyBindingKey> {
    if request.authorization_mode == QueryAuthorizationMode::TrustedServing {
        return None;
    }
    policy_binding(&request.policy)
}

fn policy_binding(policy: &PolicyContext) -> Option<PolicyBindingKey> {
    match policy {
        PolicyContext::Identity {
            permission_subject,
            claims,
            ..
        } => Some(PolicyBindingKey {
            identity: *permission_subject,
            canonical_claims: CanonicalPolicyClaims::new(claims.clone()),
        }),
        PolicyContext::System | PolicyContext::AuthorizationSubplan { .. } => None,
    }
}

pub(super) fn is_current_app_source(source: &SourceExpr<RequestedSourceStage>) -> bool {
    match source {
        SourceExpr::VisibleCurrent {
            data: DataSource::Current,
            ..
        } => true,
        SourceExpr::SettledBindingView {
            current_default, ..
        } => *current_default,
        SourceExpr::WithOverlays { input, overlays }
            if overlays.entries == [OverlayRef::PendingLocal] =>
        {
            is_current_app_source(input)
        }
        _ => false,
    }
}

impl<S: OrderedKvStorage> NodeState<S> {
    /// Capture the effective app identity without formatting raw claims.
    #[allow(dead_code)] // Network integration consumes this internal primitive.
    pub(crate) fn local_read_policy_binding(
        &self,
        identity: AuthorSubject,
    ) -> Option<PolicyBindingKey> {
        policy_binding(&self.query_program_policy_context(identity))
    }

    pub(crate) fn is_local_row_unavailable(
        &self,
        scope: &PolicyBindingKey,
        table: crate::ids::GlobalPhysicalTableId,
        row: RowUuid,
    ) -> bool {
        self.query
            .local_unavailable_inputs
            .get(&(scope.clone(), table))
            .is_some_and(|input| input.rows.contains(&row))
    }

    pub(crate) fn local_availability_table_id(
        &self,
        schema: SchemaVersionId,
        table: &str,
    ) -> Result<crate::ids::GlobalPhysicalTableId, Error> {
        self.catalogue
            .physical_mappings
            .get(&schema)
            .and_then(|mapping| mapping.identities.tables.get(table))
            .map(|table| table.id)
            .ok_or(Error::InvalidStoredValue(
                "local availability source has no global table identity",
            ))
    }

    /// Runtime-only setup primitive. Production receipt owners must use the
    /// durable typed apply API so a clear retains its ordering watermark.
    #[allow(dead_code)]
    pub(crate) async fn set_local_row_unavailable(
        &mut self,
        scope: &PolicyBindingKey,
        table: &str,
        row: RowUuid,
        unavailable: bool,
    ) -> Result<bool, Error> {
        if scope.identity == AuthorSubject::SYSTEM {
            return Ok(false);
        }
        let table =
            self.local_availability_table_id(self.catalogue.current_schema_version_id, table)?;
        self.update_local_unavailable_rows(scope, &[(table, row, unavailable)])
            .await
    }

    pub(super) async fn update_local_unavailable_rows(
        &mut self,
        scope: &PolicyBindingKey,
        rows: &[(crate::ids::GlobalPhysicalTableId, RowUuid, bool)],
    ) -> Result<bool, Error> {
        let mut changes =
            BTreeMap::<crate::ids::GlobalPhysicalTableId, (Vec<Vec<u8>>, Vec<Vec<u8>>)>::new();
        let mut changed = false;
        for (table, row, unavailable) in rows {
            let input = self
                .query
                .local_unavailable_inputs
                .get(&(scope.clone(), *table));
            let prior = input.is_some_and(|input| input.rows.contains(row));
            if prior == *unavailable {
                continue;
            }
            changed = true;
            if input.is_some_and(|input| {
                input.id.is_some() && input.runtime_token == self.groove_runtime_token
            }) {
                let record = descriptor().create(&[Value::Uuid(row.0)])?;
                let (adds, removes) = changes.entry(*table).or_default();
                if *unavailable {
                    adds.push(record);
                } else {
                    removes.push(record);
                }
            }
        }
        let deltas = changes
            .into_iter()
            .map(|(table, (adds, removes))| groove::ivm::InputSourceDelta {
                id: self.query.local_unavailable_inputs[&(scope.clone(), table)]
                    .id
                    .expect("live input"),
                descriptor: descriptor(),
                adds,
                removes,
            })
            .collect::<Vec<_>>();
        if !deltas.is_empty() {
            self.database
                .apply_input_source_deltas(deltas)
                .await
                .map_err(Error::Groove)?;
        }
        for (table, row, unavailable) in rows {
            let input = self
                .query
                .local_unavailable_inputs
                .entry((scope.clone(), *table))
                .or_default();
            if *unavailable {
                input.rows.insert(*row);
            } else {
                input.rows.remove(row);
            }
        }
        Ok(changed)
    }

    pub(super) fn require_local_availability_context_capacity(
        &self,
        scope: &PolicyBindingKey,
    ) -> Result<(), Error> {
        let live_scopes = self
            .query
            .local_unavailable_inputs
            .iter()
            .filter(|(_, input)| {
                input.id.is_some() && input.runtime_token == self.groove_runtime_token
            })
            .map(|((scope, _), _)| scope)
            .chain(self.query.local_availability_authorities.keys())
            .collect::<BTreeSet<_>>();
        if !live_scopes.contains(scope)
            && live_scopes.len() >= crate::authorization_scope::MAX_AUTHORIZATION_SCOPES
        {
            return Err(Error::QueryCapability("local availability context capacity reached; retire inactive scope graphs before retrying".to_owned()));
        }
        Ok(())
    }

    async fn local_unavailable_input(
        &mut self,
        scope: &PolicyBindingKey,
        table: crate::ids::GlobalPhysicalTableId,
    ) -> Result<InputSourceId, Error> {
        let key = (scope.clone(), table);
        if let Some(input) = self.query.local_unavailable_inputs.get(&key)
            && input.runtime_token == self.groove_runtime_token
            && let Some(id) = input.id
        {
            return Ok(id);
        }
        self.require_local_availability_context_capacity(scope)?;
        let records = self
            .query
            .local_unavailable_inputs
            .get(&key)
            .into_iter()
            .flat_map(|input| input.rows.iter())
            .map(|row| descriptor().create(&[Value::Uuid(row.0)]))
            .collect::<Result<Vec<_>, _>>()?;
        let id = self.database.allocate_input_source(descriptor());
        if !records.is_empty() {
            self.database
                .replace_input_sources([InputSourceReplacement {
                    id,
                    descriptor: descriptor(),
                    records,
                }])
                .await
                .map_err(Error::Groove)?;
        }
        let input = self.query.local_unavailable_inputs.entry(key).or_default();
        input.id = Some(id);
        input.runtime_token = self.groove_runtime_token;
        Ok(id)
    }

    /// Called only after this scope's query/subscription owners have stopped.
    /// Durable markers remain resident and seed a later scope reopening.
    #[allow(dead_code)]
    pub(crate) async fn retire_local_availability_scope_inputs(
        &mut self,
        scope: &PolicyBindingKey,
    ) -> Result<(), Error> {
        let ids = self
            .query
            .local_unavailable_inputs
            .iter()
            .filter(|((key, _), input)| {
                key == scope && input.runtime_token == self.groove_runtime_token
            })
            .filter_map(|(_, input)| input.id)
            .collect::<Vec<_>>();
        if !ids.is_empty() {
            self.database
                .retire_input_sources(ids)
                .await
                .map_err(Error::Groove)?;
        }
        for ((key, _), input) in &mut self.query.local_unavailable_inputs {
            if key == scope {
                input.id = None;
            }
        }
        self.query.query_shape_cache.clear();
        self.query.local_availability_authorities.remove(scope);
        Ok(())
    }

    pub(super) async fn exclude_local_unavailable_graph(
        &mut self,
        scope: &PolicyBindingKey,
        schema: SchemaVersionId,
        source: &SourceRequest,
        graph: GraphBuilder,
        pending_ahead: bool,
    ) -> Result<GraphBuilder, Error> {
        let table = self.local_availability_table_id(schema, &source.source.table)?;
        let id = self.local_unavailable_input(scope, table).await?;
        let unavailable = GraphBuilder::input_source(id, descriptor());
        if pending_ahead {
            // Ahead also holds Edge-accepted versions. Keep exact version keys:
            // a settled predecessor cannot suppress a pending sibling/successor.
            let fields = ["row_uuid", "tx_time", "tx_node_id"];
            let settled = GraphBuilder::join(
                graph.clone(),
                read_sources::edge_accepted_transaction_source_graph(),
                ["tx_time", "tx_node_id"],
                ["time", "node_id"],
            )
            .project_fields(fields.map(|field| ProjectField::renamed(left_field(field), field)));
            let blocked = GraphBuilder::join(settled, unavailable, ["row_uuid"], ["row_uuid"])
                .project_fields(
                    fields.map(|field| ProjectField::renamed(left_field(field), field)),
                );
            return Ok(GraphBuilder::anti_join(graph, blocked, fields, fields));
        }
        Ok(GraphBuilder::anti_join(
            graph,
            unavailable,
            ["row_uuid"],
            ["row_uuid"],
        ))
    }
}
