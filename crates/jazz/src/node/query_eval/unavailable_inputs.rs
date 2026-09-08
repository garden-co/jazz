//! Runtime-only exclusions confirmed by an external authorization owner.
//!
//! This primitive owns no network decision, ordering, or durable receipt. Its
//! caller must establish those before marking or clearing an unavailable row.
//! Stored content remains intact and authorization proofs never read this input.
//!
//! This prototype is limited to current/default reads in the schema that
//! recorded the marker. Reopen, catalogue migration, retention bounds, and
//! ordered authority admission are deliberately unresolved integration work;
//! enabling it must not silently treat a new schema as clearing a withdrawal.

use super::*;
use crate::protocol::{CanonicalPolicyClaims, PolicyBindingKey};

#[derive(Clone, Debug)]
pub(crate) struct LocalUnavailableInput {
    id: InputSourceId,
    #[allow(dead_code)] // Marker ingestion is connected in the subsequent integration change.
    rows: BTreeSet<RowUuid>,
}

fn descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("row_uuid", ValueType::Uuid)])
}

pub(super) fn local_unavailable_policy_binding(
    request: &QueryProgramRequest,
) -> Option<PolicyBindingKey> {
    if request.authorization_mode != QueryAuthorizationMode::ClientLocal {
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
    /// Capture the same effective identity and exact claims used by ordinary
    /// app compilation. No raw claims are formatted or logged.
    #[allow(dead_code)] // Internal prototype API; network ingestion is deliberately separate.
    pub(crate) fn local_read_policy_binding(
        &self,
        identity: AuthorSubject,
    ) -> Option<PolicyBindingKey> {
        policy_binding(&self.query_program_policy_context(identity))
    }

    /// Mark/clear one current-schema row for an already validated context.
    /// Returns whether the input changed. This is runtime-only: callers must
    /// not mistake it for a durable withdrawal or an authorization decision.
    #[allow(dead_code)] // Internal prototype API; network ingestion is deliberately separate.
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
        self.table(table)?;
        let schema = self.catalogue.current_schema_version_id;
        let id = self.local_unavailable_input(scope, schema, table);
        let key = (scope.clone(), schema, table.to_owned());
        if self.query.local_unavailable_inputs[&key]
            .rows
            .contains(&row)
            == unavailable
        {
            return Ok(false);
        }
        let descriptor = descriptor();
        let record = descriptor.create(&[Value::Uuid(row.0)])?;
        self.database
            .apply_input_source_deltas([groove::ivm::InputSourceDelta {
                id,
                descriptor,
                adds: if unavailable {
                    vec![record.clone()]
                } else {
                    Vec::new()
                },
                removes: if unavailable {
                    Vec::new()
                } else {
                    vec![record]
                },
            }])
            .await
            .map_err(Error::Groove)?;
        let rows = &mut self
            .query
            .local_unavailable_inputs
            .get_mut(&key)
            .expect("allocated input")
            .rows;
        if unavailable {
            rows.insert(row);
        } else {
            rows.remove(&row);
        }
        Ok(true)
    }

    fn local_unavailable_input(
        &mut self,
        scope: &PolicyBindingKey,
        schema: SchemaVersionId,
        table: &str,
    ) -> InputSourceId {
        self.query
            .local_unavailable_inputs
            .entry((scope.clone(), schema, table.to_owned()))
            .or_insert_with(|| LocalUnavailableInput {
                id: self.database.allocate_input_source(descriptor()),
                rows: BTreeSet::new(),
            })
            .id
    }

    pub(super) fn exclude_local_unavailable_rows(
        &mut self,
        scope: &PolicyBindingKey,
        schema: SchemaVersionId,
        source: &SourceRequest,
        resolved: &mut ResolvedSource,
    ) {
        let id = self.local_unavailable_input(scope, schema, &source.source.table);
        resolved.graph = GraphBuilder::anti_join(
            resolved.graph.clone(),
            GraphBuilder::input_source(id, descriptor()),
            [resolved.row_shape.row_uuid_field.clone()],
            ["row_uuid".to_owned()],
        );
    }
}
