//! Preparation and coercion of query-binding values.
//!
//! The query evaluator owns the policy and plan lifecycle; this module owns the
//! deterministic source-shape key and conversion of values into those plans.

use super::*;
pub use crate::node::query_engine::coerce_prepared_binding_value;

pub(super) fn query_binding_value_signature(binding: &Binding) -> String {
    binding
        .values()
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn policy_plan_cache_signature(
    binding: &Binding,
    identity: AuthorSubject,
    claims_scope: &str,
) -> String {
    // Authorization lowering still embeds the permission subject in source
    // plans. Claim values are routed at bind time, but plans from different
    // subjects are not interchangeable until that subject is parameterized.
    // Immutable request claims do not advance the ambient session revision;
    // callers must distinguish their active scope as well.
    format!(
        "{}|subject={identity:?}|claims={claims_scope}",
        query_binding_value_signature(binding)
    )
}

pub(super) fn exact_known_state_declaration_if_within_limits(
    _shape_id: ShapeId,
    _subscription: SubscriptionKey,
    _values: &[Value],
    refs: Vec<RowVersionRef>,
) -> Option<KnownStateDeclaration> {
    if refs.len() > MAX_KNOWN_STATE_EXACT_REFS {
        return None;
    }
    Some(KnownStateDeclaration::ExactVersionSet { versions: refs })
}

pub(super) fn query_binding_source_shape_for_prepared_params(
    params: &[PreparedQueryParam],
) -> String {
    let mut user_params = BTreeMap::new();
    let mut claim_params = BTreeMap::new();
    for param in params {
        match &param.source {
            PreparedQueryParamSource::User => {
                user_params.insert(param.name.clone(), param.ty.clone());
            }
            PreparedQueryParamSource::Claim(path) => {
                claim_params.insert(
                    param.name.clone(),
                    ProgramClaimParam {
                        path: path.clone(),
                        ty: param.ty.clone(),
                    },
                );
            }
        }
    }
    query_binding_source_shape_for_parts(&user_params, &claim_params)
}

pub(super) fn query_binding_source_shape_for_parts(
    param_types: &BTreeMap<String, ColumnType>,
    claim_params: &BTreeMap<String, ProgramClaimParam>,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-binding-source-v1");
    push_usize(&mut bytes, param_types.len());
    for (name, ty) in param_types {
        push_str(&mut bytes, name);
        push_str(&mut bytes, &format!("{ty:?}"));
    }
    push_usize(&mut bytes, claim_params.len());
    for (name, claim) in claim_params {
        push_str(&mut bytes, name);
        push_usize(&mut bytes, claim.path.0.len());
        for segment in &claim.path.0 {
            push_str(&mut bytes, segment);
        }
        push_str(&mut bytes, &format!("{:?}", claim.ty));
    }
    let hash = blake3::hash(&bytes);
    format!("jazz-query-binding:{}", hash.to_hex())
}

pub(super) fn query_binding_source_shape_for_parts_if_needed(
    param_types: &BTreeMap<String, ColumnType>,
    claim_params: &BTreeMap<String, ProgramClaimParam>,
) -> Option<String> {
    (!param_types.is_empty() || !claim_params.is_empty())
        .then(|| query_binding_source_shape_for_parts(param_types, claim_params))
}

pub(super) fn authorization_binding_source_shape(
    shape: &ValidatedQuery,
    extra_user_params: &BTreeMap<String, ColumnType>,
    claim_params: &BTreeMap<String, ProgramClaimParam>,
) -> Option<String> {
    let mut param_types = shape.params().clone();
    param_types.extend(extra_user_params.clone());
    (!param_types.is_empty() || !claim_params.is_empty())
        .then(|| query_binding_source_shape_for_parts(&param_types, claim_params))
}

fn push_usize(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(&(value as u64).to_le_bytes());
}
fn push_str(bytes: &mut Vec<u8>, value: &str) {
    push_usize(bytes, value.len());
    bytes.extend_from_slice(value.as_bytes());
}

pub(super) fn binding_values_for_plan(
    binding: &Binding,
    params: &[PreparedQueryParam],
    policy: &PolicyContext,
    prepared_claim_binding_mode: PreparedClaimBindingMode,
) -> Result<Vec<Value>, Error> {
    params
        .iter()
        .map(|param| match param.source {
            PreparedQueryParamSource::User => {
                let value = binding
                    .values()
                    .get(&param.name)
                    .cloned()
                    .ok_or_else(|| QueryError::MissingParam(param.name.clone()))?;
                Ok::<_, Error>(coerce_prepared_binding_value(value, &param.ty))
            }
            PreparedQueryParamSource::Claim(ref path) => {
                let value = match prepared_claim_value(path, policy)? {
                    Some(value) => value,
                    None if prepared_claim_binding_mode
                        == PreparedClaimBindingMode::FailClosedAuthorizationSupport =>
                    {
                        return Err(Error::AuthorizationSupportMissingClaim(path.0.join(".")));
                    }
                    None => {
                        return Err(Error::InvalidStoredValue(
                            "claim prepared param is not bound",
                        ));
                    }
                };
                Ok::<_, Error>(coerce_prepared_binding_value(value, &param.ty))
            }
        })
        .collect()
}

pub(super) fn prepared_claim_value(
    path: &ClaimPath,
    policy: &PolicyContext,
) -> Result<Option<Value>, Error> {
    let (permission_subject, claims) = match policy {
        PolicyContext::Identity {
            permission_subject,
            claims,
            ..
        }
        | PolicyContext::AuthorizationSubplan {
            permission_subject,
            claims,
            ..
        } => (permission_subject, claims),
        PolicyContext::System => {
            return Err(Error::InvalidStoredValue(
                "claim prepared params require an identity policy context",
            ));
        }
    };
    if let Some(name) = crate::query::author_claim_path_key(&path.0) {
        return Ok(
            crate::model::policy_claims::author_policy_claims(*permission_subject).remove(&name),
        );
    }
    if let Some(value) = crate::model::policy_claims::policy_claim_at_path(claims, &path.0) {
        return Ok(Some(value));
    }
    if let [name] = path.0.as_slice() {
        return Ok(default_policy_claim_values(*permission_subject).remove(name));
    }
    Ok(None)
}

pub(super) fn coerce_binding_values_for_shape(
    shape: &ValidatedQuery,
    values: &mut BTreeMap<String, Value>,
) {
    for (name, value) in values {
        let Some(ty) = shape.params().get(name) else {
            continue;
        };
        *value = coerce_prepared_binding_value(value.clone(), ty);
    }
}
