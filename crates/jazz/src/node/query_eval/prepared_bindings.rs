//! Preparation and coercion of query-binding values.
//!
//! The query evaluator owns the policy and plan lifecycle; this module owns the
//! deterministic source-shape key and conversion of values into those plans.

use super::*;

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
    claims_revision: u64,
) -> String {
    // Authorization lowering still embeds the permission subject in source
    // plans. Claim values are routed at bind time, but plans from different
    // subjects are not interchangeable until that subject is parameterized.
    format!(
        "{}|subject={identity:?}|claims={claims_revision}",
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
    let name = match path.0.as_slice() {
        [name] => name.clone(),
        [claims, name] if claims == "claims" => crate::query::provider_claim_key(name),
        _ => return Err(Error::InvalidStoredValue("unsupported session claim path")),
    };
    if let Some(value) = claims.get(&name) {
        return Ok(Some(value.clone()));
    }
    if let Some(value) = default_policy_claim_values(*permission_subject).get(&name) {
        return Ok(Some(value.clone()));
    }
    Ok(None)
}

/// Canonicalizes a bound value to the descriptor representation used by a
/// prepared Groove binding source. Lowering literal-only routed terminals
/// uses the same conversion so their route predicates compare like-for-like.
pub(crate) fn coerce_prepared_binding_value(
    value: Value,
    column_type: &groove::schema::ColumnType,
) -> Value {
    if let Some(value) = coerce_prepared_integer_value(&value, column_type) {
        return value;
    }
    match (value, column_type) {
        (Value::Uuid(value), groove::schema::ColumnType::String) => {
            Value::String(value.to_string())
        }
        (Value::String(value), groove::schema::ColumnType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(Value::Uuid)
            .unwrap_or(Value::String(value)),
        (Value::Nullable(value), groove::schema::ColumnType::Nullable(inner)) => Value::Nullable(
            value.map(|value| Box::new(coerce_prepared_binding_value(*value, inner))),
        ),
        (Value::Nullable(Some(value)), column_type) => Value::Nullable(Some(Box::new(
            coerce_prepared_binding_value(*value, column_type),
        ))),
        (value @ Value::Nullable(None), _) => value,
        (Value::Array(values), groove::schema::ColumnType::Array(inner)) => Value::Array(
            values
                .into_iter()
                .map(|value| coerce_prepared_binding_value(value, inner))
                .collect(),
        ),
        (Value::Tuple(values), groove::schema::ColumnType::Tuple(types))
            if values.len() == types.len() =>
        {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, column_type)| coerce_prepared_binding_value(value, column_type))
                    .collect(),
            )
        }
        (value, groove::schema::ColumnType::Nullable(inner))
            if !matches!(value, Value::Nullable(_)) =>
        {
            Value::Nullable(Some(Box::new(coerce_prepared_binding_value(value, inner))))
        }
        (value, _) => value,
    }
}

/// Normalizes prepared integer values. Failed conversions intentionally return
/// `None`, so the original typed value stays in the binding and cannot wrap
/// into an authorized value.
fn coerce_prepared_integer_value(
    value: &Value,
    column_type: &groove::schema::ColumnType,
) -> Option<Value> {
    let value = match value {
        Value::U8(value) => i128::from(*value),
        Value::U16(value) => i128::from(*value),
        Value::U32(value) => i128::from(*value),
        Value::U64(value) => i128::from(*value),
        Value::I32(value) => i128::from(*value),
        Value::I64(value) => i128::from(*value),
        _ => return None,
    };
    match column_type {
        groove::schema::ColumnType::U8 => u8::try_from(value).ok().map(Value::U8),
        groove::schema::ColumnType::U16 => u16::try_from(value).ok().map(Value::U16),
        groove::schema::ColumnType::U32 => u32::try_from(value).ok().map(Value::U32),
        groove::schema::ColumnType::U64 => u64::try_from(value).ok().map(Value::U64),
        groove::schema::ColumnType::I32 => i32::try_from(value).ok().map(Value::I32),
        groove::schema::ColumnType::I64 => i64::try_from(value).ok().map(Value::I64),
        _ => None,
    }
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
