//! The application's mechanism selection and allow-list
//! (`E2EE_PROTOCOL.md` §0, "Mechanism agility"), from the `mechanisms`
//! entry of its top-level `encryption` settings:
//!
//! ```json
//! { "selected": { "keySeal": { "id": "jazz.wrap.hpke-x25519", "version": 1 } },
//!   "allowed":  { "keySeal": [ { "id": "jazz.wrap.hpke-x25519", "version": 1 } ] } }
//! ```
//!
//! The roles are `cell`, `keyWrap`, `keySeal`, `signature`, `equality` and
//! `stream`. A role a list leaves out keeps the built-in mechanism. Readers
//! route only allowed mechanisms (anything else is `unsupported-format`),
//! and writers may select only allowed ones. Mechanisms other than the
//! built-ins are compiled in by the host and passed in its [`Registry`].

use jazz_e2ee::agility::{MechanismAllowList, MechanismConfig, MechanismId};
use jazz_e2ee::registry::Registry;
use serde_json::Value;

use crate::error::ClientError;

const ROLES: [&str; 6] = [
    "cell",
    "keyWrap",
    "keySeal",
    "signature",
    "equality",
    "stream",
];

fn settings(what: impl Into<String>) -> ClientError {
    ClientError::Settings(what.into())
}

fn mechanism(value: &Value) -> Result<MechanismId, ClientError> {
    let Some(object) = value.as_object() else {
        return Err(settings("a mechanism is an object { id, version }"));
    };
    if object.keys().any(|key| key != "id" && key != "version") {
        return Err(settings("a mechanism has only `id` and `version`"));
    }
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| settings("a mechanism's `id` is a string"))?;
    let version = object
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| settings("a mechanism's `version` is a 32-bit unsigned integer"))?;
    let id = MechanismId::new(id, version);
    id.as_mechanism().validate().map_err(|_| {
        settings(format!(
            "`{}` v{} is not a valid mechanism",
            id.id, id.version
        ))
    })?;
    Ok(id)
}

/// The role entries of one of `selected` or `allowed`.
fn roles(value: Option<&Value>, what: &str) -> Result<Vec<(usize, Value)>, ClientError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| settings(format!("`{what}` is an object of roles")))?;
    object
        .iter()
        .map(|(role, entry)| {
            let index = ROLES
                .iter()
                .position(|known| known == role)
                .ok_or_else(|| settings(format!("unknown mechanism role `{role}`")))?;
            Ok((index, entry.clone()))
        })
        .collect()
}

fn allowed_role(list: &mut MechanismAllowList, index: usize) -> &mut Vec<MechanismId> {
    match index {
        0 => &mut list.cell,
        1 => &mut list.key_wrap,
        2 => &mut list.key_seal,
        3 => &mut list.signature,
        4 => &mut list.equality,
        _ => &mut list.stream,
    }
}

fn selected_role(config: &mut MechanismConfig, index: usize) -> &mut MechanismId {
    match index {
        0 => &mut config.cell,
        1 => &mut config.key_wrap,
        2 => &mut config.key_seal,
        3 => &mut config.signature,
        4 => &mut config.equality,
        _ => &mut config.stream,
    }
}

/// The readers' registry (the host's, restricted to the allow-list) and
/// the writers' selection.
pub(crate) fn configure(
    host: &Registry,
    mechanisms: Option<&Value>,
) -> Result<(Registry, MechanismConfig), ClientError> {
    let mut allowed = MechanismAllowList::default();
    let mut selected = MechanismConfig::default();
    if let Some(value) = mechanisms {
        let object = value
            .as_object()
            .ok_or_else(|| settings("`mechanisms` is an object"))?;
        if let Some(key) = object
            .keys()
            .find(|key| *key != "selected" && *key != "allowed")
        {
            return Err(settings(format!("unknown `mechanisms` entry `{key}`")));
        }
        for (index, entry) in roles(object.get("allowed"), "allowed")? {
            let list = entry
                .as_array()
                .filter(|list| !list.is_empty())
                .ok_or_else(|| settings("an allowed role is a non-empty list of mechanisms"))?
                .iter()
                .map(mechanism)
                .collect::<Result<Vec<_>, _>>()?;
            *allowed_role(&mut allowed, index) = list;
        }
        for (index, entry) in roles(object.get("selected"), "selected")? {
            let id = mechanism(&entry)?;
            *selected_role(&mut selected, index) = id;
        }
    }
    let registry = host
        .allowing(&allowed)
        .map_err(|_| settings("the allow-list names a mechanism this build does not include"))?;
    registry
        .select(&selected)
        .map_err(|_| settings("a selected mechanism is not allowed for its role"))?;
    Ok((registry, selected))
}
