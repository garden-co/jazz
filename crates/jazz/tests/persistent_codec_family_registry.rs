use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use jazz::storage_codec_profile::JAZZ_EPOCH_1_STORAGE_CODECS;
use serde::Deserialize;

const REGISTRY_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/persistent_codec_family_registry.json"
);

#[derive(Debug, Deserialize)]
struct Registry {
    schema_version: u8,
    purpose: String,
    families: Vec<Family>,
}

#[derive(Debug, Deserialize)]
struct Family {
    id: String,
    boundary: String,
    #[serde(default)]
    profile: Option<String>,
    spec: Receipt,
    semantic_fixture: Receipt,
    rejection_receipt: Receipt,
    evidence: Vec<Receipt>,
}

#[derive(Debug, Deserialize)]
struct Receipt {
    path: String,
    anchor: String,
}

fn registry() -> Registry {
    serde_json::from_str(&fs::read_to_string(REGISTRY_PATH).expect("registry fixture exists"))
        .expect("registry fixture is valid JSON")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("jazz lives under the workspace crates directory")
        .to_owned()
}

fn profile_ids(registry: &Registry, profile: &str) -> Vec<String> {
    let mut ids = registry
        .families
        .iter()
        .filter(|family| family.profile.as_deref() == Some(profile))
        .map(|family| family.id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn validate_registry(registry: &Registry) -> Result<(), String> {
    if registry.schema_version != 1 || registry.purpose.trim().is_empty() {
        return Err("registry must use schema version one and explain its purpose".to_owned());
    }

    let root = workspace_root();
    let mut ids = BTreeSet::new();
    for family in &registry.families {
        if family.id.trim().is_empty() {
            return Err("codec family ID must not be empty".to_owned());
        }
        if !ids.insert(family.id.as_str()) {
            return Err(format!("duplicate codec family `{}`", family.id));
        }
        if !matches!(
            family.profile.as_deref(),
            None | Some("groove-root" | "jazz-root" | "server-catalogue-root")
        ) {
            return Err(format!("{} has an unknown codec profile", family.id));
        }
        if !matches!(
            family.boundary.as_str(),
            "durable-storage" | "wire-binding-abi" | "local-auth-secret"
        ) {
            return Err(format!("{} has an unknown boundary", family.id));
        }
        if family.semantic_fixture.anchor.trim().is_empty()
            || family.rejection_receipt.anchor.trim().is_empty()
            || family.evidence.is_empty()
        {
            return Err(format!(
                "{} is missing a required compatibility receipt",
                family.id
            ));
        }
        for receipt in std::iter::once(&family.spec)
            .chain(std::iter::once(&family.semantic_fixture))
            .chain(std::iter::once(&family.rejection_receipt))
            .chain(family.evidence.iter())
        {
            let path = root.join(&receipt.path);
            let text = fs::read_to_string(&path).map_err(|error| {
                format!(
                    "{} references unreadable {}: {error}",
                    family.id, receipt.path
                )
            })?;
            if receipt.anchor.trim().is_empty() || !text.contains(&receipt.anchor) {
                return Err(format!(
                    "{} has a stale receipt anchor `{}` in {}",
                    family.id, receipt.anchor, receipt.path
                ));
            }
        }
    }

    let groove_profile = groove::storage::StorageCodecProfile::groove_epoch_1()
        .codec_ids()
        .map(str::to_owned)
        .collect();
    let expected: BTreeMap<&str, Vec<String>> = BTreeMap::from([
        ("groove-root", groove_profile),
        (
            "jazz-root",
            JAZZ_EPOCH_1_STORAGE_CODECS
                .iter()
                .map(|id| (*id).to_owned())
                .collect(),
        ),
        (
            "server-catalogue-root",
            vec!["jazz.server-catalogue-entry.v1".to_owned()],
        ),
    ]);
    for (profile, expected) in expected {
        let actual = profile_ids(registry, profile);
        if actual != expected {
            return Err(format!(
                "{profile} profile registry drifted: expected {expected:?}, found {actual:?}"
            ));
        }
    }

    // These names are the deliberately non-profile families enumerated by the
    // #2160 storage audit. Keep this list small and explicit: its job is to
    // ensure that a later registry edit cannot quietly lose a boundary that is
    // authoritative despite not being a top-level manifest codec ID.
    for required in [
        "groove.typed-record.v1",
        "groove.storage-epoch-manifest.v1",
        "groove.jazz-physical-class.v1",
        "jazz.history-version-current.v1",
        "jazz.contribution-provenance.v1",
        "jazz.merge-heads.v1",
        "jazz.idb-page.v2",
        "jazz.wire-frame.v1",
        "jazz.binding-abi.v1",
        "jazz.local-auth-secret.v1",
    ] {
        if !ids.contains(required) {
            return Err(format!("registry omits audited codec family `{required}`"));
        }
    }
    Ok(())
}

#[test]
fn authoritative_persistent_codec_family_registry_is_complete_and_current() {
    validate_registry(&registry()).expect("codec registry must remain complete and current");
}

#[test]
fn registry_verifier_rejects_a_planted_known_profile_omission() {
    let mut registry = registry();
    registry
        .families
        .retain(|family| family.id != "jazz.branch-key.v1");
    let error = validate_registry(&registry).expect_err("a missing profile family must fail CI");
    assert!(
        error.contains("jazz-root profile registry drifted"),
        "{error}"
    );
}
