use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use jazz::storage_codec_profile::JAZZ_EPOCH_1_STORAGE_CODECS;
use serde::Deserialize;

const REGISTRY_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/persistent_codec_family_registry.json"
);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema_version: u8,
    purpose: String,
    families: Vec<Family>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

fn receipt_text(root: &Path, family: &Family, receipt: &Receipt) -> Result<String, String> {
    let listed_path = Path::new(&receipt.path);
    if receipt.path.is_empty()
        || listed_path.is_absolute()
        || listed_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "{} has an unsafe receipt path `{}`",
            family.id, receipt.path
        ));
    }

    let root = root.canonicalize().map_err(|error| {
        format!(
            "{} cannot canonicalize workspace root {}: {error}",
            family.id,
            root.display()
        )
    })?;
    let path = root.join(listed_path).canonicalize().map_err(|error| {
        format!(
            "{} references unreadable {}: {error}",
            family.id, receipt.path
        )
    })?;
    if !path.starts_with(&root) {
        return Err(format!(
            "{} receipt path escapes the workspace: {}",
            family.id, receipt.path
        ));
    }
    fs::read_to_string(path)
        .map_err(|error| format!("{} cannot read {}: {error}", family.id, receipt.path))
}

fn validate_receipt(root: &Path, family: &Family, receipt: &Receipt) -> Result<(), String> {
    if receipt.anchor.trim().is_empty() {
        return Err(format!("{} has an empty receipt anchor", family.id));
    }
    let text = receipt_text(root, family, receipt)?;
    let appearances = text.matches(&receipt.anchor).count();
    if appearances != 1 {
        return Err(format!(
            "{} receipt anchor `{}` in {} must identify exactly one receipt, found {appearances}",
            family.id, receipt.anchor, receipt.path
        ));
    }
    Ok(())
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

fn expected_profile_ids() -> BTreeMap<&'static str, Vec<String>> {
    let groove_profile = groove::storage::StorageCodecProfile::groove_epoch_1()
        .codec_ids()
        .map(str::to_owned)
        .collect();
    BTreeMap::from([
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
    ])
}

fn validate_registry_with_profiles(
    registry: &Registry,
    expected_profiles: BTreeMap<&str, Vec<String>>,
) -> Result<(), String> {
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
        if family.evidence.is_empty() {
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
            validate_receipt(&root, family, receipt)?;
        }
    }

    for (profile, expected) in expected_profiles {
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
        "jazz.idb-page.v1",
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

fn validate_registry(registry: &Registry) -> Result<(), String> {
    validate_registry_with_profiles(registry, expected_profile_ids())
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

#[test]
fn registry_verifier_rejects_a_planted_future_profile_addition() {
    let mut profiles = expected_profile_ids();
    profiles
        .get_mut("jazz-root")
        .expect("known Jazz profile")
        .push("jazz.future-codec.v2".to_owned());
    let error = validate_registry_with_profiles(&registry(), profiles)
        .expect_err("a future profile family needs a reviewed registry row");
    assert!(
        error.contains("jazz-root profile registry drifted"),
        "{error}"
    );
}

#[test]
fn registry_verifier_rejects_planted_stale_and_unsafe_receipts() {
    let mut stale = registry();
    stale.families[0].semantic_fixture.anchor = "no such exact receipt".to_owned();
    assert!(
        validate_registry(&stale)
            .expect_err("a stale receipt must fail")
            .contains("must identify exactly one receipt")
    );

    let mut unsafe_path = registry();
    unsafe_path.families[0].semantic_fixture.path = "../AGENTS.md".to_owned();
    assert!(
        validate_registry(&unsafe_path)
            .expect_err("a receipt must not escape its checkout")
            .contains("unsafe receipt path")
    );
}

#[test]
fn registry_parser_rejects_unknown_fields() {
    // The registry is a CI control surface. Rejecting unknown fields keeps a
    // misspelled receipt role from silently becoming documentation-only data.
    let error = serde_json::from_str::<Registry>(
        r#"{
            "schema_version": 1,
            "purpose": "test",
            "families": [],
            "unreviewed_extra_field": true
        }"#,
    )
    .expect_err("registry schema must fail closed on unknown fields");
    assert!(
        error.to_string().contains("unreviewed_extra_field"),
        "{error}"
    );
}
