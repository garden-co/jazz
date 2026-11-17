// Union arm labels derived from the arm itself (`jazz.union-arm-label.v2`).
//
// A union's arm labels are part of its canonical bytes and of its rows'
// `ResultKey`s, so every client must label an unnamed arm the same way.
// SPEC 6 forbids inventing an ordinal: the label is a pure function of the
// arm's own canonical relation bytes, the same bytes shape identity uses.

/// Name of the union-arm label derivation, the first bytes of its preimage.
pub const UNION_ARM_LABEL_ENCODING: &str = "jazz.union-arm-label.v2";

/// Prefix of every derived union-arm label.
pub const DERIVED_UNION_ARM_LABEL_PREFIX: &str = "derived:v2:";

/// The bytes a derived label hashes: the encoding name, a NUL, then the
/// arm's canonical relation bytes (`jazz-relation-v1`).
pub fn union_arm_label_preimage(arm: &RelationExpr) -> Result<Vec<u8>, QueryError> {
    let mut preimage = Vec::from(UNION_ARM_LABEL_ENCODING.as_bytes());
    preimage.push(0);
    preimage.extend(canonical_relation_query_key(&RelationQuery { rel: arm.clone() })?);
    Ok(preimage)
}

/// The label of an unnamed union arm: `derived:v2:` and the lowercase hex
/// SHA-256 of [`union_arm_label_preimage`]. Unions inside the arm must
/// already carry their labels (see [`fill_union_arm_labels`]).
pub fn derive_union_arm_label(arm: &RelationExpr) -> Result<String, QueryError> {
    let digest = jazz_types::sha256::hash(&union_arm_label_preimage(arm)?);
    Ok(format!("{DERIVED_UNION_ARM_LABEL_PREFIX}{}", digest.to_hex()))
}

/// Give every unnamed (empty-labelled) union arm in `expr` its derived
/// label, innermost unions first. Named arms keep their label. Two arms of
/// one union that end up with the same label are refused: identical
/// unnamed arms need names to stay apart.
pub fn fill_union_arm_labels(expr: &mut RelationExpr) -> Result<(), QueryError> {
    match expr {
        RelationExpr::TableScan { .. } => Ok(()),
        RelationExpr::Filter { input, .. }
        | RelationExpr::Project { input, .. }
        | RelationExpr::Distinct { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => fill_union_arm_labels(input),
        RelationExpr::Join { left, right, .. } => {
            fill_union_arm_labels(left)?;
            fill_union_arm_labels(right)
        }
        RelationExpr::Gather { seed, step, .. } => {
            fill_union_arm_labels(seed)?;
            fill_union_arm_labels(step)
        }
        RelationExpr::Union { inputs } => {
            let mut labels = BTreeSet::new();
            for arm in inputs.iter_mut() {
                fill_union_arm_labels(&mut arm.input)?;
                if arm.label.is_empty() {
                    arm.label = derive_union_arm_label(&arm.input)?;
                }
                if !labels.insert(arm.label.clone()) {
                    return Err(duplicate_union_arm(&arm.label));
                }
            }
            Ok(())
        }
    }
}

/// Whether some union in `expr` has an unnamed (empty-labelled) arm.
pub fn has_unnamed_union_arm(expr: &RelationExpr) -> bool {
    match expr {
        RelationExpr::TableScan { .. } => false,
        RelationExpr::Filter { input, .. }
        | RelationExpr::Project { input, .. }
        | RelationExpr::Distinct { input, .. }
        | RelationExpr::OrderBy { input, .. }
        | RelationExpr::Offset { input, .. }
        | RelationExpr::Limit { input, .. } => has_unnamed_union_arm(input),
        RelationExpr::Join { left, right, .. } => {
            has_unnamed_union_arm(left) || has_unnamed_union_arm(right)
        }
        RelationExpr::Gather { seed, step, .. } => {
            has_unnamed_union_arm(seed) || has_unnamed_union_arm(step)
        }
        RelationExpr::Union { inputs } => inputs
            .iter()
            .any(|arm| arm.label.is_empty() || has_unnamed_union_arm(&arm.input)),
    }
}

fn duplicate_union_arm(label: &str) -> QueryError {
    QueryError::UnsupportedRelationQuery(if label.starts_with(DERIVED_UNION_ARM_LABEL_PREFIX) {
        "union(...) arms must differ; name the arms to union the same query twice".to_owned()
    } else {
        format!("union(...) labels must differ: {label:?} repeats")
    })
}

std::thread_local! {
    static UNNAMED_UNION_ARMS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether the decode running on this thread is a client query's, which may
/// carry unnamed union arms. Every other decoder (storage, peers) refuses them.
fn accepts_unnamed_union_arms() -> bool {
    UNNAMED_UNION_ARMS.with(std::cell::Cell::get)
}

/// Decode a query a client hands its binding or a thin server (the native
/// query Postcard carrier), where a union arm with an empty label is unnamed,
/// and give each unnamed arm its derived label. The result carries no
/// unnamed arm, so what is stored or sent on is what every decoder accepts.
pub fn decode_client_query_postcard(bytes: &[u8]) -> Result<Query, String> {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            UNNAMED_UNION_ARMS.with(|flag| flag.set(self.0));
        }
    }
    let _reset = Reset(UNNAMED_UNION_ARMS.with(|flag| flag.replace(true)));
    let mut query: Query = postcard::from_bytes(bytes).map_err(|error| error.to_string())?;
    if let Some(relation) = &mut query.relation {
        fill_union_arm_labels(&mut relation.rel).map_err(|error| error.to_string())?;
    }
    Ok(query)
}

#[cfg(test)]
mod union_arm_label_tests {
    //! Byte-level tests: the label is a cross-client encoding, so its
    //! preimage and digest are pinned by `fixtures/union_arm_label_v2.json`,
    //! which the TypeScript tests read too.
    use super::*;

    const FIXTURE: &str = include_str!("../../../../fixtures/union_arm_label_v2.json");

    fn expr(value: &serde_json::Value) -> RelationExpr {
        serde_json::from_value(value.clone()).unwrap()
    }

    fn filled(value: &serde_json::Value) -> RelationExpr {
        let mut relation = expr(value);
        fill_union_arm_labels(&mut relation).unwrap();
        relation
    }

    /// Every pinned arm hashes to its pinned preimage and label, and the
    /// pinned unnamed union fills to its pinned labels, innermost first.
    #[test]
    fn union_arm_label_fixture_is_current() {
        let mut fixture: serde_json::Value = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(fixture["encoding"], UNION_ARM_LABEL_ENCODING);
        let update = std::env::var_os("JAZZ_UPDATE_UNION_ARM_LABEL_FIXTURE").is_some();
        for case in fixture["arms"].as_array_mut().unwrap() {
            let arm = expr(&case["arm"]);
            let preimage = hex::encode(union_arm_label_preimage(&arm).unwrap());
            let label = derive_union_arm_label(&arm).unwrap();
            if update {
                case["preimage_hex"] = preimage.into();
                case["label"] = label.into();
                continue;
            }
            assert_eq!(case["preimage_hex"], preimage.as_str(), "{}", case["name"]);
            assert_eq!(case["label"], label.as_str(), "{}", case["name"]);
        }
        let fill = &mut fixture["fill"];
        let actual = serde_json::to_value(filled(&fill["relation"])).unwrap();
        if update {
            fill["filled"] = actual;
            let mut text = serde_json::to_string_pretty(&fixture).unwrap();
            text.push('\n');
            std::fs::write(
                concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/union_arm_label_v2.json"),
                text,
            )
            .unwrap();
            return;
        }
        assert_eq!(fill["filled"], actual);
    }

    /// The preimage starts with the encoding name and a NUL, then the arm's
    /// canonical relation bytes, the ones shape identity hashes.
    #[test]
    fn preimage_is_encoding_name_then_canonical_relation_bytes() {
        let arm = RelationExpr::TableScan {
            table: "todos".into(),
            alias: None,
        };
        let preimage = union_arm_label_preimage(&arm).unwrap();
        let (name, rest) = preimage.split_at(UNION_ARM_LABEL_ENCODING.len() + 1);
        assert_eq!(name, b"jazz.union-arm-label.v2\0");
        assert_eq!(
            rest,
            canonical_relation_query_key(&RelationQuery { rel: arm }).unwrap()
        );
    }

    /// Named arms keep their label; reordering arms keeps each arm's label;
    /// two identical unnamed arms, or two equal names, are refused.
    #[test]
    fn filling_keeps_names_is_order_free_and_refuses_repeats() {
        let scan = || RelationExpr::TableScan {
            table: "todos".into(),
            alias: None,
        };
        let limited = || RelationExpr::Limit {
            input: Box::new(scan()),
            limit: 1,
        };
        let union = |arms: Vec<(&str, RelationExpr)>| RelationExpr::Union {
            inputs: arms
                .into_iter()
                .map(|(label, input)| RelationUnionArm {
                    label: label.into(),
                    input,
                })
                .collect(),
        };
        let labels = |mut relation: RelationExpr| {
            fill_union_arm_labels(&mut relation).unwrap();
            let RelationExpr::Union { inputs } = relation else {
                unreachable!()
            };
            inputs.into_iter().map(|arm| arm.label).collect::<Vec<_>>()
        };

        let forward = labels(union(vec![("", scan()), ("", limited()), ("mine", scan())]));
        assert_eq!(forward[0], derive_union_arm_label(&scan()).unwrap());
        assert_eq!(forward[1], derive_union_arm_label(&limited()).unwrap());
        assert_eq!(forward[2], "mine");
        let backward = labels(union(vec![("", limited()), ("", scan())]));
        assert_eq!(backward, [forward[1].clone(), forward[0].clone()]);

        let mut twice = union(vec![("", scan()), ("", scan())]);
        let refused = fill_union_arm_labels(&mut twice).unwrap_err().to_string();
        assert!(refused.contains("name the arms"), "{refused}");
        let mut same_name = union(vec![("x", scan()), ("x", limited())]);
        assert!(fill_union_arm_labels(&mut same_name).is_err());
        assert!(!has_unnamed_union_arm(&union(vec![("x", scan())])));
        assert!(has_unnamed_union_arm(&RelationExpr::Limit {
            input: Box::new(union(vec![("x", union(vec![("", scan())]))])),
            limit: 1,
        }));
    }

    /// A client's query bytes with an unnamed arm (the native query Postcard
    /// carrier with an empty label, as the TS bindings send it). Encoding
    /// them takes the client-query mode too: nothing else may write them.
    fn client_bytes(relation: RelationExpr) -> Vec<u8> {
        let mut query = Query::from("todos");
        query.relation = Some(RelationQuery { rel: relation });
        let before = UNNAMED_UNION_ARMS.with(|flag| flag.replace(true));
        let bytes = postcard::to_allocvec(&query);
        UNNAMED_UNION_ARMS.with(|flag| flag.set(before));
        bytes.unwrap()
    }

    fn scan() -> RelationExpr {
        RelationExpr::TableScan {
            table: "todos".into(),
            alias: None,
        }
    }

    fn arm(label: &str, input: RelationExpr) -> RelationUnionArm {
        RelationUnionArm {
            label: label.into(),
            input,
        }
    }

    /// The client-query decoder fills unnamed arms; the result is what every
    /// other decoder (storage, peers) accepts, and those still refuse the
    /// unnamed bytes themselves.
    #[test]
    fn client_queries_may_leave_arms_unnamed_and_nothing_else_may() {
        let limited = RelationExpr::Limit {
            input: Box::new(scan()),
            limit: 1,
        };
        let bytes = client_bytes(RelationExpr::Union {
            inputs: vec![arm("", scan()), arm("", limited.clone())],
        });

        assert!(postcard::from_bytes::<Query>(&bytes).is_err());
        let query = decode_client_query_postcard(&bytes).unwrap();
        let RelationExpr::Union { inputs } = &query.relation.as_ref().unwrap().rel else {
            panic!("a union");
        };
        assert_eq!(inputs[0].label, derive_union_arm_label(&scan()).unwrap());
        assert_eq!(inputs[1].label, derive_union_arm_label(&limited).unwrap());
        let stored = postcard::to_allocvec(&query).unwrap();
        assert_eq!(postcard::from_bytes::<Query>(&stored).unwrap(), query);
        // The mode ends with the decode.
        assert!(postcard::from_bytes::<Query>(&bytes).is_err());

        let twice = client_bytes(RelationExpr::Union {
            inputs: vec![arm("", scan()), arm("", scan())],
        });
        let refused = decode_client_query_postcard(&twice).unwrap_err();
        assert!(refused.contains("name the arms"), "{refused}");
    }
}
