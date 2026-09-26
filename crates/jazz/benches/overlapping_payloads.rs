//! Attribute payload repetition across fresh, overlapping subscriptions.
//!
//! This intentionally uses the Node/Peer sync boundary: exact carrier counts
//! and separate sender/receiver CPU phases are not exposed by the client API.
//! Schemas and queries use public builders. This is a synchronous, in-memory,
//! SYSTEM fixture, not a browser/network/authorization timing claim. The second
//! arm uses the existing ExactVersionSet protocol after confirmed ingestion;
//! it does not infer possession merely from having sent a body.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use jazz::{
    block_on,
    groove::{records::Value, storage::MemoryStorage},
    ids::{AuthorSubject, NodeUuid, RowUuid},
    node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS},
    peer::PeerState,
    protocol::{
        DelegatedSessionBinding, KnownStateDeclaration, RegisterShapeOptions, RowVersionRef,
        ShapeAst, Subscribe, SubscriptionKey, SyncMessage, expand_version_carriers,
    },
    query::{Query, col, eq, gte, param},
    schema::JazzSchema,
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
    tx::{DurabilityTier, Fate},
    wire::{decode_sync_message_trusted, encode_sync_message},
};
use serde_json::json;

mod support;

fn millis(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.
}

fn open(schema: JazzSchema, id: u8) -> NodeState<MemoryStorage> {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(NodeState::new_with_shared_test_catalogue(
        NodeUuid::from_bytes([id; 16]),
        schema,
        MemoryStorage::new(&refs).unwrap(),
    ))
    .unwrap()
}

fn row(index: usize) -> RowUuid {
    RowUuid::from_bytes((index as u128 + 1).to_le_bytes())
}

fn run(rows: usize, queries: usize, width: usize, layout: &str, declare_resident: bool) {
    let overlapping = layout != "disjoint";
    let setup = Instant::now();
    let mut table = TableSchemaBuilder::new("items")
        .column("category", ColumnType::BigInt)
        .column("sequence", ColumnType::BigInt);
    for column in 0..16 {
        table = table.column(&format!("field_{column}"), ColumnType::Text);
    }
    let schema = JazzSchema::new(&SchemaBuilder::new().table(table).build()).unwrap();
    let mut writer = open(schema.clone(), 0x61);
    let mut sender = open(schema.clone(), 0x62);
    let mut receiver = open(schema.clone(), 0x63);
    for index in 0..rows {
        let mut fields = BTreeMap::from([
            ("category".into(), Value::I64((index % queries) as i64)),
            ("sequence".into(), Value::I64(index as i64)),
        ]);
        fields.extend((0..16).map(|column| {
            (
                format!("field_{column}"),
                Value::String(format!("value-{index}-{column}-{}", "x".repeat(width))),
            )
        }));
        let commit = MergeableCommit::new("items", row(index), 1_000 + index as u64).cells(fields);
        let (publication, unit) = block_on(writer.commit_mergeable_unit(commit)).unwrap();
        support::settle_transaction(&mut writer, publication);
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("commit unit")
        };
        let outcome =
            block_on(sender.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS))
                .unwrap();
        let fate = support::settle_outcome(&mut sender, outcome);
        assert!(matches!(
            fate.as_slice(),
            [SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }]
        ));
    }
    let shape = Query::from("items")
        .filter(if overlapping {
            gte(col("sequence"), param("value"))
        } else {
            eq(col("category"), param("value"))
        })
        .validate(&schema)
        .unwrap();
    let setup_ms = millis(setup);
    let mut peer = PeerState::new();
    let opts = RegisterShapeOptions::default();
    let mut resident_refs = BTreeSet::<RowVersionRef>::new();
    let mut physical_bodies = BTreeSet::<Vec<u8>>::new();
    let mut phases = Vec::new();
    let mut all_results = Vec::new();
    let measurement = Instant::now();

    for query_index in 0..queries {
        let floor = match layout {
            "disjoint" => query_index,
            "nested" => query_index * rows / queries,
            // Two empty views, two small overlapping views, the full list,
            // then a one-row detail. This avoids extrapolating a deliberately
            // heavy overlap curve to an ordinary list/detail startup.
            "mixed" => match query_index {
                0 => rows,
                1 => rows + 1,
                2 => rows - rows / 12,
                3 => rows - rows / 12 + 1,
                4 => 0,
                5 => rows - 1,
                _ => unreachable!("mixed layout has six views"),
            },
            _ => unreachable!("known layout"),
        };
        let setup = Instant::now();
        let binding = shape
            .bind(BTreeMap::from([("value".into(), Value::I64(floor as i64))]))
            .unwrap();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        let known_state = declare_resident.then(|| KnownStateDeclaration::ExactVersionSet {
            versions: resident_refs.iter().cloned().collect(),
        });
        let declaration_bytes = postcard::to_allocvec(&known_state).unwrap().len();
        support::apply_and_settle(
            &mut receiver,
            SyncMessage::RegisterShape {
                shape_id: shape.shape_id(),
                ast: ShapeAst::from_validated(&shape),
                opts: opts.clone(),
            },
        );
        support::apply_and_settle(
            &mut receiver,
            SyncMessage::Subscribe(Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: binding.values().values().cloned().collect(),
                known_state: known_state.clone(),
                delegated_session: Some(DelegatedSessionBinding {
                    identity: AuthorSubject::SYSTEM,
                    claims: BTreeMap::new(),
                }),
            }),
        );
        peer.declare_known_state(subscription, known_state);
        let registration_ms = millis(setup);

        let serving = Instant::now();
        let message = block_on(peer.rehydrate_query_for_subscription_with_opts(
            &mut sender,
            subscription,
            &shape,
            &binding,
            opts.clone(),
        ))
        .unwrap()
        .unwrap_or_else(|| {
            block_on(peer.query_update_for_subscription(
                &mut sender,
                subscription,
                &shape,
                &binding,
            ))
            .unwrap()
        });
        let serving_ms = millis(serving);

        // Accounting is kept separate from the measured encode/ingest phases.
        // Full encoded VersionRecord plus TxId pins physical schema, branch,
        // layer and contents; a logical row id alone is insufficient.
        let accounting = Instant::now();
        let SyncMessage::ViewUpdate(view) = &message else {
            panic!("view update")
        };
        let bundles = expand_version_carriers(&view.version_carriers).unwrap();
        let mut body_count = 0;
        let mut duplicate_bodies = 0;
        let mut body_bytes = 0;
        let mut duplicate_body_bytes = 0;
        let mut new_refs = Vec::new();
        for bundle in &bundles {
            for version in &bundle.versions {
                let encoded = postcard::to_allocvec(&(bundle.tx.tx_id, version)).unwrap();
                body_count += 1;
                body_bytes += encoded.len();
                if !physical_bodies.insert(encoded.clone()) {
                    duplicate_bodies += 1;
                    duplicate_body_bytes += encoded.len();
                }
                new_refs.push(RowVersionRef::new(
                    version.table(),
                    version.row_uuid(),
                    bundle.tx.tx_id,
                ));
            }
        }
        let supporting_rows = view.supporting_rows.added_rows().len();
        let accounting_ms = millis(accounting);

        let encode = Instant::now();
        let encoded = encode_sync_message(&message).unwrap();
        let encode_ms = millis(encode);
        let decode = Instant::now();
        let decoded = decode_sync_message_trusted(&encoded).unwrap();
        let decode_ms = millis(decode);
        let apply = Instant::now();
        let replies = support::apply_and_settle(&mut receiver, decoded);
        let apply_ms = millis(apply);
        assert!(
            replies.is_empty(),
            "fully supplied view should not need repair: {replies:?}"
        );
        // The explicit declaration is only advanced after receiver ingestion
        // and settlement succeeds, never immediately after sender publication.
        resident_refs.extend(new_refs);

        let read = Instant::now();
        let actual =
            block_on(receiver.query_rows(&shape, &binding, DurabilityTier::Global)).unwrap();
        let materialize_ms = millis(read);
        let validation = Instant::now();
        for current in &actual {
            let table = &schema.tables()[0];
            let Some(Value::I64(sequence)) = current.cell(table, "sequence") else {
                panic!("missing sequence cell");
            };
            let index = usize::try_from(sequence).unwrap();
            assert_eq!(current.row_uuid(), row(index));
            assert_eq!(
                current.cell(table, "category"),
                Some(Value::I64((index % queries) as i64))
            );
            for column in 0..16 {
                assert_eq!(
                    current.cell(table, &format!("field_{column}")),
                    Some(Value::String(format!(
                        "value-{index}-{column}-{}",
                        "x".repeat(width)
                    ))),
                    "complete row payload remains identical",
                );
            }
        }
        let ids = actual.iter().map(|r| r.row_uuid()).collect::<BTreeSet<_>>();
        let expected = (0..rows)
            .filter(|index| {
                if overlapping {
                    *index >= floor
                } else {
                    index % queries == query_index
                }
            })
            .map(row)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids, expected, "exact rows for query {query_index}");
        assert_eq!(actual.len(), expected.len(), "no duplicate result rows");
        all_results.push(ids);
        let validation_ms = millis(validation);
        phases.push(json!({
            "query":query_index,"results":actual.len(),"body_count":body_count,
            "duplicate_bodies":duplicate_bodies,"body_bytes":body_bytes,"duplicate_body_bytes":duplicate_body_bytes,
            "supporting_rows":supporting_rows,"declaration_bytes":declaration_bytes,
            "semantic_message_bytes":encoded.len(),
            "registration_ms":registration_ms,"serving_ms":serving_ms,
            "encode_ms":encode_ms,"decode_ms":decode_ms,"apply_ms":apply_ms,
            "materialize_ms":materialize_ms,"accounting_ms":accounting_ms,"validation_ms":validation_ms,
        }));
    }
    let wall_ms = millis(measurement);
    let signature = blake3::hash(&postcard::to_allocvec(&all_results).unwrap())
        .to_hex()
        .to_string();
    println!(
        "{}",
        json!({"benchmark":"overlapping_payloads","rows":rows,"queries":queries,"field_width":width,"layout":layout,"overlapping":overlapping,"declare_resident":declare_resident,"setup_ms":setup_ms,"wall_ms_including_accounting":wall_ms,"result_signature":signature,"unique_physical_bodies":physical_bodies.len(),"phases":phases})
    );
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = support::env_usize("JAZZ_PAYLOAD_ROWS", 600);
    let queries = support::env_usize("JAZZ_PAYLOAD_QUERIES", 12);
    assert!(rows >= queries && queries > 0);
    let arm = std::env::var("JAZZ_PAYLOAD_ARM").unwrap_or_else(|_| "both".to_owned());
    assert!(matches!(arm.as_str(), "both" | "control" | "resident"));
    for width in support::csv_usizes("JAZZ_PAYLOAD_WIDTHS", "0,128") {
        for (layout, count) in [("disjoint", queries), ("mixed", 6), ("nested", queries)] {
            for declare_resident in [false, true] {
                if arm == "control" && declare_resident || arm == "resident" && !declare_resident {
                    continue;
                }
                run(rows, count, width, layout, declare_resident);
            }
        }
    }
}
