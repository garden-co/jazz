//! Sync-wire codecs for the protocol types from [`ShapeAst`] onwards: shape
//! registration, subscription attach, result members and program facts, and
//! the catalogue payloads.

use std::borrow::Cow;

use super::*;
use crate::codec::{Bytes, CodecError, Opt, WireCodec, get_bytes, get_str, record_encode_error};

crate::wire_struct! { for ShapeAst { version, schema_version, body } }
crate::wire_enum! { for ShapeBody { 0 => Query(query), 1 => Relation(relation) } }

crate::wire_struct! { for RegisterShapeOptions { tier, read_view, binding_source } }
crate::wire_enum! { for BindingSource { 0 => Ordinary, 1 => RelayAuthoritySession } }
crate::wire_struct! { for ReadViewSpec { source } }
crate::wire_struct! { for ReadViewKey { id } }
crate::wire_enum! {
    for ReadViewSourceSpec {
        0 => Current,
        1 => BranchView { head, base },
        2 => Snapshot { snapshot },
    }
}

crate::wire_struct! {
    for Subscribe { shape_id, subscription, values, known_state, delegated_session }
}
crate::wire_struct! { for DelegatedSessionBinding { identity, claims } }
crate::wire_enum! {
    for KnownStateDeclaration {
        0 => Fast { completeness, position },
        1 => FastWithAuthorizationProgress { completeness, position, authorization_progress },
        2 => Reserved2(reserved),
        3 => Watermark { position, authorization_progress, supporting_revision },
    }
}
crate::wire_enum! { for KnownStateCompleteness { 0 => FastCurrentMembership } }
crate::wire_enum! {
    for SubscribeRejectReason {
        0 => UnsupportedShapeCapability { detail },
        1 => ShapeRegistrationPendingCatalogueAdmission,
        2 => ServerFailure { code },
        3 => InvalidAuthoritySourceClosure { transition },
    }
}
crate::wire_enum! {
    for SubscribeServerFailureCode {
        0 => TableNotFound,
        1 => SchemaResolution,
        2 => QueryValidation,
        3 => QueryLowering,
        4 => PolicyEvaluation,
        5 => Internal,
    }
}

// A newtype struct over a byte vector: postcard writes just the vector.
impl WireCodec for SyntheticReplacementToken {
    fn put(&self, buf: &mut Vec<u8>) {
        crate::codec::put_bytes(buf, &self.0);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        get_bytes(data, offset).map(|bytes| Self(bytes.to_vec()))
    }
}

crate::wire_enum! {
    for ResultMemberEntry {
        0 => Row(row),
        1 => Synthetic { table, row: Bytes, replacement },
        2 => PathTuple {
            path,
            source_table,
            source_row,
            target_table,
            target_row,
            edge_id: Opt(Bytes),
            revision: Bytes,
        },
        3 => TypedRow { row, occurrence_key },
    }
}
crate::wire_struct! {
    for RealRowMemberEntry {
        table,
        row_uuid,
        occurrence_id,
        content_tx,
        layer,
        deletion_tx,
        source,
        read_view,
        schema_version,
        branch_or_prefix: Opt(Bytes),
        row_digest: Opt(Bytes),
        batch,
        settle_position,
    }
}
crate::wire_enum! { for ResultRowLayer { 0 => Content, 1 => Deletion, 2 => ContentOrDeletion } }
crate::wire_enum! {
    for ResultRowSource {
        0 => Current,
        1 => Snapshot { snapshot },
        2 => HistoryCut { global_time },
        3 => Merge { inputs },
        4 => LensProjection { schema_version, base },
        5 => Overlay { tx, base },
    }
}

// `version_table` decodes through the same borrowing intern cache as the
// serde `deserialize_with` hook; it is written as a plain string.
impl WireCodec for SupportingRow {
    fn put(&self, buf: &mut Vec<u8>) {
        self.physical_table.put(buf);
        crate::codec::put_bytes(buf, self.version_table.as_bytes());
        self.row.put(buf);
        self.version.put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Ok(Self {
            physical_table: WireCodec::get(data, offset)?,
            version_table: supporting_table_name::intern(Cow::Borrowed(get_str(data, offset)?)),
            row: WireCodec::get(data, offset)?,
            version: WireCodec::get(data, offset)?,
        })
    }
}

crate::wire_enum! {
    for ProgramFactEntry {
        0 => ResultPayload(entry),
        1 => RelationEdge(entry),
        2 => PathCorrelationCoverage(entry),
        3 => ProgramSourceCoverage(entry),
        4 => ReadFrontierSettled(entry),
        5 => CompleteTxPayloadCoverage(entry),
        6 => ViewCompleteExclusiveCoverage(entry),
        7 => PolicyDecision(entry),
        8 => VersionWitness(entry),
        9 => CoveredInput(entry),
        10 => PolicyWitness(entry),
        11 => ContributingMembers(entry),
        12 => PredicateRead(entry),
        13 => PredicateOutputSet(entry),
        14 => PointRead(entry),
    }
}
crate::wire_struct! { for ResultMemberPayloadEntry { member, descriptor: Bytes, record: Bytes } }
crate::wire_struct! {
    for RelationEdgeEntry {
        path,
        source_table,
        source_row,
        target_table,
        target_row,
        kind,
        source_version,
        target_version,
        depth,
        edge_id: Opt(Bytes),
        branch: Opt(Bytes),
        role,
        order: Opt(Bytes),
        hole_state,
    }
}
crate::wire_struct! {
    for RowVersionRefEntry {
        tx,
        schema_version,
        layer,
        batch,
        branch_or_prefix: Opt(Bytes),
        row_digest: Opt(Bytes),
    }
}
crate::wire_struct! { for VersionWitnessEntry { role, version, member } }
crate::wire_struct! { for CoveredInputEntry { source, version_table, source_row, version } }
crate::wire_struct! { for ProgramSourceId { table, path } }
crate::wire_enum! {
    for ProgramSourceRole {
        0 => Root,
        1 => Alias(name),
        2 => RecursiveSeed(name),
        3 => RecursiveStep(name),
        4 => CorrelatedChild(name),
        5 => Policy(name),
    }
}
crate::wire_struct! { for PolicyWitnessEntry { protected, policy_path, witness, edge_kind } }
crate::wire_struct! { for ContributingMembersEntry { result, contributor, batch, role } }
crate::wire_struct! {
    for PredicateReadEntry { role, shape_id, binding_id, predicate: Bytes, frontier: Bytes }
}
crate::wire_struct! { for PointReadEntry { present, table, row, version, shape_id, binding_id } }
crate::wire_enum! {
    for RelationEdgeKind { 0 => Include, 1 => Join, 2 => Relation, 3 => Recursive, 4 => Policy }
}
crate::wire_enum! { for RelationEdgeRole { 0 => Intermediate, 1 => Frontier, 2 => Terminal } }
crate::wire_enum! { for PathHoleState { 0 => Matched, 1 => Hole } }
crate::wire_struct! {
    for PathCorrelationCoverageEntry {
        path,
        source_table,
        source_row,
        correlation_key: Bytes,
        complete,
    }
}
crate::wire_struct! { for ProgramSourceCoverageEntry { source, complete } }
crate::wire_struct! { for ReadFrontierSettledEntry { scope, tier, stream, frontier: Bytes } }
crate::wire_struct! { for CompleteTxPayloadCoverageEntry { tx, tier, payload_digest: Bytes } }
crate::wire_struct! {
    for ViewCompleteExclusiveCoverageEntry {
        tx,
        scope,
        result,
        tier,
        covered_members_digest: Bytes,
    }
}
crate::wire_struct! { for PolicyDecisionEntry { decision: Bytes, outcome, reason } }
crate::wire_enum! {
    for PolicyDecisionOutcomeEntry {
        0 => Allowed,
        1 => Denied,
        2 => IndeterminateRequiresInput { input },
        3 => RequiresCoverage { scope, frontier: Bytes },
    }
}
crate::wire_struct! {
    for PredicateOutputSetEntry { role, table, row, version, shape_id, binding_id }
}
crate::wire_enum! { for PredicateOutputSetRoleEntry { 0 => Base, 1 => Now } }

// Postcard spelled this as `SchemaVersionWire { id, public_schema_json }`.
impl WireCodec for SchemaVersion {
    fn put(&self, buf: &mut Vec<u8>) {
        self.id.put(buf);
        let json = durable_public_schema_json(&self.schema).unwrap_or_else(|message| {
            record_encode_error(CodecError::Encode { message });
            Vec::new()
        });
        crate::codec::put_bytes(buf, &json);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let id = WireCodec::get(data, offset)?;
        let schema = compile_public_schema_json(get_bytes(data, offset)?)
            .map_err(|message| CodecError::DecodeError { message })?;
        Ok(Self { id, schema })
    }
}

crate::wire_struct! {
    for SchemaLineagePublication { id, schema, predecessors, physical_identities }
}
crate::wire_struct! { for SchemaPredecessor { lens, new_tables, dropped_tables } }
crate::wire_struct! { for PhysicalIdentityManifest { tables } }
crate::wire_struct! { for PhysicalTableIdentity { id, columns } }
crate::wire_struct! { for PhysicalColumnIdentity { id, enum_variants } }

/// Whether the canonical lens grammar can spell `value` (its writer panics on
/// these engine-internal values instead).
fn lens_value_encodable(value: &Value) -> bool {
    match value {
        Value::Enum(_) | Value::U48(_) => false,
        Value::Tuple(values) | Value::Array(values) => values.iter().all(lens_value_encodable),
        Value::Nullable(Some(value)) => lens_value_encodable(value),
        _ => true,
    }
}

// Postcard spelled this as one byte blob holding the canonical lens bytes;
// decoding goes through the validating canonical parser.
impl WireCodec for MigrationLens {
    fn put(&self, buf: &mut Vec<u8>) {
        let encodable = self
            .table_lenses
            .iter()
            .flat_map(|table| &table.ops)
            .all(|op| match op {
                LensOp::AddColumn { default: value, .. }
                | LensOp::DropColumn {
                    backwards_default: value,
                    ..
                } => lens_value_encodable(value),
                _ => true,
            });
        if encodable {
            crate::codec::put_bytes(buf, &canonical_lens_bytes(self));
        } else {
            record_encode_error(CodecError::Encode {
                message: "migration lens default is not a Jazz protocol value".to_owned(),
            });
            crate::codec::put_bytes(buf, &[]);
        }
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let bytes = get_bytes(data, offset)?;
        if bytes.len() > ProtocolCodecCursor::MAX_TOTAL_BYTES {
            return Err(CodecError::DecodeError {
                message: "migration lens blob exceeds aggregate byte budget".to_owned(),
            });
        }
        decode_canonical_lens_bytes(bytes).map_err(|message| CodecError::DecodeError {
            message: message.to_owned(),
        })
    }
}

crate::wire_struct! { for TableLens { source_table, target_table, ops } }
crate::wire_enum! {
    for LensOp {
        0 => RenameTable { from, to },
        1 => RenameColumn { from, to },
        2 => CopyColumn { from, to },
        3 => AddColumn { column, default },
        4 => DropColumn { column, backwards_default },
        5 => TransformColumn { column, transform },
        6 => RejectSourceDelta { reason },
    }
}
crate::wire_struct! { for CurrentWriteSchema { revision, schema } }
crate::wire_struct! { for CatalogueAck { revision, schema, lens, applied } }

#[cfg(test)]
mod tests {
    //! Differential tests against the serde/postcard layout these codecs
    //! replace. Byte layout is not observable through the public sync API, so
    //! these are deliberately white-box.

    use super::*;
    use crate::codec::{decode_exact, encode, take_encode_error};
    use crate::model::public_schema::{
        ColumnType as PublicColumnType, PolicyExpr, SchemaBuilder, TablePolicies,
        TableSchemaBuilder,
    };
    use crate::query::RelationExpr;

    /// The value decodes exactly from its encoding, and the decoded value
    /// encodes to the same bytes. (A row member's legacy `occurrence_id`
    /// field is not on the wire, so decoded values compare by bytes.)
    fn same<T>(value: &T)
    where
        T: WireCodec + std::fmt::Debug,
    {
        let bytes = crate::codec::try_encode(value).unwrap();
        let decoded = decode_exact::<T>(&bytes).unwrap();
        assert_eq!(encode(&decoded), bytes, "{value:?}");
    }

    fn uuid(byte: u8) -> uuid::Uuid {
        uuid::Uuid::from_bytes([byte; 16])
    }

    fn tx(time: u64, node: u8) -> TxId {
        TxId::new(TxTime(time), NodeUuid::from_bytes([node; 16]))
    }

    fn intern(name: &str) -> groove::Intern<String> {
        groove::Intern::from_ref(name)
    }

    fn snapshot() -> SnapshotRef {
        SnapshotRef {
            owner: NodeUuid::from_bytes([1; 16]),
            global_base: GlobalTime(u64::MAX),
            local_base: TxTime(7),
            dots: vec![tx(3, 4), tx(0, 0)],
        }
    }

    fn selector() -> BranchSelector {
        BranchSelector {
            values: BTreeMap::from([("b".to_owned(), BranchColumnValue(vec![1, 2]))]),
        }
    }

    fn version(populated: bool) -> RowVersionRefEntry {
        RowVersionRefEntry {
            tx: tx(300, 5),
            schema_version: populated.then(|| SchemaVersionId::from_bytes([6; 16])),
            layer: ResultRowLayer::Deletion,
            batch: populated.then(|| tx(1, 7)),
            branch_or_prefix: populated.then(|| vec![0, 255]),
            row_digest: populated.then(Vec::new),
        }
    }

    fn real_row(populated: bool) -> RealRowMemberEntry {
        let mut row = RealRowMemberEntry::current_content((
            intern("todos"),
            RowUuid::from_bytes([1; 16]),
            tx(2, 3),
        ));
        if populated {
            row.occurrence_id = None;
            row.deletion_tx = Some(tx(4, 4));
            row.layer = ResultRowLayer::ContentOrDeletion;
            row.source = ResultRowSource::Snapshot {
                snapshot: snapshot(),
            };
            row.read_view = ReadViewKey { id: uuid(8) };
            row.schema_version = Some(SchemaVersionId::from_bytes([9; 16]));
            row.branch_or_prefix = Some(vec![1]);
            row.row_digest = Some(vec![2; 40]);
            row.batch = Some(tx(5, 5));
            row.settle_position = Some(GlobalTime(1 << 40));
        }
        row
    }

    fn members() -> Vec<ResultMemberEntry> {
        let root = ObjectId::from_uuid(uuid(1));
        let occurrence = OutputOccurrenceId::with_union_arms(
            root,
            [ObjectId::from_uuid(uuid(2))],
            [(0, "d".to_owned())],
        )
        .unwrap();
        vec![
            ResultMemberEntry::Row(real_row(false)),
            ResultMemberEntry::Row(real_row(true)),
            real_row(false).with_occurrence_id(occurrence).into(),
            ResultMemberEntry::Synthetic {
                table: "agg".to_owned(),
                row: vec![1, 2, 3],
                replacement: SyntheticReplacementToken::from_encoded_record(vec![6; 200]),
            },
            ResultMemberEntry::PathTuple {
                path: "p".to_owned(),
                source_table: intern("a"),
                source_row: RowUuid::from_bytes([2; 16]),
                target_table: intern("b"),
                target_row: RowUuid::from_bytes([3; 16]),
                edge_id: Some(vec![4]),
                revision: Vec::new(),
            },
            ResultMemberEntry::PathTuple {
                path: String::new(),
                source_table: intern("a"),
                source_row: RowUuid::from_bytes([2; 16]),
                target_table: intern("b"),
                target_row: RowUuid::from_bytes([3; 16]),
                edge_id: None,
                revision: vec![9],
            },
        ]
    }

    fn sources() -> Vec<ResultRowSource> {
        vec![
            ResultRowSource::Current,
            ResultRowSource::Snapshot {
                snapshot: snapshot(),
            },
            ResultRowSource::HistoryCut {
                global_time: GlobalTime(0),
            },
            ResultRowSource::Merge { inputs: Vec::new() },
            ResultRowSource::Merge {
                inputs: vec![
                    ResultRowSource::Current,
                    ResultRowSource::LensProjection {
                        schema_version: SchemaVersionId::from_bytes([1; 16]),
                        base: Box::new(ResultRowSource::Overlay {
                            tx: tx(1, 1),
                            base: Box::new(ResultRowSource::Current),
                        }),
                    },
                ],
            },
        ]
    }

    fn lens() -> MigrationLens {
        MigrationLens::new(
            SchemaVersionId::from_bytes([1; 16]),
            SchemaVersionId::from_bytes([2; 16]),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "status".to_owned(),
                        default: Value::Nullable(Some(Box::new(Value::String("open".into())))),
                    },
                ],
            }],
        )
        .unwrap()
    }

    fn schema_version() -> SchemaVersion {
        let source = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build();
        SchemaVersion::new(crate::schema::JazzSchema::new(&source).unwrap())
    }

    /// Shape registration bodies, including the typed relation wire form.
    #[test]
    fn shape_registration_matches_postcard() {
        let schema = SchemaVersionId::from_bytes([3; 16]);
        same(&ShapeAst::new(Query::from("todos"), schema));
        same(&ShapeAst::new_relation(
            RelationQuery {
                rel: RelationExpr::TableScan {
                    table: "t".into(),
                    alias: Some("a".into()),
                },
            },
            schema,
        ));
        let branch = ReadViewSpec::branch_view(selector(), None);
        let frozen = ReadViewSpec::branch_view(
            selector(),
            Some(BranchViewBase::Snapshot {
                branch: selector(),
                snapshot: snapshot(),
            }),
        );
        for read_view in [
            ReadViewSpec::default(),
            branch,
            frozen,
            ReadViewSpec {
                source: ReadViewSourceSpec::Snapshot {
                    snapshot: snapshot(),
                },
            },
        ] {
            for binding_source in [
                BindingSource::Ordinary,
                BindingSource::RelayAuthoritySession,
            ] {
                let options = RegisterShapeOptions {
                    tier: DurabilityTier::Local,
                    read_view: read_view.clone(),
                    binding_source,
                };
                same(&options);
                same(&options.read_view_key());
            }
        }
    }

    /// Subscription attach, known-state declarations and rejection reasons.
    #[test]
    fn subscription_attach_matches_postcard() {
        let subscription = SubscriptionKey {
            shape_id: ShapeId(uuid(1)),
            binding_id: BindingId(uuid(2)),
            read_view: ReadViewKey::default(),
        };
        let declarations = [
            KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: GlobalTime(5),
            },
            KnownStateDeclaration::FastWithAuthorizationProgress {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: GlobalTime(0),
                authorization_progress: u64::MAX,
            },
            KnownStateDeclaration::Watermark {
                position: GlobalTime(128),
                authorization_progress: None,
                supporting_revision: [7; 16],
            },
            KnownStateDeclaration::Watermark {
                position: GlobalTime(1),
                authorization_progress: Some(0),
                supporting_revision: [0; 16],
            },
        ];
        for declaration in &declarations {
            same(declaration);
        }
        // Retired tag 2 is uninhabited and must not decode.
        assert!(decode_exact::<KnownStateDeclaration>(&[2]).is_err());
        same(&Subscribe {
            shape_id: ShapeId(uuid(1)),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        });
        same(&Subscribe {
            shape_id: ShapeId(uuid(1)),
            subscription,
            values: vec![Value::U32(1), Value::String("x".into())],
            known_state: Some(declarations[2].clone()),
            delegated_session: Some(DelegatedSessionBinding {
                identity: AuthorSubject::System,
                claims: BTreeMap::from([("role".to_owned(), Value::String("admin".into()))]),
            }),
        });
        for reason in [
            SubscribeRejectReason::UnsupportedShapeCapability { detail: "d".into() },
            SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
            SubscribeRejectReason::InvalidAuthoritySourceClosure {
                transition: String::new(),
            },
        ] {
            same(&reason);
        }
        for code in [
            SubscribeServerFailureCode::TableNotFound,
            SubscribeServerFailureCode::SchemaResolution,
            SubscribeServerFailureCode::QueryValidation,
            SubscribeServerFailureCode::QueryLowering,
            SubscribeServerFailureCode::PolicyEvaluation,
            SubscribeServerFailureCode::Internal,
        ] {
            same(&SubscribeRejectReason::ServerFailure { code });
        }
    }

    /// Result members, their layers and sources.
    #[test]
    fn result_members_round_trip() {
        for member in members() {
            same(&member);
        }
        for source in sources() {
            same(&source);
        }
        let token = SyntheticReplacementToken::from_encoded_record(vec![1, 2]);
        let bytes = encode(&token);
        assert_eq!(
            decode_exact::<SyntheticReplacementToken>(&bytes).unwrap(),
            token
        );
        let entry: ResultRowEntry = (intern("t"), RowUuid::from_bytes([1; 16]), tx(9, 9));
        same(&entry);
    }

    /// Supporting rows keep the interning string decode.
    #[test]
    fn supporting_rows_round_trip() {
        for name in ["", "todos", "résumé/倉庫"] {
            for populated in [false, true] {
                let row = SupportingRow {
                    physical_table: crate::ids::GlobalPhysicalTableId(uuid(3)),
                    version_table: intern(name),
                    row: RowUuid::from_bytes([4; 16]),
                    version: version(populated),
                };
                same(&row);
            }
        }
        let mut bad = encode(&SupportingRow {
            physical_table: crate::ids::GlobalPhysicalTableId(uuid(3)),
            version_table: intern("a"),
            row: RowUuid::from_bytes([4; 16]),
            version: version(false),
        });
        bad[18] = 0xff;
        assert!(decode_exact::<SupportingRow>(&bad).is_err());
    }

    /// Every program-fact variant and its payload entries.
    #[test]
    fn program_facts_round_trip() {
        let member = members().remove(1);
        let source = ProgramSourceId {
            table: intern("todos"),
            path: vec![
                ProgramSourceRole::Root,
                ProgramSourceRole::Alias("a".into()),
                ProgramSourceRole::RecursiveSeed("s".into()),
                ProgramSourceRole::RecursiveStep("t".into()),
                ProgramSourceRole::CorrelatedChild("c".into()),
                ProgramSourceRole::Policy("p".into()),
            ],
        };
        let edge = |populated: bool| RelationEdgeEntry {
            path: "p".into(),
            source_table: intern("a"),
            source_row: RowUuid::from_bytes([1; 16]),
            target_table: intern("b"),
            target_row: RowUuid::from_bytes([2; 16]),
            kind: populated.then_some(RelationEdgeKind::Recursive),
            source_version: populated.then(|| version(true)),
            target_version: populated.then(|| version(false)),
            depth: populated.then_some(u32::MAX),
            edge_id: populated.then(|| vec![1]),
            branch: populated.then(Vec::new),
            role: populated.then_some(RelationEdgeRole::Terminal),
            order: populated.then(|| vec![3]),
            hole_state: populated.then_some(PathHoleState::Hole),
        };
        let mut facts = vec![
            ProgramFactEntry::ResultPayload(ResultMemberPayloadEntry {
                member: member.clone(),
                descriptor: vec![1],
                record: Vec::new(),
            }),
            ProgramFactEntry::RelationEdge(edge(false)),
            ProgramFactEntry::RelationEdge(edge(true)),
            ProgramFactEntry::PathCorrelationCoverage(PathCorrelationCoverageEntry {
                path: "p".into(),
                source_table: intern("a"),
                source_row: RowUuid::from_bytes([1; 16]),
                correlation_key: vec![2],
                complete: true,
            }),
            ProgramFactEntry::ProgramSourceCoverage(ProgramSourceCoverageEntry {
                source: source.clone(),
                complete: false,
            }),
            ProgramFactEntry::ReadFrontierSettled(ReadFrontierSettledEntry {
                scope: "s".into(),
                tier: DurabilityTier::Global,
                stream: Some("x".into()),
                frontier: vec![1, 2],
            }),
            ProgramFactEntry::CompleteTxPayloadCoverage(CompleteTxPayloadCoverageEntry {
                tx: tx(1, 1),
                tier: DurabilityTier::None,
                payload_digest: vec![0; 32],
            }),
            ProgramFactEntry::ViewCompleteExclusiveCoverage(ViewCompleteExclusiveCoverageEntry {
                tx: tx(1, 2),
                scope: "v".into(),
                result: Some(member.clone()),
                tier: DurabilityTier::Local,
                covered_members_digest: vec![5],
            }),
            ProgramFactEntry::VersionWitness(VersionWitnessEntry {
                role: "payload".into(),
                version: version(true),
                member: None,
            }),
            ProgramFactEntry::CoveredInput(CoveredInputEntry {
                source,
                version_table: intern("todos"),
                source_row: RowUuid::from_bytes([3; 16]),
                version: version(false),
            }),
            ProgramFactEntry::PolicyWitness(PolicyWitnessEntry {
                protected: member.clone(),
                policy_path: "pp".into(),
                witness: version(true),
                edge_kind: Some(RelationEdgeKind::Policy),
            }),
            ProgramFactEntry::ContributingMembers(ContributingMembersEntry {
                result: member.clone(),
                contributor: members().remove(3),
                batch: Some(tx(2, 2)),
                role: None,
            }),
            ProgramFactEntry::PredicateRead(PredicateReadEntry {
                role: PredicateOutputSetRoleEntry::Base,
                shape_id: ShapeId(uuid(1)),
                binding_id: BindingId(uuid(2)),
                predicate: vec![1],
                frontier: vec![2],
            }),
            ProgramFactEntry::PredicateOutputSet(PredicateOutputSetEntry {
                role: PredicateOutputSetRoleEntry::Now,
                table: intern("t"),
                row: RowUuid::from_bytes([4; 16]),
                version: version(false),
                shape_id: ShapeId(uuid(1)),
                binding_id: BindingId(uuid(2)),
            }),
        ];
        for present in [false, true] {
            facts.push(ProgramFactEntry::PointRead(PointReadEntry {
                present,
                table: intern("t"),
                row: RowUuid::from_bytes([5; 16]),
                version: present.then(|| version(true)),
                shape_id: ShapeId(uuid(1)),
                binding_id: BindingId(uuid(2)),
            }));
        }
        for outcome in [
            PolicyDecisionOutcomeEntry::Allowed,
            PolicyDecisionOutcomeEntry::Denied,
            PolicyDecisionOutcomeEntry::IndeterminateRequiresInput { input: "i".into() },
            PolicyDecisionOutcomeEntry::RequiresCoverage {
                scope: "s".into(),
                frontier: vec![1],
            },
        ] {
            facts.push(ProgramFactEntry::PolicyDecision(PolicyDecisionEntry {
                decision: vec![9],
                outcome,
                reason: Some("r".into()),
            }));
        }
        for fact in &facts {
            same(fact);
        }
    }

    /// Catalogue payloads: schema versions, lenses, publications and acks.
    #[test]
    fn catalogue_payloads_round_trip() {
        let schema = schema_version();
        same(&schema);
        same(&lens());
        let empty = MigrationLens::new(
            SchemaVersionId::from_bytes([1; 16]),
            SchemaVersionId::from_bytes([2; 16]),
            Vec::new(),
        )
        .unwrap();
        same(&empty);
        same(&TableLens {
            source_table: "a".into(),
            target_table: "b".into(),
            ops: vec![
                LensOp::RenameColumn {
                    from: "c".into(),
                    to: "d".into(),
                },
                LensOp::CopyColumn {
                    from: "e".into(),
                    to: "f".into(),
                },
                LensOp::DropColumn {
                    column: "g".into(),
                    backwards_default: Value::I64(-1),
                },
                LensOp::TransformColumn {
                    column: "h".into(),
                    transform: "i".into(),
                },
                LensOp::RejectSourceDelta { reason: "j".into() },
            ],
        });
        let manifest = PhysicalIdentityManifest {
            tables: BTreeMap::from([(
                "todos".to_owned(),
                PhysicalTableIdentity {
                    id: crate::ids::GlobalPhysicalTableId(uuid(1)),
                    columns: BTreeMap::from([(
                        "status".to_owned(),
                        PhysicalColumnIdentity {
                            id: crate::ids::GlobalPhysicalColumnId(uuid(2)),
                            enum_variants: BTreeMap::from([(
                                "root".to_owned(),
                                vec![crate::ids::GlobalPhysicalEnumVariantId(uuid(3))],
                            )]),
                        },
                    )]),
                },
            )]),
        };
        same(&manifest);
        same(&PhysicalIdentityManifest {
            tables: BTreeMap::new(),
        });
        same(&SchemaLineagePublication {
            id: SchemaLineagePublicationId::from_bytes([4; 16]),
            schema,
            predecessors: vec![SchemaPredecessor {
                lens: lens(),
                new_tables: vec!["n".into()],
                dropped_tables: Vec::new(),
            }],
            physical_identities: manifest,
        });
        same(&CurrentWriteSchema {
            revision: 300,
            schema: SchemaVersionId::from_bytes([5; 16]),
        });
        same(&CatalogueAck {
            revision: None,
            schema: None,
            lens: None,
            applied: false,
        });
        same(&CatalogueAck {
            revision: Some(1),
            schema: Some(SchemaVersionId::from_bytes([5; 16])),
            lens: Some(MigrationLensId::from_bytes([6; 16])),
            applied: true,
        });
    }

    /// Lens blobs keep the canonical parser's validation and size cap, and an
    /// unspellable default is an encode error rather than a panic.
    #[test]
    fn migration_lens_wire_keeps_its_validation() {
        let mut trailing = canonical_lens_bytes(&lens());
        trailing.push(0);
        let mut wire = Vec::new();
        crate::codec::put_bytes(&mut wire, &trailing);
        assert!(decode_exact::<MigrationLens>(&wire).is_err());

        let oversized = vec![0xff; ProtocolCodecCursor::MAX_TOTAL_BYTES + 1];
        let mut wire = Vec::new();
        crate::codec::put_bytes(&mut wire, &oversized);
        assert_eq!(
            decode_exact::<MigrationLens>(&wire),
            Err(CodecError::DecodeError {
                message: "migration lens blob exceeds aggregate byte budget".to_owned()
            })
        );

        let mut lens = lens();
        lens.table_lenses[0].ops.push(LensOp::AddColumn {
            column: "x".into(),
            default: Value::Array(vec![Value::U48(1)]),
        });
        let _ = take_encode_error();
        let _ = encode(&lens);
        assert!(matches!(
            take_encode_error(),
            Some(CodecError::Encode { .. })
        ));
        assert_eq!(take_encode_error(), None);
    }
}
