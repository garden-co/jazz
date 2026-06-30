use super::*;

/// Requested read set selected before source resolution.
pub(crate) type RequestedReadSet = QueryReadSet<RequestedSourceStage>;

/// Resolved read identity used by shared maintained work.
pub(crate) type ResolvedReadSet = QueryReadSet<ResolvedSourceStage>;

/// All read views a lowered program may use.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct QueryReadSet<R: SourceResolution> {
    /// Primary read view used by ordinary row-set semantics.
    pub(crate) primary: ReadView<R>,
    /// Fact-specific comparison/read views, such as exclusive validation base
    /// and now predicate output sets.
    pub(crate) fact_reads: BTreeMap<FactReadRole, ReadView<R>>,
}

impl<R: SourceResolution> QueryReadSet<R> {
    /// Build a read set with only a primary read view.
    pub(crate) fn primary(primary: ReadView<R>) -> Self {
        Self {
            primary,
            fact_reads: BTreeMap::new(),
        }
    }
}

/// Role for a non-primary read view inside one program request.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FactReadRole {
    /// Predicate output set at the exclusive transaction base snapshot.
    PredicateOutputBase,
    /// Predicate output set at validation/comparison time.
    PredicateOutputNow,
}

/// Requested read view selected before source resolution.
pub(crate) type RequestedReadView = ReadView<RequestedSourceStage>;

/// Resolved read identity used by shared maintained work.
pub(crate) type ResolvedReadKey = ReadView<ResolvedSourceStage>;

/// Concrete read view at a particular source-resolution stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReadView<R: SourceResolution> {
    /// Schema version exposed to the query and application rows.
    pub(crate) read_schema: SchemaVersionId,
    /// Effective authorization schema/policy head used to evaluate read/write
    /// policies. This identity must include permission definitions, not just
    /// application column layout.
    pub(crate) policy_schema: SchemaVersionId,
    /// Canonical source expression for every source the normalized/augmented
    /// program may read. Policy augmentation introduces ordinary `SourceId`s
    /// with policy roles here; it does not get a separate source universe.
    pub(crate) sources: SourceGraph<R>,
}

impl<R: SourceResolution> ReadView<R> {
    /// Return the schema version visible to application/query semantics.
    pub(crate) fn read_schema(&self) -> SchemaVersionId {
        self.read_schema
    }
}

impl RequestedReadView {
    /// Return the current durability tier for one source expression.
    pub(crate) fn source_current_tier(&self, source: &SourceId) -> Option<DurabilityTier> {
        self.sources.get(source)?.current_tier()
    }
}

/// Resolves protocol/API read options into the source graph used by lowering.
///
/// Unsupported historic, branch, partition, lens, or overlay requests should
/// fail here as `CapabilityReport::Source` instead of being rejected by ad-hoc
/// facade gates. Runtime propagation and callback lifecycles stay outside this
/// resolver; they consume the facts emitted by the lowered program.
pub(crate) trait ReadViewResolver {
    /// Translate unresolved API options into a requested read view.
    fn requested_read_view(
        &self,
        opts: &RegisterShapeOptions,
        shape: &NormalizedRowSetShape,
    ) -> CapabilityResult<RequestedReadSet>;

    /// Resolve requested schema/source choices into cache/shareable identities.
    fn resolve_read_view(&mut self, read: &RequestedReadSet) -> CapabilityResult<ResolvedReadSet>;
}

/// Requested read expression for one logical source.
pub(crate) type RequestedSourceExpr = SourceExpr<RequestedSourceStage>;

/// Resolved read expression for one logical source.
pub(crate) type ResolvedSourceExpr = SourceExpr<ResolvedSourceStage>;

/// Canonical source-expression set at a particular resolution stage.
pub(crate) type SourceGraph<R> = BTreeMap<SourceId, SourceExpr<R>>;

/// Source expression algebra. Branches, snapshots, overlays, transactions, and
/// schema projections compose here instead of selecting a separate query path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceExpr<R: SourceResolution> {
    /// Visible current rows at one durability tier.
    VisibleCurrent {
        /// Schema/storage/lens projection used by this source.
        projection: SchemaProjection<R>,
        /// Data branch/prefix selected for this source.
        data: DataSource<R::DataBranch>,
        /// Local, edge, or global source currency.
        tier: DurabilityTier,
    },
    /// Historical global cut.
    HistoryCut {
        /// Schema/storage/lens projection used by this source.
        projection: SchemaProjection<R>,
        /// Data branch/prefix selected for this source.
        data: DataSource<R::DataBranch>,
        /// Inclusive global sequence cut.
        position: GlobalSeq,
    },
    /// Dotted snapshot ref.
    SnapshotRef {
        /// Schema/storage/lens projection used by this source.
        projection: SchemaProjection<R>,
        /// Data branch/prefix selected for this source.
        data: DataSource<R::DataBranch>,
        /// Snapshot frontier.
        snapshot: Snapshot,
    },
    /// Overlay local/branch/transactional writes on top of another source.
    WithOverlays {
        /// Base source expression.
        input: Box<SourceExpr<R>>,
        /// Overlays visible above the input.
        overlays: OverlayStack<R::Overlay>,
    },
    /// Project one source expression through a schema/lens path.
    LensProject {
        /// Source expression to project.
        input: Box<SourceExpr<R>>,
        /// Projection to apply.
        projection: SchemaProjection<R>,
    },
    /// Merge or union source alternatives.
    Merge {
        /// Source expressions to combine.
        inputs: Vec<SourceExpr<R>>,
        /// Merge semantics.
        mode: SourceMergeMode,
        /// Resolution metadata used by caches and explain output.
        resolution: R::MergedSources,
    },
}

impl<R: SourceResolution> SourceExpr<R> {
    /// Return the current durability tier if this expression is a simple
    /// current-source expression after transparent projection/overlay nodes.
    pub(crate) fn current_tier(&self) -> Option<DurabilityTier> {
        match self {
            SourceExpr::VisibleCurrent { tier, .. } => Some(*tier),
            SourceExpr::WithOverlays { input, .. } | SourceExpr::LensProject { input, .. } => {
                input.current_tier()
            }
            SourceExpr::Merge { inputs, .. } => {
                let mut tiers = inputs.iter().filter_map(SourceExpr::current_tier);
                let first = tiers.next()?;
                if tiers.all(|tier| tier == first) {
                    Some(first)
                } else {
                    None
                }
            }
            SourceExpr::HistoryCut { .. } | SourceExpr::SnapshotRef { .. } => None,
        }
    }
}

/// Data branch/prefix selected for a source expression.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DataSource<B> {
    /// Current/default data branch.
    Current,
    /// Explicit data branch/prefix.
    Branch(B),
}

/// Schema/storage/lens projection attached to a source-expression boundary.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SchemaProjection<R: SourceResolution> {
    /// Schema family branch for this logical source.
    pub(crate) schema_family: R::SchemaFamily,
    /// Stored schema partitions considered by source resolution.
    pub(crate) storage: R::Storage,
    /// Lens/projection path from stored partitions into `read_schema`.
    pub(crate) lens: R::Lens,
}

/// How multiple source expressions combine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceMergeMode {
    /// LWW merge by row identity and transaction/version order.
    LastWriteWins,
    /// Preserve alternatives as a union. Downstream distinct/project nodes own
    /// any deduplication.
    Union,
}

/// Branch/schema-family selector for schema resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SchemaFamilySelection {
    /// Current/default branch for the runtime.
    Current,
    /// Explicit schema-family branch.
    SchemaFamilyBranch(BranchId),
}

/// Stored schema partitions to read before projecting into `read_schema`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum StorageSchemaSelection {
    /// Resolver chooses every compatible partition for the selected branch
    /// family and lens path.
    CompatiblePartitions,
    /// Read one stored schema partition.
    Single(SchemaVersionId),
    /// Read an explicit partition set, usually for tests or snapshot refs.
    Explicit(BTreeSet<SchemaVersionId>),
}

/// Lens/projection selector for schema-compatible reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LensSelection {
    /// Resolver chooses the current canonical lens path into `read_schema`.
    Canonical,
    /// Require this already-resolved lens path fingerprint.
    Fingerprint(Vec<u8>),
}

/// Marker for metadata attached to requested vs resolved source selectors.
pub(crate) trait SourceResolution {
    /// Schema-family identity at this resolution stage.
    type SchemaFamily: Clone + std::fmt::Debug + PartialEq + Eq + std::hash::Hash;
    /// Storage partition identity at this resolution stage.
    type Storage: Clone + std::fmt::Debug + PartialEq + Eq + std::hash::Hash;
    /// Lens/projection identity at this resolution stage.
    type Lens: Clone + std::fmt::Debug + PartialEq + Eq + std::hash::Hash;
    /// Own-pending overlay identity at this resolution stage.
    type Overlay: Clone + std::fmt::Debug + PartialEq + Eq + std::hash::Hash;
    /// Data branch/prefix identity at this resolution stage.
    type DataBranch: Clone + std::fmt::Debug + PartialEq + Eq + std::hash::Hash;
    /// Merged-source metadata at this resolution stage.
    type MergedSources: Clone + std::fmt::Debug + PartialEq + Eq + std::hash::Hash;
}

/// Requested, unresolved source selector metadata.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RequestedSourceStage;

impl SourceResolution for RequestedSourceStage {
    type SchemaFamily = SchemaFamilySelection;
    type Storage = StorageSchemaSelection;
    type Lens = LensSelection;
    type Overlay = OverlayRef;
    type DataBranch = BranchId;
    type MergedSources = ();
}

/// Resolved source selector metadata.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedSourceStage;

impl SourceResolution for ResolvedSourceStage {
    type SchemaFamily = BranchId;
    type Storage = Vec<ResolvedPartitionLens>;
    type Lens = ();
    type Overlay = ResolvedOverlay;
    type DataBranch = BranchId;
    type MergedSources = Vec<u8>;
}

/// Ordered own-pending overlays included in current reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OverlayStack<O> {
    /// Local overlays not yet visible at the selected durability tier, in
    /// application order. Open transactions are entries in the same sequence,
    /// not a second local-read path.
    pub(crate) entries: Vec<O>,
}

impl<O> Default for OverlayStack<O> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

/// Requested own-pending overlay included in current reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OverlayRef {
    /// Include one own-pending direct/local batch.
    DirectBatch(BatchId),
    /// Include one accepted transaction whose payload is local but not yet
    /// visible at the requested durability tier.
    AcceptedTransaction(TxId),
    /// Include one open mutable transaction.
    OpenTransaction(OpenTxId),
}

/// Resolved own-pending overlay identity used by shared maintained work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedOverlay {
    /// Requested overlay identity.
    pub(crate) overlay: OverlayRef,
    /// Canonical manifest/completeness fingerprint.
    pub(crate) manifest_fingerprint: Vec<u8>,
}

/// Canonical batch identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct BatchId(pub(crate) Vec<u8>);

/// Settled-frontier fact identity. Propagation, waiting, and retry behavior are
/// runtime policy outside the compiler; the lowered program only emits the fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CoverageFrontier {
    /// Source/path scope.
    pub(crate) scope: CoverageScope,
    /// Frontier that must be settled.
    pub(crate) frontier: FrontierRequirement,
}

/// Identity of one coverage scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CoverageScope {
    /// Whole row-set program.
    Program,
    /// Logical source.
    Source(SourceId),
    /// Include/join/relation path.
    Path(ProgramPathId),
}

/// Concrete settled-frontier requirement.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FrontierRequirement {
    /// No async frontier gate beyond synchronous local settle.
    None,
    /// Wait until this resolved frontier has been applied.
    Through(ResolvedFrontier),
}

/// Resolved ordered frontier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ResolvedFrontier {
    /// Durability tier that must settle.
    pub(crate) tier: DurabilityTier,
    /// Ordered stream whose through-position must be applied before the settled
    /// signal is consumable.
    pub(crate) stream: Option<String>,
    /// Concrete stream/frontier position.
    pub(crate) through: FrontierPosition,
}

/// Stream/frontier position required by a settled signal.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FrontierPosition {
    /// A concrete global sequence cut.
    GlobalSeq(GlobalSeq),
    /// A concrete snapshot frontier fingerprint.
    Snapshot(Vec<u8>),
    /// A concrete local/edge transaction frontier.
    Transaction(TxId),
    /// Opaque ordered stream position for transports that do not expose Jazz
    /// sequence ids directly.
    StreamPosition(Vec<u8>),
}
/// One resolved stored-schema partition plus its lens path into read schema.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedPartitionLens {
    /// Concrete stored schema partition.
    pub(crate) storage_schema: SchemaVersionId,
    /// Canonical fingerprint of applied lens/projection path.
    pub(crate) lens_path_fingerprint: Vec<u8>,
}
