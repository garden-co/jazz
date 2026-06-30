use super::*;

/// Parameter domains attached to one lowered graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParameterDomain {
    /// User-supplied binding parameters.
    pub(crate) user_params: BTreeMap<String, ColumnType>,
    /// Server-derived hidden parameters such as claims.
    pub(crate) hidden_params: BTreeMap<String, ColumnType>,
    /// Parameters retained in terminal rows for usage-site routing.
    pub(crate) routing_params: BTreeSet<String>,
}

/// Result of lowering one query program.
pub(crate) type QueryCompileResult = CapabilityResult<QueryProgram>;

/// Lower one Jazz query program into the unified Groove-backed program.
pub(crate) fn lower_query_program(request: QueryProgramRequest) -> QueryCompileResult {
    let read = format!("{:?}", request.reads);
    let policy = format!("{:?}", request.policy);
    let input = format!("{:?}", request.input);
    let output = format!("{:?}", request.output);

    Err(Box::new(CapabilityReport {
        gaps: vec![UnsupportedReason::Runtime(
            "query engine lowering is not implemented".to_owned(),
        )],
        explain: ExplainPlan {
            input,
            read: vec![read],
            policy: vec![policy],
            output: vec![output],
            capabilities: vec!["stubbed lowering".to_owned()],
            physical: Vec::new(),
        },
    }))
}

/// Runnable lowered query program.
#[derive(Clone, Debug)]
pub(crate) struct QueryProgram {
    /// Original request.
    pub(crate) request: QueryProgramRequest,
    /// Groove graph and its boundary contracts.
    pub(crate) lowered: LoweredGraph,
    /// Human-readable debugging and test artifact.
    pub(crate) explain: ExplainPlan,
}

/// Groove graph plus the semantic contracts needed to consume it.
#[derive(Clone, Debug)]
pub(crate) struct LoweredGraph {
    /// Executable groove graph.
    pub(crate) graph: GraphBuilder,
    /// Parameter domains expected by the graph.
    pub(crate) parameters: ParameterDomain,
    /// App row and fact schemas emitted by the graph.
    pub(crate) output: ProgramOutputSchemas,
}
