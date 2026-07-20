//! Shared rung-3 text merge strategy interface and test harness.
//!
//! Strategies are deliberately outside schema lowering, storage encoding, and
//! sync protocol details. The merge creation path gives them materialized text
//! and op history; they return plaintext ops against the supplied base.

use crate::ids::SchemaVersionId;
use crate::schema::TextMergeSpec;
use crate::text_merge::{TextMergeError, TextOp};
use crate::tx::TxId;

/// A 32-byte hash of the declared column merge spec in force.
pub type ColumnSpecHash = [u8; 32];

/// One side of a two-head text merge presented to a rung-3 strategy.
#[derive(Clone, Debug)]
pub struct MergeSide {
    /// Head transaction for this side.
    pub head: TxId,
    /// Materialized document at the side's head.
    pub materialized: Vec<u8>,
    /// Plain-text ops from the selected base to this head, in causal order.
    pub ops: Vec<(TxId, TextOp)>,
}

/// Strategy input for a single declared text column.
#[derive(Clone, Debug)]
pub struct MergeStrategyInput {
    /// Schema version containing the column declaration.
    pub schema_version: SchemaVersionId,
    /// Table name for diagnostics and deterministic strategy choices.
    pub table: String,
    /// Column name for diagnostics and deterministic strategy choices.
    pub column: String,
    /// Declared column merge spec.
    pub spec: TextMergeSpec,
    /// Hash of [`Self::spec`] as recorded on the merge transaction.
    pub spec_hash: ColumnSpecHash,
    /// Materialized common base document.
    pub base: Vec<u8>,
    /// Lower TxId side after deterministic ordering.
    pub left: MergeSide,
    /// Higher TxId side after deterministic ordering.
    pub right: MergeSide,
}

/// Strategy input for write-time canonicalization of a whole authored value.
#[derive(Clone, Debug)]
pub struct CanonicalizeInput {
    /// Schema version containing the column declaration.
    pub schema_version: SchemaVersionId,
    /// Table name for diagnostics and deterministic strategy choices.
    pub table: String,
    /// Column name for diagnostics and deterministic strategy choices.
    pub column: String,
    /// Declared column merge spec.
    pub spec: TextMergeSpec,
    /// Hash of [`Self::spec`] in force for this column.
    pub spec_hash: ColumnSpecHash,
}

/// Rung-3 strategy output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeStrategyOutput {
    /// Plain-text op to apply to `input.base`.
    pub op_against_base: TextOp,
    /// Stable strategy id that produced the op.
    pub strategy_id: String,
    /// Strategy implementation version that produced the op.
    pub strategy_version: u32,
}

/// Deterministic rung-3 strategy contract for declared text formats.
///
/// Implementations must be deterministic: the same input, including the same
/// side ordering/tie-breaks, must produce the same output. Strategy failures
/// are treated by merge creation as degradation events; the builtin char-walk
/// merge remains the convergence fallback. Merges with more than two text heads
/// currently bypass rung 3 and fall back to rung 2; general N-head strategy
/// input is a named staging limitation.
pub trait MergeStrategy: Send + Sync {
    /// Stable strategy id.
    fn id(&self) -> &str;

    /// Strategy implementation version.
    fn version(&self) -> u32;

    /// Conservative structural-proximity hook for deciding whether rung 3 runs.
    ///
    /// Returning `false` keeps the rung-2 char-walk result. Returning `true`
    /// allows the strategy to produce a replacement op.
    fn structural_proximity(&self, _input: &MergeStrategyInput) -> bool {
        false
    }

    /// Canonicalize a whole authored value before op encoding and storage.
    ///
    /// Returning `Ok(None)` leaves bytes exactly as authored; this is also what
    /// happens when a node lacks a registered strategy for the declared spec.
    /// Returning `Err` rejects the local authoring write loudly. This hook is
    /// intentionally whole-value only: op-stream edits on format-declared
    /// columns are rejected by the write path until op-preserving
    /// canonicalization has a reviewed design.
    fn canonicalize(
        &self,
        _authored: &[u8],
        _input: &CanonicalizeInput,
    ) -> Result<Option<Vec<u8>>, TextMergeError> {
        Ok(None)
    }

    /// Merge two concurrent sides and return an op against `input.base`.
    fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError>;
}

/// Apply and validate a strategy output against an input base.
pub fn materialize_strategy_output(
    input: &MergeStrategyInput,
    output: &MergeStrategyOutput,
) -> Result<Vec<u8>, TextMergeError> {
    output.op_against_base.apply(&input.base)
}

#[cfg(test)]
/// Test support for strategy intention cases and toy implementations.
pub mod testing {
    use super::*;
    use crate::ids::NodeUuid;
    use crate::text_merge::diff;
    use crate::time::TxTime;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Stable schema version used by strategy intention tests.
    pub fn intention_schema_version() -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::from_u128(1))
    }

    /// Stable TxId constructor used by strategy intention tests.
    pub fn intention_tx(time: u64, node: u128) -> TxId {
        TxId::new(TxTime(time), NodeUuid(uuid::Uuid::from_u128(node)))
    }

    /// Expected result for a strategy intention test.
    pub enum Expected {
        /// Exact materialized document.
        Exact(Vec<u8>),
        /// Caller-supplied property over the materialized document.
        Property(Box<dyn Fn(&[u8]) -> bool>),
    }

    /// Strategy-side intention test case shape.
    pub struct IntentionCase {
        /// Common base document.
        pub base: Vec<u8>,
        /// Materialized side A document.
        pub side_a: Vec<u8>,
        /// Materialized side B document.
        pub side_b: Vec<u8>,
        /// Declared column spec supplied to the strategy.
        pub spec: TextMergeSpec,
        /// Expected materialized output.
        pub expected: Expected,
    }

    /// Run a strategy intention case against any registered implementation.
    pub fn run_intention_case<S: MergeStrategy>(
        strategy: &S,
        case: IntentionCase,
        schema_version: SchemaVersionId,
        left_head: TxId,
        right_head: TxId,
    ) -> Result<(), TextMergeError> {
        let input = MergeStrategyInput {
            schema_version,
            table: "intention".to_owned(),
            column: "body".to_owned(),
            spec_hash: case.spec.spec_hash(),
            spec: case.spec.clone(),
            base: case.base.clone(),
            left: MergeSide {
                head: left_head,
                materialized: case.side_a.clone(),
                ops: vec![(left_head, diff(&case.base, &case.side_a))],
            },
            right: MergeSide {
                head: right_head,
                materialized: case.side_b.clone(),
                ops: vec![(right_head, diff(&case.base, &case.side_b))],
            },
        };
        let output = strategy.merge(&input)?;
        let materialized = materialize_strategy_output(&input, &output)?;
        match case.expected {
            Expected::Exact(expected) => assert_eq!(materialized, expected),
            Expected::Property(property) => assert!(property(&materialized)),
        }
        Ok(())
    }

    /// Run an exact string-output intention case with the standard test clock.
    pub fn run_exact_intention_case<S: MergeStrategy>(
        strategy: &S,
        base: &str,
        left: &str,
        right: &str,
        spec: TextMergeSpec,
        expected: &str,
    ) -> Result<(), TextMergeError> {
        run_intention_case(
            strategy,
            IntentionCase {
                base: base.as_bytes().to_vec(),
                side_a: left.as_bytes().to_vec(),
                side_b: right.as_bytes().to_vec(),
                spec,
                expected: Expected::Exact(expected.as_bytes().to_vec()),
            },
            intention_schema_version(),
            intention_tx(1, 1),
            intention_tx(2, 2),
        )
    }

    /// Test-only strategy that chooses the longer side, with TxId tie-break.
    pub struct PreferLongerStrategy {
        invocations: Arc<AtomicUsize>,
    }

    impl PreferLongerStrategy {
        /// Construct a test strategy and share its invocation counter.
        pub fn new(invocations: Arc<AtomicUsize>) -> Self {
            Self { invocations }
        }
    }

    impl MergeStrategy for PreferLongerStrategy {
        fn id(&self) -> &str {
            "test.prefer-longer"
        }

        fn version(&self) -> u32 {
            1
        }

        fn structural_proximity(&self, input: &MergeStrategyInput) -> bool {
            input.spec.config == b"trigger"
        }

        fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            let chosen = if input.left.materialized.len() > input.right.materialized.len() {
                &input.left
            } else if input.right.materialized.len() > input.left.materialized.len() {
                &input.right
            } else if input.left.head <= input.right.head {
                &input.left
            } else {
                &input.right
            };
            Ok(MergeStrategyOutput {
                op_against_base: diff(&input.base, &chosen.materialized),
                strategy_id: self.id().to_owned(),
                strategy_version: self.version(),
            })
        }
    }

    /// Test-only strategy that always errors after its trigger fires.
    pub struct FailingStrategy {
        invocations: Arc<AtomicUsize>,
    }

    impl FailingStrategy {
        /// Construct a failing strategy and share its invocation counter.
        pub fn new(invocations: Arc<AtomicUsize>) -> Self {
            Self { invocations }
        }
    }

    impl MergeStrategy for FailingStrategy {
        fn id(&self) -> &str {
            "test.failing"
        }

        fn version(&self) -> u32 {
            1
        }

        fn structural_proximity(&self, input: &MergeStrategyInput) -> bool {
            input.spec.config == b"trigger"
        }

        fn merge(
            &self,
            _input: &MergeStrategyInput,
        ) -> Result<MergeStrategyOutput, TextMergeError> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            Err(TextMergeError::OperationConsumesPastEnd)
        }
    }

    /// Test-only strategy that returns metadata for a different strategy id.
    pub struct MismatchedIdStrategy {
        invocations: Arc<AtomicUsize>,
    }

    impl MismatchedIdStrategy {
        /// Construct a mismatched-id strategy and share its invocation counter.
        pub fn new(invocations: Arc<AtomicUsize>) -> Self {
            Self { invocations }
        }
    }

    impl MergeStrategy for MismatchedIdStrategy {
        fn id(&self) -> &str {
            "test.mismatched"
        }

        fn version(&self) -> u32 {
            1
        }

        fn structural_proximity(&self, input: &MergeStrategyInput) -> bool {
            input.spec.config == b"trigger"
        }

        fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            Ok(MergeStrategyOutput {
                op_against_base: diff(&input.base, &input.left.materialized),
                strategy_id: "test.other".to_owned(),
                strategy_version: self.version(),
            })
        }
    }

    /// Test-only strategy that uppercases whole-value writes.
    pub struct UppercaseCanonicalStrategy;

    impl MergeStrategy for UppercaseCanonicalStrategy {
        fn id(&self) -> &str {
            "test.uppercase"
        }

        fn version(&self) -> u32 {
            1
        }

        fn canonicalize(
            &self,
            authored: &[u8],
            _input: &CanonicalizeInput,
        ) -> Result<Option<Vec<u8>>, TextMergeError> {
            Ok(Some(
                authored
                    .iter()
                    .map(|byte| byte.to_ascii_uppercase())
                    .collect(),
            ))
        }

        fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError> {
            Ok(MergeStrategyOutput {
                op_against_base: diff(&input.base, &input.left.materialized),
                strategy_id: self.id().to_owned(),
                strategy_version: self.version(),
            })
        }
    }

    /// Test-only strategy that rejects whole-value writes.
    pub struct RejectingCanonicalStrategy;

    impl MergeStrategy for RejectingCanonicalStrategy {
        fn id(&self) -> &str {
            "test.reject-canonical"
        }

        fn version(&self) -> u32 {
            1
        }

        fn canonicalize(
            &self,
            _authored: &[u8],
            _input: &CanonicalizeInput,
        ) -> Result<Option<Vec<u8>>, TextMergeError> {
            Err(TextMergeError::StrategyInputInvalid)
        }

        fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError> {
            Ok(MergeStrategyOutput {
                op_against_base: diff(&input.base, &input.left.materialized),
                strategy_id: self.id().to_owned(),
                strategy_version: self.version(),
            })
        }
    }

    #[test]
    fn intention_case_runs_against_any_strategy() {
        let calls = Arc::new(AtomicUsize::new(0));
        let strategy = PreferLongerStrategy::new(calls.clone());
        let spec = TextMergeSpec::new("test.prefer-longer", 1, b"trigger".to_vec());
        run_intention_case(
            &strategy,
            IntentionCase {
                base: b"abc".to_vec(),
                side_a: b"abc-long".to_vec(),
                side_b: b"axc".to_vec(),
                spec,
                expected: Expected::Exact(b"abc-long".to_vec()),
            },
            intention_schema_version(),
            intention_tx(1, 1),
            intention_tx(2, 2),
        )
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn default_canonicalize_hook_is_noop() {
        let calls = Arc::new(AtomicUsize::new(0));
        let strategy = PreferLongerStrategy::new(calls);
        let spec = TextMergeSpec::new("test.prefer-longer", 1, b"trigger".to_vec());
        let input = CanonicalizeInput {
            schema_version: SchemaVersionId(uuid::Uuid::from_u128(1)),
            table: "intention".to_owned(),
            column: "body".to_owned(),
            spec_hash: spec.spec_hash(),
            spec,
        };

        assert_eq!(strategy.canonicalize(b"as-authored", &input).unwrap(), None);
    }
}
