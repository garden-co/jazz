//! Semantic limits on recursively encoded policy expressions. Re-exported
//! from `crate::protocol_limits` with the other admission limits.

/// Maximum recursive policy-predicate nodes on one root-to-leaf path.
///
/// Policy predicates are decoded with a bounded seed before a complete
/// attacker-controlled tree exists. Keep this aligned with the executable
/// `MAX_ROW_SET_NESTING_DEPTH` planning ceiling: valid generated policy shapes
/// may reach that boundary before lowering.
pub const MAX_POLICY_EXPRESSION_DEPTH: usize = 256;

/// Maximum nodes in one recursively encoded policy expression.
///
/// This is independent of encoded bytes: compact `All`/`Any` children can
/// otherwise create large retained trees below the shape byte ceiling. Keep
/// this aligned with the established `MAX_CATALOGUE_COLLECTION_ITEMS` protocol
/// tier rather than introducing a lower policy-only cardinality class.
pub const MAX_POLICY_EXPRESSION_NODES: usize = 16_384;

/// A named semantic limit crossed while decoding a recursive policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolicyExpressionLimitError {
    /// Stable protocol-limit name.
    pub limit: &'static str,
    /// Configured inclusive boundary.
    pub max: usize,
    /// First observed value outside the boundary.
    pub actual: usize,
}

impl std::fmt::Display for PolicyExpressionLimitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} exceeded: observed {}, maximum {}",
            self.limit, self.actual, self.max
        )
    }
}

impl std::error::Error for PolicyExpressionLimitError {}
