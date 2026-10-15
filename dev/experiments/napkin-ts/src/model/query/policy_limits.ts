/** Maximum recursive policy-predicate nodes on one root-to-leaf path. */
export const MAX_POLICY_EXPRESSION_DEPTH = 256;
/** Maximum nodes in one recursively encoded policy expression. */
export const MAX_POLICY_EXPRESSION_NODES = 16_384;

export class PolicyExpressionLimitError extends Error {
  constructor(
    readonly limit: string,
    readonly max: number,
    readonly actual: number,
  ) {
    super(`${limit} exceeded: observed ${actual}, maximum ${max}`);
  }
}

export class PolicyExpressionBudget {
  private nodes = 0;

  /** Counts one node at `depth` (the root is depth 1). */
  enter(depth: number): void {
    if (depth > MAX_POLICY_EXPRESSION_DEPTH)
      throw new PolicyExpressionLimitError(
        "MAX_POLICY_EXPRESSION_DEPTH",
        MAX_POLICY_EXPRESSION_DEPTH,
        depth,
      );
    if (this.nodes >= MAX_POLICY_EXPRESSION_NODES)
      throw new PolicyExpressionLimitError(
        "MAX_POLICY_EXPRESSION_NODES",
        MAX_POLICY_EXPRESSION_NODES,
        this.nodes + 1,
      );
    this.nodes++;
  }

  /** Refuses a declared child count the remaining node budget cannot hold. */
  reserve(children: number): void {
    if (children > MAX_POLICY_EXPRESSION_NODES - this.nodes)
      throw new PolicyExpressionLimitError(
        "MAX_POLICY_EXPRESSION_NODES",
        MAX_POLICY_EXPRESSION_NODES,
        this.nodes + children,
      );
  }
}
