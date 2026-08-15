export type BrokerAuthOutcome = "authenticated" | "invalid" | "deferred";

export type BrokerAuthRefreshState = {
  generation: number;
  state: "pending" | "authenticated" | "invalid";
  requesterTabId: string;
  authJson: string;
  sessionClaims: Record<string, unknown>;
  dispatchedLeadershipId: number | null;
  confirmedLeadershipId: number | null;
  reason?: string;
};

/** Pure state machine for namespace-scoped browser auth refresh coordination. */
export class BrokerAuthRefreshController {
  private generation = 0;
  private current: BrokerAuthRefreshState | null = null;

  snapshot(): BrokerAuthRefreshState | null {
    return this.current;
  }

  request(
    requesterTabId: string,
    authJson: string,
    sessionClaims: Record<string, unknown>,
  ): BrokerAuthRefreshState {
    this.generation += 1;
    this.current = {
      generation: this.generation,
      state: "pending",
      requesterTabId,
      authJson,
      sessionClaims,
      dispatchedLeadershipId: null,
      confirmedLeadershipId: null,
    };
    return this.current;
  }

  /** Every carrier must independently apply and confirm retained credentials. */
  beginLeadership(leadershipId: number): BrokerAuthRefreshState | null {
    const current = this.current;
    if (!current) return null;
    if (
      current.confirmedLeadershipId === leadershipId ||
      current.dispatchedLeadershipId === leadershipId
    ) {
      return current;
    }
    if (
      current.state === "pending" &&
      current.dispatchedLeadershipId === null &&
      current.confirmedLeadershipId === null
    ) {
      return current;
    }
    this.current = {
      ...current,
      state: "pending",
      dispatchedLeadershipId: null,
      confirmedLeadershipId: null,
      reason: undefined,
    };
    return this.current;
  }

  dispatch(leadershipId: number): BrokerAuthRefreshState | null {
    const current = this.current;
    if (!current || current.state !== "pending") return null;
    if (current.dispatchedLeadershipId === leadershipId) return null;
    current.dispatchedLeadershipId = leadershipId;
    return current;
  }

  acceptResult(
    generation: number,
    leadershipId: number,
    outcome: BrokerAuthOutcome,
    reason?: string,
  ): { accepted: boolean; stateChanged: boolean; state: BrokerAuthRefreshState | null } {
    const current = this.current;
    // Terminal state is monotonic within a generation. This also rejects a
    // duplicate/error callback racing a successful callback from one carrier.
    if (
      !current ||
      current.generation !== generation ||
      current.state !== "pending" ||
      current.dispatchedLeadershipId !== leadershipId
    ) {
      return { accepted: false, stateChanged: false, state: current };
    }
    if (outcome === "deferred") {
      current.dispatchedLeadershipId = null;
      return { accepted: true, stateChanged: false, state: current };
    }
    current.state = outcome;
    current.confirmedLeadershipId = leadershipId;
    current.reason = reason;
    return { accepted: true, stateChanged: true, state: current };
  }
}
