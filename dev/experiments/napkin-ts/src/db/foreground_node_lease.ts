import type { NodeUuid, TxTime } from "../_extern/db-core.js";
import { nodeUuidKey } from "../_extern/db-core.js";

export interface ForegroundNodeLease {
  node: NodeUuid;
  confirmedTxTime: TxTime;
}

export class ForegroundNodeLeaseError extends Error {
  constructor(readonly kind: "DuplicateNode" | "InactiveLease") {
    super(
      kind === "DuplicateNode"
        ? "foreground node identity is already known to this lease pool"
        : "foreground node lease is not active",
    );
  }
}

export class ForegroundNodeLeasePool {
  private reusable = new Map<string, ForegroundNodeLease>();
  private active = new Set<string>();
  private retired = new Set<string>();

  acquireReusable(): ForegroundNodeLease | undefined {
    const keys = [...this.reusable.keys()].sort();
    if (!keys.length) return undefined;
    const lease = this.reusable.get(keys[0])!;
    this.reusable.delete(keys[0]);
    this.active.add(keys[0]);
    return { ...lease };
  }

  acquireFresh(node: NodeUuid): ForegroundNodeLease {
    const k = nodeUuidKey(node);
    if (this.active.has(k) || this.reusable.has(k) || this.retired.has(k))
      throw new ForegroundNodeLeaseError("DuplicateNode");
    this.active.add(k);
    return { node, confirmedTxTime: 0 };
  }

  cleanHandoff(lease: ForegroundNodeLease): void {
    const k = nodeUuidKey(lease.node);
    if (!this.active.delete(k)) throw new ForegroundNodeLeaseError("InactiveLease");
    this.reusable.set(k, lease);
  }

  retire(lease: ForegroundNodeLease): void {
    const k = nodeUuidKey(lease.node);
    if (!this.active.delete(k)) throw new ForegroundNodeLeaseError("InactiveLease");
    this.retired.add(k);
  }
}
