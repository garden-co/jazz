import * as X from "../../_extern/db-core.js";
import { SortedSet, cmpNum } from "../../support/collections.js";
import type { SyncMessage, TxId } from "../../_extern/db-core.js";

export class ReadBarrierSender {
  private commitUnitsSent = 0;
  private barrierSent = 0;
  commitUnitSent(): void {
    this.commitUnitsSent++;
  }
  pendingBarrier(): SyncMessage | undefined {
    return this.commitUnitsSent > this.barrierSent
      ? { kind: "ReadBarrier", writesThrough: this.commitUnitsSent }
      : undefined;
  }
  barrierAccepted(): void {
    this.barrierSent = this.commitUnitsSent;
  }
}

export interface HeldRequest {
  message: SyncMessage;
  lease: X.BufferLease | undefined;
  receiptsValidated: boolean;
}

export class ReadBarrierReceiver {
  private received = 0;
  private unanswered = new SortedSet<number>(cmpNum);
  private unansweredByTx = new Map<string, number[]>();
  private answered = new Set<string>();
  private held: HeldRequest[] = [];
  private released: HeldRequest[] = [];

  commitUnitReceived(txId: TxId): void {
    this.received++;
    const k = X.txIdKey(txId);
    if (this.answered.has(k)) return;
    this.unanswered.add(this.received);
    let q = this.unansweredByTx.get(k);
    if (!q) this.unansweredByTx.set(k, (q = []));
    q.push(this.received);
  }

  answerSent(message: SyncMessage): boolean {
    if (message.kind !== "FateUpdate") return false;
    const k = X.txIdKey(message.txId);
    const settled = message.fate.kind === "Rejected" || message.durability === "Global";
    if (settled) this.answered.delete(k);
    else this.answered.add(k);
    const ordinals = this.unansweredByTx.get(k);
    if (!ordinals) return false;
    const ordinal = ordinals.shift();
    if (ordinal === undefined) throw new Error("no empty ordinal queues are kept");
    if (!ordinals.length) this.unansweredByTx.delete(k);
    this.unanswered.delete(ordinal);
    return this.releaseSatisfied();
  }

  private satisfied(writesThrough: number): boolean {
    const oldest = this.unanswered.first();
    return this.received >= writesThrough && (oldest === undefined || oldest > writesThrough);
  }

  admit(request: HeldRequest): HeldRequest | undefined {
    if (X.messageClass(request.message)[0] !== "Requests") return request;
    if (this.held.length) {
      this.held.push(request);
      return undefined;
    }
    if (request.message.kind === "ReadBarrier") {
      if (!this.satisfied(request.message.writesThrough)) this.held.push(request);
      return undefined;
    }
    return request;
  }

  private releaseSatisfied(): boolean {
    let releasedAny = false;
    while (this.held.length) {
      const front = this.held[0].message;
      if (front.kind !== "ReadBarrier" || !this.satisfied(front.writesThrough)) break;
      this.held.shift();
      while (this.held.length && this.held[0].message.kind !== "ReadBarrier") {
        this.released.push(this.held.shift()!);
        releasedAny = true;
      }
    }
    return releasedAny;
  }

  popReleased(): HeldRequest | undefined {
    return this.released.shift();
  }
  isHolding(): boolean {
    return this.held.length > 0;
  }
  hasReleased(): boolean {
    return this.released.length > 0;
  }
}
