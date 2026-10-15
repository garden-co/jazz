import type { AuthorityResultKey } from "../../../_extern/node-qe-a.js";
import { authorityResultKeyEq } from "../../../_extern/node-qe-a.js";

/** Exact authority-closure generation tracking for a receiver-owned graph. */
export class LocalAuthorityReconciliation {
  private source: AuthorityResultKey | undefined;
  private generation = 0;

  isDue(source: AuthorityResultKey, generation: number): boolean {
    return !this.sameSource(source) || generation !== this.generation;
  }

  replaceSource(source: AuthorityResultKey, generation: number): void {
    if (this.sameSource(source)) {
      this.generation = Math.max(this.generation, generation);
      return;
    }
    this.source = source;
    this.generation = generation;
  }

  private sameSource(source: AuthorityResultKey): boolean {
    return this.source !== undefined && authorityResultKeyEq(this.source, source);
  }
}
