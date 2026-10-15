import type { CurrentRow, OutputOccurrenceId } from "../_extern/db-core.js";
import { occurrenceEq } from "../_extern/db-core.js";

export type ResultRelation =
  | { kind: "Array"; nodes: ResultNode[] }
  | { kind: "Null" }
  | { kind: "Hole" };

export interface ResultNode {
  occurrence: OutputOccurrenceId;
  row: CurrentRow;
  relations: Map<string, ResultRelation>;
}

export interface ResultTreeReplacement {
  occurrence: OutputOccurrenceId;
  parent: ResultNode;
}

export const emptyRelation = (): ResultRelation => ({ kind: "Array", nodes: [] });

export class ResultTree {
  roots: ResultNode[] = [];

  applyReset(reset: ResultTree): void {
    this.roots = reset.roots;
  }

  applyReplacement(replacement: ResultTreeReplacement): void {
    const i = this.roots.findIndex((n) => occurrenceEq(n.occurrence, replacement.occurrence));
    if (i >= 0) this.roots[i] = replacement.parent;
    else this.roots.push(replacement.parent);
  }
}
