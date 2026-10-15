// One physical input frontier for one maintained authority scope.
import { SortedMap } from "../../support/collections.js";
import { type SupportingRow, cmpSupportingRow } from "../../_extern/node-views.js";

const anyPositive = (w: number[]) => w.some((x) => x > 0);

export class SupportingFrontier {
  private weights = new SortedMap<SupportingRow, number[]>(cmpSupportingRow);
  private unpublished: SortedMap<SupportingRow, boolean> | undefined;

  contains(row: SupportingRow): boolean {
    const w = this.weights.get(row);
    return w !== undefined && anyPositive(w);
  }

  apply(origin: number, row: SupportingRow, weight: number): boolean | undefined {
    const before = this.contains(row);
    if (this.unpublished && !this.unpublished.has(row)) this.unpublished.set(row, before);
    const weights = this.weights.getOrInsert(row, () => [0, 0, 0, 0]);
    weights[origin] += weight;
    const after = anyPositive(weights);
    if (weights.every((w) => w === 0)) this.weights.delete(row);
    return before !== after ? after : undefined;
  }

  *rows(): IterableIterator<SupportingRow> {
    for (const [row, w] of this.weights) if (anyPositive(w)) yield row;
  }

  *acknowledgedRows(): IterableIterator<SupportingRow> {
    for (const row of this.rows()) if (this.unpublished?.get(row) !== false) yield row;
    if (this.unpublished)
      for (const [row, before] of this.unpublished) if (before && !this.contains(row)) yield row;
  }

  delta(): [SupportingRow[], SupportingRow[]] | undefined {
    if (!this.unpublished) return undefined;
    const adds: SupportingRow[] = [];
    const removes: SupportingRow[] = [];
    for (const [row, before] of this.unpublished) {
      const now = this.contains(row);
      if (!before && now) adds.push(row);
      else if (before && !now) removes.push(row);
    }
    return [adds, removes];
  }

  acknowledge(): void {
    if (this.unpublished) this.unpublished.clear();
    else this.unpublished = new SortedMap(cmpSupportingRow);
  }

  forgetPredecessor(): void {
    this.unpublished = undefined;
  }

  retainedBytes(): number {
    const rowBytes = (row: SupportingRow) =>
      64 + (row.version.branchOrPrefix?.length ?? 0) + (row.version.rowDigest?.length ?? 0);
    let total = 0;
    for (const [row] of this.weights) total += rowBytes(row) + 32;
    if (this.unpublished) for (const [row] of this.unpublished) total += rowBytes(row) + 1;
    return total;
  }
}
