import type { Bytes } from "../../../../support/bytes.js";
import type { ResultKey } from "../../../../_extern/model-core.js";
import type { RowProvenance } from "../../metadata.js";
import type { TransactionId } from "../../transaction.js";
import type { Value } from "./value.js";

export type QueryResultField = { name: string; value: Value };

export class QueryResult {
  constructor(
    readonly key: ResultKey,
    readonly fields: QueryResultField[],
  ) {}
  get(name: string): Value | undefined {
    return this.fields.find((f) => f.name === name)?.value;
  }
  intoValues(): Value[] {
    return this.fields.map((f) => f.value);
  }
}

/** A maintained output row with its result key, binary data, and transaction identity. */
export type Row = {
  id: ResultKey;
  data: Bytes;
  transaction_id: TransactionId;
  provenance: RowProvenance;
};

export const debugRowBytes = (b: Bytes) => `RowBytes { len: ${b.length} }`;

export type RowDelta = {
  added: Row[];
  removed: Row[];
  moved: ResultKey[];
  updated: [Row, Row][];
};
export const emptyRowDelta = (): RowDelta => ({ added: [], removed: [], moved: [], updated: [] });
export const rowDeltaIsEmpty = (d: RowDelta) =>
  !d.added.length && !d.removed.length && !d.moved.length && !d.updated.length;

export type OrderedAdded = { id: ResultKey; index: number; row: Row };
export type OrderedRemoved = { id: ResultKey; index: number };
export type OrderedUpdated = { id: ResultKey; old_index: number; new_index: number; row?: Row };
export type OrderedRowDelta = {
  added: OrderedAdded[];
  removed: OrderedRemoved[];
  updated: OrderedUpdated[];
  pending: boolean;
};
export const orderedRowDeltaIsEmpty = (d: OrderedRowDelta) =>
  !d.added.length && !d.removed.length && !d.updated.length;
