// Lens types for schema migrations: declarative, auto-invertible row transforms.
import { concatBytes } from "../../../support/bytes.js";
import { uuidV5 } from "../../../support/hash.js";
import { objectIdFromUuid, type ObjectId } from "../../../_extern/model-core.js";
import { SchemaHash, NAMESPACE_DNS } from "../public_api/types/branch.js";
import type { ColumnType, TableSchema } from "../public_api/types/schema.js";
import type { Value } from "../public_api/types/value.js";

export type Direction = "Forward" | "Backward";
export const reverseDirection = (d: Direction): Direction =>
  d === "Forward" ? "Backward" : "Forward";

export type LensOp =
  | { kind: "RenameTable"; old_name: string; new_name: string }
  | { kind: "AddColumn"; table: string; column: string; column_type: ColumnType; default: Value }
  | { kind: "RemoveColumn"; table: string; column: string; column_type: ColumnType; default: Value }
  | { kind: "RenameColumn"; table: string; old_name: string; new_name: string }
  | { kind: "AddTable"; table: string; schema: TableSchema }
  | { kind: "RemoveTable"; table: string; schema: TableSchema };

export function invertLensOp(op: LensOp): LensOp {
  switch (op.kind) {
    case "RenameTable":
    case "RenameColumn":
      return { ...op, old_name: op.new_name, new_name: op.old_name };
    case "AddColumn":
      return { ...op, kind: "RemoveColumn" };
    case "RemoveColumn":
      return { ...op, kind: "AddColumn" };
    case "AddTable":
      return { ...op, kind: "RemoveTable" };
    case "RemoveTable":
      return { ...op, kind: "AddTable" };
  }
}

export class LensTransform {
  constructor(
    public ops: LensOp[] = [],
    public draft_ops: number[] = [],
  ) {}
  push(op: LensOp, isDraft: boolean): void {
    if (isDraft) this.draft_ops.push(this.ops.length);
    this.ops.push(op);
  }
  hasDrafts(): boolean {
    return this.draft_ops.length > 0;
  }
  invert(): LensTransform {
    return new LensTransform(
      this.ops.slice().reverse().map(invertLensOp),
      this.draft_ops.map((i) => this.ops.length - 1 - i),
    );
  }
}

export class Lens {
  constructor(
    readonly source_hash: SchemaHash,
    readonly target_hash: SchemaHash,
    readonly forward: LensTransform,
    readonly backward: LensTransform = forward.invert(),
  ) {}
  objectId(): ObjectId {
    return objectIdFromUuid(
      uuidV5(NAMESPACE_DNS, concatBytes(this.source_hash.bytes, this.target_hash.bytes)),
    );
  }
  isDraft(): boolean {
    return this.forward.hasDrafts() || this.backward.hasDrafts();
  }
  tableChanges(): [string[], string[]] {
    const added: string[] = [];
    const removed: string[] = [];
    for (const op of this.forward.ops) {
      if (op.kind === "AddTable") added.push(op.table);
      else if (op.kind === "RemoveTable") removed.push(op.table);
    }
    return [added, removed];
  }
}
