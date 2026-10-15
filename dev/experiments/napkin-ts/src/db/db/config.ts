import type { AuthorSubject, JazzSchema, NodeUuid, RowUuid } from "../../_extern/db-core.js";
import { RowUuidT, uuidV7Row } from "../../_extern/db-core.js";
import type { DbIdentity } from "../db.js";
export { ClientRelayScope } from "../../_extern/db-core.js";

export interface RowIdSource {
  nextRowId(): RowUuid;
}

export interface DbConfig<S> {
  schema: JazzSchema;
  storage: S;
  identity: DbIdentity;
  idSource?: RowIdSource;
}

export const dbConfig = <S>(schema: JazzSchema, storage: S, identity: DbIdentity): DbConfig<S> => ({
  schema,
  storage,
  identity,
});

export class ProductionRowIdSource implements RowIdSource {
  nextRowId(): RowUuid {
    return uuidV7Row();
  }
}

const MASK64 = (1n << 64n) - 1n;
const MASK48 = (1n << 48n) - 1n;

function splitmix64(cell: { state: bigint }): bigint {
  cell.state = (cell.state + 0x9e3779b97f4a7c15n) & MASK64;
  let z = cell.state;
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK64;
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK64;
  return z ^ (z >> 31n);
}

export class SeededRowIdSource implements RowIdSource {
  private millis: bigint;
  private cell: { state: bigint };
  constructor(seed: bigint) {
    this.millis = seed & MASK48;
    this.cell = { state: seed ^ 0x9e3779b97f4a7c15n };
  }
  nextRowId(): RowUuid {
    const millis = this.millis & MASK48;
    this.millis = (this.millis + 1n) & MASK64;
    const randA = splitmix64(this.cell) & 0x0fffn;
    const randB = splitmix64(this.cell) & ((1n << 62n) - 1n);
    const bytes = new Uint8Array(16);
    const view = new DataView(bytes.buffer);
    for (let i = 0; i < 6; i++) bytes[i] = Number((millis >> BigInt(8 * (5 - i))) & 0xffn);
    view.setUint16(6, 0x7000 | Number(randA));
    view.setBigUint64(8, 0x8000000000000000n | randB);
    return RowUuidT.fromBytes(bytes);
  }
}

export type { AuthorSubject, NodeUuid };
