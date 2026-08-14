/**
 * Conventional mutable files built from ordinary Jazz rows.
 *
 * The mutable `files` row names one immutable extent-tree root.  Leaf nodes
 * name bounded byte parts; interior nodes name two children.  No storage or
 * protocol feature knows that these rows collectively represent a file.
 */
import type { DurabilityTier } from "./client.js";
import { Db, type QueryBuilder, type QueryOptions, type TableProxy } from "./db.js";

export const MAX_FILE_PART_BYTES = 256 * 1024;
export const DEFAULT_FILE_INLINE_BYTES = 64 * 1024;
const MAX_FILE_NODES = 1_000_000;

type WhereTable<Row, Init> = TableProxy<Row, Init> & {
  where(c: Record<string, unknown>): QueryBuilder<Row>;
};
export interface FileRow {
  id: string;
  rootId: string;
  byteLength: number;
  inlineBytes: Uint8Array;
}
export interface FileNodeRow {
  id: string;
  childIds: string[];
  childLengths: number[];
  height: number;
}
export interface FilePartRow {
  id: string;
  data: Uint8Array;
}
export interface ConventionalFileApp<
  F extends FileRow = FileRow,
  N extends FileNodeRow = FileNodeRow,
  P extends FilePartRow = FilePartRow,
> {
  files: WhereTable<F, Omit<F, "id">>;
  file_nodes: WhereTable<N, Omit<N, "id">>;
  file_parts: WhereTable<P, Omit<P, "id">>;
}
export interface FileSnapshot {
  readonly fileId: string;
  readonly rootId: string;
  readonly byteLength: number;
  readonly inlineBytes: Uint8Array;
}
export interface FileStorageOptions {
  inlineBytes?: number;
  fanout?: number;
}
export interface WriteFileOptions {
  waitForAuthority?: boolean;
}
export interface CreateFileOptions {
  tier?: DurabilityTier;
}
export class FileNotFoundError extends Error {
  constructor(readonly fileId: string) {
    super(`File "${fileId}" was not found.`);
    this.name = "FileNotFoundError";
  }
}
export class InvalidFileDataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidFileDataError";
  }
}
type Ref = { id: string; length: number; height: number };

function checked(n: number, what: string) {
  if (!Number.isSafeInteger(n) || n < 0)
    throw new InvalidFileDataError(`${what} must be a non-negative safe integer.`);
}
function snapshot(file: FileRow): FileSnapshot {
  checked(file.byteLength, "File byteLength");
  if (!(file.inlineBytes instanceof Uint8Array))
    throw new InvalidFileDataError("File inlineBytes must be Uint8Array.");
  return {
    fileId: file.id,
    rootId: file.rootId,
    byteLength: file.byteLength,
    inlineBytes: file.inlineBytes.slice(),
  };
}
function join(chunks: Uint8Array[], length?: number) {
  const n = length ?? chunks.reduce((a, c) => a + c.length, 0);
  if (chunks.reduce((a, c) => a + c.length, 0) !== n)
    throw new InvalidFileDataError("File extent metadata does not match readable bytes.");
  const out = new Uint8Array(n);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

export interface FileStorage<F extends FileRow = FileRow> {
  create(options?: CreateFileOptions): Promise<F>;
  snapshot(file: string | F, options?: QueryOptions): Promise<FileSnapshot>;
  read(
    file: string | F | FileSnapshot,
    options?: QueryOptions & { start?: number; end?: number },
  ): Promise<Uint8Array>;
  readRange(
    file: string | F | FileSnapshot,
    start: number,
    end?: number,
    options?: QueryOptions,
  ): Promise<Uint8Array>;
  append(file: string | F, bytes: Uint8Array, options?: WriteFileOptions): Promise<FileSnapshot>;
  overwrite(
    file: string | F,
    start: number,
    bytes: Uint8Array,
    options?: WriteFileOptions,
  ): Promise<FileSnapshot>;
  insert(
    file: string | F,
    start: number,
    bytes: Uint8Array,
    options?: WriteFileOptions,
  ): Promise<FileSnapshot>;
}

export function createConventionalFileStorage<
  F extends FileRow,
  N extends FileNodeRow,
  P extends FilePartRow,
>(db: Db, app: ConventionalFileApp<F, N, P>, options: FileStorageOptions = {}): FileStorage<F> {
  const inlineLimit = options.inlineBytes ?? DEFAULT_FILE_INLINE_BYTES;
  const fanout = options.fanout ?? 2;
  if (!Number.isSafeInteger(inlineLimit) || inlineLimit < 0 || inlineLimit > MAX_FILE_PART_BYTES)
    throw new RangeError(`inlineBytes must be between 0 and ${MAX_FILE_PART_BYTES}.`);
  if (!Number.isSafeInteger(fanout) || fanout < 2 || fanout > 32)
    throw new RangeError("fanout must be between 2 and 32.");
  const load = async (file: string | F, q?: QueryOptions) => {
    if (typeof file !== "string") return file;
    const row = await db.one(app.files.where({ id: file }), q);
    if (!row) throw new FileNotFoundError(file);
    return row;
  };
  const validateNode = (id: string, node: N) => {
    if (
      !Number.isInteger(node.height) ||
      node.height < 0 ||
      node.childIds.length < 1 ||
      node.childIds.length > fanout ||
      node.childIds.length !== node.childLengths.length
    )
      throw new InvalidFileDataError(`File tree node "${id}" has invalid child metadata.`);
    for (const n of node.childLengths) checked(n, `File node "${id}" child length`);
    return node;
  };
  const tree = async (
    root: string,
    getNode: (id: string) => Promise<N>,
    getPart: (id: string) => Promise<Uint8Array>,
  ) => {
    const nodes = new Map<string, N>(),
      visiting = new Set<string>();
    let count = 0;
    const visit = async (id: string, expected?: number): Promise<number> => {
      if (visiting.has(id))
        throw new InvalidFileDataError(`File tree contains a cycle at node "${id}".`);
      if (++count > MAX_FILE_NODES)
        throw new InvalidFileDataError("File tree exceeds validation limit.");
      visiting.add(id);
      try {
        const n = validateNode(id, await getNode(id));
        nodes.set(id, n);
        if (expected !== undefined && n.height !== expected)
          throw new InvalidFileDataError(
            `File node "${id}" has height ${n.height}, expected ${expected}.`,
          );
        let total = 0;
        for (let i = 0; i < n.childIds.length; i++) {
          const actual =
            n.height === 0
              ? (await getPart(n.childIds[i]!)).length
              : await visit(n.childIds[i]!, n.height - 1);
          if (actual !== n.childLengths[i])
            throw new InvalidFileDataError(`File node "${id}" extent metadata is corrupt.`);
          total += actual;
          checked(total, "File tree length");
        }
        return total;
      } finally {
        visiting.delete(id);
      }
    };
    return { length: await visit(root), nodes };
  };
  const fromSnapshot = async (s: FileSnapshot, q?: QueryOptions) => {
    if (!s.rootId) {
      if (s.byteLength !== s.inlineBytes.length)
        throw new InvalidFileDataError("Rootless file length is corrupt.");
      return s.inlineBytes.slice();
    }
    const result = await tree(
      s.rootId,
      async (id) => {
        const n = await db.one(app.file_nodes.where({ id }), q);
        if (!n) throw new InvalidFileDataError(`File node "${id}" is missing.`);
        return n;
      },
      async (id) => {
        const p = await db.one(app.file_parts.where({ id }), q);
        if (!p) throw new InvalidFileDataError(`File part "${id}" is missing.`);
        return p.data;
      },
    );
    if (result.length + s.inlineBytes.length !== s.byteLength)
      throw new InvalidFileDataError("File root length is corrupt.");
    const leaves: Uint8Array[] = [];
    const walk = async (id: string) => {
      const n = result.nodes.get(id)!;
      for (const child of n.childIds) {
        if (n.height === 0) {
          const p = await db.one(app.file_parts.where({ id: child }), q);
          if (!p) throw new InvalidFileDataError(`File part "${child}" is missing.`);
          leaves.push(p.data);
        } else await walk(child);
      }
    };
    await walk(s.rootId);
    leaves.push(s.inlineBytes);
    return join(leaves, s.byteLength);
  };
  const mutate = async (file: string | F, change: (old: Uint8Array) => Uint8Array, wait = true) => {
    const id = typeof file === "string" ? file : file.id;
    const r = await db.exclusiveTransaction(async (tx) => {
      const current = await tx.one(app.files.where({ id }), {
        tier: "local",
        propagation: "local-only",
      });
      if (!current) throw new FileNotFoundError(id);
      const old = await fromSnapshot(snapshot(current), {
        tier: "local",
        propagation: "local-only",
      });
      const next = change(old);
      let rootId = "",
        inline = next;
      if (next.length > inlineLimit) {
        const refs: Ref[] = [];
        for (let at = 0; at < next.length; at += MAX_FILE_PART_BYTES) {
          const data = next.slice(at, at + MAX_FILE_PART_BYTES);
          const p = tx.insert(app.file_parts, { data } as Omit<P, "id">);
          refs.push({ id: p.id, length: data.length, height: -1 });
        }
        let level = refs;
        let height = 0;
        while (level.length > 1) {
          const following: Ref[] = [];
          for (let at = 0; at < level.length; at += fanout) {
            const children = level.slice(at, at + fanout);
            const n = tx.insert(app.file_nodes, {
              childIds: children.map((x) => x.id),
              childLengths: children.map((x) => x.length),
              height,
            } as Omit<N, "id">);
            following.push({
              id: n.id,
              length: children.reduce((a, x) => a + x.length, 0),
              height,
            });
          }
          level = following;
          height++;
        }
        const leaf = level[0]!;
        if (leaf.height === -1) {
          const n = tx.insert(app.file_nodes, {
            childIds: [leaf.id],
            childLengths: [leaf.length],
            height: 0,
          } as Omit<N, "id">);
          rootId = n.id;
        } else rootId = leaf.id;
        inline = new Uint8Array();
      }
      tx.update(app.files, id, { rootId, byteLength: next.length, inlineBytes: inline } as Partial<
        Omit<F, "id">
      >);
      return {
        fileId: id,
        rootId,
        byteLength: next.length,
        inlineBytes: inline,
      } satisfies FileSnapshot;
    });
    return wait ? r.wait() : r.value;
  };
  return {
    async create(o = {}) {
      const r = db.insert(app.files, {
        rootId: "",
        byteLength: 0,
        inlineBytes: new Uint8Array(),
      } as Omit<F, "id">);
      return o.tier ? r.wait({ tier: o.tier }) : r.value;
    },
    async snapshot(f, q) {
      return snapshot(await load(f, q));
    },
    async read(f, o = {}) {
      const { start = 0, end, ...q } = o;
      return this.readRange(f, start, end, q);
    },
    async readRange(f, start, end, q) {
      const s = typeof f === "object" && "fileId" in f ? f : await this.snapshot(f, q);
      const e = end ?? s.byteLength;
      if (
        !Number.isSafeInteger(start) ||
        !Number.isSafeInteger(e) ||
        start < 0 ||
        e < start ||
        e > s.byteLength
      )
        throw new RangeError(`Invalid file range [${start}, ${e}) for ${s.byteLength} bytes.`);
      return (await fromSnapshot(s, q)).slice(start, e);
    },
    async append(f, b, o = {}) {
      const copy = b.slice();
      return mutate(f, (x) => join([x, copy]), o.waitForAuthority !== false);
    },
    async overwrite(f, start, b, o = {}) {
      const copy = b.slice();
      return mutate(
        f,
        (x) => {
          checked(start, "Overwrite offset");
          if (start > x.length || start + copy.length > x.length)
            throw new RangeError("Overwrite range is outside file.");
          return join([x.slice(0, start), copy, x.slice(start + copy.length)]);
        },
        o.waitForAuthority !== false,
      );
    },
    async insert(f, start, b, o = {}) {
      const copy = b.slice();
      return mutate(
        f,
        (x) => {
          checked(start, "Insert offset");
          if (start > x.length) throw new RangeError("Insert offset is outside file.");
          return join([x.slice(0, start), copy, x.slice(start)]);
        },
        o.waitForAuthority !== false,
      );
    },
  };
}
