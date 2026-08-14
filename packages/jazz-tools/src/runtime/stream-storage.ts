import type { DurabilityTier } from "./client.js";
import { Db, type QueryBuilder, type QueryOptions, type TableProxy } from "./db.js";
import type { SubscriptionDelta } from "./subscription-manager.js";

export const DEFAULT_STREAM_INLINE_TAIL_BYTES = 64 * 1024;
export const DEFAULT_STREAM_TREE_FANOUT = 32;
export const MAX_STREAM_PART_BYTES = 1_048_576;

type WhereTable<Row, Init> = TableProxy<Row, Init> & {
  where(conditions: Record<string, unknown>): QueryBuilder<Row>;
};

export interface StreamRow {
  id: string;
  rootId: string;
  prefixBytes: number;
  inlineTail: Uint8Array;
}

export interface StreamNodeRow {
  id: string;
  childIds: string[];
  childLengths: number[];
  height: number;
}

export interface StreamPartRow {
  id: string;
  data: Uint8Array;
}

export interface ConventionalStreamApp<
  TStream extends StreamRow = StreamRow,
  TNode extends StreamNodeRow = StreamNodeRow,
  TPart extends StreamPartRow = StreamPartRow,
> {
  streams: WhereTable<TStream, Omit<TStream, "id">>;
  stream_nodes: WhereTable<TNode, Omit<TNode, "id">>;
  stream_parts: WhereTable<TPart, Omit<TPart, "id">>;
}

export interface StreamSnapshot {
  readonly streamId: string;
  readonly rootId: string;
  readonly prefixBytes: number;
  readonly inlineTail: Uint8Array;
  readonly length: number;
}

export interface CreateStreamOptions {
  tier?: DurabilityTier;
}

export interface AppendStreamOptions {
  /** Wait for global authority acceptance. Defaults to true. */
  waitForAuthority?: boolean;
}

export interface ReadStreamOptions extends QueryOptions {
  start?: number;
  end?: number;
}

export interface StreamStorageOptions {
  inlineTailBytes?: number;
  fanout?: number;
}

export class StreamNotFoundError extends Error {
  constructor(readonly streamId: string) {
    super(`Stream "${streamId}" was not found.`);
    this.name = "StreamNotFoundError";
  }
}

export class InvalidStreamDataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidStreamDataError";
  }
}

type TreeRef = { id: string; length: number; height: number };

export interface StreamStorage<TStream extends StreamRow = StreamRow> {
  create(options?: CreateStreamOptions): Promise<TStream>;
  snapshot(streamOrId: string | TStream, options?: QueryOptions): Promise<StreamSnapshot>;
  append(
    streamOrId: string | TStream,
    data: Uint8Array,
    options?: AppendStreamOptions,
  ): Promise<StreamSnapshot>;
  read(
    streamOrSnapshot: string | TStream | StreamSnapshot,
    options?: ReadStreamOptions,
  ): Promise<Uint8Array>;
  readRange(
    streamOrSnapshot: string | TStream | StreamSnapshot,
    start: number,
    end?: number,
    options?: QueryOptions,
  ): Promise<Uint8Array>;
  subscribe(
    streamId: string,
    callback: (snapshot: StreamSnapshot | null) => void,
    options?: QueryOptions,
  ): () => void;
}

function concat(
  chunks: Uint8Array[],
  length = chunks.reduce((sum, chunk) => sum + chunk.length, 0),
) {
  const actualLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  if (actualLength !== length) {
    throw new InvalidStreamDataError(
      `Stream extent metadata promised ${length} bytes, but ${actualLength} bytes were readable.`,
    );
  }
  const output = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.length;
  }
  return output;
}

function asSnapshot(stream: StreamRow): StreamSnapshot {
  const tail = stream.inlineTail.slice();
  return {
    streamId: stream.id,
    rootId: stream.rootId,
    prefixBytes: stream.prefixBytes,
    inlineTail: tail,
    length: stream.prefixBytes + tail.length,
  };
}

function validateOptions(options: StreamStorageOptions) {
  const inlineTailBytes = options.inlineTailBytes ?? DEFAULT_STREAM_INLINE_TAIL_BYTES;
  const fanout = options.fanout ?? DEFAULT_STREAM_TREE_FANOUT;
  if (
    !Number.isSafeInteger(inlineTailBytes) ||
    inlineTailBytes < 0 ||
    inlineTailBytes > MAX_STREAM_PART_BYTES
  ) {
    throw new RangeError(
      `inlineTailBytes must be an integer between 0 and ${MAX_STREAM_PART_BYTES}.`,
    );
  }
  if (!Number.isSafeInteger(fanout) || fanout < 4 || fanout > 256 || fanout % 2 !== 0) {
    throw new RangeError("fanout must be an even integer between 4 and 256.");
  }
  return { inlineTailBytes, fanout };
}

export function createConventionalStreamStorage<
  TStream extends StreamRow,
  TNode extends StreamNodeRow,
  TPart extends StreamPartRow,
>(
  db: Db,
  app: ConventionalStreamApp<TStream, TNode, TPart>,
  options: StreamStorageOptions = {},
): StreamStorage<TStream> {
  const { inlineTailBytes, fanout } = validateOptions(options);

  const loadStream = async (streamOrId: string | TStream, queryOptions?: QueryOptions) => {
    if (typeof streamOrId !== "string") return streamOrId;
    const stream = await db.one(app.streams.where({ id: streamOrId }), queryOptions);
    if (!stream) throw new StreamNotFoundError(streamOrId);
    return stream;
  };

  const readNode = async (id: string, queryOptions?: QueryOptions): Promise<TNode> => {
    const node = await db.one(app.stream_nodes.where({ id }), queryOptions);
    if (!node) throw new InvalidStreamDataError(`Stream tree node "${id}" is missing.`);
    if (node.childIds.length === 0 || node.childIds.length !== node.childLengths.length) {
      throw new InvalidStreamDataError(`Stream tree node "${id}" has invalid child metadata.`);
    }
    return node;
  };

  const readPart = async (id: string, expectedLength: number, queryOptions?: QueryOptions) => {
    const part = await db.one(app.stream_parts.where({ id }), queryOptions);
    if (!part) throw new InvalidStreamDataError(`Stream part "${id}" is missing.`);
    if (part.data.length !== expectedLength) {
      throw new InvalidStreamDataError(
        `Stream part "${id}" expected ${expectedLength} bytes, got ${part.data.length}.`,
      );
    }
    return part.data;
  };

  const readTreeRange = async (
    nodeId: string,
    start: number,
    end: number,
    queryOptions?: QueryOptions,
  ): Promise<Uint8Array[]> => {
    const node = await readNode(nodeId, queryOptions);
    const chunks: Uint8Array[] = [];
    let cursor = 0;
    for (let index = 0; index < node.childIds.length; index += 1) {
      const childLength = node.childLengths[index]!;
      const childEnd = cursor + childLength;
      if (childEnd > start && cursor < end) {
        const localStart = Math.max(0, start - cursor);
        const localEnd = Math.min(childLength, end - cursor);
        if (node.height === 0) {
          const part = await readPart(node.childIds[index]!, childLength, queryOptions);
          chunks.push(part.slice(localStart, localEnd));
        } else {
          chunks.push(
            ...(await readTreeRange(node.childIds[index]!, localStart, localEnd, queryOptions)),
          );
        }
      }
      cursor = childEnd;
      if (cursor >= end) break;
    }
    return chunks;
  };

  return {
    async create(createOptions = {}) {
      const result = db.insert(app.streams, {
        rootId: "",
        prefixBytes: 0,
        inlineTail: new Uint8Array(),
      } as Omit<TStream, "id">);
      return createOptions.tier ? result.wait({ tier: createOptions.tier }) : result.value;
    },

    async snapshot(streamOrId, queryOptions) {
      return asSnapshot(await loadStream(streamOrId, queryOptions));
    },

    async append(streamOrId, data, appendOptions = {}) {
      const streamId = typeof streamOrId === "string" ? streamOrId : streamOrId.id;
      const bytes = data.slice();
      if (bytes.length === 0) return this.snapshot(streamId);
      const result = await db.exclusiveTransaction(async (tx) => {
        const current = await tx.one(app.streams.where({ id: streamId }), {
          tier: "local",
          propagation: "local-only",
        });
        if (!current) throw new StreamNotFoundError(streamId);
        const combinedTail = concat([current.inlineTail, bytes]);
        if (combinedTail.length <= inlineTailBytes) {
          tx.update(app.streams, streamId, { inlineTail: combinedTail } as Partial<
            Omit<TStream, "id">
          >);
          return asSnapshot({ ...current, inlineTail: combinedTail });
        }

        const insertedNodes = new Map<
          string,
          TreeRef & { childIds: string[]; childLengths: number[] }
        >();
        const getNode = async (id: string) => {
          const inserted = insertedNodes.get(id);
          if (inserted) return inserted;
          const node = await tx.one(app.stream_nodes.where({ id }), {
            tier: "local",
            propagation: "local-only",
          });
          if (!node) throw new InvalidStreamDataError(`Stream tree node "${id}" is missing.`);
          return { ...node, length: node.childLengths.reduce((sum, length) => sum + length, 0) };
        };
        const insertNode = (children: TreeRef[], height: number) => {
          const row = tx.insert(app.stream_nodes, {
            childIds: children.map((child) => child.id),
            childLengths: children.map((child) => child.length),
            height,
          } as Omit<TNode, "id">);
          const node = {
            id: row.id,
            length: children.reduce((sum, child) => sum + child.length, 0),
            height,
            childIds: children.map((child) => child.id),
            childLengths: children.map((child) => child.length),
          };
          insertedNodes.set(node.id, node);
          return node;
        };
        const appendAt = async (node: Awaited<ReturnType<typeof getNode>>, leaf: TreeRef) => {
          if (node.height === 0) {
            const children = node.childIds.map((id, index) => ({
              id,
              length: node.childLengths[index]!,
              height: -1,
            }));
            children.push(leaf);
            if (children.length <= fanout) return [insertNode(children, 0)] as const;
            const split = children.length / 2;
            return [
              insertNode(children.slice(0, split), 0),
              insertNode(children.slice(split), 0),
            ] as const;
          }
          const last = await getNode(node.childIds.at(-1)!);
          const [replacement, split] = await appendAt(last, leaf);
          const children = node.childIds.slice(0, -1).map((id, index) => ({
            id,
            length: node.childLengths[index]!,
            height: node.height - 1,
          }));
          children.push(replacement);
          if (split) children.push(split);
          if (children.length <= fanout) return [insertNode(children, node.height)] as const;
          const splitAt = children.length / 2;
          return [
            insertNode(children.slice(0, splitAt), node.height),
            insertNode(children.slice(splitAt), node.height),
          ] as const;
        };
        const appendLeaf = async (root: TreeRef | null, leaf: TreeRef): Promise<TreeRef> => {
          if (!root) return insertNode([leaf], 0);
          const [replacement, split] = await appendAt(await getNode(root.id), leaf);
          return split ? insertNode([replacement, split], root.height + 1) : replacement;
        };

        let root: TreeRef | null = current.rootId
          ? {
              id: current.rootId,
              length: current.prefixBytes,
              height: (await getNode(current.rootId)).height,
            }
          : null;
        for (let offset = 0; offset < combinedTail.length; offset += MAX_STREAM_PART_BYTES) {
          const partData = combinedTail.slice(offset, offset + MAX_STREAM_PART_BYTES);
          const part = tx.insert(app.stream_parts, { data: partData } as Omit<TPart, "id">);
          root = await appendLeaf(root, { id: part.id, length: partData.length, height: -1 });
        }
        tx.update(app.streams, streamId, {
          rootId: root!.id,
          prefixBytes: current.prefixBytes + combinedTail.length,
          inlineTail: new Uint8Array(),
        } as Partial<Omit<TStream, "id">>);
        return {
          streamId,
          rootId: root!.id,
          prefixBytes: current.prefixBytes + combinedTail.length,
          inlineTail: new Uint8Array(),
          length: current.prefixBytes + combinedTail.length,
        } satisfies StreamSnapshot;
      });
      return appendOptions.waitForAuthority === false ? result.value : result.wait();
    },

    async read(streamOrSnapshot, readOptions = {}) {
      const { start = 0, end, ...queryOptions } = readOptions;
      return this.readRange(streamOrSnapshot, start, end, queryOptions);
    },

    async readRange(streamOrSnapshot, start, end, queryOptions) {
      const snapshot =
        typeof streamOrSnapshot === "object" && "streamId" in streamOrSnapshot
          ? streamOrSnapshot
          : await this.snapshot(streamOrSnapshot, queryOptions);
      const effectiveEnd = end ?? snapshot.length;
      if (
        !Number.isSafeInteger(start) ||
        !Number.isSafeInteger(effectiveEnd) ||
        start < 0 ||
        effectiveEnd < start ||
        effectiveEnd > snapshot.length
      ) {
        throw new RangeError(
          `Invalid stream range [${start}, ${effectiveEnd}) for ${snapshot.length} bytes.`,
        );
      }
      const chunks: Uint8Array[] = [];
      if (start < snapshot.prefixBytes && snapshot.rootId) {
        chunks.push(
          ...(await readTreeRange(
            snapshot.rootId,
            start,
            Math.min(effectiveEnd, snapshot.prefixBytes),
            queryOptions,
          )),
        );
      }
      if (effectiveEnd > snapshot.prefixBytes) {
        const tailStart = Math.max(0, start - snapshot.prefixBytes);
        const tailEnd = effectiveEnd - snapshot.prefixBytes;
        chunks.push(snapshot.inlineTail.slice(tailStart, tailEnd));
      }
      return concat(chunks, effectiveEnd - start);
    },

    subscribe(streamId, callback, queryOptions) {
      return db.subscribeAll(
        app.streams.where({ id: streamId }),
        (delta: SubscriptionDelta<TStream>) => {
          const stream = delta.all?.[0];
          callback(stream ? asSnapshot(stream) : null);
        },
        queryOptions,
      );
    },
  };
}
