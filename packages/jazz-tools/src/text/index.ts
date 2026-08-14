import { col } from "../dsl.js";
import type { DurabilityTier } from "../runtime/client.js";
import type { Db, QueryBuilder, TableProxy } from "../runtime/db.js";
import { defineTable } from "../typed-app.js";

const s = Object.assign({}, col, { table: defineTable });

const DOCUMENT_TABLE = "jazz_text_documents";
const VERSION_TABLE = "jazz_text_versions";
const NODE_TABLE = "jazz_text_nodes";

/** Ordinary table definitions required by {@link createTextStore}. */
export const textTableDefinitions = {
  [DOCUMENT_TABLE]: s.table({
    current_version: s.string(),
    text_length: s.int(),
  }),
  [VERSION_TABLE]: s.table({
    document: s.ref(DOCUMENT_TABLE),
    base_root: s.ref(NODE_TABLE),
    patches: s.string(),
    text_length: s.int(),
    previous_version: s.ref(VERSION_TABLE).optional(),
  }),
  [NODE_TABLE]: s.table({
    document: s.ref(DOCUMENT_TABLE),
    kind: s.string(),
    payload: s.string(),
    text_length: s.int(),
  }),
} as const;

export type TextDocumentRow = {
  id: string;
  current_version: string;
  text_length: number;
};

export type TextVersionRow = {
  id: string;
  document: string;
  base_root: string;
  patches: string;
  text_length: number;
  previous_version: string | null;
};

export type TextNodeRow = {
  id: string;
  document: string;
  kind: string;
  payload: string;
  text_length: number;
};

type TextTable<Row, Init> = TableProxy<Row, Init> & {
  where(input: unknown): QueryBuilder<Row>;
};

export interface TextTables {
  documents: TextTable<TextDocumentRow, Omit<TextDocumentRow, "id">>;
  versions: TextTable<
    TextVersionRow,
    Omit<TextVersionRow, "id" | "previous_version"> & { previous_version?: string | null }
  >;
  nodes: TextTable<TextNodeRow, Omit<TextNodeRow, "id">>;
}

export interface TextStoreOptions {
  /** Maximum number of inline inserts retained by one independently readable version. */
  maxPatches?: number;
  /** Maximum UTF-8 byte length of the encoded patch frontier. */
  maxPatchBytes?: number;
  /** Maximum UTF-8 byte length of a rope leaf. */
  leafBytes?: number;
  /** Durability awaited by create/insert. Defaults to local. */
  durability?: DurabilityTier;
}

export interface TextSnapshot {
  documentId: string;
  versionId: string;
  text: string;
  length: number;
  baseRoot: string;
  patchCount: number;
  patchBytes: number;
}

type TextPatch = { at: number; text: string };
type PendingNode = Omit<TextNodeRow, "document">;
type LeafPayload = { text: string };
type BranchPayload = { left: string; right: string };

const encoder = new TextEncoder();
const snapshotPatches = new WeakMap<TextSnapshot, readonly TextPatch[]>();

function byteLength(value: string): number {
  return encoder.encode(value).byteLength;
}

function codePointLength(value: string): number {
  return Array.from(value).length;
}

function insertAtCodePoint(value: string, at: number, inserted: string): string {
  if (!Number.isInteger(at)) throw new RangeError("Text insertion offset must be an integer");
  const points = Array.from(value);
  if (at < 0 || at > points.length) {
    throw new RangeError(`Text insertion offset ${at} is outside 0..${points.length}`);
  }
  points.splice(at, 0, inserted);
  return points.join("");
}

function encodePatches(patches: readonly TextPatch[]): string {
  return JSON.stringify(patches);
}

function decodePatches(encoded: string): TextPatch[] {
  const value: unknown = JSON.parse(encoded);
  if (!Array.isArray(value)) throw new Error("Invalid text patch frontier");
  return value.map((entry) => {
    if (
      typeof entry !== "object" ||
      entry === null ||
      !Number.isInteger((entry as TextPatch).at) ||
      typeof (entry as TextPatch).text !== "string"
    ) {
      throw new Error("Invalid text patch frontier entry");
    }
    return entry as TextPatch;
  });
}

function applyPatches(base: string, patches: readonly TextPatch[]): string {
  return patches.reduce((value, patch) => insertAtCodePoint(value, patch.at, patch.text), base);
}

function splitUtf8(value: string, maximumBytes: number): string[] {
  const leaves: string[] = [];
  let current = "";
  let currentBytes = 0;
  for (const point of value) {
    const pointBytes = byteLength(point);
    if (pointBytes > maximumBytes) {
      throw new Error("leafBytes cannot hold one Unicode scalar value");
    }
    if (current && currentBytes + pointBytes > maximumBytes) {
      leaves.push(current);
      current = "";
      currentBytes = 0;
    }
    current += point;
    currentBytes += pointBytes;
  }
  if (current || leaves.length === 0) leaves.push(current);
  return leaves;
}

function buildRope(value: string, leafBytes: number): { root: string; nodes: PendingNode[] } {
  const nodes: PendingNode[] = splitUtf8(value, leafBytes).map((text) => ({
    id: crypto.randomUUID(),
    kind: "leaf",
    payload: JSON.stringify({ text } satisfies LeafPayload),
    text_length: codePointLength(text),
  }));
  let level = nodes.map((node) => ({ id: node.id, length: node.text_length }));
  while (level.length > 1) {
    const next: typeof level = [];
    for (let index = 0; index < level.length; index += 2) {
      const left = level[index]!;
      const right = level[index + 1];
      if (!right) {
        next.push(left);
        continue;
      }
      const node: PendingNode = {
        id: crypto.randomUUID(),
        kind: "branch",
        payload: JSON.stringify({ left: left.id, right: right.id } satisfies BranchPayload),
        text_length: left.length + right.length,
      };
      nodes.push(node);
      next.push({ id: node.id, length: node.text_length });
    }
    level = next;
  }
  return { root: level[0]!.id, nodes };
}

export class TextStore {
  private readonly maxPatches: number;
  private readonly maxPatchBytes: number;
  private readonly leafBytes: number;
  private readonly durability: DurabilityTier;

  constructor(
    private readonly db: Db,
    private readonly tables: TextTables,
    options: TextStoreOptions = {},
  ) {
    this.maxPatches = options.maxPatches ?? 32;
    this.maxPatchBytes = options.maxPatchBytes ?? 4096;
    this.leafBytes = options.leafBytes ?? 4096;
    this.durability = options.durability ?? "local";
    if (!Number.isInteger(this.maxPatches) || this.maxPatches < 1) {
      throw new RangeError("maxPatches must be a positive integer");
    }
    if (!Number.isInteger(this.maxPatchBytes) || this.maxPatchBytes < 2) {
      throw new RangeError("maxPatchBytes must be an integer of at least 2");
    }
    if (!Number.isInteger(this.leafBytes) || this.leafBytes < 4) {
      throw new RangeError("leafBytes must be an integer of at least 4");
    }
  }

  async create(initialText = ""): Promise<TextSnapshot> {
    const documentId = crypto.randomUUID();
    const versionId = crypto.randomUUID();
    const rope = buildRope(initialText, this.leafBytes);
    const result = await this.db.transaction((tx) => {
      tx.insert(
        this.tables.documents,
        { current_version: versionId, text_length: codePointLength(initialText) },
        { id: documentId },
      );
      for (const node of rope.nodes) {
        tx.insert(this.tables.nodes, { ...node, document: documentId }, { id: node.id });
      }
      tx.insert(
        this.tables.versions,
        {
          document: documentId,
          base_root: rope.root,
          patches: "[]",
          text_length: codePointLength(initialText),
        },
        { id: versionId },
      );
    });
    await result.wait({ tier: this.durability });
    return this.snapshot(documentId, versionId, initialText, rope.root, []);
  }

  async read(documentId: string): Promise<TextSnapshot> {
    const document = await this.db.one(this.tables.documents.where({ id: documentId }), {
      tier: this.durability,
    });
    if (!document) throw new Error(`Text document ${documentId} was not found`);
    return this.readVersion(document.current_version);
  }

  async readVersion(versionId: string): Promise<TextSnapshot> {
    const version = await this.db.one(this.tables.versions.where({ id: versionId }), {
      tier: this.durability,
    });
    if (!version) throw new Error(`Text version ${versionId} was not found`);
    const patches = decodePatches(version.patches);
    const text = applyPatches(await this.readNode(version.base_root), patches);
    if (codePointLength(text) !== version.text_length) {
      throw new Error(`Text version ${versionId} length does not match its content`);
    }
    return this.snapshot(version.document, version.id, text, version.base_root, patches);
  }

  async insert(snapshot: TextSnapshot, at: number, inserted: string): Promise<TextSnapshot> {
    if (!inserted) return snapshot;
    const text = insertAtCodePoint(snapshot.text, at, inserted);
    let root = snapshot.baseRoot;
    const retainedPatches = snapshotPatches.get(snapshot) ?? (await this.loadPatches(snapshot));
    let patches = [...retainedPatches, { at, text: inserted }];
    const encoded = encodePatches(patches);
    const consolidate =
      patches.length > this.maxPatches || byteLength(encoded) > this.maxPatchBytes;
    const rope = consolidate ? buildRope(text, this.leafBytes) : null;
    if (rope) {
      root = rope.root;
      patches = [];
    }
    const versionId = crypto.randomUUID();
    const result = await this.db.transaction((tx) => {
      if (rope) {
        for (const node of rope.nodes) {
          tx.insert(this.tables.nodes, { ...node, document: snapshot.documentId }, { id: node.id });
        }
      }
      tx.insert(
        this.tables.versions,
        {
          document: snapshot.documentId,
          base_root: root,
          patches: encodePatches(patches),
          text_length: codePointLength(text),
          previous_version: snapshot.versionId,
        },
        { id: versionId },
      );
      tx.update(this.tables.documents, snapshot.documentId, {
        current_version: versionId,
        text_length: codePointLength(text),
      });
    });
    await result.wait({ tier: this.durability });
    return this.snapshot(snapshot.documentId, versionId, text, root, patches);
  }

  private snapshot(
    documentId: string,
    versionId: string,
    text: string,
    baseRoot: string,
    patches: readonly TextPatch[],
  ): TextSnapshot {
    const snapshot = {
      documentId,
      versionId,
      text,
      length: codePointLength(text),
      baseRoot,
      patchCount: patches.length,
      patchBytes: byteLength(encodePatches(patches)),
    };
    snapshotPatches.set(snapshot, [...patches]);
    return snapshot;
  }

  private async loadPatches(snapshot: TextSnapshot): Promise<TextPatch[]> {
    const version = await this.db.one(this.tables.versions.where({ id: snapshot.versionId }), {
      tier: this.durability,
    });
    if (!version) throw new Error(`Text version ${snapshot.versionId} was not found`);
    return decodePatches(version.patches);
  }

  private async readNode(id: string): Promise<string> {
    const node = await this.db.one(this.tables.nodes.where({ id }), {
      tier: this.durability,
    });
    if (!node) throw new Error(`Text rope node ${id} was not found`);
    if (node.kind === "leaf") {
      const payload = JSON.parse(node.payload) as LeafPayload;
      if (typeof payload.text !== "string") throw new Error(`Invalid text leaf ${id}`);
      return payload.text;
    }
    if (node.kind === "branch") {
      const payload = JSON.parse(node.payload) as BranchPayload;
      if (typeof payload.left !== "string" || typeof payload.right !== "string") {
        throw new Error(`Invalid text branch ${id}`);
      }
      const [left, right] = await Promise.all([
        this.readNode(payload.left),
        this.readNode(payload.right),
      ]);
      return left + right;
    }
    throw new Error(`Unknown text rope node kind ${node.kind}`);
  }
}

export function createTextStore(db: Db, tables: TextTables, options?: TextStoreOptions): TextStore {
  return new TextStore(db, tables, options);
}
