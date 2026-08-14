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
    current_version: s.ref(VERSION_TABLE),
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
    text: s.string().optional(),
    left: s.ref(NODE_TABLE).optional(),
    right: s.ref(NODE_TABLE).optional(),
    text_length: s.int(),
    height: s.int(),
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
  text: string | null;
  left: string | null;
  right: string | null;
  text_length: number;
  height: number;
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
  nodes: TextTable<
    TextNodeRow,
    Omit<TextNodeRow, "id" | "text" | "left" | "right"> & {
      text?: string | null;
      left?: string | null;
      right?: string | null;
    }
  >;
}

export interface TextAppTables {
  jazz_text_documents: TextTables["documents"];
  jazz_text_versions: TextTables["versions"];
  jazz_text_nodes: TextTables["nodes"];
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
  readonly documentId: string;
  readonly versionId: string;
  readonly text: string;
  readonly length: number;
  readonly baseRoot: string;
  readonly patchCount: number;
  readonly patchBytes: number;
}

type TextPatch = { at: number; text: string };
type RopeLeaf = { id: string; kind: "leaf"; text: string; length: number; height: number };
type RopeBranch = {
  id: string;
  kind: "branch";
  left: RopeNode;
  right: RopeNode;
  length: number;
  height: number;
};
type RopeNode = RopeLeaf | RopeBranch;

const encoder = new TextEncoder();
const snapshotPatches = new WeakMap<TextSnapshot, readonly TextPatch[]>();
const snapshotRoots = new WeakMap<TextSnapshot, RopeNode>();

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

function makeLeaf(text: string, created: Map<string, RopeNode>): RopeLeaf {
  const node: RopeLeaf = {
    id: crypto.randomUUID(),
    kind: "leaf",
    text,
    length: codePointLength(text),
    height: 1,
  };
  created.set(node.id, node);
  return node;
}

function makeBranch(left: RopeNode, right: RopeNode, created: Map<string, RopeNode>): RopeBranch {
  const node: RopeBranch = {
    id: crypto.randomUUID(),
    kind: "branch",
    left,
    right,
    length: left.length + right.length,
    height: Math.max(left.height, right.height) + 1,
  };
  created.set(node.id, node);
  return node;
}

function balance(left: RopeNode, right: RopeNode, created: Map<string, RopeNode>): RopeNode {
  if (left.height > right.height + 1 && left.kind === "branch") {
    if (left.left.height >= left.right.height) {
      return makeBranch(left.left, makeBranch(left.right, right, created), created);
    }
    if (left.right.kind === "branch") {
      return makeBranch(
        makeBranch(left.left, left.right.left, created),
        makeBranch(left.right.right, right, created),
        created,
      );
    }
  }
  if (right.height > left.height + 1 && right.kind === "branch") {
    if (right.right.height >= right.left.height) {
      return makeBranch(makeBranch(left, right.left, created), right.right, created);
    }
    if (right.left.kind === "branch") {
      return makeBranch(
        makeBranch(left, right.left.left, created),
        makeBranch(right.left.right, right.right, created),
        created,
      );
    }
  }
  return makeBranch(left, right, created);
}

function buildBalanced(nodes: RopeNode[], created: Map<string, RopeNode>): RopeNode {
  if (nodes.length === 1) return nodes[0]!;
  const middle = Math.floor(nodes.length / 2);
  return makeBranch(
    buildBalanced(nodes.slice(0, middle), created),
    buildBalanced(nodes.slice(middle), created),
    created,
  );
}

function buildRope(value: string, leafBytes: number): { root: RopeNode; nodes: RopeNode[] } {
  const created = new Map<string, RopeNode>();
  const leaves = splitUtf8(value, leafBytes).map((text) => makeLeaf(text, created));
  const root = buildBalanced(leaves, created);
  return { root, nodes: [...created.values()] };
}

function insertIntoRope(
  node: RopeNode,
  at: number,
  inserted: string,
  leafBytes: number,
  created: Map<string, RopeNode>,
): RopeNode {
  if (node.kind === "leaf") {
    const value = insertAtCodePoint(node.text, at, inserted);
    const leaves = splitUtf8(value, leafBytes).map((text) => makeLeaf(text, created));
    return buildBalanced(leaves, created);
  }
  if (at <= node.left.length) {
    return balance(
      insertIntoRope(node.left, at, inserted, leafBytes, created),
      node.right,
      created,
    );
  }
  return balance(
    node.left,
    insertIntoRope(node.right, at - node.left.length, inserted, leafBytes, created),
    created,
  );
}

function reachableCreatedNodes(root: RopeNode, created: Map<string, RopeNode>): RopeNode[] {
  const reachable: RopeNode[] = [];
  const visit = (node: RopeNode) => {
    if (!created.has(node.id)) return;
    reachable.push(node);
    if (node.kind === "branch") {
      visit(node.left);
      visit(node.right);
    }
  };
  visit(root);
  return reachable;
}

function nodeInsert(node: RopeNode, document: string): Omit<TextNodeRow, "id"> {
  return node.kind === "leaf"
    ? {
        document,
        kind: "leaf",
        text: node.text,
        left: null,
        right: null,
        text_length: node.length,
        height: node.height,
      }
    : {
        document,
        kind: "branch",
        text: null,
        left: node.left.id,
        right: node.right.id,
        text_length: node.length,
        height: node.height,
      };
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
        tx.insert(this.tables.nodes, nodeInsert(node, documentId), { id: node.id });
      }
      tx.insert(
        this.tables.versions,
        {
          document: documentId,
          base_root: rope.root.id,
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
    const root = await this.readNode(version.base_root, version.document);
    const text = applyPatches(this.materialize(root), patches);
    if (codePointLength(text) !== version.text_length) {
      throw new Error(`Text version ${versionId} length does not match its content`);
    }
    return this.snapshot(version.document, version.id, text, root, patches);
  }

  async insert(snapshot: TextSnapshot, at: number, inserted: string): Promise<TextSnapshot> {
    if (!inserted) return snapshot;
    const text = insertAtCodePoint(snapshot.text, at, inserted);
    let root = snapshot.baseRoot;
    let rootNode = snapshotRoots.get(snapshot);
    const retainedPatches = snapshotPatches.get(snapshot);
    if (!retainedPatches) {
      throw new Error("Text insert requires a snapshot returned by this module");
    }
    let patches = [...retainedPatches, { at, text: inserted }];
    const encoded = encodePatches(patches);
    const consolidate =
      patches.length > this.maxPatches || byteLength(encoded) > this.maxPatchBytes;
    let createdNodes: RopeNode[] = [];
    if (consolidate) {
      rootNode ??= await this.readNode(snapshot.baseRoot, snapshot.documentId);
      const created = new Map<string, RopeNode>();
      for (const patch of patches) {
        rootNode = insertIntoRope(rootNode, patch.at, patch.text, this.leafBytes, created);
      }
      createdNodes = reachableCreatedNodes(rootNode, created);
      // A scattered frontier can retain O(patches * depth) path-copied nodes.
      // Rebuilding a balanced root is cheaper once that exceeds the complete
      // leaf tree; localized typing still keeps the structurally shared path.
      const rebuilt = buildRope(text, this.leafBytes);
      if (rebuilt.nodes.length < createdNodes.length) {
        rootNode = rebuilt.root;
        createdNodes = rebuilt.nodes;
      }
      root = rootNode.id;
      patches = [];
    }
    const versionId = crypto.randomUUID();
    const result = await this.db.transaction((tx) => {
      for (const node of createdNodes) {
        tx.insert(this.tables.nodes, nodeInsert(node, snapshot.documentId), { id: node.id });
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
    return this.snapshot(snapshot.documentId, versionId, text, rootNode ?? root, patches);
  }

  private snapshot(
    documentId: string,
    versionId: string,
    text: string,
    root: RopeNode | string,
    patches: readonly TextPatch[],
  ): TextSnapshot {
    const baseRoot = typeof root === "string" ? root : root.id;
    const snapshot: TextSnapshot = {
      documentId,
      versionId,
      text,
      length: codePointLength(text),
      baseRoot,
      patchCount: patches.length,
      patchBytes: byteLength(encodePatches(patches)),
    };
    snapshotPatches.set(snapshot, [...patches]);
    if (typeof root !== "string") snapshotRoots.set(snapshot, root);
    return Object.freeze(snapshot);
  }

  private async readNode(
    id: string,
    expectedDocument: string,
    ancestors: ReadonlySet<string> = new Set(),
  ): Promise<RopeNode> {
    if (ancestors.has(id)) throw new Error(`Text rope contains a cycle at node ${id}`);
    const nextAncestors = new Set(ancestors).add(id);
    const node = await this.db.one(this.tables.nodes.where({ id }), {
      tier: this.durability,
    });
    if (!node) throw new Error(`Text rope node ${id} was not found`);
    if (node.document !== expectedDocument) {
      throw new Error(`Text rope node ${id} belongs to a different document`);
    }
    if (node.kind === "leaf") {
      if (typeof node.text !== "string") throw new Error(`Invalid text leaf ${id}`);
      if (node.text_length !== codePointLength(node.text) || node.height !== 1) {
        throw new Error(`Text leaf ${id} metadata does not match its content`);
      }
      return { id, kind: "leaf", text: node.text, length: node.text_length, height: node.height };
    }
    if (node.kind === "branch") {
      if (typeof node.left !== "string" || typeof node.right !== "string") {
        throw new Error(`Invalid text branch ${id}`);
      }
      const [left, right] = await Promise.all([
        this.readNode(node.left, expectedDocument, nextAncestors),
        this.readNode(node.right, expectedDocument, nextAncestors),
      ]);
      if (node.text_length !== left.length + right.length) {
        throw new Error(`Text branch ${id} length does not match its children`);
      }
      if (node.height !== Math.max(left.height, right.height) + 1) {
        throw new Error(`Text branch ${id} height does not match its children`);
      }
      if (Math.abs(left.height - right.height) > 1) {
        throw new Error(`Text branch ${id} is not balanced`);
      }
      return { id, kind: "branch", left, right, length: node.text_length, height: node.height };
    }
    throw new Error(`Unknown text rope node kind ${node.kind}`);
  }

  private materialize(node: RopeNode): string {
    return node.kind === "leaf"
      ? node.text
      : this.materialize(node.left) + this.materialize(node.right);
  }
}

export function createTextStore(db: Db, tables: TextTables, options?: TextStoreOptions): TextStore {
  return new TextStore(db, tables, options);
}

/** Select the text table handles from an app containing {@link textTableDefinitions}. */
export function textTablesFromApp(app: TextAppTables): TextTables {
  return {
    documents: app.jazz_text_documents,
    versions: app.jazz_text_versions,
    nodes: app.jazz_text_nodes,
  };
}
