import type { Node } from "prosemirror-model";
import {
  type EditorState,
  Plugin,
  PluginKey,
  TextSelection,
  type Transaction,
} from "prosemirror-state";
import { type MappedMarkdown, type MarkdownSpec, type Side, toMarkdown } from "./markdown.js";
import { changedMiddle, mapOffset } from "./text-map.js";

export interface MarkdownSyncOptions {
  /** Parses a stored value into a document of the editor's schema, for
   * example `defaultMarkdownParser.parse` from `prosemirror-markdown`. */
  parse(markdown: string): Node;
  /** Writers for nodes and marks beyond CommonMark's. */
  spec?: MarkdownSpec;
  /** Called with the document's markdown to write to the text column: after
   * each local change, or at most every {@link writeEvery} milliseconds. */
  onChange?(markdown: string): void;
  /** Writes at most this often while typing: the first change at once, then
   * the latest one each time the interval passes. Throttle here rather than
   * around `onChange`, so that a merged value arriving between writes is
   * merged with the edits not written yet instead of replacing them. */
  writeEvery?: number;
  /** How many values this editor wrote are remembered, so that their echoes
   * are not applied as changes. */
  rememberWrites?: number;
}

interface SyncState {
  remote: boolean;
}

interface SyncConfig {
  options: MarkdownSyncOptions;
  written: string[];
  /** The latest value both this editor and the text column have: the last
   * one written or applied. Local edits since are not written yet. */
  base: string | null;
}

const key = new PluginKey<SyncState>("jazzMarkdownSync");
const configs = new WeakMap<Plugin, SyncConfig>();
const mappedDocs = new WeakMap<Node, MappedMarkdown>();

/**
 * Keeps a ProseMirror editor and a markdown text column in step, with no
 * collaboration logic in the editor: local changes are written as whole
 * values, and the authority's merged values are applied with
 * {@link applyMarkdown} as the smallest change, keeping the cursor where its
 * text went.
 */
export function markdownSync(options: MarkdownSyncOptions): Plugin<SyncState> {
  const config: SyncConfig = { options, written: [], base: null };
  const plugin: Plugin<SyncState> = new Plugin<SyncState>({
    key,
    state: {
      init: () => ({ remote: false }),
      apply: (tr, value) => (tr.docChanged ? { remote: tr.getMeta(key)?.remote === true } : value),
    },
    view: (initial) => {
      config.base ??= markdownOf(initial.state).text;
      let view = initial;
      let timer: ReturnType<typeof setTimeout> | null = null;
      let dirty = false;
      const write = () => {
        dirty = false;
        const markdown = ownChanges(config, markdownOf(view.state).text);
        if (markdown === config.base) return;
        remember(config, markdown);
        options.onChange?.(markdown);
      };
      const tick = () => {
        timer = null;
        if (!dirty) return;
        write();
        timer = setTimeout(tick, options.writeEvery);
      };
      return {
        update(next, previous) {
          view = next;
          if (next.state.doc === previous.doc || key.getState(next.state)?.remote) return;
          dirty = true;
          if (!options.writeEvery) return write();
          if (timer) return;
          write();
          timer = setTimeout(tick, options.writeEvery);
        },
        destroy() {
          if (timer) clearTimeout(timer);
          if (dirty) write();
        },
      };
    },
  });
  configs.set(plugin, config);
  return plugin;
}

/**
 * `markdown`, the document's canonical markdown, as a change to the last
 * value the column had: only the text this editor changed is rewritten.
 * Where that value is written differently than this editor would write it
 * (a trailing space someone is typing, another editor's markup), a write
 * leaves it as it is instead of undoing it for everyone.
 */
function ownChanges(config: SyncConfig, markdown: string): string {
  const base = config.base;
  if (base === null || base === markdown) return markdown;
  const canonical = toMarkdown(config.options.parse(base), config.options.spec).text;
  if (canonical === base || canonical === markdown) return canonical === base ? markdown : base;
  const { start, endBefore, endAfter } = changedMiddle(canonical, markdown);
  return (
    base.slice(0, mapOffset(canonical, base, start, "before")) +
    markdown.slice(start, endAfter) +
    base.slice(mapOffset(canonical, base, endBefore, "after"))
  );
}

function remember(config: SyncConfig, markdown: string) {
  const limit = config.options.rememberWrites ?? 64;
  const index = config.written.indexOf(markdown);
  if (index >= 0) config.written.splice(index, 1);
  config.written.push(markdown);
  if (config.written.length > limit) config.written.shift();
  config.base = markdown;
}

/** Whether `tr` applied a value read from the text column, rather than a
 * local edit. */
export function isRemoteChange(tr: Transaction): boolean {
  return tr.getMeta(key)?.remote === true;
}

/** The document's canonical markdown and its position map. */
export function markdownOf(state: EditorState): MappedMarkdown {
  let mapped = mappedDocs.get(state.doc);
  if (!mapped) {
    mapped = toMarkdown(state.doc, configOf(state).options.spec);
    mappedDocs.set(state.doc, mapped);
  }
  return mapped;
}

/** The markdown offset of a document position, for anchors and ranges. */
export function textPosition(state: EditorState, pos: number, side: Side = "after"): number {
  return markdownOf(state).toText(pos, side);
}

/** The document position of a markdown offset in the current value. */
export function docPosition(state: EditorState, offset: number, side: Side = "after"): number {
  return markdownOf(state).toDoc(offset, side);
}

function configOf(state: EditorState): SyncConfig {
  const plugin = key.get(state);
  const config = plugin && configs.get(plugin);
  if (!config) throw new Error("The editor state has no markdownSync plugin");
  return config;
}

/**
 * The transaction that brings the editor to `markdown`, a value read from
 * the text column, or null when it already shows it or it is one this
 * editor wrote. Only the top-level blocks the change reaches are parsed
 * into the editor, so what markdown cannot say elsewhere (such as trailing
 * spaces being typed) stays as it is. The transaction stays out of the
 * undo history.
 */
export function applyMarkdown(state: EditorState, markdown: string): Transaction | null {
  const config = configOf(state);
  const current = markdownOf(state);
  if (markdown === current.text || config.written.includes(markdown)) return null;
  const base = config.base ?? current.text;
  config.base = markdown;
  // Edits made here since the last write are not in the value: carry them
  // over onto it rather than letting the value replace them.
  if (base !== current.text) {
    const canonical = (value: string) =>
      toMarkdown(config.options.parse(value), config.options.spec).text;
    const ownBase = canonical(base);
    if (ownBase !== current.text) markdown = rebase(ownBase, current.text, canonical(markdown));
  }
  const doc = config.options.parse(markdown);
  const next = toMarkdown(doc, config.options.spec);
  mappedDocs.set(doc, next);
  if (next.text === current.text) return null;

  const old = state.doc;
  // Where the value changed, by its text: blocks outside that stay as they
  // are, even where they would not write back the same.
  const { start, endBefore, endAfter } = changedMiddle(current.text, markdown);
  const inNext = (offset: number, side: Side) =>
    next.toDoc(mapOffset(markdown, next.text, offset, side), side);
  // The top-level blocks the change reaches, the same count from each end
  // in both documents.
  const first = Math.min(
    blockIndex(old, current.toDoc(start, "before")),
    blockIndex(doc, inNext(start, "before")),
  );
  let fromEnd = Math.min(
    old.childCount - 1 - blockIndex(old, current.toDoc(endBefore, "after")),
    doc.childCount - 1 - blockIndex(doc, inNext(endAfter, "after")),
  );
  fromEnd = Math.max(0, Math.min(fromEnd, old.childCount - first, doc.childCount - first));
  const from = blockStart(old, first);
  const to = blockStart(old, old.childCount - fromEnd);
  const nextFrom = blockStart(doc, first);
  const nextTo = blockStart(doc, doc.childCount - fromEnd);

  // The smallest change inside those blocks.
  const before = old.slice(from, to).content;
  const after = doc.slice(nextFrom, nextTo).content;
  const diffStart = before.findDiffStart(after);
  if (diffStart === null) return null;
  let { a: diffEnd, b: nextDiffEnd } = before.findDiffEnd(after)!;
  const overlap = diffStart - Math.min(diffEnd, nextDiffEnd);
  if (overlap > 0) {
    diffEnd += overlap;
    nextDiffEnd += overlap;
  }
  const tr = state.tr.replace(
    from + diffStart,
    from + diffEnd,
    doc.slice(nextFrom + diffStart, nextFrom + nextDiffEnd),
  );

  // A selection inside the replaced text follows its text, through the
  // markdown both versions share.
  const { anchor, head } = state.selection;
  const touched = (pos: number) => pos >= from + diffStart && pos <= from + diffEnd;
  if (state.selection instanceof TextSelection && (touched(anchor) || touched(head))) {
    const follow = (pos: number, side: Side) => {
      const offset = mapOffset(current.text, markdown, current.toText(pos, side), side);
      // Positions in the new document shift by the blocks kept before.
      return Math.max(0, Math.min(tr.doc.content.size, inNext(offset, side) - nextFrom + from));
    };
    // A cursor moves past text inserted at it, as ProseMirror maps it; a
    // selection does not grow over text inserted at its edges.
    const sideOf = (pos: number): Side =>
      anchor === head || pos === Math.min(anchor, head) ? "after" : "before";
    const newAnchor = follow(anchor, sideOf(anchor));
    const newHead = anchor === head ? newAnchor : follow(head, sideOf(head));
    tr.setSelection(TextSelection.between(tr.doc.resolve(newAnchor), tr.doc.resolve(newHead)));
  }
  return tr.setMeta(key, { remote: true }).setMeta("addToHistory", false);
}

/**
 * `theirs` with the change from `base` to `ours` applied as well. Where both
 * changed the same text, `ours` is kept there: it is written next, and is the
 * later edit for the authority's merge too.
 */
function rebase(base: string, ours: string, theirs: string): string {
  const mine = changedMiddle(base, ours);
  const other = changedMiddle(base, theirs);
  const otherInsert = other.start === other.endBefore;
  const overlaps =
    (other.start < mine.endBefore && mine.start < other.endBefore) ||
    (otherInsert && mine.start === mine.endBefore && other.start === mine.start);
  if (overlaps) return ours;
  const inserted = theirs.slice(other.start, other.endAfter);
  if (other.endBefore <= mine.start) {
    return ours.slice(0, other.start) + inserted + ours.slice(other.endBefore);
  }
  const shift = mine.endAfter - mine.endBefore;
  return ours.slice(0, other.start + shift) + inserted + ours.slice(other.endBefore + shift);
}

/** The index of the top-level block holding `pos`. */
function blockIndex(doc: Node, pos: number): number {
  if (doc.childCount === 0) return 0;
  return Math.min(doc.resolve(Math.min(pos, doc.content.size)).index(0), doc.childCount - 1);
}

function blockStart(doc: Node, index: number): number {
  let pos = 0;
  for (let i = 0; i < index; i++) pos += doc.child(i).nodeSize;
  return pos;
}
