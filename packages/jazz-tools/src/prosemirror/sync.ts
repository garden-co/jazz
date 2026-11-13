import type { Node } from "prosemirror-model";
import {
  type EditorState,
  Plugin,
  PluginKey,
  TextSelection,
  type Transaction,
} from "prosemirror-state";
import { type MappedMarkdown, type MarkdownSpec, type Side, toMarkdown } from "./markdown.js";
import { changedMiddle, mapOffset, mapOffsetExactly } from "./text-map.js";

export interface MarkdownSyncOptions {
  /** Parses a stored value into a document of the editor's schema, for
   * example `defaultMarkdownParser.parse` from `prosemirror-markdown`. */
  parse(markdown: string): Node;
  /** Writers for nodes and marks beyond CommonMark's. */
  spec?: MarkdownSpec;
  /** Called with the document's markdown after each local change: write it
   * to the text column right away, as an edit of `editedFrom`, the value
   * the editor showed or wrote last:
   *
   * ```ts
   * onChange: (body, { editedFrom }) =>
   *   db.update(app.notes, id, { body }, { editedFrom: { body: editedFrom } })
   * ```
   *
   * The column may hold a newer merged value than the editor shows yet (it
   * is applied on the next render); the write is then merged over it, where
   * a plain `{ body: text }` write would undo it. Every write must reach the
   * column before the next merged value is applied, so do not throttle or
   * debounce it. */
  onChange?(markdown: string, change: { editedFrom: string }): void;
}

interface SyncState {
  remote: boolean;
}

interface SyncConfig {
  readonly mappedDocs: WeakMap<Node, MappedMarkdown>;
  options: MarkdownSyncOptions;
  /** The column's value as this editor last wrote or applied it. A value
   * read from the column that equals it is that write's echo. */
  base: string | null;
}

const key = new PluginKey<SyncState>("jazzMarkdownSync");
const configs = new WeakMap<Plugin, SyncConfig>();

/**
 * Keeps a ProseMirror editor and a markdown text column in step, with no
 * collaboration logic in the editor: local changes are written as whole
 * values, and the authority's merged values are applied with
 * {@link applyMarkdown} as the smallest change, keeping the cursor where its
 * text went.
 */
export function markdownSync(options: MarkdownSyncOptions): Plugin<SyncState> {
  const config: SyncConfig = { options, base: null, mappedDocs: new WeakMap() };
  const plugin: Plugin<SyncState> = new Plugin<SyncState>({
    key,
    state: {
      init: () => ({ remote: false }),
      apply: (tr, value) => (tr.docChanged ? { remote: tr.getMeta(key)?.remote === true } : value),
    },
    view: (initial) => {
      config.base ??= markdownOf(initial.state).text;
      return {
        update(view, previous) {
          if (view.state.doc === previous.doc || key.getState(view.state)?.remote) return;
          const markdown = ownChanges(config, markdownOf(view.state).text);
          if (markdown === config.base) return;
          const editedFrom = config.base ?? markdown;
          config.base = markdown;
          options.onChange?.(markdown, { editedFrom });
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
 * leaves it as it is instead of undoing it for everyone. When the edit's
 * place in that value cannot be found exactly, the canonical markdown is
 * written whole: splicing at a guess would delete the text in between.
 */
function ownChanges(config: SyncConfig, markdown: string): string {
  const base = config.base;
  if (base === null || base === markdown) return markdown;
  const canonical = toMarkdown(config.options.parse(base), config.options.spec).text;
  if (canonical === base || canonical === markdown) return canonical === base ? markdown : base;
  const { start, endBefore, endAfter } = changedMiddle(canonical, markdown);
  const from = mapOffsetExactly(canonical, base, start, "before");
  const to = mapOffsetExactly(canonical, base, endBefore, "after");
  if (from === null || to === null || to < from) return markdown;
  return base.slice(0, from) + markdown.slice(start, endAfter) + base.slice(to);
}

/** Whether `tr` applied a value read from the text column, rather than a
 * local edit. */
export function isRemoteChange(tr: Transaction): boolean {
  return tr.getMeta(key)?.remote === true;
}

/** The document's canonical markdown and its position map. */
export function markdownOf(state: EditorState): MappedMarkdown {
  const { options, mappedDocs } = configOf(state);
  let mapped = mappedDocs.get(state.doc);
  if (!mapped) {
    mapped = toMarkdown(state.doc, options.spec);
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
 * the text column, or null when it already shows it. The local store shows
 * an editor's writes as soon as it makes them, so the only echo is of its
 * latest write; any other value is applied, even one equal to an earlier
 * write (someone deleted what this editor just typed). Only the top-level blocks the change reaches are parsed
 * into the editor, so what markdown cannot say elsewhere (such as trailing
 * spaces being typed) stays as it is. The transaction stays out of the
 * undo history.
 */
export function applyMarkdown(state: EditorState, markdown: string): Transaction | null {
  const config = configOf(state);
  const current = markdownOf(state);
  if (markdown === config.base || markdown === current.text) {
    config.base = markdown;
    return null;
  }
  config.base = markdown;
  const doc = config.options.parse(markdown);
  const next = toMarkdown(doc, config.options.spec);
  config.mappedDocs.set(doc, next);
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
