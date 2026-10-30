/**
 * Overlays: annotations, comments and other people's selections drawn over
 * the document as decorations, never written into the markdown. Each one is
 * stored as a text anchor or range (in any table the app likes) and found
 * again in the editor's current value, so it follows its text through every
 * merge; one whose text is gone comes back orphaned for the app to show
 * elsewhere.
 */
import { Plugin, PluginKey, type EditorState, type Transaction } from "prosemirror-state";
import { Decoration, DecorationSet } from "prosemirror-view";
import type { TextAnchor, TextAnchors, TextRange } from "../runtime/text-anchors.js";
import { resolveAnchor, resolveRange } from "./anchors.js";
import { isRemoteChange } from "./sync.js";

/** Found with less confidence than this, an overlay is orphaned. */
export const DEFAULT_MIN_CONFIDENCE = 0.6;
/** Found with less confidence than this, an overlay drifted: its owner
 * should store it again, captured from the current value. */
export const DEFAULT_DRIFT_CONFIDENCE = 0.95;

export interface Overlay {
  /** Stable across updates, such as the annotation row's id. */
  id: string;
  /** A span to highlight, or a point to mark. Give one. */
  range?: TextRange;
  anchor?: TextAnchor;
  /** Attributes of the highlight over a range, such as `class`. */
  attrs?: Record<string, string>;
  /** A widget at the point, or at the range's end: a caret, a pin. */
  widget?: () => HTMLElement;
  /** Overrides the plugin's orphaning threshold for this overlay. */
  minConfidence?: number;
}

/** Where an overlay landed in the current document. */
export interface PlacedOverlay {
  overlay: Overlay;
  from: number;
  to: number;
  confidence: number;
  ambiguous: boolean;
  /** Not drawn: its text is gone, or another place matched as well. */
  orphaned: boolean;
  /** Found, but its stored text no longer matches well; store it again. */
  drifted: boolean;
}

export interface OverlayOptions {
  /** `db.textAnchors()`, or a function returning it once the db is ready. */
  anchors: TextAnchors | (() => TextAnchors);
  minConfidence?: number;
  driftConfidence?: number;
  /** Called after overlays are placed again, for sidebars and re-anchoring. */
  onPlaced?(placed: readonly PlacedOverlay[]): void;
}

interface OverlayState {
  overlays: readonly Overlay[];
  placed: readonly PlacedOverlay[];
  decorations: DecorationSet;
}

const key = new PluginKey<OverlayState>("jazzOverlays");
/** @internal For tests. */
export const overlaysKey = key;

/**
 * Draws overlays set with {@link setOverlays}. Local edits move the drawn
 * overlays with the text, as ProseMirror maps decorations; a new list, or
 * a merged value from {@link applyMarkdown}, places them again from their
 * stored anchors.
 */
export function overlays(options: OverlayOptions): Plugin<OverlayState> {
  const anchors = () =>
    typeof options.anchors === "function" ? options.anchors() : options.anchors;
  let placedChanged = false;
  return new Plugin<OverlayState>({
    key,
    state: {
      init: () => ({ overlays: [], placed: [], decorations: DecorationSet.empty }),
      apply(tr, value, _old, state) {
        const set = tr.getMeta(key) as readonly Overlay[] | undefined;
        const remote = isRemoteChange(tr);
        if (set === undefined && !remote) {
          if (!tr.docChanged) return value;
          return { ...value, decorations: value.decorations.map(tr.mapping, tr.doc) };
        }
        const list = set ?? value.overlays;
        const placed = list.map((overlay) => place(state, anchors(), overlay, options));
        placedChanged = true;
        return { overlays: list, placed, decorations: decorate(state, placed) };
      },
    },
    props: {
      decorations: (state) => key.getState(state)?.decorations,
    },
    view: () => ({
      update(view) {
        if (!placedChanged) return;
        placedChanged = false;
        options.onPlaced?.(key.getState(view.state)?.placed ?? []);
      },
    }),
  });
}

/** Replaces the overlays drawn over the document. */
export function setOverlays(tr: Transaction, list: readonly Overlay[]): Transaction {
  return tr.setMeta(key, list);
}

/** Where each overlay landed when last placed. */
export function placedOverlays(state: EditorState): readonly PlacedOverlay[] {
  return key.getState(state)?.placed ?? [];
}

function place(
  state: EditorState,
  anchors: TextAnchors,
  overlay: Overlay,
  options: OverlayOptions,
): PlacedOverlay {
  const min = overlay.minConfidence ?? options.minConfidence ?? DEFAULT_MIN_CONFIDENCE;
  const drift = options.driftConfidence ?? DEFAULT_DRIFT_CONFIDENCE;
  const found = overlay.range
    ? resolveRange(state, anchors, overlay.range)
    : overlay.anchor
      ? (() => {
          const point = resolveAnchor(state, anchors, overlay.anchor!);
          return { ...point, from: point.pos, to: point.pos };
        })()
      : null;
  if (!found) throw new Error(`Overlay ${overlay.id} needs a range or an anchor`);
  const orphaned = found.confidence < min || found.ambiguous;
  return {
    overlay,
    from: found.from,
    to: found.to,
    confidence: found.confidence,
    ambiguous: found.ambiguous,
    orphaned,
    drifted: !orphaned && found.confidence < drift,
  };
}

function decorate(state: EditorState, placed: readonly PlacedOverlay[]): DecorationSet {
  const decorations: Decoration[] = [];
  for (const { overlay, from, to, orphaned } of placed) {
    if (orphaned) continue;
    if (to > from && overlay.attrs) {
      decorations.push(Decoration.inline(from, to, overlay.attrs, { id: overlay.id }));
    }
    if (overlay.widget) {
      decorations.push(
        Decoration.widget(to, overlay.widget, { key: overlay.id, side: 1, id: overlay.id }),
      );
    }
  }
  return DecorationSet.create(state.doc, decorations);
}
