/**
 * Selection presence: each editor stores where its user's cursor and
 * selection are, as a text anchor and range, and draws everyone else's as
 * overlays. Where the presence lives is up to the app: a row per editor
 * session, overwritten as the selection moves (throttle the writes).
 */
import type { EditorState } from "prosemirror-state";
import type { TextAnchor, TextAnchors, TextRange } from "../runtime/text-anchors.js";
import { captureAnchor, captureRange } from "./anchors.js";
import type { Overlay } from "./overlays.js";

/** One user's place in the document. */
export interface SelectionPresence {
  /** The caret, where the selection's head is. */
  cursor: TextAnchor;
  /** The selected text, when the selection is not empty. */
  selection?: TextRange;
}

/** Someone else's presence, ready to draw. */
export interface Collaborator extends SelectionPresence {
  id: string;
  name: string;
  /** A CSS colour for their caret and selection. */
  color: string;
}

/** The editor's selection as presence to store. */
export function capturePresence(
  state: EditorState,
  anchors: TextAnchors,
  version?: number,
): SelectionPresence {
  const { head, from, to, empty } = state.selection;
  // The caret stays with the text before it, as typing at it extends that.
  const cursor = captureAnchor(state, anchors, head, "before", version);
  return empty
    ? { cursor }
    : { cursor, selection: captureRange(state, anchors, from, to, version) };
}

/** Whether two presences point at the same place, to skip needless writes. */
export function samePresence(a: SelectionPresence | undefined, b: SelectionPresence): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/** Overlays for collaborators' carets (with their name) and selections. */
export function presenceOverlays(collaborators: readonly Collaborator[]): Overlay[] {
  return collaborators.flatMap((person) => {
    const caret: Overlay = {
      id: `presence:${person.id}:cursor`,
      anchor: person.cursor,
      // Presence is short-lived: show it a little more readily than comments.
      minConfidence: 0.5,
      widget: () => caretElement(person),
    };
    if (!person.selection) return [caret];
    return [
      {
        id: `presence:${person.id}:selection`,
        range: person.selection,
        minConfidence: 0.5,
        attrs: {
          class: "jazz-presence-selection",
          style: `background-color: color-mix(in srgb, ${person.color} 25%, transparent)`,
        },
      },
      caret,
    ];
  });
}

function caretElement(person: Collaborator): HTMLElement {
  const caret = document.createElement("span");
  caret.className = "jazz-presence-caret";
  caret.style.borderLeft = `2px solid ${person.color}`;
  caret.style.marginLeft = "-1px";
  caret.style.position = "relative";
  const label = document.createElement("span");
  label.className = "jazz-presence-name";
  label.textContent = person.name;
  Object.assign(label.style, {
    position: "absolute",
    bottom: "100%",
    left: "-1px",
    background: person.color,
    color: "white",
    fontSize: "0.7em",
    lineHeight: "1.2",
    padding: "0 0.3em",
    borderRadius: "0.2em",
    whiteSpace: "nowrap",
    pointerEvents: "none",
  });
  caret.append(label);
  return caret;
}
