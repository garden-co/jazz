import { mapOffset } from "../prosemirror/text-map.js";

/** A `<textarea>` or `<input>` that edits a text merge column. */
export type TextField = Pick<
  HTMLTextAreaElement,
  | "value"
  | "selectionStart"
  | "selectionEnd"
  | "selectionDirection"
  | "setSelectionRange"
  | "addEventListener"
  | "removeEventListener"
  | "getRootNode"
>;

export interface TextFieldSyncOptions {
  /** Called with the field's text after each local change: write it to the
   * column right away, as an edit of `editedFrom`, the value the field
   * showed or wrote last:
   *
   * ```ts
   * onChange: (body, { editedFrom }) =>
   *   db.update(app.notes, id, { body }, { editedFrom: { body: editedFrom } })
   * ```
   *
   * Do not throttle or debounce it: every write must reach the column
   * before the next merged value is applied. */
  onChange(text: string, change: { editedFrom: string }): void;
}

export interface TextFieldSync {
  /** Shows `value`, read from the column, as the smallest change to what
   * the field shows, keeping the cursor and selection with their text. */
  apply(value: string): void;
  /** Stops listening to the field. */
  destroy(): void;
}

/**
 * Keeps a plain `<textarea>` or `<input>` and a text merge column in step.
 * Local changes are written as edits of the value the field last showed or
 * wrote; values read from the column are applied as a change, not a reset,
 * so the cursor stays where its text went.
 *
 * Leave the field uncontrolled and give it its first value once (in React,
 * a `defaultValue` that never changes), so that only this sync sets its
 * text. Setting the text drops the browser's own undo history for the
 * field.
 */
export function textFieldSync(field: TextField, options: TextFieldSyncOptions): TextFieldSync {
  // The value the field last wrote or applied: each write edits it.
  let base = field.value;
  // This field's writes not yet seen back from the column, oldest first. A
  // subscription can deliver them a little after later keystrokes were
  // written, so an earlier write coming back is its echo, not a deletion.
  let pending: string[] = [];
  // A value that arrived while an input method was composing text.
  let held: string | null = null;
  let composing = false;
  let destroyed = false;

  const onInput = () => {
    const text = field.value;
    if (text === base) return;
    options.onChange(text, { editedFrom: base });
    // Only a write that was made moves the base: after a write that threw,
    // the next one still edits what the column holds.
    base = text;
    pending.push(text);
    // A value held back is older than this write; the write's merge with
    // it comes next.
    held = null;
  };
  const onCompositionStart = () => {
    composing = true;
  };
  const onCompositionEnd = () => {
    composing = false;
    // Composed text not written yet is written first.
    onInput();
    if (held !== null) {
      const value = held;
      held = null;
      apply(value);
    }
  };

  function apply(value: string): void {
    if (destroyed) return;
    if (composing) {
      // Changing the text under an input method breaks the composition.
      held = value;
      return;
    }
    const shown = field.value;
    // Text typed but not written yet (its input event still to come) is
    // written now and merged over `value`; the merged value comes next.
    if (shown !== base) return onInput();
    const echoed = pending.indexOf(value);
    if (echoed >= 0) {
      // One of this field's own writes coming back.
      pending = pending.slice(echoed + 1);
      return;
    }
    pending = [];
    base = value;
    if (value === shown) return;
    const root = field.getRootNode() as Document | ShadowRoot;
    const focused = root.activeElement === (field as unknown);
    const start = field.selectionStart;
    const end = field.selectionEnd;
    const direction = field.selectionDirection ?? undefined;
    field.value = value;
    if (focused && start !== null && end !== null) {
      // A cursor stays ahead of text inserted where it is, so the next
      // keystroke continues the word it typed; a selection keeps to the
      // text it selected.
      const collapsed = start === end;
      const from = wholeCharacter(
        value,
        mapOffset(shown, value, start, collapsed ? "before" : "after"),
        collapsed ? "before" : "after",
      );
      const to = collapsed
        ? from
        : wholeCharacter(value, mapOffset(shown, value, end, "before"), "before");
      field.setSelectionRange(from, Math.max(from, to), direction);
    }
  }

  field.addEventListener("input", onInput);
  field.addEventListener("compositionstart", onCompositionStart);
  field.addEventListener("compositionend", onCompositionEnd);
  return {
    apply,
    destroy() {
      destroyed = true;
      field.removeEventListener("input", onInput);
      field.removeEventListener("compositionstart", onCompositionStart);
      field.removeEventListener("compositionend", onCompositionEnd);
    },
  };
}

/** `offset`, moved out of the middle of a surrogate pair (an emoji, say)
 * to the side it leans to. */
function wholeCharacter(text: string, offset: number, side: "before" | "after"): number {
  if (offset <= 0 || offset >= text.length) return offset;
  const previous = text.charCodeAt(offset - 1);
  const next = text.charCodeAt(offset);
  const splitsPair = previous >= 0xd800 && previous <= 0xdbff && next >= 0xdc00 && next <= 0xdfff;
  if (!splitsPair) return offset;
  return side === "before" ? offset - 1 : offset + 1;
}
