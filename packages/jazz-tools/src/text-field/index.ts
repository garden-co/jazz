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
  | "ownerDocument"
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
 * Leave the field uncontrolled (in React, `defaultValue`, not `value`), so
 * that only this sync sets its text.
 */
export function textFieldSync(field: TextField, options: TextFieldSyncOptions): TextFieldSync {
  // The column's value as this field last wrote or applied it. A value read
  // from the column that equals it is that write's echo.
  let base = field.value;
  // A value that arrived while an input method was composing text.
  let held: string | null = null;
  let composing = false;

  const onInput = () => {
    const text = field.value;
    if (text === base) return;
    // A value held back is older than this write; the write's merge with
    // it comes next.
    held = null;
    const editedFrom = base;
    base = text;
    options.onChange(text, { editedFrom });
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
    if (composing) {
      // Changing the text under an input method breaks the composition.
      held = value;
      return;
    }
    const shown = field.value;
    if (value === base || value === shown) {
      base = value;
      return;
    }
    // Text typed but not written yet (its input event still to come) is
    // written now and merged over `value`; the merged value comes next.
    if (shown !== base) return onInput();
    base = value;
    const focused = field.ownerDocument?.activeElement === (field as unknown);
    const start = field.selectionStart;
    const end = field.selectionEnd;
    const direction = field.selectionDirection ?? undefined;
    field.value = value;
    if (focused && start !== null && end !== null) {
      // A cursor stays ahead of text inserted where it is, so the next
      // keystroke continues the word it typed; a selection keeps to the
      // text it selected.
      const collapsed = start === end;
      const from = mapOffset(shown, value, start, collapsed ? "before" : "after");
      const to = collapsed ? from : mapOffset(shown, value, end, "before");
      field.setSelectionRange(from, Math.max(from, to), direction);
    }
  }

  field.addEventListener("input", onInput);
  field.addEventListener("compositionstart", onCompositionStart);
  field.addEventListener("compositionend", onCompositionEnd);
  return {
    apply,
    destroy() {
      field.removeEventListener("input", onInput);
      field.removeEventListener("compositionstart", onCompositionStart);
      field.removeEventListener("compositionend", onCompositionEnd);
    },
  };
}
