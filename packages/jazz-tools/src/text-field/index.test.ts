// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vitest";
import { textFieldSync } from "./index.js";

const fields: HTMLTextAreaElement[] = [];
afterEach(() => {
  for (const field of fields.splice(0)) field.remove();
});

function setup(initial: string) {
  const field = document.createElement("textarea");
  field.value = initial;
  document.body.append(field);
  fields.push(field);
  field.focus();
  const writes: Array<[string, string]> = [];
  const sync = textFieldSync(field, {
    onChange: (text, { editedFrom }) => writes.push([text, editedFrom]),
  });
  /** Types `text` at the cursor, as the browser does: the field changes,
   * then an input event. */
  const type = (text: string) => {
    const at = field.selectionStart;
    field.setRangeText(text, at, field.selectionEnd, "end");
    field.dispatchEvent(new Event("input"));
  };
  return { field, sync, writes, type };
}

describe("textFieldSync", () => {
  it("writes each change as an edit of the value the field wrote last", () => {
    const { field, writes, type } = setup("Hello");
    field.setSelectionRange(5, 5);
    type(" w");
    type("o");
    expect(writes).toEqual([
      ["Hello w", "Hello"],
      ["Hello wo", "Hello w"],
    ]);
  });

  it("takes the echo of its latest write as nothing new", () => {
    const { field, sync, writes, type } = setup("Hi");
    field.setSelectionRange(2, 2);
    type("!");
    sync.apply("Hi!");
    expect(field.value).toBe("Hi!");
    expect(field.selectionStart).toBe(3);
    expect(writes).toHaveLength(1);
  });

  it("applies a merged value as a change, keeping the cursor with its text", () => {
    const { field, sync, writes, type } = setup("Start  end");
    field.setSelectionRange(6, 6);
    type("ab");
    // Someone typed at the start and right where this cursor is.
    sync.apply("> Start abXY end");
    expect(field.value).toBe("> Start abXY end");
    // Ahead of the text inserted where it was, after the text before it.
    expect(field.selectionStart).toBe(10);
    expect(field.selectionEnd).toBe(10);
    type("c");
    expect(field.value).toBe("> Start abcXY end");
    expect(writes.at(-1)).toEqual(["> Start abcXY end", "> Start abXY end"]);
  });

  it("keeps a selection on the text it selected", () => {
    const { field, sync } = setup("one two three");
    field.setSelectionRange(4, 7, "backward");
    sync.apply("zero one two three");
    expect(field.value.slice(field.selectionStart, field.selectionEnd)).toBe("two");
    expect(field.selectionDirection).toBe("backward");
  });

  it("applies a value equal to an earlier write: someone deleted what it typed", () => {
    const { field, sync, type } = setup("a");
    field.setSelectionRange(1, 1);
    type("b");
    type("c");
    sync.apply("ab");
    expect(field.value).toBe("ab");
  });

  it("writes text typed before its input event instead of applying over it", () => {
    const { field, sync, writes } = setup("abc");
    field.value = "abcd"; // input event still to come
    sync.apply("Xabc");
    expect(field.value).toBe("abcd");
    expect(writes).toEqual([["abcd", "abc"]]);
    // The merged value of that write is then applied.
    sync.apply("Xabcd");
    expect(field.value).toBe("Xabcd");
    expect(writes).toHaveLength(1);
  });

  it("holds a merged value while an input method composes, and applies it after", () => {
    const { field, sync, type } = setup("ka");
    field.setSelectionRange(2, 2);
    field.dispatchEvent(new Event("compositionstart"));
    type("n");
    sync.apply("Okan");
    expect(field.value).toBe("kan");
    field.dispatchEvent(new Event("compositionend"));
    expect(field.value).toBe("Okan");
    expect(field.selectionStart).toBe(4);
  });

  it("drops a held value once the field writes past it", () => {
    const { field, sync, writes, type } = setup("ka");
    field.setSelectionRange(2, 2);
    field.dispatchEvent(new Event("compositionstart"));
    type("n");
    sync.apply("Okan");
    type("ji");
    field.dispatchEvent(new Event("compositionend"));
    // That write's merge with the held value comes next; until then the
    // field keeps what was typed.
    expect(writes.map(([text]) => text)).toEqual(["kan", "kanji"]);
    expect(field.value).toBe("kanji");
    sync.apply("Okanji");
    expect(field.value).toBe("Okanji");
  });

  it("stops listening once destroyed", () => {
    const { field, sync, writes, type } = setup("x");
    sync.destroy();
    field.setSelectionRange(1, 1);
    type("y");
    expect(writes).toEqual([]);
  });

  it("sets an unfocused field's text without moving focus to it", () => {
    const { field, sync } = setup("abc");
    field.blur();
    sync.apply("abcd");
    expect(field.value).toBe("abcd");
    expect(document.activeElement).not.toBe(field);
  });
});
