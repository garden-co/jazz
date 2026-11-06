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

  it("takes a late echo of an earlier write as nothing new", () => {
    const { field, sync, writes, type } = setup("a");
    field.setSelectionRange(1, 1);
    type("b");
    type("c");
    // The subscription delivers each write, a little behind the keystrokes.
    sync.apply("ab");
    expect(field.value).toBe("abc");
    sync.apply("abc");
    type("d");
    expect(writes.at(-1)).toEqual(["abcd", "abc"]);
    // Once a merged value is shown, an earlier write is a value like any
    // other: someone deleted what was typed.
    sync.apply("Xabcd");
    sync.apply("abc");
    expect(field.value).toBe("abc");
  });

  it("keeps the cursor out of the middle of an emoji", () => {
    const { field, sync } = setup("a\u{1F600}");
    field.setSelectionRange(3, 3);
    sync.apply("a\u{1F603}");
    expect(field.selectionStart === 1 || field.selectionStart === 3).toBe(true);
    field.setSelectionRange(1, 3);
    sync.apply("a\u{1F604}b");
    const { selectionStart, selectionEnd } = field;
    for (const at of [selectionStart!, selectionEnd!]) {
      const code = field.value.charCodeAt(at);
      expect(code >= 0xdc00 && code <= 0xdfff).toBe(false);
    }
  });

  it("writes typed text first even when a remote value equals it", () => {
    const { field, sync, writes } = setup("abc");
    field.value = "abcd"; // input event still to come
    sync.apply("abcd"); // someone else typed the same letter
    expect(writes).toEqual([["abcd", "abc"]]);
  });

  it("keeps editing from the column's value when a write throws", () => {
    const field = document.createElement("textarea");
    field.value = "a";
    document.body.append(field);
    fields.push(field);
    const writes: Array<[string, string]> = [];
    let fail = true;
    textFieldSync(field, {
      onChange: (text, { editedFrom }) => {
        if (fail) throw new Error("refused");
        writes.push([text, editedFrom]);
      },
    });
    // The DOM reports a listener's error instead of throwing it.
    const reported: unknown[] = [];
    const onError = (event: ErrorEvent) => {
      reported.push(event.error);
      event.preventDefault();
    };
    window.addEventListener("error", onError);
    field.value = "ab";
    field.dispatchEvent(new Event("input"));
    window.removeEventListener("error", onError);
    expect(String(reported[0])).toContain("refused");
    fail = false;
    field.value = "abc";
    field.dispatchEvent(new Event("input"));
    expect(writes).toEqual([["abc", "a"]]);
  });

  it("takes a write's echo delivered synchronously from inside onChange", () => {
    const field = document.createElement("textarea");
    field.value = "a";
    document.body.append(field);
    fields.push(field);
    const writes: string[] = [];
    let sync: ReturnType<typeof textFieldSync> | null = null;
    sync = textFieldSync(field, {
      onChange: (text) => {
        writes.push(text);
        // A store that delivers its new value before the write returns.
        sync!.apply(text);
      },
    });
    field.value = "ab";
    field.dispatchEvent(new Event("input"));
    expect(writes).toEqual(["ab"]);
    expect(field.value).toBe("ab");
  });

  it("takes its latest write's echo first when earlier writes match too", () => {
    const { field, sync, writes, type } = setup("x");
    field.setSelectionRange(1, 1);
    type("a");
    field.setRangeText("", 1, 2, "end"); // Backspace
    field.dispatchEvent(new Event("input"));
    type("a");
    expect(writes.map(([text]) => text)).toEqual(["xa", "x", "xa"]);
    // The subscription skipped to the latest value: caught up.
    sync.apply("xa");
    // Someone deleting the "a" is then a change, not an old echo.
    sync.apply("x");
    expect(field.value).toBe("x");
  });

  it("syncs a single-line input too", () => {
    const field = document.createElement("input");
    field.value = "title";
    document.body.append(field);
    field.focus();
    const writes: string[] = [];
    const sync = textFieldSync(field, { onChange: (text) => writes.push(text) });
    field.setSelectionRange(5, 5);
    field.setRangeText("!", 5, 5, "end");
    field.dispatchEvent(new Event("input"));
    sync.apply("A title!");
    expect(field.value).toBe("A title!");
    expect(field.selectionStart).toBe(8);
    expect(writes).toEqual(["title!"]);
    field.remove();
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

  it("applies a held value after composition when its input event comes last", () => {
    const { field, sync, writes } = setup("ka");
    field.setSelectionRange(2, 2);
    field.dispatchEvent(new Event("compositionstart"));
    sync.apply("Oka");
    // Some browsers change the text and send its input event only after
    // compositionend.
    field.setRangeText("n", 2, 2, "end");
    field.dispatchEvent(new Event("compositionend"));
    field.dispatchEvent(new Event("input"));
    expect(writes).toEqual([["kan", "ka"]]);
    expect(field.value).toBe("kan");
    sync.apply("Okan");
    expect(field.value).toBe("Okan");
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

  it("stops listening and applying once destroyed", () => {
    const { field, sync, writes, type } = setup("x");
    sync.destroy();
    field.setSelectionRange(1, 1);
    field.dispatchEvent(new Event("compositionstart"));
    type("y");
    field.dispatchEvent(new Event("compositionend"));
    expect(writes).toEqual([]);
    sync.apply("z");
    expect(field.value).toBe("xy");
  });

  it("sets an unfocused field's text without moving focus to it", () => {
    const { field, sync } = setup("abc");
    field.blur();
    sync.apply("abcd");
    expect(field.value).toBe("abcd");
    expect(document.activeElement).not.toBe(field);
  });
});
