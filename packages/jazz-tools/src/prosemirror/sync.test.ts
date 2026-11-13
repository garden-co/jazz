import { describe, expect, it } from "vitest";
import { defaultMarkdownParser } from "prosemirror-markdown";
import { EditorState, TextSelection } from "prosemirror-state";
import { applyMarkdown, docPosition, markdownOf, markdownSync, textPosition } from "./sync.js";
import { mapOffset } from "./text-map.js";

const parse = (markdown: string) => defaultMarkdownParser.parse(markdown);

/** An editor on `markdown` with the cursor at `‸` (or the selection
 * between `‹` and `›`), recording what it writes. */
function editor(marked: string) {
  const written: string[] = [];
  const editedFrom: string[] = [];
  const plugin = markdownSync({
    parse,
    onChange: (markdown, change) => {
      written.push(markdown);
      editedFrom.push(change.editedFrom);
    },
  });
  const markdown = marked.replace(/[‸‹›]/g, "");
  let state = EditorState.create({ doc: parse(markdown), plugins: [plugin] });
  const at = (marker: string) => {
    const index = marked.replace(/[‸‹›]/g, (m) => (m === marker ? m : "")).indexOf(marker);
    return index < 0 ? null : docPosition(state, index);
  };
  const cursor = at("‸");
  const from = at("‹");
  const to = at("›");
  if (cursor !== null) {
    state = state.apply(state.tr.setSelection(TextSelection.create(state.doc, cursor)));
  } else if (from !== null && to !== null) {
    state = state.apply(state.tr.setSelection(TextSelection.create(state.doc, from, to)));
  }
  const view = plugin.spec.view!({ state } as never);
  return {
    written,
    editedFrom,
    get state() {
      return state;
    },
    /** Applies a local transaction, as the view would. */
    local(build: (state: EditorState) => ReturnType<EditorState["tr"]["insertText"]>) {
      const previous = state;
      state = state.apply(build(state));
      view.update!({ state } as never, previous);
    },
    remote(markdown: string) {
      const tr = applyMarkdown(state, markdown);
      if (!tr) return false;
      const previous = state;
      state = state.apply(tr);
      view.update!({ state } as never, previous);
      return true;
    },
    /** The markdown with `‸` at the cursor, or `‹ ›` around the selection. */
    marked() {
      const { text } = markdownOf(state);
      const { from, to, empty } = state.selection;
      const start = textPosition(state, from, "after");
      if (empty) return text.slice(0, start) + "‸" + text.slice(start);
      const end = textPosition(state, to, "before");
      return text.slice(0, start) + "‹" + text.slice(start, end) + "›" + text.slice(end);
    },
  };
}

describe("markdownSync", () => {
  it.each([false, true])(
    "maps a shared document with the active plugin's writers (custom first: %s)",
    (customFirst) => {
      const plain = EditorState.create({
        doc: parse("**cat**"),
        plugins: [markdownSync({ parse })],
      });
      const custom = plain.reconfigure({
        plugins: [
          markdownSync({
            parse,
            spec: {
              marks: { strong: { open: () => "<strong>", close: () => "</strong>" } },
            },
          }),
        ],
      });
      expect(custom.doc).toBe(plain.doc);
      for (const state of customFirst ? [custom, plain] : [plain, custom]) {
        const mapped = markdownOf(state);
        const offset = state === custom ? 9 : 3;
        expect(mapped.text).toBe(state === custom ? "<strong>cat</strong>" : "**cat**");
        expect(textPosition(state, 2)).toBe(offset);
        expect(docPosition(state, offset)).toBe(2);
        expect(markdownOf(state)).toBe(mapped);
      }
      expect(() => markdownOf(plain.reconfigure({ plugins: [] }))).toThrow(
        "no markdownSync plugin",
      );
    },
  );

  it("writes only its own change, leaving text it would write differently", () => {
    const ed = editor("Every stop is a reason to stay\n\nIntro: palm muted‸");
    // Someone else is typing after "stay": the value ends that line in a
    // space this editor's markdown cannot hold.
    expect(ed.remote("Every stop is a reason to stay \n\nIntro: palm muted")).toBe(false);
    ed.local((s) => s.tr.insertText(", then a bass slide"));
    expect(ed.written).toEqual([
      "Every stop is a reason to stay \n\nIntro: palm muted, then a bass slide",
    ]);
  });

  it("writes local changes as markdown", () => {
    const ed = editor("Hello‸");
    ed.local((s) => s.tr.insertText(" world"));
    expect(ed.written).toEqual(["Hello world"]);
  });

  it("says which value each write edits: the last one written or shown", () => {
    const ed = editor("Hello‸");
    ed.local((s) => s.tr.insertText("!"));
    ed.remote("Hello! and more");
    ed.local((s) => s.tr.insertText("?"));
    expect(ed.written).toEqual(["Hello!", "Hello! and more?"]);
    expect(ed.editedFrom).toEqual(["Hello", "Hello! and more"]);
  });

  it("types into an empty paragraph as plain text", () => {
    const ed = editor("First\n\n‸[]()\n\nLast");
    expect(ed.state.doc.child(1).content.size).toBe(0);
    ed.local((s) => s.tr.insertText("Hello"));
    expect(ed.written).toEqual(["First\n\nHello\n\nLast"]);
  });

  it("applies a remote change elsewhere and keeps the cursor", () => {
    const ed = editor("# Notes\n\nFirst paragraph.\n\nSecond ‸paragraph.");
    expect(ed.remote("# Notes\n\nFirst paragraph, edited remotely.\n\nSecond paragraph.")).toBe(
      true,
    );
    expect(ed.marked()).toBe("# Notes\n\nFirst paragraph, edited remotely.\n\nSecond ‸paragraph.");
    expect(ed.written).toEqual([]);
  });

  it("keeps the cursor on its text when the same paragraph changes before it", () => {
    const ed = editor("Load-in at 2pm, soundcheck at ‸5pm.");
    ed.remote("Load-in at 1pm sharp, soundcheck at 5pm.");
    expect(ed.marked()).toBe("Load-in at 1pm sharp, soundcheck at ‸5pm.");
  });

  it("moves the cursor past text inserted right at it", () => {
    const ed = editor("Setlist: Intro, ‸Encore.");
    ed.remote("Setlist: Intro, Night Drive, Encore.");
    expect(ed.marked()).toBe("Setlist: Intro, Night Drive, ‸Encore.");
  });

  it("keeps a selection from growing over text inserted at its edges", () => {
    const ed = editor("Bring ‹the van keys› tonight.");
    ed.remote("Bring all of the van keys please tonight.");
    expect(ed.marked()).toBe("Bring all of ‹the van keys› please tonight.");
  });

  it("follows formatting a remote writer added around the cursor", () => {
    const ed = editor("Remember the set‸list printouts.");
    ed.remote("Remember the **setlist** printouts.");
    expect(ed.marked()).toBe("Remember the **set‸list** printouts.");
  });

  it("ignores the echo of its latest write", () => {
    const ed = editor("A‸");
    ed.local((s) => s.tr.insertText("b"));
    ed.local((s) => s.tr.insertText("c"));
    expect(ed.remote("Abc")).toBe(false);
    expect(markdownOf(ed.state).text).toBe("Abc");
  });

  it("applies a merged value that equals one of its earlier writes", () => {
    const ed = editor("Bring the ‸");
    ed.local((s) => s.tr.insertText("van"));
    // Someone deleted the word this editor just typed.
    expect(ed.remote("Bring the")).toBe(true);
    expect(markdownOf(ed.state).text).toBe("Bring the");
    ed.local((s) => s.tr.insertText(" keys", s.doc.content.size - 1));
    expect(ed.written.at(-1)).toBe("Bring the keys");
  });

  it("writes the canonical value when its edit cannot be placed in a non-canonical one", () => {
    // Two spots written differently, far apart: the part between them is
    // too large to diff, so the edit's place in the stored value is unknown.
    const middle = Array.from({ length: 300 }, (_, i) => `Line ${i} of the set notes.`).join(
      "\n\n",
    );
    const stored = `__Tour__\n\n${middle}\n\n* last item`;
    const ed = editor("Start‸");
    expect(ed.remote(stored)).toBe(true);
    const target = "Line 150 of the set notes.";
    const at = docPosition(ed.state, markdownOf(ed.state).text.indexOf(target) + "Line 150".length);
    ed.local((s) => s.tr.insertText(" (encore)", at));
    const write = ed.written.at(-1)!;
    // Nothing the editors show is lost: the write holds every line.
    expect(markdownOf(editor(write).state).text).toBe(markdownOf(ed.state).text);
    expect(write).toContain("Line 150 (encore) of the set notes.");
    expect(write).toContain("Line 0 of the set notes.");
    expect(write).toContain("Line 299 of the set notes.");
  });

  it("leaves blocks the change does not reach as they are", () => {
    const ed = editor("Typing at the end‸\n\nOther paragraph.");
    // The local paragraph ends in a space markdown cannot keep.
    ed.local((s) => s.tr.insertText(" "));
    expect(markdownOf(ed.state).text).toBe("Typing at the end \n\nOther paragraph.");
    // The authority merged that value with a remote edit.
    ed.remote("Typing at the end \n\nOther paragraph, edited.");
    expect(ed.state.doc.firstChild!.textContent).toBe("Typing at the end ");
    expect(ed.state.doc.lastChild!.textContent).toBe("Other paragraph, edited.");
  });

  it("parses a non-canonical value into the canonical document", () => {
    const ed = editor("Item list:\n\n- one‸");
    expect(ed.remote("Item list:\n\n* one\n* two")).toBe(true);
    expect(markdownOf(ed.state).text).toBe("Item list:\n\n- one\n- two");
  });

  it("applies a remote change that splits the cursor's paragraph", () => {
    const ed = editor("We drove to Leeds. The show was ‸sold out.");
    ed.remote("We drove to Leeds.\n\nThe show was sold out.");
    expect(ed.marked()).toBe("We drove to Leeds.\n\nThe show was ‸sold out.");
  });

  it("does not record remote changes as local writes", () => {
    const ed = editor("Hello‸");
    ed.remote("Hello there");
    ed.local((s) => s.tr.insertText("!", s.doc.content.size - 1));
    expect(ed.written).toEqual(["Hello there!"]);
  });
});

describe("mapOffset", () => {
  it("shifts offsets after the change", () => {
    expect(mapOffset("abc def", "abc XX def", 5, "after")).toBe(8);
  });

  it("keeps offsets before the change", () => {
    expect(mapOffset("abc def", "abc def ghi", 2, "after")).toBe(2);
  });

  it("follows kept characters inside the change", () => {
    // "c" is kept; text was inserted on both sides of it.
    expect(mapOffset("abcd", "aXbYcZd", 2, "after")).toBe(4);
    expect(mapOffset("abcd", "aXbYcZd", 2, "before")).toBe(3);
  });

  it("collapses an offset in deleted text to the deletion", () => {
    expect(mapOffset("keep this gone keep", "keep  keep", 12, "after")).toBe(5);
  });
});
