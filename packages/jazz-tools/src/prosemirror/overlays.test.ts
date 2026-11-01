import { describe, expect, it } from "vitest";
import { defaultMarkdownParser } from "prosemirror-markdown";
import { EditorState, TextSelection } from "prosemirror-state";
import { textAnchorsFrom, type TextAnchors } from "../runtime/text-anchors.js";
import { loadWasmModule } from "../runtime/wasm-loader.js";
import { captureRange } from "./anchors.js";
import { overlays, overlaysKey, placedOverlays, setOverlays } from "./overlays.js";
import type { DecorationSet } from "prosemirror-view";
import { capturePresence, presenceOverlays } from "./presence.js";
import { applyMarkdown, docPosition, markdownSync } from "./sync.js";

const parse = (markdown: string) => defaultMarkdownParser.parse(markdown);

function editor(markdown: string, anchors: TextAnchors) {
  return EditorState.create({
    doc: parse(markdown),
    plugins: [markdownSync({ parse }), overlays({ anchors })],
  });
}

/** The document text each placed overlay covers, or "orphaned". */
function covered(state: EditorState) {
  return Object.fromEntries(
    placedOverlays(state).map((placed) => [
      placed.overlay.id,
      placed.orphaned ? "orphaned" : state.doc.textBetween(placed.from, placed.to),
    ]),
  );
}

describe("overlays", () => {
  it("keeps a comment on its text through a merged value, and orphans a deleted one", async () => {
    const anchors = textAnchorsFrom(await loadWasmModule());
    const author = editor("Bring the red van keys. Ask Sam about the merch table.", anchors);
    const at = (text: string) => docPosition(author, author.doc.textContent.indexOf(text));
    const span = (text: string) => captureRange(author, anchors, at(text), at(text) + text.length);
    const keys = span("the red van keys");
    const sam = span("Ask Sam about the merch table");

    let state = editor("Bring the red van keys. Ask Sam about the merch table.", anchors);
    state = state.apply(
      setOverlays(state.tr, [
        { id: "keys", range: keys, attrs: { class: "comment" } },
        { id: "sam", range: sam, attrs: { class: "comment" } },
      ]),
    );
    expect(covered(state)).toEqual({
      keys: "the red van keys",
      sam: "Ask Sam about the merch table",
    });

    state = state.apply(applyMarkdown(state, "Please bring *the red van keys*.")!);
    expect(covered(state)).toEqual({ keys: "the red van keys", sam: "orphaned" });
  });

  it("moves drawn overlays with local typing", async () => {
    const anchors = textAnchorsFrom(await loadWasmModule());
    let state = editor("Soundcheck at 5pm.", anchors);
    const range = captureRange(state, anchors, docPosition(state, 14), docPosition(state, 17));
    state = state.apply(setOverlays(state.tr, [{ id: "time", range, attrs: { class: "c" } }]));
    state = state.apply(state.tr.insertText("Final ", 1));
    const [decoration] = overlaysOf(state);
    expect(state.doc.textBetween(decoration!.from, decoration!.to)).toBe("5pm");
  });

  it("draws a collaborator's caret and selection", async () => {
    const anchors = textAnchorsFrom(await loadWasmModule());
    let theirs = editor("Doors open at 8pm.", anchors);
    theirs = theirs.apply(
      theirs.tr.setSelection(
        TextSelection.create(theirs.doc, docPosition(theirs, 14), docPosition(theirs, 17)),
      ),
    );
    const presence = capturePresence(theirs, anchors);
    expect(presence.selection?.matched).toBe("8pm");

    let mine = editor("The doors open at 8pm.", anchors);
    mine = mine.apply(
      setOverlays(
        mine.tr,
        presenceOverlays([{ id: "sam", name: "Sam", color: "#146AFF", ...presence }]),
      ),
    );
    expect(covered(mine)).toEqual({ "presence:sam:selection": "8pm", "presence:sam:cursor": "" });
    const caret = placedOverlays(mine).find((p) => p.overlay.id === "presence:sam:cursor")!;
    expect(mine.doc.textBetween(caret.from - 3, caret.from)).toBe("8pm");
  });
});

/** The drawn decorations, in document order. */
function overlaysOf(state: EditorState) {
  const plugin = state.plugins.find((p) => p.spec.key === overlaysKey)!;
  return (plugin.getState(state) as { decorations: DecorationSet }).decorations.find();
}
