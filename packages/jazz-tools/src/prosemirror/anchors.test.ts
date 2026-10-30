import { describe, expect, it } from "vitest";
import { defaultMarkdownParser } from "prosemirror-markdown";
import { EditorState } from "prosemirror-state";
import { textAnchorsFrom } from "../runtime/text-anchors.js";
import { loadWasmModule } from "../runtime/wasm-loader.js";
import { captureAnchor, captureRange, resolveAnchor, resolveRange } from "./anchors.js";
import { docPosition, markdownSync } from "./sync.js";

const parse = (markdown: string) => defaultMarkdownParser.parse(markdown);
const stateOf = (markdown: string) =>
  EditorState.create({ doc: parse(markdown), plugins: [markdownSync({ parse })] });

describe("editor anchors", () => {
  it("finds a remote cursor in another editor's later document", async () => {
    const anchors = textAnchorsFrom(await loadWasmModule());
    const theirs = stateOf("# Notes\n\nThe crowd in Leeds was loud.");
    const cursor = captureAnchor(
      theirs,
      anchors,
      docPosition(theirs, "# Notes\n\nThe crowd in ".length),
    );
    const mine = stateOf("# Tour notes\n\nDay one. The crowd in **Leeds** was loud.");
    const found = resolveAnchor(mine, anchors, cursor);
    expect(mine.doc.textBetween(found.pos, found.pos + 5)).toBe("Leeds");
    expect(found.ambiguous).toBe(false);
  });

  it("keeps a comment on its words when they gain formatting", async () => {
    const anchors = textAnchorsFrom(await loadWasmModule());
    const theirs = stateOf("Bring the red van keys before Friday.");
    const from = docPosition(theirs, "Bring ".length);
    const to = docPosition(theirs, "Bring the red van keys".length);
    const comment = captureRange(theirs, anchors, from, to);
    const mine = stateOf("Please bring *the red van keys* before Friday.");
    const found = resolveRange(mine, anchors, comment);
    expect(mine.doc.textBetween(found.from, found.to)).toBe("the red van keys");
  });
});
