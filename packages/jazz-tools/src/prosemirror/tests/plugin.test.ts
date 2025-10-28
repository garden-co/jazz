// @vitest-environment jsdom

import { CoRichText } from "jazz-tools";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { EditorState, TextSelection } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  onTestFinished,
} from "vitest";
import { createJazzPlugin } from "../lib/plugin";
import { Schema } from "prosemirror-model";
import { schema as basicSchema } from "prosemirror-schema-basic";
import { addListNodes } from "prosemirror-schema-list";

const schema = new Schema({
  nodes: addListNodes(basicSchema.spec.nodes, "paragraph block*", "block"),
  marks: basicSchema.spec.marks,
});

async function setupTest(initialContent = "<p>Hello</p>") {
  // Create a real CoRichText with the test account as owner
  const coRichText = CoRichText.create(initialContent);

  const plugin = createJazzPlugin(coRichText);
  const state = EditorState.create({
    schema,
    plugins: [plugin],
  });

  // Create a DOM element for the editor
  const editorElement = document.createElement("div");
  document.body.appendChild(editorElement);

  // Initialize the editor view
  const view = new EditorView(editorElement, {
    state,
  });

  onTestFinished(() => {
    view.destroy();
    editorElement.remove();
  });

  return { coRichText, plugin, state, view, editorElement };
}

beforeEach(async () => {
  await setupJazzTestSync();
  await createJazzTestAccount({ isCurrentActiveAccount: true });
});

describe("createJazzPlugin", () => {
  it("initializes editor with CoRichText content", async () => {
    const { state } = await setupTest();
    expect(state.doc.textContent).toContain("Hello");
  });

  it("updates editor when CoRichText changes", async () => {
    const { coRichText, view } = await setupTest();

    // Update CoRichText content
    coRichText.$jazz.applyDiff("<p>Updated content</p>");

    // Wait for the next tick to allow the update to propagate
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(view.state.doc.textContent).toContain("Updated content");
  });

  it("updates CoRichText when editor content changes", async () => {
    const { coRichText, view } = await setupTest();

    // Create a transaction to update the editor content
    const tr = view.state.tr.insertText(" World", 6);
    view.dispatch(tr);

    // Verify CoRichText was updated
    expect(coRichText.toString()).toContain("Hello World");
  });

  it("handles empty CoRichText initialization", async () => {
    const emptyCoRichText = CoRichText.create("");
    const emptyPlugin = createJazzPlugin(emptyCoRichText);
    const emptyState = EditorState.create({
      schema,
      plugins: [emptyPlugin],
    });

    expect(emptyState.doc.textContent).toBe("");
  });

  it("handles undefined CoRichText", async () => {
    const undefinedPlugin = createJazzPlugin(undefined);
    const undefinedState = EditorState.create({
      schema,
      plugins: [undefinedPlugin],
    });

    expect(undefinedState.doc.textContent).toBe("");
  });

  it("prevents infinite update loops", async () => {
    const { coRichText, view } = await setupTest();

    // Create a transaction that would normally trigger a CoRichText update
    const tr = view.state.tr.insertText(" Loop", 6);

    // Mark the transaction as coming from Jazz
    tr.setMeta("fromJazz", true);
    view.dispatch(tr);

    // Verify the content was updated in the editor
    expect(view.state.doc.textContent).toContain("Hello Loop");

    // Verify CoRichText was NOT updated (to prevent infinite loop)
    expect(coRichText.toString()).not.toContain("Loop");
  });

  it("preserves selection when CoRichText changes", async () => {
    const { coRichText, view } = await setupTest();

    // Set a selection in the editor
    const tr = view.state.tr.setSelection(
      TextSelection.create(view.state.doc, 2, 5),
    );
    view.dispatch(tr);

    // Verify initial selection is set
    expect(view.state.selection.from).toBe(2);
    expect(view.state.selection.to).toBe(5);

    // Update CoRichText content
    coRichText.$jazz.applyDiff("<p>Hello world</p>");

    await new Promise((resolve) => setTimeout(resolve, 0));

    // Verify selection is preserved after content update
    expect(view.state.selection.from).toBe(2);
    expect(view.state.selection.to).toBe(5);
  });

  it("falls back to creating a new EditorState when the transform fails", async () => {
    const { coRichText, editorElement } = await setupTest(
      "<p>A <strong>hu<em>man</strong></em>.</p>",
    );

    // Wait for the next tick to allow the update to propagate
    await new Promise((resolve) => setTimeout(resolve, 0));

    // Update CoRichText content
    coRichText.$jazz.applyDiff(
      "<ol><li><p>A <strong>hu</strong><em><strong>man</strong></em>.</p></li></ol>",
    );

    // Wait for the next tick to allow the update to propagate
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(editorElement.querySelector(".ProseMirror")?.innerHTML).toBe(
      "<ol><li><p>A <strong>hu</strong><em><strong>man</strong></em>.</p></li></ol>",
    );
  });

  it("handles updates with emojis", async () => {
    const { coRichText, editorElement } = await setupTest(
      "<p>A <strong>hu</strong><em><strong>man</strong></em>.</p>",
    );

    // Update CoRichText content
    coRichText.$jazz.applyDiff("<p>A human💪</p>");

    // Wait for the next tick to allow the update to propagate
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(editorElement.querySelector(".ProseMirror")?.innerHTML).toBe(
      "<p>A human💪</p>",
    );
  });
});
