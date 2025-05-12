import { Account, CoRichText } from "jazz-tools";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { schema } from "prosemirror-schema-basic";
import { EditorState, TextSelection } from "prosemirror-state";
import { Plugin } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createJazzPlugin } from "../lib/plugin";

let account: Account;
let coRichText: CoRichText;
let plugin: Plugin;
let state: EditorState;
let view: EditorView;

beforeEach(async () => {
  await setupJazzTestSync();
  account = await createJazzTestAccount({ isCurrentActiveAccount: true });

  // Create a real CoRichText with the test account as owner
  coRichText = CoRichText.create("<p>Hello</p>", account);

  plugin = createJazzPlugin(coRichText);
  state = EditorState.create({
    schema,
    plugins: [plugin],
  });

  // Create a DOM element for the editor
  const editorElement = document.createElement("div");
  document.body.appendChild(editorElement);

  // Initialize the editor view
  view = new EditorView(editorElement, {
    state,
  });
});

afterEach(() => {
  // Clean up the editor view
  if (view) {
    view.destroy();
    view.dom.remove();
  }
});

describe("createJazzPlugin", () => {
  it("initializes editor with CoRichText content", () => {
    expect(state.doc.textContent).toContain("Hello");
  });

  it("updates editor when CoRichText changes", async () => {
    // Update CoRichText content
    coRichText.applyDiff("<p>Updated content</p>");

    // Wait for the next tick to allow the update to propagate
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(view.state.doc.textContent).toContain("Updated content");
  });

  it("updates CoRichText when editor content changes", () => {
    // Create a transaction to update the editor content
    const tr = view.state.tr.insertText(" World", 6);
    view.dispatch(tr);

    // Verify CoRichText was updated
    expect(coRichText.toString()).toContain("Hello World");
  });

  it("handles empty CoRichText initialization", () => {
    const emptyCoRichText = CoRichText.create("", account);
    const emptyPlugin = createJazzPlugin(emptyCoRichText);
    const emptyState = EditorState.create({
      schema,
      plugins: [emptyPlugin],
    });

    expect(emptyState.doc.textContent).toBe("");
  });

  it("handles undefined CoRichText", () => {
    const undefinedPlugin = createJazzPlugin(undefined);
    const undefinedState = EditorState.create({
      schema,
      plugins: [undefinedPlugin],
    });

    expect(undefinedState.doc.textContent).toBe("");
  });

  it("prevents infinite update loops", () => {
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

  it.skip("preserves selection when CoRichText changes", () => {
    // Set a selection in the editor
    const tr = view.state.tr.setSelection(
      TextSelection.create(view.state.doc, 2, 5),
    );
    view.dispatch(tr);

    // Verify initial selection is set
    expect(view.state.selection.from).toBe(2);
    expect(view.state.selection.to).toBe(5);

    // Update CoRichText content
    coRichText.applyDiff("<p>Updated content</p>");

    // Verify selection is preserved after content update
    expect(view.state.selection.from).toBe(2);
    expect(view.state.selection.to).toBe(5);
  });
});
