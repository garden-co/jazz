import { fireEvent, screen } from "@testing-library/dom";
import { CoRichText, Group, Marks } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { baseKeymap } from "prosemirror-commands";
import { keymap } from "prosemirror-keymap";
import { schema } from "prosemirror-schema-basic";
import { EditorState, TextSelection } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { afterEach, describe, expect, it } from "vitest";
import { applyTrToRichText, richTextToProsemirrorDoc } from "../index.js";

// Helper class for testing ProseMirror editor interactions with Jazz synchronization
class EditorTestHelper {
  view: EditorView;
  text: CoRichText;
  element: HTMLElement;

  constructor(text: CoRichText) {
    this.text = text;
    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({
      doc,
      schema,
      plugins: [keymap(baseKeymap)],
    });

    this.element = document.createElement("div");
    // Add contenteditable and role for better querying
    this.element.setAttribute("contenteditable", "true");
    this.element.setAttribute("role", "textbox");
    this.element.setAttribute("aria-label", "Rich text editor");
    document.body.appendChild(this.element);

    this.view = new EditorView(this.element, {
      state,
      dispatchTransaction: (tr) => {
        this.view.updateState(this.view.state.apply(tr));
        applyTrToRichText(this.text, tr);
      },
    });
  }

  cleanup() {
    this.element.remove();
  }

  getContent() {
    // Get content from ProseMirror's doc instead of CoRichText
    // This ensures we get the correct representation after edits
    return this.view.state.doc.textContent;
  }

  // Insert text at current selection or specified position (0-based)
  insertText(text: string, pos?: number) {
    const tr = this.view.state.tr;
    if (typeof pos === "number") {
      // For text insertion, we need to account for the paragraph node
      const targetPos = pos === 0 ? 1 : pos + 1;
      tr.setSelection(TextSelection.create(tr.doc, targetPos));
    }
    tr.insertText(text);
    this.view.dispatch(tr);
  }

  // Set cursor position (0-based)
  setCursor(pos: number) {
    const tr = this.view.state.tr;
    // For cursor position, we need to account for the paragraph node
    const targetPos = pos === 0 ? 1 : pos + 1;
    tr.setSelection(TextSelection.create(tr.doc, targetPos));
    this.view.dispatch(tr);
  }

  // Add mark to text range (0-based positions)
  addMark(from: number, to: number, markType: string) {
    const tr = this.view.state.tr;
    const mark = schema.marks[markType];
    if (!mark) {
      console.error(`Mark type ${markType} not found in schema`);
      return;
    }
    // For marks, we need to account for the paragraph node
    const pmFrom = from === 0 ? 1 : from + 1;
    const pmTo = to === 0 ? 1 : to + 1;
    tr.addMark(pmFrom, pmTo, mark.create());
    this.view.dispatch(tr);
  }

  // Split paragraph at position (0-based)
  splitParagraph(pos: number) {
    const tr = this.view.state.tr;
    try {
      // For splits, we need to account for the paragraph node
      const pmPos = pos === 0 ? 1 : pos + 1;
      tr.split(pmPos);
      this.view.dispatch(tr);
    } catch (e) {
      console.error("Error splitting paragraph:", e);
    }
  }
}

describe("Editor Interactions", async () => {
  let editor: EditorTestHelper;
  const account = await createJazzTestAccount();
  const group = Group.create({ owner: account });

  afterEach(() => {
    editor?.cleanup();
  });

  it("should find editor by role", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    const editorElement = screen.getByRole("textbox", {
      name: "Rich text editor",
    });
    expect(editorElement).toBeInTheDocument();
  });

  it("should handle basic typing", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    expect(editor.getContent()).toBe("Hello world");
  });

  it("should handle text selection and replacement", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    // Directly dispatch transaction for selection and replacement
    const tr = editor.view.state.tr;
    tr.setSelection(TextSelection.create(tr.doc, 0, tr.doc.content.size - 1));
    const fragment = schema.text("New text");
    tr.replaceSelectionWith(fragment);
    editor.view.dispatch(tr);

    const pmContent = editor.view.state.doc.textContent;
    const jazzContent = editor.text.toString();

    expect(pmContent).toBe("New text");
    expect(jazzContent).toBe("New text");
  });

  it("should handle text formatting", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    // Directly apply mark through transaction
    const tr = editor.view.state.tr;
    tr.addMark(1, 6, schema.marks.strong.create());
    editor.view.dispatch(tr);

    // Verify ProseMirror marks
    const pmMarks = editor.view.state.doc.resolve(1).marks();
    const jazzMarks = editor.text.resolveMarks();

    // Verify mark synchronization
    expect(pmMarks.some((m) => m.type.name === "strong")).toBe(true);
    expect(jazzMarks.some((m) => m.sourceMark.tag === "strong")).toBe(true);
  });

  it("should handle paragraph splits", () => {
    const text = CoRichText.createFromPlainText("First line", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });

    editor = new EditorTestHelper(text);

    // Split at position 5 (after "First")
    const tr = editor.view.state.tr;
    tr.split(6);
    editor.view.dispatch(tr);

    // Insert text in second paragraph
    editor.insertText("Second line", 6);

    // Verify paragraph marks
    const paragraphMarks = editor.text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");
    expect(paragraphMarks).toHaveLength(2);
  });

  it("should handle deleting text across paragraphs", () => {
    const text = CoRichText.createFromPlainText("First\nSecond", {
      owner: group,
    });
    text.insertMark(0, 4, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(6, 11, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    // Delete across paragraphs
    const tr = editor.view.state.tr;
    tr.delete(4, 9); // Delete "t\nSe"
    editor.view.dispatch(tr);

    // Verify content and marks
    expect(editor.getContent()).toBe("Fircond");
    const paragraphMarks = editor.text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");
    expect(paragraphMarks).toHaveLength(1);
  });

  it("should handle merging paragraphs with backspace", () => {
    const text = CoRichText.createFromPlainText("First\nSecond", {
      owner: group,
    });
    text.insertMark(0, 4, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(6, 11, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    expect(editor.getContent()).toBe("First\nSecond");

    // Delete newline with backspace
    const tr = editor.view.state.tr;
    tr.delete(6, 7); // Delete "\n" by removing both positions
    editor.view.dispatch(tr);

    // Verify content and marks
    expect(editor.getContent()).toBe("FirstSecond");
    const paragraphMarks = editor.text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");
    expect(paragraphMarks).toHaveLength(1);
  });

  it("should handle multiple marks on same text", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    // Add both strong and em marks to "Hell"
    const tr = editor.view.state.tr;
    tr.addMark(1, 5, schema.marks.strong.create());
    tr.addMark(1, 5, schema.marks.em.create());
    editor.view.dispatch(tr);

    // Verify marks
    const marks = editor.text.resolveMarks();
    const strongMarks = marks.filter((m) => m.sourceMark.tag === "strong");
    const emMarks = marks.filter((m) => m.sourceMark.tag === "em");

    expect(strongMarks).toHaveLength(1);
    expect(emMarks).toHaveLength(1);
    expect(strongMarks[0]!.startAfter).toBe(0);
    expect(strongMarks[0]!.endBefore).toBe(6);
    expect(emMarks[0]!.startAfter).toBe(0);
    expect(emMarks[0]!.endBefore).toBe(6);
  });

  it("should handle multi-line text deletion with backspace", () => {
    const text = CoRichText.createFromPlainText("First\nSecond\nThird", {
      owner: group,
    });
    // Add paragraph marks for each line
    text.insertMark(0, 4, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(6, 11, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(13, 17, Marks.Paragraph, { tag: "paragraph" });
    editor = new EditorTestHelper(text);

    // Select and delete across multiple lines
    const tr = editor.view.state.tr;
    tr.delete(4, 16); // Delete from "st\nSecond\nTh"
    editor.view.dispatch(tr);

    // Log the transaction steps and results
    console.log("Editor content after deletion:", editor.getContent());
    console.log(
      "Paragraph marks:",
      editor.text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "paragraph"),
    );

    // Verify content and marks
    expect(editor.getContent()).toBe("Firird");
    const paragraphMarks = editor.text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");
    expect(paragraphMarks).toHaveLength(1);
  });
});
