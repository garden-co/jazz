import { CoRichText, Group, Marks, ResolvedMark } from "jazz-tools";
import { CoRichTextDebug } from "jazz-tools/src/coValues/coRichText.js";
import { createJazzTestAccount } from "jazz-tools/testing";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { describe, expect, it, vi } from "vitest";
import { applyTrToRichText, richTextToProsemirrorDoc } from "../index.js";
import {
  handleParagraphMerge,
  handleParagraphSplit,
  handleTextDeletion,
  handleTextInsertion,
} from "../lib/transactions.js";

describe("applyTrToRichText", async () => {
  const account = await createJazzTestAccount();
  const group = Group.create({ owner: account });

  it("should handle text insertion", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;
    tr.insertText(" test", text.length);

    applyTrToRichText(text, tr);
    expect(text.toString()).toBe("Hello world test");
  });

  it("should handle text deletion", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;
    tr.delete(6, 11); // Delete " world"

    applyTrToRichText(text, tr);
    expect(text.toString()).toBe("Hello");
  });

  it("should handle adding strong mark", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;
    tr.addMark(6, 11, schema.marks.strong.create());

    applyTrToRichText(text, tr);
    const marks = text.resolveMarks();
    expect(marks.length).toBe(2); // Paragraph + Strong
    expect((marks[1] as any).tag).toBe("strong");
  });

  it("should handle removing strong mark", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(6, 11, Marks.Strong, { tag: "strong" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;
    tr.removeMark(6, 11, schema.marks.strong);

    applyTrToRichText(text, tr);
    const marks = text.resolveMarks();
    expect(marks.length).toBe(1); // Just Paragraph
    expect((marks[0] as ResolvedMark).sourceMark.tag).toBe("paragraph");
  });

  it("should handle paragraph splits", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;
    tr.split(6); // Split at space between Hello and world

    applyTrToRichText(text, tr);
    const marks = text.resolveMarks();
    expect(marks.length).toBe(2); // Two paragraphs
    expect((marks[0] as ResolvedMark).sourceMark.tag).toBe("paragraph");
    expect((marks[1] as ResolvedMark).sourceMark.tag).toBe("paragraph");
  });

  it("should handle multiple operations in single transaction", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;

    // Multiple operations: insert text, add mark, remove text
    tr.insertText(" test", text.length);
    tr.addMark(0, 5, schema.marks.strong.create());
    tr.delete(6, 11); // Delete " world"

    applyTrToRichText(text, tr);
    expect(text.toString()).toBe("Hello test");
    const marks = text.resolveMarks();
    expect(
      (marks.find((m: ResolvedMark) => m?.sourceMark.tag === "strong") as any)
        .tag,
    ).toBeDefined();
  });

  it("should handle text insertion at document start", () => {
    const text = CoRichText.createFromPlainText("world", {
      owner: group,
    });
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;

    // Insert "Hello " at the start
    tr.insertText("Hello ", 0);
    applyTrToRichText(text, tr);

    // Verify content
    expect(text.toString()).toBe("Hello world");

    // Verify paragraph mark is preserved and spans the entire content
    const paragraphMarks = text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");
    expect(paragraphMarks).toHaveLength(1);
    expect(paragraphMarks[0]!.startBefore).toBe(0);
    expect(paragraphMarks[0]!.endAfter).toBe(text.length - 1);
  });

  it("should warn on unsupported mark type", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;

    const consoleSpy = vi.spyOn(console, "warn");

    // Create a custom mark that's not in our MARK_TYPE_LOOKUP
    const customMark = schema.mark("code");
    tr.addMark(0, 5, customMark);

    applyTrToRichText(text, tr);

    expect(consoleSpy).toHaveBeenCalledWith(
      "Unsupported mark type",
      expect.anything(),
    );
    consoleSpy.mockRestore();
  });

  it("should handle complex mark removal", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(0, 7, Marks.Strong, { tag: "strong" });
    text.insertMark(5, 11, Marks.Em, { tag: "em" });

    CoRichTextDebug.log(text, { label: "Before merge" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;

    // Remove strong mark from the overlapping region
    tr.removeMark(5, 7, schema.marks.strong);

    applyTrToRichText(text, tr);

    CoRichTextDebug.log(text, { label: "After merge" });

    const marks = text.resolveMarks();
    const strongMarks = marks.filter(
      (m: ResolvedMark) => m?.sourceMark.tag === "strong",
    );
    const emMarks = marks.filter(
      (m: ResolvedMark) => m?.sourceMark.tag === "em",
    );

    // Should still have both types of marks
    expect(strongMarks.length).toBeGreaterThan(0);
    expect(emMarks.length).toBeGreaterThan(0);

    // Get resolved marks which have numeric positions
    const resolvedMarks = text.resolveMarks();
    const resolvedStrongMarks = resolvedMarks.filter(
      (m) => m.sourceMark.tag === "strong",
    );

    // Strong mark should now end at position 4 (covering "Hello")
    expect(resolvedStrongMarks[0]!.endAfter).toBe(4);
  });

  it("should handle paragraph splits correctly", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    // Add initial paragraph mark
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
    // Add a strong mark to part of the text
    text.insertMark(0, 4, Marks.Strong, { tag: "strong" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;

    // Split at position 6 (after "Hello ")
    tr.split(6);
    applyTrToRichText(text, tr);

    // Verify content is unchanged
    expect(text.toString()).toBe("Hello \nworld");

    // Verify paragraph marks
    const paragraphMarks = text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");
    expect(paragraphMarks).toHaveLength(2);
    expect(paragraphMarks[0]!.endAfter).toBe(4); // First paragraph ends after "Hello"
    expect(paragraphMarks[1]!.startBefore).toBe(5); // Second paragraph starts before "world"

    // Verify strong mark is preserved
    const strongMarks = text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "strong");
    expect(strongMarks).toHaveLength(1);
    expect(strongMarks[0]!.startBefore).toBe(0);
    expect(strongMarks[0]!.endAfter).toBe(4);
  });
});

describe("Helper Functions", async () => {
  const account = await createJazzTestAccount();
  const group = Group.create({ owner: account });

  describe("handleTextInsertion", () => {
    it("should insert text at document start", () => {
      const text = CoRichText.createFromPlainText("world", { owner: group });
      handleTextInsertion(text, 0, "Hello ");
      expect(text.toString()).toBe("Hello world");
    });

    it("should insert text in middle of document", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: group,
      });
      handleTextInsertion(text, 5, " beautiful");
      expect(text.toString()).toBe("Hello beautiful world");
    });

    it("should insert text at document end", () => {
      const text = CoRichText.createFromPlainText("Hello", { owner: group });
      handleTextInsertion(text, 5, " world");
      expect(text.toString()).toBe("Hello world");
    });
  });

  describe("handleTextDeletion", () => {
    it("should delete text from middle of document", () => {
      const text = CoRichText.createFromPlainText("Hello, hello world", {
        owner: group,
      });
      handleTextDeletion(text, 6, 12); // Delete ", hello"
      expect(text.toString()).toBe("Hello world");
    });

    it("should delete text from start of document", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: group,
      });
      handleTextDeletion(text, 1, 5); // Delete "Hello"
      expect(text.toString()).toBe(" world");
    });

    it("should delete text from end of document", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: group,
      });
      handleTextDeletion(text, 7, 11); // Delete "world"
      expect(text.toString()).toBe("Hello ");
    });

    it("should preserve paragraph mark when all text is deleted", () => {
      const text = CoRichText.createFromPlainText("Hello", {
        owner: group,
      });
      text.insertMark(0, text.length - 1, Marks.Paragraph, {
        tag: "paragraph",
      });
      text.insertMark(0, 4, Marks.Strong, { tag: "strong" });

      // Delete all text
      handleTextDeletion(text, 0, text.length);

      // Verify content is empty
      expect(text.toString()).toBe("");

      // Verify paragraph mark is preserved
      const marks = text.resolveMarks();
      expect(marks.length).toBe(1);
      expect(marks[0]!.sourceMark.tag).toBe("paragraph");
      expect(marks[0]!.startBefore).toBe(0);
      expect(marks[0]!.endAfter).toBe(0);
    });
  });

  describe("handleParagraphSplit", () => {
    it("should split single paragraph into two", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: group,
      });
      text.insertMark(0, text.length - 1, Marks.Paragraph, {
        tag: "paragraph",
      });

      handleParagraphSplit(text, 5); // Split after "Hello"

      const marks = text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "paragraph");
      expect(marks).toHaveLength(2);
      expect(marks[0]!.endAfter).toBe(4); // First paragraph ends after "Hello"
      expect(marks[1]!.startBefore).toBe(5); // Second paragraph starts before "world"
    });

    it("should preserve other marks when splitting", () => {
      const text = CoRichText.createFromPlainText("Hello world", {
        owner: group,
      });
      text.insertMark(0, text.length - 1, Marks.Paragraph, {
        tag: "paragraph",
      });
      text.insertMark(0, 4, Marks.Strong, { tag: "strong" }); // "Hello" is strong

      handleParagraphSplit(text, 5); // Split after "Hello"

      const strongMarks = text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "strong");
      expect(strongMarks).toHaveLength(1);
      expect(strongMarks[0]!.startBefore).toBe(0);
      expect(strongMarks[0]!.endAfter).toBe(4);
    });
  });

  describe("handleParagraphMerge", () => {
    it("should merge two paragraphs into one", () => {
      const text = CoRichText.createFromPlainText("Hello\nworld", {
        owner: group,
      });
      text.insertMark(0, 4, Marks.Paragraph, { tag: "paragraph" }); // "Hello"
      text.insertMark(6, 10, Marks.Paragraph, { tag: "paragraph" }); // "world"

      handleParagraphMerge(text, 4); // Merge at newline

      const marks = text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "paragraph");
      expect(marks).toHaveLength(1);
      expect(marks[0]!.startBefore).toBe(0);
      expect(marks[0]!.endAfter).toBe(9); // "Helloworld".length
    });

    it("should preserve other marks when merging", () => {
      const text = CoRichText.createFromPlainText("Hello\nworld", {
        owner: group,
      });
      text.insertMark(0, 4, Marks.Paragraph, { tag: "paragraph" });
      text.insertMark(6, 10, Marks.Paragraph, { tag: "paragraph" });
      text.insertMark(0, 5, Marks.Strong, { tag: "strong" }); // "Hello" is strong
      text.insertMark(6, 10, Marks.Em, { tag: "em" }); // "world" is em

      CoRichTextDebug.log(text, { label: "Before merge" });

      handleParagraphMerge(text, 4); // Merge at newline

      const paragraphs = text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "paragraph");
      const strongMarks = text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "strong");
      const emMarks = text
        .resolveMarks()
        .filter((m) => m.sourceMark.tag === "em");

      CoRichTextDebug.log(text, { label: "After merge" });

      expect(strongMarks).toHaveLength(1);
      expect(strongMarks[0]!.startBefore).toBe(0);
      expect(strongMarks[0]!.endAfter).toBe(5);

      expect(emMarks).toHaveLength(1);
      expect(emMarks[0]!.startBefore).toBe(5);
      expect(emMarks[0]!.endAfter).toBe(9);

      expect(paragraphs).toHaveLength(1);
      expect(paragraphs[0]!.startBefore).toBe(0);
      expect(paragraphs[0]!.endAfter).toBe(9);
    });
  });
});
