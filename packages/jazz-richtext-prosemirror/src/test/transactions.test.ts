import { CoRichText, Group, Marks, ResolvedMark } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/src/testing.js";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { describe, expect, it, vi } from "vitest";
import { applyTrToRichText, richTextToProsemirrorDoc } from "../index.js";

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
    tr.insertText(" test");

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
    tr.delete(7, 12); // Delete "world"

    applyTrToRichText(text, tr);
    expect(text.toString()).toBe("Hello ");
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
    tr.insertText(" test");
    tr.addMark(0, 5, schema.marks.strong.create());
    tr.delete(7, 13); // Delete "world"

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
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;
    tr.insertText("Hello ", 0);

    applyTrToRichText(text, tr);
    expect(text.toString()).toBe("Hello world");
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

    const doc = richTextToProsemirrorDoc(text)!;
    const state = EditorState.create({ doc, schema });
    const tr = state.tr;

    // Remove strong mark from the overlapping region
    tr.removeMark(5, 7, schema.marks.strong);

    applyTrToRichText(text, tr);

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

    // Strong mark should now end at position 4 (0-based index) (covering "Hell")
    expect(resolvedStrongMarks[0]!.endAfter).toBe(3);
  });
});
