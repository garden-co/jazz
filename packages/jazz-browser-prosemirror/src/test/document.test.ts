import { CoRichText, Group, Marks } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { describe, expect, it } from "vitest";
import { richTextToProsemirrorDoc } from "../index.js";

describe("richTextToProsemirrorDoc", async () => {
  const account = await createJazzTestAccount();
  const group = Group.create({ owner: account });

  it("should convert plain text to prosemirror doc", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();
    expect(doc?.textContent).toBe("Hello world");
    expect(doc?.type.name).toBe("doc");
    expect(doc?.firstChild?.type.name).toBe("paragraph");
  });

  it("should handle strong marks", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(6, 11, Marks.Strong, { tag: "strong" });

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();
    expect(doc?.textContent).toBe("Hello world");
    expect(doc?.firstChild?.childCount).toBe(2);
    expect(doc?.firstChild?.child(1).marks[0]!.type.name).toBe("strong");
  });

  it("should handle em marks", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(0, 5, Marks.Em, { tag: "em" });

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();
    expect(doc?.textContent).toBe("Hello world");
    expect(doc?.firstChild?.childCount).toBe(2);
    expect(doc?.firstChild?.child(0).marks[0]!.type.name).toBe("em");
  });

  it("should handle empty document", () => {
    const text = CoRichText.createFromPlainText("", {
      owner: group,
    });
    text.insertMark(0, 0, Marks.Paragraph, { tag: "paragraph" });

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();
    expect(doc?.textContent).toBe("");
    expect(doc?.firstChild?.type.name).toBe("paragraph");
  });

  it("should handle multiple marks on same text", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(0, 5, Marks.Strong, { tag: "strong" });
    text.insertMark(0, 5, Marks.Em, { tag: "em" });

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();
    expect(doc?.firstChild?.child(0).marks).toHaveLength(2);
    expect(
      doc?.firstChild
        ?.child(0)
        .marks.map((m) => m.type.name)
        .sort(),
    ).toEqual(["em", "strong"]);
  });

  it("should handle undefined input", () => {
    const doc = richTextToProsemirrorDoc(undefined as any);
    expect(doc).toBeUndefined();
  });

  it("should handle overlapping marks", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(0, 7, Marks.Strong, { tag: "strong" }); // "Hello w"
    text.insertMark(5, 11, Marks.Em, { tag: "em" }); // " world"

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();

    // Text should be split into three parts:
    // 1. "Hello" (strong only)
    // 2. " w" (both strong and em)
    // 3. "orld" (em only)
    const paragraph = doc?.firstChild;
    expect(paragraph?.childCount).toBe(3);

    // First part: "Hello" with strong
    expect(paragraph?.child(0).text).toBe("Hello");
    expect(paragraph?.child(0).marks.map((m) => m.type.name)).toEqual([
      "strong",
    ]);

    // Second part: " w" with both strong and em
    expect(paragraph?.child(1).text).toBe(" w");
    expect(
      paragraph
        ?.child(1)
        .marks.map((m) => m.type.name)
        .sort(),
    ).toEqual(["em", "strong"]);

    // Third part: "orld" with em
    expect(paragraph?.child(2).text).toBe("orld");
    expect(paragraph?.child(2).marks.map((m) => m.type.name)).toEqual(["em"]);
  });
});
