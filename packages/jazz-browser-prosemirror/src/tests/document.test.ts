import { CoRichText, Group, Marks, TreeLeaf, TreeNode } from "jazz-tools";
import { CoRichTextDebug } from "jazz-tools/src/coValues/coRichText.js";
import { createJazzTestAccount } from "jazz-tools/testing";
import { schema } from "prosemirror-schema-basic";
import { describe, expect, it } from "vitest";
import { richTextToProsemirrorDoc } from "../index.js";
import { collectInlineMarks } from "../lib/document.js";
import { MARK_TYPE_LOOKUP, NODE_TYPE_LOOKUP } from "../lib/types.js";
import { debugDoc } from "../lib/utils/debug.js";

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
    text.insertMark(0, 6, Marks.Strong, { tag: "strong" }); // "Hello w"
    text.insertMark(5, 10, Marks.Em, { tag: "em" }); // " world"

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

  it("should handle overlapping marks with correct text splitting", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
    text.insertMark(0, 4, Marks.Strong, { tag: "strong" }); // "Hello"
    text.insertMark(5, 10, Marks.Em, { tag: "em" }); // " world"

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();

    const paragraph = doc?.firstChild;
    expect(paragraph?.childCount).toBe(2);

    // First part: "Hello" with strong
    expect(paragraph?.child(0).text).toBe("Hello");
    expect(paragraph?.child(0).marks.map((m) => m.type.name)).toEqual([
      "strong",
    ]);

    // Second part: " world" with em
    expect(paragraph?.child(1).text).toBe(" world");
    expect(paragraph?.child(1).marks.map((m) => m.type.name)).toEqual(["em"]);
  });

  it("should preserve paragraph mark when all text is deleted", () => {
    const text = CoRichText.createFromPlainText("Hello", {
      owner: group,
    });
    text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });

    // Create an empty text and replace the content
    const emptyText = CoRichText.createFromPlainText("", {
      owner: group,
    });
    text.text = emptyText.text;

    const doc = richTextToProsemirrorDoc(text);
    expect(doc).toBeDefined();

    // Verify paragraph node exists and is empty
    const paragraph = doc?.firstChild;
    expect(paragraph?.type.name).toBe("paragraph");
    expect(paragraph?.textContent).toBe("");
  });

  it("should handle nodes with multiple children", () => {
    const text = CoRichText.createFromPlainText("Hello world", {
      owner: group,
    });
    text.insertMark(0, 4, Marks.Strong, { tag: "strong" }); // "Hello"
    text.insertMark(6, 10, Marks.Strong, { tag: "strong" }); // "world"

    const tree = text.toTree([
      ...Object.keys(NODE_TYPE_LOOKUP),
      ...Object.keys(MARK_TYPE_LOOKUP),
    ]);

    // Test for strong node
    const firstNode = tree.children[0] as TreeNode;
    const result1 = collectInlineMarks(text.toString(), firstNode, []);
    expect(result1.type.name).toBe("text");
    expect(result1.text).toBe("Hello");
    expect(result1.marks).toHaveLength(1);
    expect(result1.marks[0]?.type.name).toBe("strong");

    // Test for no mark
    const secondNode = tree.children[1] as TreeNode;
    const result2 = collectInlineMarks(text.toString(), secondNode, []);
    expect(result2.type.name).toBe("text");
    expect(result2.text).toBe(" ");
    expect(result2.marks).toHaveLength(0);

    // Test for second strong node
    const thirdNode = tree.children[2] as TreeNode;
    const result3 = collectInlineMarks(text.toString(), thirdNode, []);
    expect(result3.type.name).toBe("text");
    expect(result3.text).toBe("world");
    expect(result3.marks).toHaveLength(1);
    expect(result3.marks[0]?.type.name).toBe("strong");
  });
});

describe("collectInlineMarks", () => {
  it("should handle leaf nodes", () => {
    const leaf: TreeLeaf = {
      type: "leaf",
      start: 0,
      end: 4,
    };
    const result = collectInlineMarks("Hello world", leaf, []);
    expect(result.text).toBe("Hello");
    expect(result.marks).toHaveLength(0);
  });

  it("should handle single mark nodes", () => {
    const node: TreeNode = {
      type: "node",
      tag: "strong",
      start: 0,
      end: 4,
      children: [
        {
          type: "leaf",
          start: 0,
          end: 4,
        },
      ],
    };
    const result = collectInlineMarks("Hello world", node, []);
    expect(result.text).toBe("Hello");
    expect(result.marks).toHaveLength(1);
    expect(result.marks[0]?.type.name).toBe("strong");
  });

  it("should handle nested mark nodes", () => {
    const node: TreeNode = {
      type: "node",
      tag: "strong",
      start: 0,
      end: 4,
      children: [
        {
          type: "node",
          tag: "em",
          start: 0,
          end: 4,
          children: [
            {
              type: "leaf",
              start: 0,
              end: 4,
            },
          ],
        },
      ],
    };
    const result = collectInlineMarks("Hello world", node, []);
    expect(result.text).toBe("Hello");
    expect(result.marks).toHaveLength(2);
    expect(result.marks.map((m) => m.type.name).sort()).toEqual([
      "em",
      "strong",
    ]);
  });

  it("should handle paragraph nodes", () => {
    const node: TreeNode = {
      type: "node",
      tag: "paragraph",
      start: 0,
      end: 4,
      children: [
        {
          type: "leaf",
          start: 0,
          end: 4,
        },
      ],
    };
    const result = collectInlineMarks("Hello world", node, []);
    expect(result.text).toBe("Hello");
    expect(result.marks).toHaveLength(0);
  });

  it("should handle nodes with multiple children", () => {
    const node: TreeNode = {
      type: "node",
      tag: "strong",
      start: 0,
      end: 10,
      children: [
        {
          type: "leaf",
          start: 0,
          end: 4,
        },
        {
          type: "leaf",
          start: 5,
          end: 6,
        },
        {
          type: "leaf",
          start: 7,
          end: 10,
        },
      ],
    };
    const result = collectInlineMarks("Hello world", node, []);
    expect(result.type.name).toBe("text");
    expect(result.text).toBe("Hello world");
    expect(result.marks).toHaveLength(1);
    expect(result.marks[0]?.type.name).toBe("strong");
  });
});
