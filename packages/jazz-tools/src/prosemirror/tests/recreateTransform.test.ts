// @vitest-environment jsdom

import { describe, expect, it } from "vitest";
import { Schema } from "prosemirror-model";
import { schema as basicSchema } from "prosemirror-schema-basic";
import { addListNodes } from "prosemirror-schema-list";
import { DOMParser as ProseMirrorDOMParser } from "prosemirror-model";
import { recreateTransform } from "../lib/recreateTransform";

const schema = new Schema({
  nodes: addListNodes(basicSchema.spec.nodes, "paragraph block*", "block"),
  marks: basicSchema.spec.marks,
});

function htmlToDoc(html: string) {
  const dom = new DOMParser().parseFromString(html, "text/html");
  return ProseMirrorDOMParser.fromSchema(schema).parse(dom.body);
}

function applyAndCompare(fromHtml: string, toHtml: string) {
  const fromDoc = htmlToDoc(fromHtml);
  const toDoc = htmlToDoc(toHtml);
  const tr = recreateTransform(fromDoc, toDoc);
  expect(tr.doc.eq(toDoc)).toBe(true);
  return tr;
}

describe("recreateTransform", () => {
  it("returns no steps for identical documents", () => {
    const doc = htmlToDoc("<p>Hello</p>");
    const tr = recreateTransform(doc, doc);
    expect(tr.steps).toHaveLength(0);
    expect(tr.doc.eq(doc)).toBe(true);
  });

  it("handles appending text", () => {
    const tr = applyAndCompare("<p>Hello</p>", "<p>Hello World</p>");
    expect(tr.steps).toHaveLength(1);
  });

  it("handles prepending text", () => {
    const tr = applyAndCompare("<p>World</p>", "<p>Hello World</p>");
    expect(tr.steps).toHaveLength(1);
  });

  it("handles text deletion", () => {
    const tr = applyAndCompare("<p>Hello World</p>", "<p>Hello</p>");
    expect(tr.steps).toHaveLength(1);
  });

  it("handles replacing text in the middle", () => {
    applyAndCompare("<p>Hello World</p>", "<p>Hello Jazz</p>");
  });

  it("handles complete text replacement", () => {
    applyAndCompare("<p>Hello</p>", "<p>Goodbye</p>");
  });

  it("handles empty to non-empty", () => {
    applyAndCompare("<p></p>", "<p>Hello</p>");
  });

  it("handles non-empty to empty", () => {
    applyAndCompare("<p>Hello</p>", "<p></p>");
  });

  it("handles adding a paragraph", () => {
    applyAndCompare("<p>First</p>", "<p>First</p><p>Second</p>");
  });

  it("handles removing a paragraph", () => {
    applyAndCompare("<p>First</p><p>Second</p>", "<p>First</p>");
  });

  it("handles changes in a middle paragraph", () => {
    applyAndCompare(
      "<p>One</p><p>Two</p><p>Three</p>",
      "<p>One</p><p>Changed</p><p>Three</p>",
    );
  });

  it("handles paragraph to list structural change", () => {
    applyAndCompare("<p>Item one</p>", "<ol><li><p>Item one</p></li></ol>");
  });

  it("handles list to paragraph structural change", () => {
    applyAndCompare("<ul><li><p>Item</p></li></ul>", "<p>Item</p>");
  });

  it("handles adding bold mark", () => {
    applyAndCompare("<p>Hello</p>", "<p><strong>Hello</strong></p>");
  });

  it("handles removing bold mark", () => {
    applyAndCompare("<p><strong>Hello</strong></p>", "<p>Hello</p>");
  });

  it("handles changing marks from bold to italic", () => {
    applyAndCompare("<p><strong>Hello</strong></p>", "<p><em>Hello</em></p>");
  });

  it("handles nested mark changes", () => {
    applyAndCompare(
      "<p>A <strong>bold</strong> word</p>",
      "<p>A <strong><em>bold</em></strong> word</p>",
    );
  });

  it("handles emoji content", () => {
    applyAndCompare("<p>Hello</p>", "<p>Hello 🌍</p>");
  });

  it("handles multi-byte unicode", () => {
    applyAndCompare("<p>café</p>", "<p>naïve café</p>");
  });

  it("produces a valid transform that can be applied", () => {
    const fromDoc = htmlToDoc("<p>Before</p>");
    const toDoc = htmlToDoc("<p>After</p>");
    const tr = recreateTransform(fromDoc, toDoc);

    for (const step of tr.steps) {
      const result = step.apply(fromDoc);
      expect(result.failed).toBeNull();
    }
  });

  // Tests adapted from @manuscripts/prosemirror-recreate-steps test suite.
  // Our implementation produces a single ReplaceStep rather than granular
  // addMark/removeMark/replaceAround steps, so we verify the resulting
  // document matches rather than asserting exact step shapes.

  describe("mark diffs (adapted from original library)", () => {
    it("adds em to inline text", () => {
      applyAndCompare(
        "<p>Before textitalicAfter text</p>",
        "<p>Before text<em>italic</em>After text</p>",
      );
    });

    it("removes strong from inline text", () => {
      applyAndCompare(
        "<p>Before text<strong>bold</strong>After text</p>",
        "<p>Before textboldAfter text</p>",
      );
    });

    it("adds em and strong simultaneously", () => {
      applyAndCompare(
        "<p>Before textitalic/boldAfter text</p>",
        "<p>Before text<strong><em>italic/bold</em></strong>After text</p>",
      );
    });

    it("replaces em with strong", () => {
      applyAndCompare(
        "<p>Before text<em>styled</em>After text</p>",
        "<p>Before text<strong>styled</strong>After text</p>",
      );
    });

    it("replaces em with strong in different regions", () => {
      applyAndCompare(
        "<p>Before text<em>styledAfter text</em></p>",
        "<p><strong>Before textstyled</strong>After text</p>",
      );
    });
  });

  describe("structural diffs (adapted from original library)", () => {
    it("wraps paragraph in blockquote", () => {
      applyAndCompare(
        "<p>A quoted sentence</p>",
        "<blockquote><p>A quoted sentence</p></blockquote>",
      );
    });

    it("unwraps paragraph from blockquote", () => {
      applyAndCompare(
        "<blockquote><p>A quoted sentence</p></blockquote>",
        "<p>A quoted sentence</p>",
      );
    });

    it("changes heading level", () => {
      applyAndCompare("<h1>A title</h1>", "<h2>A title</h2>");
    });
  });

  describe("text diffs (adapted from original library)", () => {
    it("replaces text in a single node", () => {
      applyAndCompare(
        "<blockquote><p>The start text</p></blockquote>",
        "<blockquote><p>The end text</p></blockquote>",
      );
    });

    it("replaces text across multiple nodes", () => {
      applyAndCompare(
        "<blockquote><p>The start text</p><p>The second text</p></blockquote>",
        "<blockquote><p>The end text</p><p>The second sentence</p></blockquote>",
      );
    });

    it("replaces multiple words in a single text node", () => {
      applyAndCompare(
        "<blockquote><p>The cat is barking at the house</p></blockquote>",
        "<blockquote><p>The dog is meauwing in the ship</p></blockquote>",
      );
    });
  });

  describe("combined content and structure changes (adapted from original library)", () => {
    it("changes both heading type and paragraph content", () => {
      applyAndCompare(
        "<h1>The title</h1><p>The fish are <em>great!</em></p>",
        "<h2>A different title</h2><p>A <strong>different</strong> sentence.</p>",
      );
    });

    it("restructures from heading+paragraph to paragraphs", () => {
      applyAndCompare(
        "<h1>The title</h1><p>The fish are <em>great!</em></p>",
        "<p>Yet another <em>first</em> line.</p><p>With a second line that is not styled.</p>",
      );
    });
  });
});
