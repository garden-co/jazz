import { CoRichText, Group, Marks } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { describe, expect, it } from "vitest";
import {
  applyDocToCoRichText,
  applyRichTextToTransaction,
} from "../lib/plugin.js";

const account = await createJazzTestAccount();
const group = Group.create({ owner: account });

describe("ProseMirror transform functions", () => {
  describe("applyRichTextToTransaction", () => {
    it("should create a transaction that updates the document content", () => {
      // Create initial state
      const state = EditorState.create({
        schema,
        doc: schema.node("doc", undefined, [
          schema.node("paragraph", undefined, [schema.text("Initial text")]),
        ]),
      });

      // Create CoRichText with new content
      const text = CoRichText.createFromPlainTextAndMark(
        "New content",
        Marks.Paragraph,
        {
          tag: "paragraph",
        },
        {
          owner: group,
        },
      );

      // Apply transformation
      const transaction = applyRichTextToTransaction(state, text);
      expect(transaction).toBeDefined();

      const newState = state.apply(transaction!);
      expect(newState.doc.textContent).toBe("New content");
    });
  });

  describe("applyDocToCoRichText", () => {
    it("should update CoRichText content based on document", () => {
      // Create document
      const doc = schema.node("doc", null, [
        schema.node("paragraph", null, [schema.text("Updated content")]),
      ]);

      // Create CoRichText
      const text = CoRichText.createFromPlainText("Initial content", {
        owner: group,
      });

      // Apply transformation
      applyDocToCoRichText(doc, text);

      expect(text.text?.toString()).toBe("Updated content");
    });
  });
});
