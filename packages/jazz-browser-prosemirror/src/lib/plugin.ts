import { CoRichText } from "jazz-tools";
import { Node } from "prosemirror-model";
import { EditorState, Plugin, PluginKey, Transaction } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { richTextToProsemirrorDoc } from "./document.js";

export const jazzPluginKey = new PluginKey("jazz");

// The update model is asymmetric, we can apply a CoRichText to a ProseMirror transaction, but we apply the ProseMirror document to CoRichText and let it work out what's changed
// This is because the CoRichText is a mutable object, and the ProseMirror transaction is an immutable object.
/*
 * Update cycle between ProseMirror and CoRichText:
 *
 *                 ┌─────────────────┐
 *                 │   ProseMirror   │
 *                 └────────┬────────┘
 *                          │
 *                     Transaction
 *                          │
 *                          ▼
 *                 ┌────────────────┐
 *                 │  CoRichText    │
 *                 └────────┬───────┘
 *                          │
 *                      Changes
 *                          │
 *                          ▼
 *                 ┌────────────────┐
 *                 │   ProseMirror  │
 *                 └────────────────┘
 *
 * 1. User makes edit in ProseMirror
 * 2. ProseMirror creates Transaction
 * 3. Transaction applied to CoRichText
 * 4. CoRichText emits changes
 * 5. Changes applied back to ProseMirror
 */

/**
 * Apply CoRichText to a ProseMirror transaction
 * Naively replaces the entire document content with the CoRichText content and marks
 * @param state - The current editor state
 * @param text - CoRichText to apply
 * @returns The transaction with the CoRichText applied
 */
export function applyRichTextToTransaction(
  state: EditorState,
  text: CoRichText,
): Transaction | undefined {
  // Convert CoRichText to ProseMirror doc (including marks)
  const doc = richTextToProsemirrorDoc(text);
  if (!doc) return;

  // Create transaction and replace entire document
  const tr = state.tr;

  // Replace the entire document with new content
  // This will preserve marks since they're part of the doc's content
  tr.replaceRangeWith(0, tr.doc.content.size, doc);

  return tr;
}

/**
 * Apply a ProseMirror transaction to a CoRichText
 * @param tr - ProseMirror transaction
 * @param text - CoRichText to apply
 */
export function applyDocToCoRichText(doc: Node, text: CoRichText) {
  text?.text?.applyDiff(doc.textBetween(0, doc.content.size, "\n"));
}

export function createJazzPlugin(text: CoRichText | undefined) {
  let view: EditorView | undefined;
  let isUpdating = false;

  text?.subscribe({ text: [], marks: [{}] }, (text) => {
    if (!view) return;
    isUpdating = true;
    const tr = applyRichTextToTransaction(view.state, text);
    if (tr) {
      view.dispatch(tr);
    }
    isUpdating = false;
  });

  return new Plugin({
    key: jazzPluginKey,

    view(editorView) {
      view = editorView;
      return {
        destroy() {
          view = undefined;
        },
      };
    },

    state: {
      init() {
        return {
          text,
        };
      },
      apply(tr, value) {
        if (tr.docChanged && !isUpdating && text) {
          applyDocToCoRichText(tr.doc, text);
        }
        return value;
      },
    },
  });
}
