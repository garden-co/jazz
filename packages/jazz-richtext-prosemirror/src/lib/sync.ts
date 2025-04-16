import { CoRichText } from "jazz-tools";
import { Slice } from "prosemirror-model";
import { Transaction } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { htmlToProseMirror, proseMirrorToHtml } from "./converter.js";

// Metadata key to identify changes originating from Jazz to prevent infinite update loops
export const META_KEY = "fromJazz";

/**
 * Creates handlers for bidirectional synchronization between CoRichText and ProseMirror
 */
export function createSyncHandlers(coRichText: CoRichText | undefined) {
  // Store the editor view in a closure
  let view: EditorView | undefined;

  /**
   * Handles changes from CoRichText by updating the ProseMirror editor
   */
  function handleCoRichTextChange(newText: CoRichText) {
    if (!view || !newText) return;

    const pmDoc = htmlToProseMirror(newText.toString());
    const tr = view.state.tr.replace(
      0,
      view.state.doc.content.size,
      new Slice(pmDoc.content, 0, 0),
    );
    tr.setMeta(META_KEY, true);
    view.dispatch(tr);
  }

  /**
   * Handles changes from ProseMirror by updating the CoRichText
   */
  function handleProseMirrorChange(tr: Transaction) {
    if (!coRichText || tr.getMeta(META_KEY)) return;

    if (tr.docChanged) {
      const str = proseMirrorToHtml(tr.doc);
      coRichText.applyDiff(str);
    }
  }

  return {
    setView: (newView?: EditorView) => {
      view = newView;
    },
    handleCoRichTextChange,
    handleProseMirrorChange,
  };
}
