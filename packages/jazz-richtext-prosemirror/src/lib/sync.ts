import { recreateTransform } from "@manuscripts/prosemirror-recreate-steps";
import { CoRichText } from "jazz-tools";
import { Transaction } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { htmlToProseMirror, proseMirrorToHtml } from "./converter.js";

/**
 * Metadata key used to identify changes originating from Jazz.
 * This is used to prevent infinite update loops between CoRichText and ProseMirror.
 */
export const META_KEY = "fromJazz";

/**
 * Creates handlers for bidirectional synchronization between CoRichText and ProseMirror.
 *
 * This function returns a set of handlers that manage the synchronization between
 * a CoRichText instance and a ProseMirror editor. It handles:
 * - Updating the ProseMirror editor when CoRichText changes
 * - Updating CoRichText when the ProseMirror editor changes
 * - Preventing infinite update loops
 *
 * @param coRichText - The CoRichText instance to synchronize with
 * @returns An object containing the synchronization handlers:
 *   - setView: Function to set the current ProseMirror view
 *   - handleCoRichTextChange: Handler for CoRichText changes
 *   - handleProseMirrorChange: Handler for ProseMirror changes
 *
 * @example
 * ```typescript
 * const handlers = createSyncHandlers(coRichText);
 * handlers.setView(editorView);
 * ```
 */
export function createSyncHandlers(coRichText: CoRichText | undefined) {
  // Store the editor view in a closure
  let view: EditorView | undefined;

  /**
   * Handles changes from CoRichText by updating the ProseMirror editor.
   *
   * When CoRichText content changes, this function:
   * 1. Converts the new content to a ProseMirror document
   * 2. Creates a transaction to replace the editor's content
   * 3. Marks the transaction as coming from Jazz to prevent loops
   * 4. Dispatches the transaction to update the editor
   *
   * @param newText - The updated CoRichText instance
   */
  function handleCoRichTextChange(newText: CoRichText) {
    if (!view || !newText) return;

    const pmDoc = htmlToProseMirror(newText.toString());
    const transform = recreateTransform(view.state.doc, pmDoc);

    // Create a new transaction
    const tr = view.state.tr;

    // Apply all steps from the transform to the transaction
    transform.steps.forEach((step) => {
      tr.step(step);
    });

    tr.setMeta(META_KEY, true);
    view.dispatch(tr);
  }

  /**
   * Handles changes from ProseMirror by updating the CoRichText.
   *
   * When the ProseMirror editor content changes, this function:
   * 1. Checks if the change originated from Jazz (to prevent loops)
   * 2. Converts the new content to HTML
   * 3. Updates the CoRichText with the new content
   *
   * @param tr - The ProseMirror transaction representing the change
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
