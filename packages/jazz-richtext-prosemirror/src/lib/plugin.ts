import { CoRichText } from "jazz-tools";
import {
  DOMParser as PMDOMParser,
  DOMSerializer as PMDOMSerializer,
  Slice,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { Plugin, PluginKey } from "prosemirror-state";
import { type EditorView } from "prosemirror-view";

// Create a unique key for the Jazz plugin to identify it in the ProseMirror state
export const jazzPluginKey = new PluginKey("jazz");

// Metadata key to identify changes originating from Jazz to prevent infinite update loops
const META_KEY = "fromJazz";

/**
 * Creates a ProseMirror document from a string of HTML content
 * @param content - The HTML content to parse
 * @returns A ProseMirror document
 */
function createProseMirrorDoc(content: string) {
  const doc = new DOMParser().parseFromString(content, "text/html");
  return PMDOMParser.fromSchema(schema).parse(doc);
}

/**
 * Creates a ProseMirror plugin that synchronizes a CoRichText instance with a ProseMirror editor.
 * This plugin handles bidirectional updates:
 * - When CoRichText changes, updates the ProseMirror editor
 * - When ProseMirror editor changes, updates the CoRichText
 *
 * @param coRichText - The CoRichText instance to synchronize with the editor
 * @returns A ProseMirror plugin instance
 */
export function createJazzPlugin(coRichText: CoRichText | undefined) {
  // Store a reference to the ProseMirror editor view
  let view: EditorView | undefined;

  /**
   * Handles changes from CoRichText by updating the ProseMirror editor
   * @param newText - The updated CoRichText content
   */
  function handleCoRichTextChange(newText: CoRichText) {
    if (!view) return;
    if (!newText) return;

    console.log("handleCoRichTextChange", newText);

    // Convert CoRichText HTML content to ProseMirror document
    const pmDoc = createProseMirrorDoc(newText.toString());

    // Create a transaction to replace the entire document content
    const tr = view.state.tr.replace(
      0,
      view.state.doc.content.size,
      new Slice(pmDoc.content, 0, 0),
    );
    // Mark the transaction as originating from Jazz to prevent update loops
    tr.setMeta(META_KEY, true);
    view.dispatch(tr);
  }

  return new Plugin({
    key: jazzPluginKey,

    /**
     * Plugin view lifecycle method that sets up the editor view and subscriptions
     */
    view(editorView) {
      view = editorView;

      // Initialize the editor with the current CoRichText content
      if (coRichText && view) {
        const pmDoc = createProseMirrorDoc(coRichText.toString());

        const tr = editorView.state.tr.replace(
          0,
          0,
          new Slice(pmDoc.content, 0, 0),
        );
        tr.setMeta(META_KEY, true);
        editorView.dispatch(tr);

        // Subscribe to CoRichText changes to keep the editor in sync
        coRichText?.subscribe(handleCoRichTextChange);
      }

      return {
        // Cleanup when the editor is destroyed
        destroy() {
          view = undefined;
        },
      };
    },

    /**
     * Plugin state management
     */
    state: {
      // Initialize plugin state with the CoRichText instance
      init() {
        return {
          coRichText,
        };
      },

      /**
       * Apply state changes and handle document updates
       * @param tr - The transaction being applied
       * @param value - Current plugin state
       * @returns Updated plugin state
       */
      apply(tr, value) {
        // Skip updates that originated from Jazz to prevent infinite loops
        if (tr.getMeta(META_KEY)) {
          return value;
        }

        // If the document changed and we have a CoRichText instance, update it
        if (tr.docChanged && coRichText) {
          const doc = tr.doc;
          // Convert ProseMirror document to HTML string
          const str = new XMLSerializer()
            .serializeToString(
              PMDOMSerializer.fromSchema(schema).serializeFragment(doc.content),
            )
            .replace(/\sxmlns="[^"]+"/g, "");
          // Apply the changes to CoRichText
          coRichText.applyDiff(str);
        }

        return value;
      },
    },
  });
}
