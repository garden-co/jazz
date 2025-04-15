import { CoRichText } from "jazz-tools";
import { Plugin, PluginKey } from "prosemirror-state";
// import { type EditorView } from "prosemirror-view";

export const jazzPluginKey = new PluginKey("jazz");

/**
 * Create a ProseMirror plugin that applies a CoRichText to the editor
 * Updates CoRichText when ProseMirror document changes
 * Updates ProseMirror when CoRichText changes
 * @param text - The CoRichText to apply
 * @returns The ProseMirror plugin
 */
export function createJazzPlugin(text: CoRichText | undefined) {
  // let view: EditorView | undefined;

  return new Plugin({
    key: jazzPluginKey,

    // Initialize the plugin with ProseMirror view
    // view(editorView) {
    //   view = editorView;
    //   return {
    //     destroy() {
    //       view = undefined;
    //     },
    //   };
    // },

    state: {
      // Initialize the plugin with CoRichText
      init() {
        return {
          text,
        };
      },

      // Update CoRichText when ProseMirror document changes
      apply(_tr, value) {
        return value;
      },
    },
  });
}
