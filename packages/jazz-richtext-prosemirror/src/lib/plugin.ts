import { CoRichText } from "jazz-tools";
import { Plugin, PluginKey } from "prosemirror-state";
import { htmlToProseMirror } from "./converter.js";
import { createSyncHandlers } from "./sync.js";

// Create a unique key for the Jazz plugin to identify it in the ProseMirror state
export const jazzPluginKey = new PluginKey("jazz");

/**
 * Creates a ProseMirror plugin that synchronizes a CoRichText instance with a ProseMirror editor.
 */
export function createJazzPlugin(coRichText: CoRichText | undefined) {
  const { setView, handleCoRichTextChange, handleProseMirrorChange } =
    createSyncHandlers(coRichText);

  return new Plugin({
    key: jazzPluginKey,

    view(editorView) {
      setView(editorView);

      if (coRichText) {
        coRichText.subscribe(handleCoRichTextChange);
      }

      return {
        destroy() {
          setView(undefined);
        },
      };
    },

    state: {
      init(_config, state) {
        if (coRichText) {
          const pmDoc = htmlToProseMirror(coRichText.toString());
          state.doc = pmDoc;
        }
        return { coRichText };
      },

      apply(tr, value) {
        handleProseMirrorChange(tr);
        return value;
      },
    },
  });
}
