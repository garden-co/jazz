import { CoRichText } from "jazz-tools";
import { Plugin, PluginKey } from "prosemirror-state";
import { Decoration, DecorationSet } from "prosemirror-view";
import { htmlToProseMirror } from "./converter.js";
import { createSyncHandlers } from "./sync.js";

/**
 * Unique key for the Jazz plugin to identify it in the ProseMirror state.
 * This key is used to access the plugin's state and view from the editor.
 */
export const jazzPluginKey = new PluginKey("jazz");

/**
 * Creates a ProseMirror plugin that synchronizes a CoRichText instance with a ProseMirror editor.
 *
 * This plugin enables bidirectional synchronization between a CoRichText instance
 * and a ProseMirror editor. It handles:
 * - Initializing the editor with CoRichText content
 * - Updating the editor when CoRichText changes
 * - Updating CoRichText when the editor changes
 * - Managing the editor view lifecycle
 *
 * @param coRichText - The CoRichText instance to synchronize with the editor.
 *                     If undefined, the plugin will still work but won't sync with CoRichText.
 * @returns A ProseMirror plugin instance that can be added to an editor
 *
 * @example
 * ```typescript
 * const coRichText = new CoRichText({ text: "<p>Hello</p>", owner: account });
 * const plugin = createJazzPlugin(coRichText);
 * const state = EditorState.create({
 *   schema,
 *   plugins: [plugin],
 * });
 * ```
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

    props: {
      decorations(state) {
        const selection = state.selection;
        if (selection.empty) return DecorationSet.empty;

        const caret = Decoration.inline(selection.from, selection.to, {
          class: "jazz-caret",
          style:
            "border-left: 2px solid red; margin-left: -2px; background: rgba(255,0,0,0.1);",
        });

        return DecorationSet.create(state.doc, [caret]);
      },
    },
  });
}
