import { CoRichText } from "jazz-tools";
import { Plugin, PluginKey } from "prosemirror-state";
import { Decoration, DecorationSet } from "prosemirror-view";
import { htmlToProseMirror } from "./converter.js";
import { createSyncHandlers } from "./sync.js";

/**
 * Configuration options for the Jazz plugin
 */
export interface JazzPluginConfig {
  /** Whether to show caret and selection decorations */
  showDecorations?: boolean;
}

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
 * @param config - Optional configuration for the plugin
 * @returns A ProseMirror plugin instance that can be added to an editor
 *
 * @example
 * ```typescript
 * const coRichText = new CoRichText({ text: "<p>Hello</p>", owner: account });
 * const plugin = createJazzPlugin(coRichText, { showDecorations: true });
 * const state = EditorState.create({
 *   schema,
 *   plugins: [plugin],
 * });
 * ```
 */
export function createJazzPlugin(
  coRichText: CoRichText | undefined,
  config: JazzPluginConfig = {},
) {
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
          const pmDoc = htmlToProseMirror(coRichText.toString(), state.schema);
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
        if (!config.showDecorations) {
          return null;
        }

        const selection = state.selection;

        if (selection.empty) {
          const caret = Decoration.widget(selection.from, () => {
            const div = document.createElement("span");
            div.className = "jazz-caret";
            div.style.borderLeft = "2px solid red";
            div.style.marginLeft = "-2px";
            div.style.backgroundColor = "rgba(255,0,0,0.1)";
            return div;
          });

          return DecorationSet.create(state.doc, [caret]);
        }

        const selectionDecoration = Decoration.inline(
          selection.from,
          selection.to,
          {
            class: "jazz-caret-selection",
            style:
              "border-left: 2px solid red; margin-left: -2px; background: rgba(255,0,0,0.1);",
          },
        );

        return DecorationSet.create(state.doc, [selectionDecoration]);
      },
    },
  });
}
