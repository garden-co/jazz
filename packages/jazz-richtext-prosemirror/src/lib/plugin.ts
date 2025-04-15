import { CoRichText } from "jazz-tools";
import {
  DOMParser as PMDOMParser,
  DOMSerializer as PMDOMSerializer,
  Slice,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { Plugin, PluginKey } from "prosemirror-state";
import { type EditorView } from "prosemirror-view";

export const jazzPluginKey = new PluginKey("jazz");

/**
 * Create a ProseMirror plugin that applies a CoRichText to the editor
 * Updates CoRichText when ProseMirror document changes
 * Updates ProseMirror when CoRichText changes
 * @param text - The CoRichText to apply
 * @returns The ProseMirror plugin
 */
export function createJazzPlugin(text: CoRichText | undefined) {
  let view: EditorView | undefined;

  return new Plugin({
    key: jazzPluginKey,

    view(editorView) {
      view = editorView;

      // Subscribe to CoRichText changes
      if (text) {
        text.subscribe((newText) => {
          if (view && newText) {
            const doc = new DOMParser().parseFromString(
              newText.toString(),
              "text/html",
            );
            const pmDoc = PMDOMParser.fromSchema(schema).parse(doc);
            const tr = view.state.tr.replace(
              0,
              view.state.doc.content.size,
              new Slice(pmDoc.content, 0, 0),
            );
            tr.setMeta("fromJazz", true);
            view.dispatch(tr);
          }
        });
      }

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
        if (tr.getMeta("fromJazz")) {
          return value;
        }

        if (tr.docChanged && text) {
          const doc = tr.doc;
          const str = new XMLSerializer()
            .serializeToString(
              PMDOMSerializer.fromSchema(schema).serializeFragment(doc.content),
            )
            .replace(/\sxmlns="[^"]+"/g, "");
          text.applyDiff(str);
        }

        return value;
      },
    },
  });
}
