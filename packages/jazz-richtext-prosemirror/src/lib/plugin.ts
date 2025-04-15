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
const META_KEY = "fromJazz";

/**
 * Create a ProseMirror plugin that applies a CoRichText to the editor
 * Updates CoRichText when ProseMirror document changes
 * Updates ProseMirror when CoRichText changes
 * @param coRichText - The CoRichText to apply
 * @returns The ProseMirror plugin
 */
export function createJazzPlugin(coRichText: CoRichText | undefined) {
  let view: EditorView | undefined;

  function handleCoRichTextChange(newText: CoRichText) {
    if (!view) return;
    if (!newText) return;

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
    tr.setMeta(META_KEY, true);
    view.dispatch(tr);
  }

  return new Plugin({
    key: jazzPluginKey,

    view(editorView) {
      view = editorView;

      // Subscribe to CoRichText changes

      coRichText?.subscribe(handleCoRichTextChange);

      return {
        destroy() {
          view = undefined;
        },
      };
    },

    state: {
      init() {
        return {
          coRichText,
        };
      },

      apply(tr, value) {
        if (tr.getMeta(META_KEY)) {
          return value;
        }

        if (tr.docChanged && coRichText) {
          const doc = tr.doc;
          const str = new XMLSerializer()
            .serializeToString(
              PMDOMSerializer.fromSchema(schema).serializeFragment(doc.content),
            )
            .replace(/\sxmlns="[^"]+"/g, "");
          coRichText.applyDiff(str);
        }

        return value;
      },
    },
  });
}
