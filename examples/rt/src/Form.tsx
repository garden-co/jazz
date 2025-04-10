import { useAccount } from "jazz-react";
import { exampleSetup } from "prosemirror-example-setup";
import {
  DOMParser as PMDOMParser,
  DOMSerializer as PMDOMSerializer,
  Slice,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef, useState } from "react";

export function Form() {
  const { me } = useAccount({ resolve: { profile: true, root: true } });

  const editorRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!me) return;

    const plugins = exampleSetup({ schema });

    const str = me.profile.bio?.toString() || "";
    const doc = new DOMParser().parseFromString(str, "text/html");
    const pmDoc = PMDOMParser.fromSchema(schema).parse(doc);

    console.log(str, doc, pmDoc);

    const view = new EditorView(editorRef.current!, {
      state: EditorState.create({
        schema,
        doc: pmDoc,
        plugins,
      }),
      dispatchTransaction: (transaction) => {
        if (transaction.getMeta("fromJazz")) {
          view.updateState(view.state.apply(transaction));
          return;
        }
        console.log(transaction);
        const doc = transaction.doc;
        const str = new XMLSerializer()
          .serializeToString(
            PMDOMSerializer.fromSchema(schema).serializeFragment(doc.content),
          )
          .replace(/\sxmlns="[^"]+"/g, "");
        console.log(str);

        if (transaction.docChanged) {
          me.profile.bio?.applyDiff(str);
        } else {
          view.updateState(view.state.apply(transaction));
        }
      },
    });

    me.profile.subscribe((profile) => {
      if (profile) {
        const doc = new DOMParser().parseFromString(
          profile.bio?.toString() || "",
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

    return () => {
      view.destroy();
    };
  }, [me]);

  if (!me) return null;

  return (
    <div className="grid gap-4 border p-8">
      <input
        type="text"
        className="border border-stone-300 rounded shadow-sm py-1 px-2 flex-1 font-mono"
        value={me.profile.bio?.toString()}
        onChange={(e) => me.profile.bio?.applyDiff(e.target.value)}
      />

      <div ref={editorRef} className="mx-auto w-full max-w-2xl" />
    </div>
  );
}
