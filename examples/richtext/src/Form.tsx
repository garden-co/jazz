import { useAccount } from "jazz-react";
import { createJazzPlugin } from "jazz-richtext-prosemirror";
import { exampleSetup } from "prosemirror-example-setup";
import { DOMParser as PMDOMParser } from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef } from "react";

export function Form() {
  const { me } = useAccount({ resolve: { profile: true, root: true } });

  const editorRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!me) return;

    const setupPlugins = exampleSetup({ schema });

    const str = me.profile.bio?.toString() || "";
    const doc = new DOMParser().parseFromString(str, "text/html");
    const pmDoc = PMDOMParser.fromSchema(schema).parse(doc);

    const jazzPlugin = createJazzPlugin(me.profile.bio!);
    const view = new EditorView(editorRef.current!, {
      state: EditorState.create({
        schema,
        doc: pmDoc,
        plugins: [...setupPlugins, jazzPlugin],
      }),
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
