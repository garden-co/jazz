import { createJazzPlugin } from "jazz-tools/prosemirror";
import { useAccount } from "jazz-tools/react";
import { exampleSetup } from "prosemirror-example-setup";
import { Schema } from "prosemirror-model";
import { schema as basicSchema } from "prosemirror-schema-basic";
import { addListNodes } from "prosemirror-schema-list";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef } from "react";
import { JazzAccount } from "./schema";

export function Editor() {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: { bio: true }, root: true },
  });
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!me || !editorRef.current || !me.profile.bio) return;

    const schema = new Schema({
      nodes: addListNodes(basicSchema.spec.nodes, "paragraph block*", "block"),
      marks: basicSchema.spec.marks,
    });

    const setupPlugins = exampleSetup({ schema });
    const jazzPlugin = createJazzPlugin(me.profile.bio);

    // Only create the editor if it doesn't exist
    if (!viewRef.current) {
      viewRef.current = new EditorView(editorRef.current, {
        state: EditorState.create({
          schema,
          plugins: [...setupPlugins, jazzPlugin],
        }),
      });
    }

    return () => {
      if (viewRef.current) {
        viewRef.current.destroy();
        viewRef.current = null;
      }
    };
  }, [me?.id, me?.profile.bio?.id]); // Only recreate if the account or the bio changes

  if (!me) return null;

  return (
    <div className="flex flex-col">
      <div className="flex-1 flex flex-col gap-4 p-8">
        <div className="flex-1 flex flex-col gap-2">
          <label className="text-sm font-medium text-stone-600">Richtext</label>
          <div
            ref={editorRef}
            className="border border-stone-200 rounded shadow-sm h-[200px] p-2"
          />
        </div>

        <div className="flex-1 flex flex-col gap-2">
          <label className="text-sm font-medium text-stone-600">
            Plaintext
          </label>
          <textarea
            className="flex-1 border border-stone-200 rounded shadow-sm py-2 px-3 font-mono text-sm bg-stone-50 text-stone-900 whitespace-pre-wrap break-words resize-none"
            value={`${me.profile.bio}`}
            onChange={(e) => me.profile.bio?.applyDiff(e.target.value)}
            rows={10}
          />
        </div>
      </div>
    </div>
  );
}
