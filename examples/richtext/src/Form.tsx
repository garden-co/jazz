import { useAccount } from "jazz-react";
import { createJazzPlugin } from "jazz-richtext-prosemirror";
import { exampleSetup } from "prosemirror-example-setup";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef } from "react";

export function Form() {
  const { me } = useAccount({ resolve: { profile: true, root: true } });
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!me || !editorRef.current || !me.profile.bio) return;

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
