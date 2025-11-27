import * as react from "react";
import { co, z } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
const note = co.plainText().create("Meeting notes");
import { createJazzPlugin } from "jazz-tools/prosemirror";
import { exampleSetup } from "prosemirror-example-setup";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef } from "react";

const JazzProfile = co.profile({
  bio: co.richText(),
});

const JazzAccount = co.account({
  profile: JazzProfile,
  root: co.map({}),
});

// #region JSX
<>
  <p>{note.toString()}</p>
  <p>{note}</p>
</>;
// #endregion

// #region TextEditor
function TextEditor({ textId }: { textId: string }) {
  const note = useCoState(co.plainText(), textId);

  return (
    note.$isLoaded && (
      <textarea
        value={note.toString()}
        onChange={(e) => {
          // Efficiently update only what the user changed
          note.$jazz.applyDiff(e.target.value);
        }}
      />
    )
  );
}
// #endregion

// #region RichTextEditor
function RichTextEditor() {
  const me = useAccount(JazzAccount, { resolve: { profile: { bio: true } } });
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const bio = me.$isLoaded ? me.profile.bio : undefined;

  useEffect(() => {
    if (!bio || !editorRef.current) return;
    // Create the Jazz plugin for ProseMirror
    // Providing a co.richText() instance to the plugin to automatically sync changes
    const jazzPlugin = createJazzPlugin(bio); // [!code ++]

    // Set up ProseMirror with the Jazz plugin
    if (!viewRef.current) {
      viewRef.current = new EditorView(editorRef.current, {
        state: EditorState.create({
          schema,
          plugins: [
            ...exampleSetup({ schema }),
            jazzPlugin, // [!code ++]
          ],
        }),
      });
    }

    return () => {
      if (viewRef.current) {
        viewRef.current.destroy();
        viewRef.current = null;
      }
    };
  }, [bio?.$jazz.id]);

  if (!me.$isLoaded) return null;

  return (
    <div className="rounded border">
      <div ref={editorRef} className="p-2" />
    </div>
  );
}
// #endregion
