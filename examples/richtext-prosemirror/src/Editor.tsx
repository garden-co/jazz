import { createJazzPlugin } from "jazz-tools/prosemirror";
import { useAccount, useCoState } from "jazz-tools/react";
import { exampleSetup } from "prosemirror-example-setup";
import { Schema } from "prosemirror-model";
import { schema as basicSchema } from "prosemirror-schema-basic";
import { addListNodes } from "prosemirror-schema-list";
import { EditorState } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { useEffect, useRef, useState } from "react";
import { JazzAccount, JazzProfile } from "./schema";
import { BranchManagement } from "./BranchManagement";

export function Editor() {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: { branches: true } },
  });
  const editorRef = useRef<HTMLDivElement>(null);

  const bioId = me.$isLoaded ? me.profile.$jazz.refs.bio.id : undefined;
  const [branch, setBranch] = useState<string | undefined>(undefined);
  const bio = useCoState(JazzProfile.shape.bio, bioId, {
    unstable_branch:
      branch && me.$isLoaded ? { name: branch, owner: me } : undefined,
  });

  const bioBranchName = bio.$isLoaded ? bio.$jazz.branchName : undefined;
  useEffect(() => {
    if (!editorRef.current || !bio.$isLoaded) {
      return;
    }

    const schema = new Schema({
      nodes: addListNodes(basicSchema.spec.nodes, "paragraph block*", "block"),
      marks: basicSchema.spec.marks,
    });

    const setupPlugins = exampleSetup({ schema });
    const jazzPlugin = createJazzPlugin(bio);

    // Only create the editor if it doesn't exist
    const view = new EditorView(editorRef.current, {
      state: EditorState.create({
        schema,
        plugins: [...setupPlugins, jazzPlugin],
      }),
    });

    return () => {
      view.destroy();
    };
  }, [bioId, bioBranchName]); // Only recreate if the account or the branch change

  if (!me.$isLoaded || !bio.$isLoaded) return null;

  return (
    <div className="flex flex-col">
      <div className="flex-1 flex flex-col gap-4 p-8">
        <BranchManagement
          currentBranch={branch}
          onBranchChange={setBranch}
          onBranchMerge={() => bio.$jazz.unstable_merge()}
        />
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
            value={`${bio}`}
            onChange={(e) => bio.$jazz.applyDiff(e.target.value)}
            rows={10}
          />
        </div>
      </div>
    </div>
  );
}
