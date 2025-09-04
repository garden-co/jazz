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

export function Editor() {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: true },
  });
  const editorRef = useRef<HTMLDivElement>(null);

  const [branch, setBranch] = useState<string | undefined>(undefined);
  const bio = useCoState(
    JazzProfile.shape.bio,
    me?.profile.$jazz.refs.bio?.id,
    {
      unstable_branch: branch ? { name: branch } : undefined,
    },
  );

  const branches = bio?.$jazz.raw.core.branches.map((b) => b.name);

  useEffect(() => {
    if (!editorRef.current || !bio) return;

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
  }, [bio?.$jazz.id, bio?.$jazz.branchName]); // Only recreate if the account or the bio changes

  function handleSetBranch(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();

    const data = new FormData(e.currentTarget);
    const branch = data.get("branch");

    if (!branch || typeof branch !== "string") return;

    setBranch(branch);
  }

  function handleMergeBranch() {
    if (!branch) return;
    bio?.$jazz.unstable_merge();
    setBranch(undefined);
  }

  function handleSelectBranch(e: React.ChangeEvent<HTMLSelectElement>) {
    const selectedBranch = e.target.value;
    if (selectedBranch === "") {
      setBranch(undefined);
    } else {
      setBranch(selectedBranch);
    }
  }

  if (!me || !bio) return null;

  return (
    <div className="flex flex-col">
      <div className="flex-1 flex flex-col gap-4 p-8">
        <div className="flex flex-col gap-4">
          {/* Branch Selection */}
          <div className="flex flex-col gap-2">
            <label className="text-sm font-medium text-stone-600">
              Branch Management
            </label>
            <div className="flex gap-2 items-center">
              <select
                value={branch || ""}
                onChange={handleSelectBranch}
                className="border border-stone-200 rounded shadow-sm py-2 px-3 font-mono text-sm bg-stone-50 text-stone-900"
              >
                <option value="">Main branch</option>
                {branches?.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
                {branch && (
                  <option value={branch} selected={true}>
                    {branch}
                  </option>
                )}
              </select>
              {branch && (
                <button
                  className="bg-green-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded"
                  type="button"
                  onClick={handleMergeBranch}
                >
                  Merge Branch
                </button>
              )}
            </div>
          </div>

          {/* Create New Branch */}
          {!bio.$jazz.isBranch && (
            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium text-stone-600">
                Create New Branch
              </label>
              <form className="flex gap-2" onSubmit={handleSetBranch}>
                <input
                  type="text"
                  name="branch"
                  required
                  placeholder="Enter branch name..."
                  className="border border-stone-200 rounded shadow-sm py-2 px-3 font-mono text-sm bg-stone-50 text-stone-900"
                />
                <button
                  type="submit"
                  className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
                >
                  Create Branch
                </button>
              </form>
            </div>
          )}
        </div>
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
