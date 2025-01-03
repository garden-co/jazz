import { useCoState } from "jazz-react";
import { ID } from "jazz-tools";
import { useEffect, useMemo, useState } from "react";
import { Issue } from "./schema.ts";

function DescriptionVersionHistory({ id }: { id: ID<Issue> }) {
  const issue = useCoState(Issue, id);
  const [version, setVersion] = useState<any | undefined>();
  const [isVersionLatest, setIsVersionLatest] = useState(true);
  const edits = useMemo(() => {
    if (!issue) return [];
    return issue._edits.description.all.reverse();
  }, [issue?._edits]);

  useEffect(() => {
    if (!version) {
      setVersion(edits[0]);
      setIsVersionLatest(true);
    }
  }, [edits]);

  if (!issue) return <div>Loading...</div>;

  const selectVersion = (version: any, isLatest: boolean) => {
    setVersion(version);
    setIsVersionLatest(isLatest);
  };

  return (
    <div>
      <h2 className="mb-3">Description version history</h2>
      <div className="grid grid-cols-3 border">
        {version && (
          <div className="col-span-2 border-r p-3 flex flex-col justify-between">
            <p>{version.value}</p>

            {!isVersionLatest && (
              <button
                className="bg-black text-white py-1 px-2 rounded"
                onClick={() => (issue.description = version.value)}
              >
                Restore
              </button>
            )}
          </div>
        )}
        <div className="flex flex-col gap-1 p-2 max-h-96 overflow-y-auto">
          {edits.map((edit, i) => (
            <button
              key={i}
              className="text-xs text-left p-2 hover:bg-stone-100"
              onClick={() => selectVersion(edit, i === 0)}
            >
              {i == 0 ? "(Latest)" : ""}
              <div className="font-medium">{edit.madeAt.toLocaleString()}</div>
              <div className="text-stone-500">{edit.by?.profile?.name}</div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

export function IssueVersionHistory({ id }: { id: ID<Issue> }) {
  const issue = useCoState(Issue, id);

  const edits = useMemo(() => {
    if (!issue) return [];

    return [
      ...issue._edits.title.all,
      ...issue._edits.estimate.all,
      ...issue._edits.status.all,
    ].sort((a, b) => (a.madeAt < b.madeAt ? -1 : a.madeAt > b.madeAt ? 1 : 0));
  }, [issue?._edits]);

  return (
    <>
      <div className="flex flex-col text-sm gap-2">
        <h2 className="sr-only">Edits</h2>
        {edits.map((edit, i) => (
          <div>
            <p className="text-xs text-stone-400">
              {edit.madeAt.toLocaleString()}
            </p>
            <p className="text-stone-600" key={i}>
              <span className="font-medium text-stone-800">
                {edit.by?.profile?.name}
              </span>{" "}
              changed{" "}
              <span className="font-medium text-stone-800">{edit.key}</span> to{" "}
              <span className="font-medium text-stone-800">{edit.value}</span>
            </p>
          </div>
        ))}
      </div>

      <hr />

      <DescriptionVersionHistory id={id} />
    </>
  );
}
