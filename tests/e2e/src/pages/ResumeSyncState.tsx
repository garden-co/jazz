import { co, z } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { useEffect, useState } from "react";

export const ResumeSyncCoMap = co
  .map({
    value: z.string(),
  })
  .withPermissions({
    onCreate: (group) => group.makePublic("writer"),
  });

function getIdParam() {
  const url = new URL(window.location.href);
  return url.searchParams.get("id") ?? undefined;
}

export function ResumeSyncState() {
  const [id, setId] = useState(getIdParam);
  const coMap = useCoState(ResumeSyncCoMap, id);
  const me = useAccount();

  useEffect(() => {
    if (id) {
      const url = new URL(window.location.href);
      url.searchParams.set("id", id);
      history.pushState({}, "", url.toString());
    }
  }, [id]);

  useEffect(() => {
    if (!me.$isLoaded || id) return;

    const map = ResumeSyncCoMap.create({ value: "" });

    setId(map.$jazz.id);
  }, [me]);

  if (!coMap.$isLoaded) return null;

  return (
    <div>
      <h1>Resume Sync State</h1>
      <p data-testid="id">{coMap.$jazz.id}</p>
      <label htmlFor="value">Value</label>
      <input
        id="value"
        value={coMap.value}
        onChange={(e) => {
          coMap.$jazz.set("value", e.target.value);
        }}
      />
    </div>
  );
}
