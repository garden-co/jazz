import { CoMap, Group, ID, coField } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { useEffect, useState } from "react";

export class ResumeSyncCoMap extends CoMap {
  value = coField.string;
}

function getIdParam() {
  const url = new URL(window.location.href);
  return (url.searchParams.get("id") as ID<ResumeSyncCoMap>) ?? undefined;
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

    const group = Group.create({ owner: me });

    group.addMember("everyone", "writer");

    setId(ResumeSyncCoMap.create({ value: "" }, { owner: group }).$jazz.id);
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
          coMap.value = e.target.value;
        }}
      />
    </div>
  );
}
