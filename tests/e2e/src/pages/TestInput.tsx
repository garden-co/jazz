import { CoMap, Group, ID, coField } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { useEffect, useState } from "react";

export class InputTestCoMap extends CoMap {
  title = coField.string;
}

export function TestInput() {
  const [id, setId] = useState<ID<InputTestCoMap> | undefined>(undefined);
  const coMap = useCoState(InputTestCoMap, id);
  const { me } = useAccount();

  useEffect(() => {
    if (!me || id) return;

    const group = Group.create({ owner: me });

    group.addMember("everyone", "writer");

    setId(InputTestCoMap.create({ title: "" }, { owner: group }).$jazz.id);
  }, [me]);

  if (!coMap) return null;

  return (
    <input
      value={coMap?.title ?? ""}
      onChange={(e) => {
        if (!coMap) return;
        coMap.$jazz.set("title", e.target.value);
      }}
    />
  );
}
