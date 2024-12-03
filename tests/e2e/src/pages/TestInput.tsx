import { CoMap, Group, ID, co } from "jazz-tools";
import { useEffect, useState } from "react";
import { useAccount, useCoState } from "../jazz";

export class InputTestCoMap extends CoMap {
  title = co.string;
}

export function TestInput() {
  const [id, setId] = useState<ID<InputTestCoMap> | undefined>(undefined);
  const coMap = useCoState(InputTestCoMap, id);
  const { me } = useAccount();

  useEffect(() => {
    if (!me || id) return;

    const group = Group.create({ owner: me });

    group.addMember("everyone", "writer");

    setId(InputTestCoMap.create({ title: "" }, { owner: group }).id);
  }, [me]);

  if (!coMap) return null;

  return (
    <input
      value={coMap?.title ?? ""}
      onChange={(e) => {
        if (!coMap) return;
        coMap.title = e.target.value;
      }}
    />
  );
}
