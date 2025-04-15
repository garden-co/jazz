import { LocalNode, RawAccount, RawCoValue } from "cojson";
import { CoID } from "cojson";
import { useEffect, useState } from "react";
import { resolveCoValue, useResolvedCoValue } from "./use-resolve-covalue.js";

export function AccountNameDisplay({
  accountId,
  node,
}: {
  accountId: CoID<RawAccount>;
  node: LocalNode;
}) {
  const { snapshot } = useResolvedCoValue(accountId, node);
  const [name, setName] = useState<string | null>(null);

  useEffect(() => {
    if (snapshot && typeof snapshot === "object" && "profile" in snapshot) {
      const profileId = snapshot.profile as CoID<RawCoValue>;
      resolveCoValue(profileId, node).then((profileResult) => {
        if (
          profileResult.snapshot &&
          typeof profileResult.snapshot === "object" &&
          "name" in profileResult.snapshot
        ) {
          setName(profileResult.snapshot.name as string);
        }
      });
    }
  }, [snapshot, node]);

  return name ? `${name} <${accountId}>` : accountId;
}
