import { CoID, LocalNode, RawCoValue, RawGroup } from "cojson";
import { useEffect, useState } from "react";
import { Button } from "../ui/button.js";
import { resolveCoValue, useResolvedCoValue } from "./use-resolve-covalue.js";

export function AccountOrGroupText({
  coId,
  node,
  showId = false,
  onClick,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  showId?: boolean;
  onClick?: (name?: string) => void;
}) {
  const { snapshot, extendedType, value } = useResolvedCoValue(coId, node);
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
  }, [snapshot, node, extendedType]);

  if (!snapshot) return <span>Loading...</span>;
  if (extendedType !== "account" && extendedType !== "group") {
    return <span>CoID is not an account or group</span>;
  }

  const groupName =
    extendedType === "group" && value && "name" in value
      ? (value as RawGroup).name
      : undefined;
  const displayName =
    extendedType === "account" ? name || "Account" : groupName || "Group";
  const displayText = showId ? `${displayName} <${coId}>` : displayName;
  const icon = extendedType === "account" ? "👤" : "👥";
  const iconAlt = extendedType === "account" ? "Account" : "Group";

  const content = (
    <span
      style={{ display: "inline-flex", alignItems: "center", gap: "0.35rem" }}
    >
      <span role="img" aria-label={iconAlt} title={iconAlt}>
        {icon}
      </span>
      {displayText}
    </span>
  );

  if (onClick) {
    return (
      <Button variant="link" onClick={() => onClick(displayName)}>
        {content}
      </Button>
    );
  }

  return content;
}
