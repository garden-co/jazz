import { CoID, LocalNode, RawCoValue } from "cojson";
import {
  CoJsonType,
  ExtendedCoJsonType,
  useResolvedCoValue,
} from "./use-resolve-covalue.ts";

export const TypeIcon = ({
  type,
  extendedType,
}: {
  type: CoJsonType;
  extendedType?: ExtendedCoJsonType;
}) => {
  const iconMap: Record<ExtendedCoJsonType | CoJsonType, string> = {
    record: "{} Record",
    image: "🖼️ Image",
    comap: "{} CoMap",
    costream: "≋ CoStream",
    colist: "☰ CoList",
    account: "👤 Account",
    group: "👥 Group",
  };

  const iconKey = extendedType || type;
  const icon = iconMap[iconKey as keyof typeof iconMap];

  return icon ? <span style={{ fontFamily: "monospace" }}>{icon}</span> : null;
};

export const ResolveIcon = ({
  coId,
  node,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
}) => {
  const { type, extendedType, snapshot } = useResolvedCoValue(coId, node);

  if (snapshot === "unavailable" && !type) {
    return <div style={{ color: "#4B5563", fontWeight: 500 }}>Unavailable</div>;
  }

  if (!type)
    return (
      <div
        style={{ whiteSpace: "pre", width: "3.5rem", fontFamily: "monospace" }}
      >
        {" "}
      </div>
    );

  return <TypeIcon type={type} extendedType={extendedType} />;
};
