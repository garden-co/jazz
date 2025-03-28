import { CoID, LocalNode, RawCoValue } from "cojson";
import {
  CoJsonType,
  ExtendedCoJsonType,
  useResolvedCoValue,
} from "./use-resolve-covalue.js";

import { classNames } from "../utils.js";

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
    costream: "≋ CoFeed",
    colist: "☰ CoList",
    account: "👤 Account",
    group: "👥 Group",
    file: "📃 FileStream",
  };

  const iconKey = extendedType || type;
  const icon = iconMap[iconKey as keyof typeof iconMap];

  return icon ? <span className={classNames("font-mono")}>{icon}</span> : null;
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
    return (
      <div className={classNames("text-gray-600 font-medium")}>Unavailable</div>
    );
  }

  if (!type)
    return <div className={classNames("whitespace-pre w-14 font-mono")}> </div>;

  return <TypeIcon type={type} extendedType={extendedType} />;
};
