import { CoID, LocalNode, RawCoValue } from "cojson";
import { styled } from "goober";
import {
  CoJsonType,
  ExtendedCoJsonType,
  useResolvedCoValue,
} from "./use-resolve-covalue.js";

const IconText = styled("span")`
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
`;

const UnavailableText = styled("div")`
  font-weight: 500;
`;

const EmptySpace = styled("div")`
  white-space: pre;
  width: 3.5rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
`;

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
    coplaintext: "📄 CoPlainText",
  };

  const iconKey = extendedType || type;
  const icon = iconMap[iconKey as keyof typeof iconMap];

  return icon ? <IconText>{icon}</IconText> : null;
};

export const ResolveIcon = ({
  coId,
  node,
}: {
  coId: CoID<RawCoValue>;
  node: LocalNode;
}) => {
  const { type, extendedType, snapshot } = useResolvedCoValue(coId, node);

  if (snapshot.value === "unavailable" && !type.value) {
    return <UnavailableText>Unavailable</UnavailableText>;
  }

  if (!type.value) return <EmptySpace> </EmptySpace>;

  return <TypeIcon type={type.value} extendedType={extendedType.value} />;
};
