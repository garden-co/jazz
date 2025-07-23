import { LocalNode, RawCoValue } from "cojson";
import { Text } from "../ui/text.js";
import { useResolvedCoValue } from "./use-resolve-covalue.js";

export function RoleDisplay({
  node,
  value,
}: { node: LocalNode; value: RawCoValue }) {
  const { snapshot } = useResolvedCoValue(value.group.id, node);

  if (!snapshot.value || snapshot.value === "unavailable") {
    return null;
  }

  let role;

  if (value.group.id == node.getCurrentAgent().id) {
    role = "owner";
  } else if ((snapshot.value as any)[node.getCurrentAgent().id]) {
    role = (snapshot.value as any)[node.getCurrentAgent().id] as string;
  } else if ((snapshot.value as any).everyone) {
    role = (snapshot.value as any).everyone as string;
  } else {
    role = "unauthorized";
  }

  return <Text>Role: {role}</Text>;
}
