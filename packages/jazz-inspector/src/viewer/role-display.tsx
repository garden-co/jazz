import { LocalNode, RawCoValue } from "cojson";
import { Text } from "../ui/text.js";
import { useResolvedCoValue } from "./use-resolve-covalue.js";

export function RoleDisplay({
  node,
  value,
}: { node: LocalNode; value: RawCoValue }) {
  const { snapshot } = useResolvedCoValue(value.group.id, node);

  if (!snapshot || snapshot === "unavailable") {
    return null;
  }

  let role;

  if (value.group.id == node.account.id) {
    role = "owner";
  } else if (snapshot[node.account.id]) {
    role = snapshot[node.account.id] as string;
  } else if (snapshot.everyone) {
    role = snapshot.everyone as string;
  } else {
    role = "unauthorized";
  }

  return <Text>Role: {role}</Text>;
}
