import { type RawCoValue } from "../coValue.js";
import { RawGroup } from "../coValues/group.js";

export function expectGroup(content: RawCoValue): RawGroup {
  if (content.type !== "comap") {
    throw new Error("Expected group");
  }

  if (content.core.verified.header.ruleset.type !== "group") {
    throw new Error("Expected group ruleset in group");
  }

  if (!(content instanceof RawGroup)) {
    throw new Error("Expected group");
  }

  return content;
}
