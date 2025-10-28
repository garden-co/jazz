import { RawUnknownCoValue } from "./coValue.js";
import type { AvailableCoValueCore } from "./coValueCore/coValueCore.js";
import { RawAccount } from "./coValues/account.js";
import { RawCoList } from "./coValues/coList.js";
import { RawCoMap } from "./coValues/coMap.js";
import { RawCoPlainText } from "./coValues/coPlainText.js";
import { RawBinaryCoStream, RawCoStream } from "./coValues/coStream.js";
import { RawGroup } from "./coValues/group.js";

export function coreToCoValue(
  core: AvailableCoValueCore,
  options?: { ignorePrivateTransactions: true },
) {
  if (core.verified.header.type === "comap") {
    if (core.verified.header.ruleset.type === "group") {
      if (
        core.verified.header.meta?.type === "account" &&
        !options?.ignorePrivateTransactions
      ) {
        return new RawAccount(core);
      } else {
        return new RawGroup(core, options);
      }
    } else {
      return new RawCoMap(core);
    }
  } else if (core.verified.header.type === "coplaintext") {
    return new RawCoPlainText(core);
  } else if (core.verified.header.type === "colist") {
    return new RawCoList(core);
  } else if (core.verified.header.type === "costream") {
    if (
      core.verified.header.meta &&
      core.verified.header.meta.type === "binary"
    ) {
      return new RawBinaryCoStream(core);
    } else {
      return new RawCoStream(core);
    }
  } else {
    return new RawUnknownCoValue(core);
  }
}
