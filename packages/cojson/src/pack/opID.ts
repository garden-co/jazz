import type { OpID } from "../coValues/coList.js";
import { RawCoID, SessionID } from "../exports.js";

export function packOpID(opID: OpID): string {
  return `${opID.sessionID}:${opID.txIndex}:${opID.changeIdx}:${opID.branch ?? ""}`;
}

export function unpackOpID(opID: string | OpID): OpID {
  if (typeof opID === "object") {
    return opID;
  }

  const [sessionID, txIndex, changeIdx, branch] = opID.split(":") as [
    SessionID,
    `${number}`,
    `${number}`,
    RawCoID | "",
  ];

  return {
    sessionID,
    txIndex: parseInt(txIndex, 10),
    changeIdx: parseInt(changeIdx, 10),
    branch: branch ? branch : undefined,
  };
}
