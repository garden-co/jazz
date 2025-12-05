import { isAccountRole, LocalNode } from "cojson";
import { TypeSym } from "./symbols.js";
import { CoValue } from "../internal.js";

export class AnonymousJazzAgent {
  [TypeSym] = "Anonymous" as const;
  constructor(public node: LocalNode) {}
  canWrite(_: CoValue) {
    return false;
  }
  canRead(value: CoValue): boolean {
    const valueOwner = value.$jazz.owner;
    if (!valueOwner) {
      // Groups and Accounts are public
      return true;
    }
    const role = valueOwner.getRoleOf("everyone");

    return isAccountRole(role);
  }
  canManage(_: CoValue) {
    return false;
  }
  canAdmin(_: CoValue) {
    return false;
  }
}
