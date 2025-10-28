import { LocalNode } from "cojson";
import { TypeSym } from "./symbols.js";

export class AnonymousJazzAgent {
  [TypeSym] = "Anonymous" as const;
  constructor(public node: LocalNode) {}
}
