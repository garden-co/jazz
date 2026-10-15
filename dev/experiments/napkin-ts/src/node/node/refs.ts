// Refs: where a row image a node holds comes from (SPEC 4 §4.7).
import { PhysicalCurrentClass } from "../../_extern/node-state.js";

/** `Local` is this node's working state; `Origin` is Core's settled state. */
export type Ref = "Local" | "Origin";

const GLOBAL_STACK: readonly Ref[] = ["Origin"];
const LOCAL_STACK: readonly Ref[] = ["Local", "Origin"];

/** The refs a read at `tier` stacks, top first. */
export const refStack = (tier: "Global" | "Local" | "None"): readonly Ref[] =>
  tier === "Global" ? GLOBAL_STACK : LOCAL_STACK;

/** The physical current class that stores this ref's layer. */
export const refPhysicalClass = (r: Ref) =>
  r === "Local" ? PhysicalCurrentClass.Ahead : PhysicalCurrentClass.Global;
