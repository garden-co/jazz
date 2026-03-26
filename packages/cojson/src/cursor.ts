import { RawCoID } from "./ids.js";
import { CoValueFrontier } from "./knownState.js";

export type RawCoValueCursor = {
  rootId: RawCoID;
  frontiers: Record<RawCoID, CoValueFrontier>;
};
