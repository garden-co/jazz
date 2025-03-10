import { CoValueCore } from "./coValueCore.js";
import { RawCoID } from "./ids.js";
import { LocalNode } from "./localNode.js";

export class CoValuesStore {
  node: LocalNode;
  coValues = new Map<RawCoID, CoValueCore>();

  constructor(node: LocalNode) {
    this.node = node;
  }

  getIfExists(id: RawCoID) {
    return this.coValues.get(id);
  }

  getOrCreateEmpty(id: RawCoID) {
    let entry = this.coValues.get(id);

    if (!entry) {
      entry = new CoValueCore(id, undefined, this.node);
      this.coValues.set(id, entry);
    }

    return entry;
  }

  setExpectNonExisting(id: RawCoID, coValue: CoValueCore) {
    if (this.coValues.has(id)) {
      throw new Error("CoValue already exists");
    }
    this.coValues.set(id, coValue);
  }

  getEntries() {
    return this.coValues.entries();
  }

  getValues() {
    return this.coValues.values();
  }

  getKeys() {
    return this.coValues.keys();
  }
}
