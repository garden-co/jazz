import { CoValueCore } from "./coValueCore.js";
import { CoValueState } from "./coValueState.js";
import { RawCoID } from "./ids.js";
import { PeerID } from "./sync.js";

export class CoValuesStore {
  coValues = new Map<RawCoID, CoValueState>();

  get(id: RawCoID) {
    let entry = this.coValues.get(id);

    if (!entry) {
      entry = new CoValueState(id);
      this.coValues.set(id, entry);
    }

    return entry;
  }

  markAsAvailable(id: RawCoID, coValue: CoValueCore, fromPeerId: PeerID) {
    const entry = this.get(id);
    entry.markAvailable(coValue, fromPeerId);
  }

  internalMarkMagicallyAvailable(id: RawCoID, coValue: CoValueCore) {
    const entry = this.get(id);
    entry.internalMarkMagicallyAvailable(coValue);
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
