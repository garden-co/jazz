import { CoValueCore } from "./coValueCore.js";
import { CoValueEntry } from "./coValueEntry.js";
import { RawCoID } from "./ids.js";

export class CoValuesStore {
  coValues = new Map<RawCoID, CoValueEntry>();

  get(id: RawCoID) {
    let entry = this.coValues.get(id);

    if (!entry) {
      entry = CoValueEntry.Unknown(id);
      this.coValues.set(id, entry);
    }

    return entry;
  }

  setAsAvailable(id: RawCoID, coValue: CoValueCore) {
    const entry = this.get(id);
    entry.dispatch({
      type: "available",
      coValue,
    });
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

  expectCoValueLoaded(id: RawCoID, expectation?: string): CoValueCore {
    const entry = this.get(id);

    if (entry.state.type !== "available") {
      throw new Error(
        `${expectation ? expectation + ": " : ""}CoValue ${id} not yet loaded. Current state: ${entry.state.type}`,
      );
    }
    return entry.state.coValue;
  }
}
