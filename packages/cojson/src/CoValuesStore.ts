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

    return entry;
  }

  getEntries() {
    return this.coValues.entries();
  }

  getValues() {
    return this.coValues.values();
  }

  getOrderedIds() {
    const coValues = new Set<RawCoID>();

    // TODO test it thoroughly
    for (const entry of this.getValues()) {
      this.getOrderedDependencies(entry.id, coValues);
    }

    return Array.from(coValues);
  }

  getKeys() {
    return this.coValues.keys();
  }

  private getOrderedDependencies(id: RawCoID, coValues: Set<RawCoID>) {
    const entry = this.get(id);
    const coValue = this.expectCoValueLoaded(entry.id);

    if (coValues.has(coValue.id)) {
      return coValues;
    }
    for (const id of coValue.getDependedOnCoValues()) {
      this.getOrderedDependencies(id, coValues);
    }
    coValues.add(coValue.id);

    return coValues;
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
