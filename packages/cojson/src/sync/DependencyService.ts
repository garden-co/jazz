import {
  CoValueCore,
  CoValueHeader,
  getDependedOnFromContent,
} from "../coValueCore.js";
import { CoValueAvailableState, CoValueEntry } from "../coValueEntry.js";
import { SyncManager } from "../sync.js";
import { LoadService } from "./LoadService.js";
import {
  CoValueContent,
  DataMessageHandlerInput,
  PushMessageHandlerInput,
} from "./types.js";

export class DependencyService {
  constructor(
    private syncManager: SyncManager,
    private loadService: LoadService,
  ) {}

  private async getUnknownDependencies(
    input: DataMessageHandlerInput | PushMessageHandlerInput,
  ) {
    const { msg, entry } = input;
    const isAvailable = entry.state.type === "available";
    if (!msg.header && !isAvailable) {
      throw new Error(`Cannot get dependencies without header ${msg.id}`);
    }

    const availableCoValue = isAvailable
      ? (entry.state as CoValueAvailableState).coValue
      : null;

    const header = availableCoValue ? availableCoValue.header : msg.header;
    const dependencies = new Set([
      ...getDependedOnFromContent({
        ...msg,
        header,
      } as Required<CoValueContent>),
      ...(availableCoValue ? availableCoValue.getDependedOnCoValues() : []),
    ]);

    const unknownDependencies: CoValueEntry[] = [];
    for (const id of dependencies) {
      const entry = this.syncManager.local.coValuesStore.get(id);
      if (entry.state.type === "loading") {
        await entry.getCoValue();
      }
      if (entry.state.type !== "available") {
        unknownDependencies.push(entry);
      }
    }

    return unknownDependencies;
  }

  async loadUnknownDependencies(
    input: DataMessageHandlerInput | PushMessageHandlerInput,
  ) {
    const unknownDependencies = await this.getUnknownDependencies(input);

    // load dependencies one by one as they can depend on each other
    for await (const dependency of unknownDependencies) {
      await this.loadService.loadCoValue(dependency);
    }
  }

  private createCoValue(header: CoValueHeader) {
    return new CoValueCore(header, this.syncManager.local);
  }

  async MakeAvailableWithDependencies(
    input: PushMessageHandlerInput | DataMessageHandlerInput,
  ) {
    if (!input.msg.header) {
      throw new Error(`Empty header for ${input.msg.id}`);
    }

    if (input.entry.state.type === "available") {
      throw new Error(
        `CoValue is already available, requested to make available for ${input.msg.id}`,
      );
    }

    await this.loadUnknownDependencies(input);

    const coValue = this.createCoValue(input.msg.header);
    input.entry.dispatch({
      type: "available",
      coValue,
    });
  }
}
