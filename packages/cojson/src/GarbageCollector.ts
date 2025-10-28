import { CoValueCore } from "./coValueCore/coValueCore.js";
import { GARBAGE_COLLECTOR_CONFIG } from "./config.js";
import { RawCoID } from "./ids.js";

export class GarbageCollector {
  private readonly interval: ReturnType<typeof setInterval>;

  constructor(
    private readonly coValues: Map<RawCoID, CoValueCore>,
    private readonly garbageCollectGroups: boolean,
  ) {
    this.interval = setInterval(() => {
      this.collect();
    }, GARBAGE_COLLECTOR_CONFIG.INTERVAL);
  }

  getCurrentTime() {
    return performance.now();
  }

  trackCoValueAccess({ verified }: CoValueCore) {
    if (verified) {
      verified.lastAccessed = this.getCurrentTime();
    }
  }

  collect() {
    const currentTime = this.getCurrentTime();
    for (const coValue of this.coValues.values()) {
      const { verified } = coValue;

      if (!verified?.lastAccessed) {
        continue;
      }

      const timeSinceLastAccessed = currentTime - verified.lastAccessed;

      if (timeSinceLastAccessed > GARBAGE_COLLECTOR_CONFIG.MAX_AGE) {
        coValue.unmount(this.garbageCollectGroups);
      }
    }
  }

  stop() {
    clearInterval(this.interval);
  }
}
