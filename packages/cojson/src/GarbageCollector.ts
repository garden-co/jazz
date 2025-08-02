import { CoValueCore } from "./coValueCore/coValueCore.js";
import { GARBAGE_COLLECTOR_CONFIG } from "./config.js";
import { RawCoID } from "./ids.js";

export class GarbageCollector {
  private readonly coValueAccess = new Map<RawCoID, number>();
  private readonly interval: ReturnType<typeof setInterval>;

  constructor(private readonly coValues: Map<RawCoID, CoValueCore>) {
    this.interval = setInterval(() => {
      this.collect();
    }, GARBAGE_COLLECTOR_CONFIG.INTERVAL);
  }

  getCurrentTime() {
    return performance.now();
  }

  markCoValueAccess(coValue: CoValueCore) {
    if (coValue.verified && coValue.verified.header.ruleset.type !== "group") {
      this.coValueAccess.set(coValue.id, this.getCurrentTime());
    }
  }

  collect() {
    const currentTime = this.getCurrentTime();
    for (const [id, accessTime] of this.coValueAccess) {
      const coValue = this.coValues.get(id);

      if (!coValue) {
        continue;
      }

      if (currentTime - accessTime > GARBAGE_COLLECTOR_CONFIG.MAX_AGE) {
        const unmounted = coValue.unmount();

        if (unmounted) {
          this.coValues.delete(id);
          this.coValueAccess.delete(id);
        }
      }
    }
  }

  stop() {
    clearInterval(this.interval);
  }
}
