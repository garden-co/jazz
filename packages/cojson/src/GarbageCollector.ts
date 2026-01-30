import { CoValueCore } from "./coValueCore/coValueCore.js";
import { GARBAGE_COLLECTOR_CONFIG } from "./config.js";
import { RawCoID } from "./ids.js";
import type { LocalNode } from "./localNode.js";

/**
 * TTL-based garbage collector for removing unused CoValues from memory.
 */
export class GarbageCollector {
  private readonly interval: ReturnType<typeof setInterval>;

  constructor(private readonly node: LocalNode) {
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
    for (const coValue of this.node.allCoValues()) {
      const { verified } = coValue;

      if (!verified?.lastAccessed) {
        continue;
      }

      const timeSinceLastAccessed = currentTime - verified.lastAccessed;

      if (timeSinceLastAccessed > GARBAGE_COLLECTOR_CONFIG.MAX_AGE) {
        this.node.internalUnmountCoValue(coValue.id);
      }
    }
  }

  stop() {
    clearInterval(this.interval);
  }
}
