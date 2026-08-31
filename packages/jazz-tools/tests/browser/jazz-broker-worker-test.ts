import { installJazzBrokerWorker } from "../../src/worker/jazz-broker-worker-core.js";
import type {
  BrowserForegroundNodeLeaseAcquireRequest,
  BrowserForegroundNodeLeaseAcquireResponse,
} from "../../src/runtime/native-runtime/browser-worker-protocol.js";

// This entry is served only by the browser test harness. Production bundles
// start `src/worker/jazz-broker-worker.ts`, which installs no hooks and never
// inspects these test-only request fields.
installJazzBrokerWorker({
  foregroundLeaseTestHooks: {
    delayBeforeLeaseAllocation(
      request: BrowserForegroundNodeLeaseAcquireRequest,
    ): number | undefined {
      return request.testDelayBeforeLeaseAllocationMs;
    },
    allocationQueued(port) {
      port.postMessage({ type: "foreground-node-lease-test-queued" });
    },
    delayAfterLeaseAllocation(
      request: BrowserForegroundNodeLeaseAcquireRequest,
    ): number | undefined {
      const delay = request.testDelayAfterLeaseAllocationMs;
      if (delay === undefined) return undefined;
      if (!Number.isSafeInteger(delay) || delay < 0 || delay > 1_000) {
        throw new Error("Invalid foreground lease test delay");
      }
      return delay;
    },
    allocationCommitted(port, node, workerRealmId) {
      port.postMessage({
        type: "foreground-node-lease-test-allocated",
        node,
        workerRealmId,
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
    },
    async cancellationRetired(pageStore, node) {
      return await pageStore.foregroundNodeLeaseNodeState(node);
    },
  },
});
