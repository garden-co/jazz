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

// A URL-scoped capability isolates destructive liveness faults to one test
// realm. No production request, runtime option or broker hook enables these.
// ABI verification evaluates the bundle outside a worker, without a location.
const faultChannelName = globalThis.location
  ? new URL(globalThis.location.href).searchParams.get("followerFault")
  : null;
if (faultChannelName) {
  const control = new BroadcastChannel(faultChannelName);
  let holdClose = false;
  let holdPendingWrites = false;
  // This module is a SharedWorker entry, not a Window.
  const worker = globalThis as unknown as {
    close(): void;
    onconnect: (event: MessageEvent & { ports: MessagePort[] }) => void;
  };
  control.onmessage = (event: MessageEvent<{ type: string }>) => {
    if (event.data.type === "hold-close") {
      holdClose = true;
      control.postMessage({ type: "holding-close" });
    } else if (event.data.type === "hold-pending-writes") {
      holdPendingWrites = true;
      control.postMessage({ type: "holding-pending-writes" });
    } else if (event.data.type === "die") {
      // Deliberately bypass every production cleanup/error notification.
      worker.close();
    }
  };
  const connect = worker.onconnect;
  worker.onconnect = (event) => {
    const port = event.ports[0]!;
    const postMessage = port.postMessage.bind(port);
    port.postMessage = ((message: { type?: string }, transfer: Transferable[] = []) => {
      postMessage(message, transfer);
      if (message.type === "runtime-pong") control.postMessage({ type: "pong-sent" });
    }) as typeof port.postMessage;
    port.addEventListener("message", (message: MessageEvent<{ type: string }>) => {
      if (holdClose && message.data.type === "close") {
        message.stopImmediatePropagation();
        control.postMessage({ type: "close-held" });
      }
      if (holdPendingWrites && message.data.type === "flush-pending-writes") {
        message.stopImmediatePropagation();
        control.postMessage({ type: "pending-writes-held" });
      }
    });
    connect(event);
  };
}
