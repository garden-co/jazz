/* eslint-disable no-restricted-globals */
import { detectSyncOpfsInWorkerScope } from "../../../src/runtime/shared-worker-leader/capability.js";

self.onconnect = (event) => {
  const port = event.ports[0];
  port.onmessage = async (msg) => {
    if (msg.data?.t !== "PROBE") return;
    let supported = false;
    try {
      supported = await detectSyncOpfsInWorkerScope();
    } catch {
      supported = false;
    }
    port.postMessage({ t: "PROBE_RESULT", supported });
  };
  port.start();
};
