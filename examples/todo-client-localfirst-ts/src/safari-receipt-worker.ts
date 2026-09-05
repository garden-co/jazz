import {
  installJazzBrokerWorker,
  type BrowserDiagnosticEvent,
} from "../../../packages/jazz-tools/src/worker/jazz-broker-worker-core.js";

const trace = new BroadcastChannel("jazz-safari-receipt-trace");
installJazzBrokerWorker({
  diagnosticHooks: {
    record(event: BrowserDiagnosticEvent) {
      trace.postMessage(event);
    },
  },
});
