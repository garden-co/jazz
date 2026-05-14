import {
  installSharedWorkerBroker,
  type SharedWorkerBrokerGlobal,
} from "./shared-worker-broker.js";

installSharedWorkerBroker(globalThis as unknown as SharedWorkerBrokerGlobal);
