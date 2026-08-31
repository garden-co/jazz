import { installJazzBrokerWorker } from "./jazz-broker-worker-core.js";

// Production worker entry: no test scheduling or diagnostic hooks exist in
// the shipped worker protocol.
installJazzBrokerWorker();
