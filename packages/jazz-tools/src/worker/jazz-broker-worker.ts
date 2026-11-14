import { installJazzBrokerWorker } from "./jazz-broker-worker-core.js";

// Production worker entry: no test scheduling or diagnostic hooks exist in
// the shipped worker protocol.
installJazzBrokerWorker();

// The shipped worker is minified, which renames every local binding. An
// entry's export names survive, so this keeps the installer's name in the
// file: tools and tests (scripts/vite-prebundled-worker.test.mjs) recognise
// the served worker by it, rather than an HTML fallback page.
export { installJazzBrokerWorker };
