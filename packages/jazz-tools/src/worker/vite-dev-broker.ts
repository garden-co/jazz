// Entry the Vite dev server serves as the broker worker when Jazz runs from
// its dependency pre-bundle (see `vitePrebundledBrokerWorkerUrl`): the
// `jazz-tools/_worker/broker` export resolves here, and the dev server serves
// the bundled worker below as this module's one import. Never shipped to a
// production bundle: bundlers build the worker from the `new SharedWorker`
// literal instead.
import "./jazz-broker-worker.js";
