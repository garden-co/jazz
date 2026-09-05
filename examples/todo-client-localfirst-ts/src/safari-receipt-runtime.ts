import wasmUrl from "../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm?url";
import workerUrl from "./safari-receipt-worker.ts?sharedworker&url";

export const safariReceiptRuntimeSources = {
  brokerWorkerUrl: workerUrl,
  wasmUrl,
  // The diagnostic worker bytes are part of this selected-ref build. Change
  // this whenever its worker/WASM inputs change so Safari cannot reuse an old
  // same-origin SharedWorker realm.
  wasmVersion: import.meta.env.VITE_SAFARI_RECEIPT_VERSION ?? "safari-receipt-dev",
};
