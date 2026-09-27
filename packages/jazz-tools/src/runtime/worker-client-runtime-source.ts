import { DefaultRuntimeSource } from "./default-runtime-source.js";

/**
 * Internal qualification path for a single ordinary persistent worker client.
 * Default browser behavior remains unchanged while lifecycle and performance
 * gates are pending. It cannot yet rebind a failed application port in place.
 */
export class WorkerClientRuntimeSource extends DefaultRuntimeSource {
  protected override readonly browserClientBinding = true;
}
