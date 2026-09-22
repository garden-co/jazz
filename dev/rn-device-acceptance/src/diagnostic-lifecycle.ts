import type { DeviceDiagnosticCode } from "./device-diagnostics.ts";

type RecordDiagnostic = (code: DeviceDiagnosticCode) => Promise<void>;

/**
 * Keep one fixed, non-secret stage visible before synchronous native work.
 * Recording is deliberately fire-and-forget: a wedged native promise must not
 * stop JavaScript from reaching the boundary the marker describes.
 */
export function createDeviceDiagnosticTracker(
  record: RecordDiagnostic,
  clear: () => Promise<void>,
) {
  let current: DeviceDiagnosticCode = "fixture-metadata-failed";
  const persist = () => {
    try {
      void record(current).catch(() => {});
    } catch {
      // A broken diagnostic sink must not replace the acceptance failure.
    }
  };
  return {
    mark(code: DeviceDiagnosticCode) {
      current = code;
      persist();
    },
    retry() {
      persist();
    },
    async clear() {
      await clear();
    },
  };
}
