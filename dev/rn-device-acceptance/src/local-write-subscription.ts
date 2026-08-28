import { createDb, schema as s } from "jazz-tools/react-native";
import { admittedNativeRelay } from "./native-fixture";
import { metroWasmSource } from "./runtime-wasm";

const acceptanceApp = s.defineApp({
  notes: s.table({ title: s.string() }),
});

const observationTimeoutMs = 30_000;

/**
 * Two independent foreground JS runtimes use one platform-admitted durable
 * relay. B subscribes before A writes; success requires B's subscription to
 * publish A's unique row, rather than merely rereading A's in-memory DB.
 */
export async function observeLocalWriteSubscription(): Promise<void> {
  const relay = await admittedNativeRelay();
  const wasmSource = await metroWasmSource();
  const [clientA, clientB] = await Promise.all([
    createDb({
      appId: "jazz-device-acceptance",
      nativeRelay: relay,
      runtimeSources: { wasmSource },
    }),
    createDb({
      appId: "jazz-device-acceptance",
      nativeRelay: relay,
      runtimeSources: { wasmSource },
    }),
  ]);
  const marker = `device-write-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  let unsubscribe: (() => void) | undefined;
  try {
    const observed = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("UI-B subscription did not observe UI-A's relay write")),
        observationTimeoutMs,
      );
      unsubscribe = clientB.subscribe(acceptanceApp.notes, (rows) => {
        if (!rows.some((row) => row.title === marker)) return;
        clearTimeout(timer);
        resolve();
      });
    });
    await clientA.insert(acceptanceApp.notes, { title: marker }).wait({ tier: "local" });
    await observed;
  } finally {
    unsubscribe?.();
    await Promise.all([clientA.shutdown(), clientB.shutdown()]);
  }
}
