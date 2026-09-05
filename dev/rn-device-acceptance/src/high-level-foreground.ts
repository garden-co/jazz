import { createJazzClient, schema as s, type JazzClientConfig } from "jazz-tools/react-native";
import { assertPersistedTitleForRun, persistedTitleForRun } from "./run-marker";
import type { DeviceDiagnosticCode } from "./device-diagnostics";
import { finishSeedClient, type SeedBoundary } from "./seed-teardown";
import { waitForPublication } from "./publication-wait";

const app = s.defineApp({
  todos: s.table({ title: s.string() }),
});

/**
 * Exercise the installed foreground through the public RN API, not the
 * byte-level fixture helpers. The native fixture chose the admitted schema,
 * identity, claims, and SQLite path; JavaScript only receives its opaque
 * capability. This is deliberately one small consumer-shaped scenario:
 * schema-backed insert, local query, subscription publication, and shutdown.
 */
function clientConfig(capability: Uint8Array): JazzClientConfig {
  return {
    appId: "jazz-device-acceptance",
    nativeRelay: { capability },
    cookieSession: {
      issuer: "https://jazz.device.test",
      user_id: "fixture-user-a",
      claims: {},
      authMode: "external",
    },
  };
}

/**
 * Seed a durable row through the public foreground API. A later app process
 * uses {@link proveHighLevelForegroundRestart} to prove that it can read this
 * same row after the native relay and its SQLite owner have been recreated.
 */
export async function seedHighLevelForegroundRuntime(
  capability: Uint8Array,
  runNonce: string,
  markFailure: (code: DeviceDiagnosticCode) => void,
  waitForCoreObservation: () => Promise<void>,
  boundary?: (code: SeedBoundary) => void,
): Promise<void> {
  markFailure("public-client-open-failed");
  const client = await createJazzClient(clientConfig(capability));
  const title = persistedTitleForRun(runNonce);
  let observed = false;
  let completed = false;
  let failed = false;
  let unsubscribe = () => {};
  try {
    markFailure("public-client-subscribe-failed");
    unsubscribe = client.db.subscribe(app.todos, (todos) => {
      observed ||= todos.some((todo) => todo.title === title);
    });
    markFailure("public-client-write-failed");
    const write = client.db.insert(app.todos, { title });
    await write.wait({ tier: "local" });
    markFailure("public-client-read-failed");
    const rows = await client.db.all(app.todos);
    if (!rows.some((row) => row.title === title)) {
      throw new Error("high-level React Native foreground did not materialize its local write");
    }
    // Native wakes arrive through React Native's CallInvoker, which needs an
    // event-loop turn rather than another Promise microtask. Keep the wait
    // bounded and use no raw foreground `tick` or byte command here.
    markFailure("public-client-publish-failed");
    if (!(await waitForPublication(() => observed))) {
      throw new Error("high-level React Native foreground did not publish its local write");
    }
    // Keep this foreground and its native relay alive until the host's
    // independent Core reader observes the same run-bound title. This fixture
    // handshake does not widen the public RN local-only write.wait contract.
    markFailure("public-client-core-observation-failed");
    await waitForCoreObservation();
    boundary?.("js-core-await-returned");
    completed = true;
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    if (completed && !failed) markFailure("public-client-shutdown-failed");
    await finishSeedClient(unsubscribe, () => client.shutdown(), failed, boundary);
  }
}

/**
 * Open a second public foreground after the seed foreground has shut down and
 * require the seed row through it. This is the pre-termination LocalFirst
 * propagation receipt: the subscription waits until the relay has delivered
 * the run-bound marker into this fresh foreground's local knowledge, then the
 * ordinary local read must materialize that same marker. Core observation is
 * proved independently by the host; the subsequent process-restart read is
 * the durable SQLite half of the end-to-end claim.
 */
export async function proveHighLevelForegroundRelayReadback(
  capability: Uint8Array,
  runNonce: string,
): Promise<void> {
  const client = await createJazzClient(clientConfig(capability));
  let unsubscribe = () => {},
    failed = false;
  try {
    const title = persistedTitleForRun(runNonce);
    let observed = false;
    unsubscribe = client.db.subscribe(app.todos, (todos) => {
      observed ||= todos.some((todo) => todo.title === title);
    });
    if (!(await waitForPublication(() => observed))) {
      throw new Error("fresh high-level React Native foreground did not receive the relay marker");
    }
    const rows = await client.db.all(app.todos);
    assertPersistedTitleForRun(
      rows.map((row) => row.title),
      runNonce,
    );
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    await finishSeedClient(unsubscribe, () => client.shutdown(), failed);
  }
}

/**
 * Materialize the row written by a prior process through `createJazzClient`.
 * This deliberately does not use the byte-level fixture or read SQLite: it is
 * the public API half of the process-restart receipt. The driver has stopped
 * upstream before this launch; the marker must arrive from local relay storage.
 */
export async function proveHighLevelForegroundRestart(
  capability: Uint8Array,
  runNonce: string,
): Promise<void> {
  const client = await createJazzClient(clientConfig(capability));
  let unsubscribe = () => {},
    failed = false;
  try {
    const title = persistedTitleForRun(runNonce);
    let observed = false;
    unsubscribe = client.db.subscribe(app.todos, (todos) => {
      observed ||= todos.some((todo) => todo.title === title);
    });
    if (!(await waitForPublication(() => observed))) {
      throw new Error(
        "restarted high-level React Native foreground did not receive the persisted marker",
      );
    }
    const rows = await client.db.all(app.todos);
    assertPersistedTitleForRun(
      rows.map((row) => row.title),
      runNonce,
    );
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    await finishSeedClient(unsubscribe, () => client.shutdown(), failed);
  }
}
