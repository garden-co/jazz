import { createJazzClient, schema as s, type JazzClientConfig } from "jazz-tools/react-native";
import { assertPersistedTitleForRun, persistedTitleForRun } from "./run-marker";
import type { DeviceDiagnosticCode } from "./device-diagnostics";
import { finishSeedClient } from "./seed-teardown";

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
    // The normal subscription orchestrator is asynchronous even for a local
    // commit. Give its owner wake a small, bounded number of microtasks; no
    // raw foreground `tick` or byte command is used here. The bound makes a
    // missing owner wake a receipt failure rather than an unbounded device
    // test hang.
    markFailure("public-client-publish-failed");
    for (let attempt = 0; attempt < 8 && !observed; attempt += 1) {
      await Promise.resolve();
    }
    if (!observed) {
      throw new Error("high-level React Native foreground did not publish its local write");
    }
    completed = true;
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    if (completed && !failed) markFailure("public-client-shutdown-failed");
    await finishSeedClient(unsubscribe, () => client.shutdown(), failed);
  }
}

/**
 * Materialize the row written by a prior process through `createJazzClient`.
 * This deliberately does not use the byte-level fixture or read SQLite: it is
 * the public API half of the process-restart receipt.
 */
export async function proveHighLevelForegroundRestart(
  capability: Uint8Array,
  runNonce: string,
): Promise<void> {
  const client = await createJazzClient(clientConfig(capability));
  try {
    const rows = await client.db.all(app.todos);
    assertPersistedTitleForRun(
      rows.map((row) => row.title),
      runNonce,
    );
  } finally {
    await client.shutdown();
  }
}
