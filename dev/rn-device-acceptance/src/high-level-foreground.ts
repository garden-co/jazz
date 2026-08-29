import { createJazzClient, schema as s } from "jazz-tools/react-native";

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
export async function proveHighLevelForegroundRuntime(capability: Uint8Array): Promise<void> {
  const client = await createJazzClient({
    appId: "jazz-device-acceptance",
    nativeRelay: { capability },
    cookieSession: {
      issuer: "https://jazz.device.test",
      user_id: "fixture-user-a",
      claims: {},
      authMode: "external",
    },
  });
  const title = "high-level-foreground-row";
  let observed = false;
  const unsubscribe = client.db.subscribe(app.todos, (todos) => {
    observed ||= todos.some((todo) => todo.title === title);
  });
  try {
    const write = client.db.insert(app.todos, { title });
    await write.wait({ tier: "local" });
    const rows = await client.db.all(app.todos);
    if (!rows.some((row) => row.title === title)) {
      throw new Error("high-level React Native foreground did not materialize its local write");
    }
    // The normal subscription orchestrator is asynchronous even for a local
    // commit. Give its owner wake a small, bounded number of microtasks; no
    // raw foreground `tick` or byte command is used here. The bound makes a
    // missing owner wake a receipt failure rather than an unbounded device
    // test hang.
    for (let attempt = 0; attempt < 8 && !observed; attempt += 1) {
      await Promise.resolve();
    }
    if (!observed) {
      throw new Error("high-level React Native foreground did not publish its local write");
    }
  } finally {
    unsubscribe();
    await client.shutdown();
  }
}
