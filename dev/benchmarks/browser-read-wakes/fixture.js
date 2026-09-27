// Standalone public-API browser fixture. See README.md before interpreting timings.
(async () => {
  const setup = {};
  const phase = async (name, run) => {
    const start = performance.now();
    try {
      return await run();
    } finally {
      setup[name] = performance.now() - start;
    }
  };
  const [{ schema: s, generateAuthSecret }, { createBrowserTestDb: createDb }] = await Promise.all([
    import("/src/index.ts"),
    import("/tests/browser/account-fixtures.ts"),
  ]);
  const { storage, count } = globalThis.fixtureConfig;
  const app = s.defineApp({
    folders: s.table({ name: s.string(), description: s.string() }, {}),
    tasks: s.table(
      { title: s.string(), body: s.string(), folderId: s.uuid() },
      { folder: s.rel("folders", "folderId") },
    ),
  });
  const db = await phase("open_ms", () =>
    createDb({
      appId: crypto.randomUUID(),
      secret: generateAuthSecret(),
      driver:
        storage === "memory"
          ? { type: "memory" }
          : { type: "persistent", dbName: `include-profile-${crypto.randomUUID()}` },
      logLevel: "warn",
    }),
  );
  const query = app.tasks.include({ folder: true });
  const folderIds = [];
  await phase("empty_query_ms", () => db.all(query, { tier: "local" }));
  const tx = await phase("seed_ms", () =>
    db.transaction((tx) => {
      for (let i = 0; i < 15; i++)
        folderIds.push(
          tx.insert(app.folders, {
            name: `Folder ${i}`,
            description: `Description ${i} ${"y".repeat(2048)}`,
          }).id,
        );
      for (let i = 0; i < count; i++)
        tx.insert(app.tasks, {
          title: `Task ${i}`,
          body: `Body ${i} ${"x".repeat(128)}`,
          folderId: folderIds[i % 15],
        });
    }),
  );
  await phase("local_settlement_ms", () => tx.wait({ tier: "local" }));
  const { NativeRuntimeAdapter } =
    await import("/src/runtime/native-runtime/native-runtime-adapter.ts");
  const original = NativeRuntimeAdapter.prototype.awaitNativeRead;
  const scheduling = globalThis.fixtureConfig.scheduling;
  const channel = scheduling === "message-channel" ? new MessageChannel() : undefined;
  let continueRead;
  if (channel)
    channel.port1.onmessage = () => {
      const resume = continueRead;
      continueRead = undefined;
      resume?.();
    };
  const hostYield = () =>
    scheduling === "timer-polling"
      ? new Promise((resolve) => setTimeout(resolve, 0))
      : channel
        ? new Promise((resolve) => {
            if (continueRead) throw new Error("Scheduling diagnostic requires serial reads");
            continueRead = resolve;
            channel.port2.postMessage(null);
          })
        : globalThis.scheduler.yield();
  if (["timer-polling", "host-yield", "message-channel"].includes(scheduling)) {
    if (scheduling === "host-yield" && !globalThis.scheduler?.yield)
      throw new Error("scheduler.yield unavailable");
    // Diagnostic only: identical pending-read loop, replacing only sleep(0).
    // This still polls while IO is pending; it is not the proposed production fix.
    NativeRuntimeAdapter.prototype.awaitNativeRead = async function (started, tier) {
      const result = await started;
      if (typeof result?.poll !== "function") return result;
      const cancel = () => result.cancel();
      this.ownerRuntime.pendingNativeReadCancels.add(cancel);
      try {
        for (;;) {
          if (this.closed || this.ownerRuntime.closed)
            throw new Error("native read was cancelled by runtime shutdown");
          if (tier) this.throwServerTransportErrorForTier(tier);
          const bytes = result.poll();
          if (bytes !== null) return bytes;
          this.pumpServerTransport();
          if (tier) this.throwServerTransportErrorForTier(tier);
          await hostYield();
        }
      } finally {
        this.ownerRuntime.pendingNativeReadCancels.delete(cancel);
        cancel();
        this.ownerRuntime.scheduleCoreTick();
      }
    };
  }
  let rows;
  globalThis.fixture = {
    setup,
    async run(included, reps) {
      const times = [];
      for (let i = 0; i < reps; i++) {
        performance.mark(`read-${i}-start`);
        const start = performance.now();
        rows = await db.all(included ? query : app.tasks, { tier: "local" });
        times.push(performance.now() - start);
        if (rows.length !== count) throw new Error("count mismatch");
        performance.mark(`read-${i}-end`);
      }
      return times;
    },
    check(included) {
      if (rows.length !== count) throw new Error("count mismatch");
      const seen = new Set();
      for (const row of rows) {
        const i = Number(row.title.slice(5));
        if (
          i < 0 ||
          i >= count ||
          seen.has(row.id) ||
          row.body !== `Body ${i} ${"x".repeat(128)}` ||
          row.folderId !== folderIds[i % 15]
        )
          throw new Error("root mismatch");
        seen.add(row.id);
        if (
          included &&
          (row.folder.id !== folderIds[i % 15] ||
            row.folder.name !== `Folder ${i % 15}` ||
            row.folder.description !== `Description ${i % 15} ${"y".repeat(2048)}`)
        )
          throw new Error("include mismatch");
      }
      return true;
    },
    close: async () => {
      NativeRuntimeAdapter.prototype.awaitNativeRead = original;
      channel?.port1.close();
      channel?.port2.close();
      await db.shutdown();
    },
  };
  return true;
})();
