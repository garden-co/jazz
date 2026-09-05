// @vitest-environment jsdom
import React, { useEffect } from "react";
import { act, render, waitFor } from "@testing-library/react";
import { expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { schema } from "../../src/schema-namespace.js";
import { serializeSchemaSource } from "../../src/drivers/schema-wire.js";
import {
  JazzProvider,
  useAll,
  useOne,
  useDb,
  useSession,
  type Db,
  type DbConfig,
} from "../../src/react-native/index.js";
import type { QueryBuilder, QueryOptions } from "../../src/runtime/db.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  notes: schema.table({ title: schema.string(), done: schema.boolean() }),
});
type Note = { id: string; title: string; done: boolean };

function Probe({
  query,
  options = { tier: "local" },
  onDb,
}: {
  query?: QueryBuilder<Note>;
  options?: QueryOptions;
  onDb: (db: Db) => void;
}) {
  const db = useDb();
  const session = useSession();
  const all = useAll(query, options);
  const one = useOne(query, options);
  useEffect(() => onDb(db), [db, onDb]);
  return (
    <>
      <output data-testid="all">
        {all.error
          ? `error:${all.error.message}`
          : all.isLoading
            ? "loading"
            : all.data === undefined
              ? "disabled"
              : all.data.map((row) => row.title).join("|")}
      </output>
      <output data-testid="one">
        {one.error
          ? `error:${one.error.message}`
          : one.isLoading
            ? "loading"
            : one.data === undefined
              ? "disabled"
              : (one.data?.title ?? "empty")}
      </output>
      <output data-testid="session">{session?.user}</output>
    </>
  );
}

async function expectClosed(db: Db) {
  await waitFor(async () => {
    await expect(db.all(app.notes, { tier: "local" })).rejects.toThrow("shutting down or closed");
  });
}

it("delivers native insert/update/delete to useAll and useOne and replaces query identity", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const writer = await fixture.createDb(); // Installs the actual native platform factory.
    let observed: Db | undefined;
    const onDb = (db: Db) => {
      observed = db;
    };
    const view = (query?: QueryBuilder<Note>) => (
      <JazzProvider config={fixture.config} fallback={<span>opening</span>}>
        <Probe query={query} onDb={onDb} />
      </JazzProvider>
    );
    const mounted = render(view(app.notes.where({ done: false }).orderBy("title")));
    try {
      await waitFor(() => expect(mounted.getByTestId("one").textContent).toBe("empty"));
      const initial = observed;
      const row = await writer
        .insert(app.notes, { title: "first", done: false })
        .wait({ tier: "local" });
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("first"));
      expect(mounted.getByTestId("one").textContent).toBe("first");
      const later = await writer
        .insert(app.notes, { title: "later", done: false })
        .wait({ tier: "local" });
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("first|later"));
      expect(mounted.getByTestId("one").textContent).toBe("first");
      await writer.delete(app.notes, later.id).wait({ tier: "local" });
      await writer.update(app.notes, row.id, { title: "edited" }).wait({ tier: "local" });
      await waitFor(() => expect(mounted.getByTestId("one").textContent).toBe("edited"));
      mounted.rerender(view(app.notes.where({ done: true })));
      await waitFor(() => expect(mounted.getByTestId("one").textContent).toBe("empty"));
      expect(mounted.getByTestId("all").textContent).toBe("");
      expect(observed).toBe(initial);
      await writer.update(app.notes, row.id, { done: true }).wait({ tier: "local" });
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("edited"));
      await writer.delete(app.notes, row.id).wait({ tier: "local" });
      await waitFor(() => expect(mounted.getByTestId("one").textContent).toBe("empty"));
      mounted.rerender(view());
      expect(mounted.getByTestId("all").textContent).toBe("disabled");
      expect(mounted.getByTestId("one").textContent).toBe("disabled");
    } finally {
      mounted.unmount();
      if (observed) await expectClosed(observed);
    }
  });
});

it("preserves equivalent provider config and isolates a replacement native identity", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const firstWriter = await fixture.createDb();
    await firstWriter
      .insert(app.notes, { title: "first scope", done: false })
      .wait({ tier: "local" });
    const capability = fixture.nativeHost.admit(
      JSON.stringify({
        scope: {
          app_namespace: fixture.config.appId,
          storage_namespace: "default",
          auth_scope: "hook-second",
        },
        sqlite_path: join(fixture.directory, "hook-second.sqlite"),
        schema_json: serializeSchemaSource(app.wasmSchema),
        identity: {
          node: randomUUID(),
          author: JSON.stringify(["https://auth.example", "hook-second"]),
        },
        claims: {},
      }),
    );
    const secondConfig = { ...fixture.config, nativeRelay: { capability } };
    const secondWriter = await fixture.createDb(secondConfig);
    await secondWriter
      .insert(app.notes, { title: "second scope", done: false })
      .wait({ tier: "local" });
    let observed: Db | undefined;
    const onDb = (db: Db) => {
      observed = db;
    };
    const view = (config: DbConfig) => (
      <JazzProvider config={config} fallback={<span>opening</span>}>
        <Probe query={app.notes.where({ done: false })} onDb={onDb} />
      </JazzProvider>
    );
    const mounted = render(view(fixture.config));
    try {
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("first scope"));
      const first = observed!;
      mounted.rerender(
        view({ ...fixture.config, nativeRelay: { capability: fixture.capability } }),
      );
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("first scope"));
      expect(observed).toBe(first);
      mounted.rerender(view(secondConfig));
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("second scope"));
      expect(mounted.getByTestId("one").textContent).toBe("second scope");
      expect(mounted.getByTestId("session").textContent).toBe(
        JSON.stringify(["https://auth.example", "hook-second"]),
      );
      expect(observed).not.toBe(first);
      await expectClosed(first);
      await firstWriter
        .insert(app.notes, { title: "retired scope update", done: false })
        .wait({ tier: "local" });
      await secondWriter
        .update(app.notes, (await secondWriter.one(app.notes))!.id, { title: "second updated" })
        .wait({ tier: "local" });
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("second updated"));
    } finally {
      mounted.unmount();
      if (observed) await expectClosed(observed);
      fixture.nativeHost.revoke(capability);
    }
  });
});

it("unmounts pending remote hooks, shuts down the provider and preserves a sibling native root", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const writer = await fixture.createDb();
    let observed: Db | undefined;
    const onDb = (db: Db) => {
      observed = db;
    };
    const mounted = render(
      <JazzProvider config={fixture.config}>
        <Probe query={app.notes} options={{ tier: "remote" }} onDb={onDb} />
      </JazzProvider>,
    );
    try {
      await waitFor(() => expect(mounted.getByTestId("all").textContent).toBe("loading"));
      expect(mounted.getByTestId("one").textContent).toBe("loading");
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 30));
      });
      expect(mounted.getByTestId("all").textContent).toBe("loading");
    } finally {
      mounted.unmount();
      if (observed) await expectClosed(observed);
    }
    const row = await writer
      .insert(app.notes, { title: "after unmount", done: false })
      .wait({ tier: "local" });
    expect(await writer.one(app.notes)).toEqual(row);
    const reopened = render(
      <JazzProvider config={fixture.config}>
        <Probe query={app.notes} onDb={onDb} />
      </JazzProvider>,
    );
    try {
      await waitFor(() => expect(reopened.getByTestId("all").textContent).toBe("after unmount"));
    } finally {
      reopened.unmount();
      if (observed) await expectClosed(observed);
    }
  });
});

it("surfaces native-backed subscription errors through both hooks after shutdown", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    await fixture.createDb();
    let observed: Db | undefined;
    const onDb = (db: Db) => {
      observed = db;
    };
    const view = (done: boolean) => (
      <JazzProvider config={fixture.config}>
        <Probe query={app.notes.where({ done })} onDb={onDb} />
      </JazzProvider>
    );
    const mounted = render(view(false));
    try {
      await waitFor(() => expect(mounted.getByTestId("one").textContent).toBe("empty"));
      await observed!.shutdown();
      mounted.rerender(view(true));
      await waitFor(() =>
        expect(mounted.getByTestId("all").textContent).toContain("error:Cannot operate on a Db"),
      );
      expect(mounted.getByTestId("one").textContent).toContain("error:Cannot operate on a Db");
    } finally {
      mounted.unmount();
      if (observed) await expectClosed(observed);
    }
  });
});
