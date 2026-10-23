import { expect, it, vi } from "vitest";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import type { CatalogueCache, CatalogueCacheScope } from "../catalogue-cache.js";
import type { WasmSchema } from "../../drivers/types.js";
const schemaA = {
  todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
} satisfies WasmSchema;
const schemaB = {
  ...schemaA,
  controls: { columns: [{ name: "value", column_type: { type: "Text" }, nullable: false }] },
} satisfies WasmSchema;
const scope = {
  registryAuthority: "https://registry.test",
  appId: "view-readiness",
  environment: "test",
};
type State = { capture?: Uint8Array | null; ready: boolean };
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function fixture(
  take: () => State | Promise<State>,
  ready: () => boolean | Promise<boolean>,
  cache?: CatalogueCache,
) {
  const viewTake = vi.fn(() => {
    throw new Error("capture APIs are owner-only");
  });
  const viewReady = vi.fn(ready);
  const ownerTake = vi.fn(take);
  const viewDb = {
    hasAuthenticatedCatalogue: viewReady,
    takeAuthenticatedCatalogueState: viewTake,
  };
  const ownerDb = {
    setTickScheduler: () => undefined,
    takeAuthenticatedCatalogueState: ownerTake,
    validateCatalogueCaptureReplacement: vi.fn(),
    registerSchema: () => viewDb,
    tick: () => undefined,
    close: vi.fn(async () => undefined),
  };
  const owner = new NativeRuntimeAdapter(
    null,
    schemaA,
    new Uint8Array(16),
    new TextEncoder().encode('["urn:jazz:test","view-readiness"]'),
    1,
    true,
    { db: ownerDb as never, catalogueCache: cache ? { cache, scope } : undefined },
  );
  return {
    owner,
    view: owner.registerSchemaView(schemaB),
    viewTake,
    viewReady,
    ownerTake,
    ownerDb,
  };
}

it("qualifies owner A readiness with non-draining requested B admission", async () => {
  let admittedB = false;
  const f = fixture(
    () => ({ ready: true }),
    () => admittedB,
  );
  await expect(f.owner.hasAuthenticatedCatalogue()).resolves.toBe(true);
  f.ownerTake.mockClear();
  await expect(f.view.hasAuthenticatedCatalogue()).resolves.toBe(false);
  expect(f.ownerTake).toHaveBeenCalledOnce();
  admittedB = true;
  await expect(f.view.hasAuthenticatedCatalogue()).resolves.toBe(true);
  expect(f.ownerTake).toHaveBeenCalledTimes(2);
  expect(f.viewReady).toHaveBeenCalledTimes(2);
  expect(f.viewTake).not.toHaveBeenCalled();
  await f.owner.close();
});

it("publishes owner A capture even when requested B is not admitted", async () => {
  const bytes = Uint8Array.of(8);
  let capture: Uint8Array | null = bytes;
  const publish = vi.fn(async (_scope: CatalogueCacheScope, actual: Uint8Array) => {
    expect(actual).toEqual(bytes);
  });
  const f = fixture(
    () => {
      const moved = capture;
      capture = null;
      return { capture: moved, ready: true };
    },
    () => false,
    { load: async () => null, publish },
  );
  await expect(f.view.hasAuthenticatedCatalogue()).resolves.toBe(false);
  expect(publish).toHaveBeenCalledOnce();
  expect(f.viewTake).not.toHaveBeenCalled();
  await f.owner.close();
});

it("preserves owner publication failure when requested B is not admitted", async () => {
  const failure = new Error("pending B must not suppress owner durable write failure");
  const bytes = Uint8Array.of(9);
  let capture: Uint8Array | null = bytes;
  let attempts = 0;
  const publish = vi.fn(async (_scope: CatalogueCacheScope, actual: Uint8Array) => {
    expect(actual).toEqual(bytes);
    if (++attempts === 1) throw failure;
  });
  const f = fixture(
    () => {
      const moved = capture;
      capture = null;
      return { capture: moved, ready: true };
    },
    () => false,
    { load: async () => null, publish },
  );
  await expect(f.view.hasAuthenticatedCatalogue()).rejects.toBe(failure);
  await expect(f.view.hasAuthenticatedCatalogue()).resolves.toBe(false);
  expect(publish).toHaveBeenCalledTimes(2);
  expect(f.viewTake).not.toHaveBeenCalled();
  await f.owner.close();
});

it("shares owner capture publication only after each requested view qualifies", async () => {
  const entered = deferred(),
    release = deferred();
  const bytes = Uint8Array.of(1);
  let capture: Uint8Array | null = bytes;
  const publish = vi.fn(async (_scope: CatalogueCacheScope, actual: Uint8Array) => {
    expect(actual).toEqual(bytes);
    entered.resolve();
    await release.promise;
  });
  const cache: CatalogueCache = { load: async () => null, publish };
  const f = fixture(
    () => {
      const moved = capture;
      capture = null;
      return { capture: moved, ready: true };
    },
    () => true,
    cache,
  );
  const other = f.owner.registerSchemaView(schemaB);
  const first = f.view.hasAuthenticatedCatalogue();
  await entered.promise;
  const second = other.hasAuthenticatedCatalogue();
  release.resolve();
  await expect(first).resolves.toBe(true);
  await expect(second).resolves.toBe(true);
  expect(f.viewReady).toHaveBeenCalledTimes(2);
  expect(f.viewTake).not.toHaveBeenCalled();
  expect(publish).toHaveBeenCalledOnce();
  await f.owner.close();
});

it("retains owner capture on failed publication and propagates failure to qualified view", async () => {
  const failure = new Error("durable publication failed");
  const events: string[] = [];
  const bytes = Uint8Array.of(2);
  let capture: Uint8Array | null = bytes,
    attempts = 0;
  const cache: CatalogueCache = {
    load: async () => null,
    publish: async (_scope, actual) => {
      expect(actual).toEqual(bytes);
      events.push("publish");
      if (++attempts === 1) throw failure;
    },
  };
  const f = fixture(
    () => {
      events.push("take-owner");
      const moved = capture;
      capture = null;
      return { capture: moved, ready: true };
    },
    () => true,
    cache,
  );
  await expect(f.view.hasAuthenticatedCatalogue()).rejects.toBe(failure);
  await expect(f.view.hasAuthenticatedCatalogue()).resolves.toBe(true);
  expect(events).toEqual(["take-owner", "publish", "publish", "take-owner"]);
  expect(f.viewTake).not.toHaveBeenCalled();
  await f.owner.close();
});

it("drains admitted owner publication before physical close and never reports ready after close", async () => {
  const entered = deferred(),
    release = deferred();
  let capture: Uint8Array | null = Uint8Array.of(3);
  const publish = vi.fn(async (_scope: CatalogueCacheScope, _actual: Uint8Array) => {
    entered.resolve();
    await release.promise;
  });
  const f = fixture(
    () => {
      const moved = capture;
      capture = null;
      return { capture: moved, ready: true };
    },
    () => true,
    { load: async () => null, publish },
  );
  const ready = f.view.hasAuthenticatedCatalogue();
  await entered.promise;
  const closing = f.owner.close();
  expect(f.owner.isClosed()).toBe(true);
  expect(f.ownerDb.close).not.toHaveBeenCalled();
  release.resolve();
  await expect(ready).resolves.toBe(false);
  await closing;
  expect(publish).toHaveBeenCalledOnce();
  expect(f.ownerDb.close).toHaveBeenCalledOnce();
  expect(f.viewTake).not.toHaveBeenCalled();
});

it("never reports a facade ready if it closes during owner publication", async () => {
  const entered = deferred(),
    release = deferred();
  let capture: Uint8Array | null = Uint8Array.of(4);
  const f = fixture(
    () => {
      const moved = capture;
      capture = null;
      return { capture: moved, ready: true };
    },
    () => true,
    {
      load: async () => null,
      publish: async () => {
        entered.resolve();
        await release.promise;
      },
    },
  );
  const ready = f.view.hasAuthenticatedCatalogue();
  await entered.promise;
  await f.view.close();
  release.resolve();
  await expect(ready).resolves.toBe(false);
  await expect(f.owner.hasAuthenticatedCatalogue()).resolves.toBe(true);
  expect(f.viewTake).not.toHaveBeenCalled();
  await f.owner.close();
});

it("preserves exact requested-view readiness errors without draining owner captures", async () => {
  const failure = new Error("schema policy metadata conflict");
  const f = fixture(
    () => ({ ready: true }),
    () => {
      throw failure;
    },
  );
  await expect(f.view.hasAuthenticatedCatalogue()).rejects.toBe(failure);
  expect(f.ownerTake).not.toHaveBeenCalled();
  expect(f.viewTake).not.toHaveBeenCalled();
  await f.owner.close();
});
