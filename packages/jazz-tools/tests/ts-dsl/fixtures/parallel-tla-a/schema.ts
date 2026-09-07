import { col, table } from "jazz-tools";

type TlaBarrier = {
  registered: Set<string>;
  promise: Promise<void>;
  resolve: () => void;
};

type TlaGlobals = typeof globalThis & {
  __jazzSchemaLoaderTlaBarrier?: TlaBarrier;
};

const globals = globalThis as TlaGlobals;
let barrier = globals.__jazzSchemaLoaderTlaBarrier;
if (!barrier) {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  barrier = { registered: new Set(), promise, resolve };
  globals.__jazzSchemaLoaderTlaBarrier = barrier;
}

table("parallel_a", {
  value: col.string(),
});
barrier.registered.add("a");
if (barrier.registered.size === 2) barrier.resolve();

try {
  await barrier.promise;
} finally {
  if (globals.__jazzSchemaLoaderTlaBarrier === barrier) {
    delete globals.__jazzSchemaLoaderTlaBarrier;
  }
}
