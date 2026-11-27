import { compiledBindings } from "../encryption/build.js";
import { EncryptionRuntime } from "jazz-napi";
import { describe, expect, it } from "vitest";
import { testApp } from "../testing/index.js";
import { testAccountId } from "../runtime/testing/account-fixtures.js";
import { memoryPrivateStore, servePrivateStore } from "./index.js";

// The private store is the app's, answered from JavaScript: what the runtime
// does with answers that come early, twice, never or out of order.

const file = {
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: { space: "projectId", scope: "projects", columns: { title: "string" } },
    },
  },
};
const app = testApp(file);

function runtime() {
  return EncryptionRuntime.fromBindings(
    compiledBindings(app),
    "store-bridge-test",
    "test",
    testAccountId("bridge"),
  );
}

describe("the private store bridge", () => {
  it("refuses an answer to a request that is not outstanding, and still builds", async () => {
    const built = runtime();
    // An answer before any request, for the id the first one will get.
    expect(built.replyStore("1", true, '"early"')).toBe(false);
    const store = memoryPrivateStore();
    const serving = servePrivateStore(built, store);
    try {
      await built.ready();
    } finally {
      serving.stop();
    }
    // An answer twice, and one for an id never issued.
    expect(built.replyStore("1", true, "null")).toBe(false);
    expect(built.replyStore("99999", true, "null")).toBe(false);
  });

  it("fails the call with a store error when an answer is dropped", async () => {
    const built = runtime();
    const store = memoryPrivateStore();
    const opened = store.openScope.bind(store);
    // The store takes the request and never answers it.
    store.openScope = () => new Promise(() => undefined);
    void opened;
    const serving = servePrivateStore(built, store);
    try {
      await expect(built.ready()).rejects.toThrow(/did not answer/);
    } finally {
      serving.stop();
    }
  }, 60_000);

  it("builds when answers are given in the reverse of the order requested", async () => {
    const built = runtime();
    const serving = servePrivateStore(
      {
        takeStoreRequests: () => built.takeStoreRequests().reverse(),
        replyStore: (id, ok, payload) => built.replyStore(id, ok, payload),
      },
      memoryPrivateStore(),
    );
    try {
      await built.ready();
    } finally {
      serving.stop();
    }
  }, 30_000);
});
