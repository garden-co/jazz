import { expect, it } from "vitest";
import {
  createAccountManager,
  createDb,
  definePermissions,
  schema as s,
  type AccountHandle,
  type Db,
  type StreamingWritePlan,
} from "jazz-tools";
import { deploy } from "../../src/dev/catalogue.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("uploads and replaces encrypted files through the public package in Chromium", async () => {
  const server = await getJazzServerInfo(`e2ee-file-upload-${crypto.randomUUID()}`);
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    files: s
      .table(
        { projectId: s.uuid(), payload: s.bytes(), name: s.string() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted({ space: "projectId", columns: ["payload", "name"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.files.allowRead.always();
    policy.files.allowInsert.always();
    policy.files.allowUpdate.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
  });
  const clients: Db[] = [];
  const accounts: AccountHandle[] = [];
  const stores = ["writer", "reader"].map(() => {
    let saved: string | null = null;
    return {
      async read() {
        return saved;
      },
      async update(transform: (value: string | null) => string) {
        saved = transform(saved);
      },
    };
  });
  try {
    await deploy({ ...server, schema: app, permissions });
    for (const store of stores) {
      let accountState: string | null = null;
      const manager = await createAccountManager({
        appId: server.appId,
        serverUrl: server.serverUrl,
        store: {
          async read() {
            return accountState;
          },
          async update(transform) {
            accountState = transform(accountState);
          },
        },
      });
      const account = await manager.createLocalFirst();
      accounts.push(account);
      clients.push(
        await createDb({
          appId: server.appId,
          serverUrl: server.serverUrl,
          account,
          driver: { type: "memory" },
          e2ee: { app, store },
        }),
      );
    }
    const writer = clients[0]!;
    let reader = clients[1]!;
    const payload = Uint8Array.from({ length: 196_633 }, (_, index) => index % 251);
    let pulls = 0;
    const result = await writer!.streamingTransaction((plan: StreamingWritePlan) => {
      const project = plan.insert(app.projects, { title: "Browser upload" });
      const file = plan.insertStreaming(app.files, {
        projectId: project.id,
        name: "browser.png",
        payload: new ReadableStream<Uint8Array>({
          pull(controller) {
            const offset = pulls++ * 32_771;
            if (offset >= payload.length) controller.close();
            else controller.enqueue(payload.subarray(offset, offset + 32_771));
          },
        }),
      });
      return { project: project.id, file: file.id };
    });
    const ids = await result.wait({ tier: "global" });
    expect(await writer!.one(app.files.where({ id: ids.file }), { tier: "global" })).toEqual({
      id: ids.file,
      projectId: ids.project,
      name: "browser.png",
      payload,
    });
    const readerAccount = accounts[1]!;
    await reader!.e2ee.devices.list();
    await writer!.e2ee.spaces.grant(app.projects, ids.project, readerAccount.id).wait();
    expect(await reader!.one(app.files.where({ id: ids.file }), { tier: "global" })).toEqual({
      id: ids.file,
      projectId: ids.project,
      name: "browser.png",
      payload,
    });
    await reader.shutdown();
    clients.splice(clients.indexOf(reader), 1);
    // A fresh runtime has no row/chunk cache: fetch the uploaded ciphertext again.
    reader = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: readerAccount,
      driver: { type: "memory" },
      e2ee: { app, store: stores[1]! },
    });
    clients.push(reader);
    expect(await reader.one(app.files.where({ id: ids.file }), { tier: "global" })).toEqual({
      id: ids.file,
      projectId: ids.project,
      name: "browser.png",
      payload,
    });
    const replacement = await writer!.updateStreaming(app.files, ids.file, {
      name: "empty",
      payload: (async function* () {
        yield new Uint8Array();
      })(),
    });
    await replacement.wait({ tier: "global" });
    expect(await reader!.one(app.files.where({ id: ids.file }), { tier: "global" })).toEqual({
      id: ids.file,
      projectId: ids.project,
      name: "empty",
      payload: new Uint8Array(),
    });
  } finally {
    await Promise.all(clients.map((client) => client.shutdown()));
    await stopJazzServer(server.serverUrl);
  }
}, 60_000);
