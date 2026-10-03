import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { strict as assert } from "node:assert";
import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createJazzSession } from "../../backend/create-jazz-session.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../device-requests.js";
import { spaceSchema } from "../spaces.js";
import { createNativeKeyEnvelope } from "../native.js";

export const app = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
});
export const permissions = {
  ...deviceRequestPermissions,
  ...definePermissions(app, ({ policy, session }) => {
    policy.projects.allowRead.always();
    policy.projects.allowInsert.always();
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    policy.__e2ee_space_successors.allowRead.always();
    policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
  }),
};

function store(path: string) {
  let writes = Promise.resolve();
  const read = async () => {
    try {
      return await readFile(path, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
  };
  return {
    read,
    update(transform: (value: string | null) => string) {
      const result = writes.then(async () =>
        writeFile(path, transform(await read()), { mode: 0o600 }),
      );
      writes = result.catch(() => {});
      return result;
    },
  };
}

async function main() {
  const config = JSON.parse(process.argv[2]!);
  const keys = await createNativeKeyEnvelope();
  const owner = await createJazzSession({
    appId: config.appId,
    serverUrl: config.serverUrl,
    app,
    permissions,
    driver: { type: "memory" },
    initial: "local-first",
    store: store(join(config.directory, "account.json")),
    e2ee: {
      app,
      store: store(join(config.directory, "device.json")),
      crypto: {
        keyEnvelope: {
          ...keys,
          async seal(publicKey, context, plaintext) {
            if (
              config.phase === "interrupt" &&
              new TextDecoder().decode(context).includes('"delivery"')
            )
              throw new Error("Interrupted process handoff");
            return keys.seal(publicKey, context, plaintext);
          },
        },
      },
    },
  });
  try {
    const db = owner.getSnapshot().client!.db;
    const devices = await db.e2ee.devices.list();
    assert.equal(devices.length, 1);
    let projectId = config.projectId;
    if (config.phase === "interrupt") {
      projectId = (
        await db.insert(app.projects, { title: "Restart handoff" }).wait({ tier: "global" })
      ).id;
      await assert.rejects(
        db.e2ee.spaces.grant(app.projects, projectId, config.recipientId).wait(),
        /Interrupted process handoff/,
      );
      assert.deepEqual(await db.all(app.__e2ee_space_deliveries, { tier: "global" }), []);
    } else {
      assert.deepEqual(await db.e2ee.explain({ scope: app.projects, identifier: projectId }), {
        state: "refused",
        reason: "not-a-space-recipient",
      });
    }
    const roots = await db.all(app.__e2ee_spaces, { tier: "global" });
    assert.equal(roots.length, 1);
    const grants = await db.all(app.__e2ee_space_grants, { tier: "global" });
    assert.equal(grants.length, 1);
    assert.equal(grants[0]!.recipientId, config.recipientId);
    process.stdout.write(
      "E2EE_RESULT " +
        JSON.stringify({
          accountId: owner.getSnapshot().account!.id,
          deviceId: devices[0]!.id,
          projectId,
          rootId: roots[0]!.id,
          epochId: roots[0]!.epochId,
        }) +
        "\n",
    );
  } finally {
    await owner.close();
  }
}
if (process.env.JAZZ_E2EE_HANDOFF_CHILD === "1")
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
