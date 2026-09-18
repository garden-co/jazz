import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";
import { createNativeDeviceSigner } from "./native.js";

// An opt-in measurement, following the existing abstract-benchmark convention.
it.skipIf(process.env.JAZZ_E2EE_HISTORY_PERF !== "1")(
  "measures cold and repeated native lifecycle reads as accepted history grows",
  async () => {
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
    });
    const policies = definePermissions(app, ({ policy, session }) => {
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
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
    const memoryStore = () => {
      let value: string | null = null;
      return {
        async read() {
          return value;
        },
        async update(transform: (current: string | null) => string) {
          value = transform(value);
        },
      };
    };
    const signer = await createNativeDeviceSigner();
    let verifications = 0;
    let rootSignature: string | undefined;
    let rootVerifications = 0;
    let verificationInvocationMs = 0;
    const verificationInputs = new Set<string>();
    const measuredSigner = {
      ...signer,
      async verify(...args: Parameters<typeof signer.verify>) {
        verifications++;
        if (Buffer.from(args[2]).toString("base64") === rootSignature) rootVerifications++;
        verificationInputs.add(
          args.map((value) => Buffer.from(value).toString("base64")).join(":"),
        );
        // Native verification runs synchronously before its promise is returned.
        // Exclude input bookkeeping and promise scheduling from this measurement.
        const start = performance.now();
        const result = signer.verify(...args);
        verificationInvocationMs += performance.now() - start;
        return result;
      },
    };
    const open = async (
      accountStore: ReturnType<typeof memoryStore>,
      deviceStore: ReturnType<typeof memoryStore>,
    ) => {
      const session = await createJazzSession({
        appId: server.appId,
        serverUrl: server.url,
        app,
        permissions: { ...deviceRequestPermissions, ...policies },
        driver: { type: "memory" },
        initial: "local-first",
        store: accountStore,
        e2ee: { app, store: deviceStore, crypto: { deviceSigner: measuredSigner } },
      });
      sessions.push(session);
      return session.getSnapshot().client!.db;
    };
    const measure = async (rotations: number, phase: string, operation: () => Promise<unknown>) => {
      const count = verifications;
      const invocationMs = verificationInvocationMs;
      verificationInputs.clear();
      const start = performance.now();
      const result = await operation();
      console.info(
        "E2EE_HISTORY_PERF",
        JSON.stringify({
          rotations,
          phase,
          elapsedMs: performance.now() - start,
          verifications: verifications - count,
          uniqueVerifications: verificationInputs.size,
          verificationInvocationMs: verificationInvocationMs - invocationMs,
        }),
      );
      return result;
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...policies },
      });
      const ownerAccount = memoryStore();
      const ownerDevice = memoryStore();
      const owner = await open(ownerAccount, ownerDevice);
      const other = await open(memoryStore(), memoryStore());
      await owner.e2ee.devices.list();
      await other.e2ee.devices.list();
      const ownerId = sessions[0]!.getSnapshot().account!.id;
      const otherId = sessions[1]!.getSnapshot().account!.id;
      const project = await owner
        .insert(app.projects, { title: "Lifecycle history measurement" })
        .wait({ tier: "global" });
      await owner.e2ee.spaces.grant(app.projects, project.id, ownerId).wait();
      await owner.e2ee.spaces.grant(app.projects, project.id, otherId).wait();
      const target = { scope: app.projects, identifier: project.id };
      const root = await owner.one(app.__e2ee_spaces.where({ identifier: project.id }), {
        tier: "global",
      });
      expect(root).toBeDefined();
      rootSignature = Buffer.from(root!.signature).toString("base64");
      let rotations = 0;
      for (const size of [0, 1, 2, 4]) {
        while (rotations < size) {
          await measure(rotations, "revoke", () =>
            owner.e2ee.spaces.revoke(app.projects, project.id, otherId).wait(),
          );
          rotations++;
          expect(await other.e2ee.explain(target)).toMatchObject({ state: "refused" });
          await measure(rotations, "grant", () =>
            owner.e2ee.spaces.grant(app.projects, project.id, otherId).wait(),
          );
          expect(await other.e2ee.explain(target)).toEqual({ state: "ready" });
        }
        // New database and lifecycle objects, same accepted account and device keys.
        const cold = await open(ownerAccount, ownerDevice);
        const beforeCold = verifications;
        expect(await measure(size, "cold", () => cold.e2ee.explain(target))).toEqual({
          state: "ready",
        });
        const coldVerifications = verifications - beforeCold;
        for (let sample = 0; sample < 3; sample++) {
          const beforeWarm = verifications;
          const beforeRoot = rootVerifications;
          expect(await measure(size, `warm-${sample}`, () => cold.e2ee.explain(target))).toEqual({
            state: "ready",
          });
          // Saving only the cold device-startup check is not history reuse.
          expect(verifications - beforeWarm).toBeLessThan(coldVerifications - 1);
          // The accepted root was already validated at every required cutoff.
          expect(rootVerifications - beforeRoot).toBe(0);
        }
        const beforePlaintext = verifications;
        await measure(size, "ordinary-local-reads-25", async () => {
          for (let sample = 0; sample < 25; sample++)
            expect(
              await owner.one(app.projects.where({ id: project.id }), { tier: "local" }),
            ).toMatchObject({ title: "Lifecycle history measurement" });
        });
        expect(verifications).toBe(beforePlaintext);
        await sessions.pop()!.close();
      }
    } finally {
      await Promise.all(sessions.map((session) => session.close()));
      await server.stop();
    }
  },
  600_000,
);
