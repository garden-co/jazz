import { describe, expect, test } from "vitest";
import { startWorker } from "./index.js";
import { createWorkerAccount, startSyncServer } from "../testing.js";
import { co, z } from "../tools";

describe("startWorker", () => {
  test("starts a worker", async () => {
    const syncServer = await startSyncServer();

    const { accountID, agentSecret } = await createWorkerAccount({
      name: "test",
      peer: `ws://localhost:${syncServer.port}`,
    });

    const { worker } = await startWorker({
      accountID,
      accountSecret: agentSecret,
      syncServer: `ws://localhost:${syncServer.port}`,
    });

    const Pet = co.map({
      name: z.string(),
    });

    const pet = Pet.create({
      name: "Fido",
    });

    await pet.waitForSync();

    expect(pet.name).toBe("Fido");
  });
});
