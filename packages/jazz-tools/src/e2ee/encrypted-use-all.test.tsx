import { expect, it } from "vitest";
import { act, render, waitFor } from "@testing-library/react";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzClient } from "../react/create-jazz-client.js";
import { JazzClientProvider } from "../react-core/provider.js";
import { useAll } from "../react-core/use-all.js";
import { createBrowserCrypto } from "./browser.js";
import { deviceRequestSchema, deviceRequestPermissions } from "./device-requests.js";
import { spaceSchema } from "./spaces.js";

it.each(["ready", "error", "update", "history"])(
  "useAll keeps encrypted data loading until decryption settles (%s)",
  async (scenario) => {
    const fail = scenario === "error";
    const app = s.defineApp({
      ...deviceRequestSchema,
      ...spaceSchema,
      projects: s.table({ title: s.string() }, {}),
      notes: s
        .table(
          { projectId: s.uuid(), title: s.string() },
          { project: s.rel("projects", "projectId") },
        )
        .encrypted({
          space: "projectId",
          columns: ["title"],
          ...(scenario === "history" ? { indexes: { title: "equality" as const } } : {}),
        }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.projects.allowRead.always();
      policy.projects.allowInsert.always();
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.__e2ee_spaces.allowRead.always();
      policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
      policy.__e2ee_space_successors.allowRead.always();
      policy.__e2ee_space_successors.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_grants.allowRead.always();
      policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
      policy.__e2ee_space_deliveries.allowRead.always();
      policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let client: Awaited<ReturnType<typeof createJazzClient>> | undefined;
    let view: ReturnType<typeof render> | undefined;
    let release!: () => void;
    let gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let decryptStarted = false;
    let historyGate: Promise<void> | undefined;
    let releaseHistory = () => {};
    let historyStarted = false;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: { ...deviceRequestPermissions, ...permissions },
      });
      const crypto = await createBrowserCrypto();
      let saved: string | null = null;
      client = await createJazzClient({
        ...(await localAccountConfig(server.appId, server.url)),
        e2ee: {
          app,
          store: {
            async read() {
              return saved;
            },
            async update(transform) {
              saved = transform(saved);
            },
          },
          crypto: {
            ...crypto,
            deviceSigner: {
              ...crypto.deviceSigner,
              async verify(...args) {
                if (historyGate) {
                  historyStarted = true;
                  await historyGate;
                }
                return crypto.deviceSigner.verify(...args);
              },
            },
            cellCipher: {
              ...crypto.cellCipher,
              async decrypt(...args) {
                decryptStarted = true;
                await gate;
                if (fail) throw new Error("Test hook decryption failure");
                return crypto.cellCipher.decrypt(...args);
              },
            },
          },
        },
      });
      const tx = client.db.beginExclusiveTransaction();
      const project = tx.insert(app.projects, { title: "Project" });
      const note = tx.insert(app.notes, { projectId: project.id, title: "Private title" });
      const unchanged =
        scenario === "update"
          ? tx.insert(app.notes, { projectId: project.id, title: "Unchanged title" })
          : undefined;
      await tx.commit().wait({ tier: "global" });
      const query =
        scenario === "history"
          ? app.notes.where({ projectId: project.id, title: note.title })
          : unchanged
            ? app.notes.where({ projectId: project.id })
            : app.notes.where({ id: note.id });
      const states: ReturnType<typeof useAll<typeof note>>[] = [];
      function Observer() {
        const state = useAll(query, { tier: "global" });
        states.push(state);
        return (
          <div>
            {state.isLoading
              ? "Loading"
              : state.error
                ? state.error.message
                : state.data?.[0]?.title}
          </div>
        );
      }
      view = render(
        <JazzClientProvider client={client}>
          <Observer />
        </JazzClientProvider>,
      );
      await waitFor(() => expect(decryptStarted).toBe(true), { timeout: 10_000 });
      expect(states.at(-1)).toEqual({ data: undefined, isLoading: true, error: null });
      await act(async () => {
        release();
      });
      await waitFor(() => expect(states.at(-1)?.isLoading).toBe(false), { timeout: 10_000 });
      if (fail) {
        expect(states.at(-1)?.data).toBeUndefined();
        expect(states.at(-1)?.error).toMatchObject({
          name: "E2eeDataError",
          code: "invalid-ciphertext",
          message: "Encrypted data could not be authenticated or decoded",
        });
        expect(states.at(-1)?.error).not.toHaveProperty("cause");
      } else {
        expect(states.at(-1)).toEqual({
          data: unchanged ? expect.arrayContaining([note, unchanged]) : [note],
          isLoading: false,
          error: null,
        });
      }
      expect(
        states.every(
          (state) =>
            state.data === undefined ||
            state.data.every(
              (row) => row.title === "Private title" || row.title === "Unchanged title",
            ),
        ),
      ).toBe(true);
      if (scenario === "history") {
        const root = await client.db.one(app.__e2ee_spaces, { tier: "global" });
        expect(root).not.toBeNull();
        historyGate = new Promise<void>((resolve) => {
          releaseHistory = resolve;
        });
        await act(async () => {
          // A new untrusted proposal must be checked before the hook says ready.
          await client!.db
            .insert(app.__e2ee_space_successors, {
              spaceId: root!.id,
              predecessor: root!.epochId,
              epochId: globalThis.crypto.randomUUID(),
              authorAccountId: root!.accountId,
              authorDeviceId: root!.deviceId,
              authorEpochId: root!.accountEpochId,
              revision: new Uint8Array(32),
              membership: new Uint8Array(),
              verification: new Uint8Array(),
              history: new Uint8Array(),
              authorEnvelope: new Uint8Array(),
              signature: new Uint8Array(64),
            })
            .wait({ tier: "global" });
        });
        await waitFor(() => expect(historyStarted).toBe(true), { timeout: 10_000 });
        expect(states.at(-1)).toEqual({ data: undefined, isLoading: true, error: null });
        await act(async () => {
          releaseHistory();
          historyGate = undefined;
        });
        await waitFor(
          () => expect(states.at(-1)).toEqual({ data: [note], isLoading: false, error: null }),
          { timeout: 10_000 },
        );
      }
      if (unchanged) {
        decryptStarted = false;
        gate = new Promise<void>((resolve) => {
          release = resolve;
        });
        await act(async () => {
          await client!.db
            .update(app.notes, note.id, { title: "Updated title" })
            .wait({ tier: "global" });
        });
        await waitFor(() => expect(decryptStarted).toBe(true), { timeout: 10_000 });
        expect(states.at(-1)).toEqual({ data: undefined, isLoading: true, error: null });
        await act(async () => {
          release();
        });
        await waitFor(() => expect(states.at(-1)?.isLoading).toBe(false), { timeout: 10_000 });
        expect(states.at(-1)?.data).toHaveLength(2);
        expect(states.at(-1)?.data).toEqual(
          expect.arrayContaining([{ ...note, title: "Updated title" }, unchanged]),
        );
      }
    } finally {
      releaseHistory();
      release();
      view?.unmount();
      await client?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
