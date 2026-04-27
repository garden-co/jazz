import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { createJazzContext, type JazzContext } from "jazz-tools/backend";
import { app } from "../schema.js";
import permissions from "../permissions.js";
import { createIssueRepository } from "../src/repository.js";

const ALICE_JAZZ_ID = "11111111-1111-4111-8111-111111111111";
const BOB_JAZZ_ID = "22222222-2222-4222-8222-222222222222";

function createContext(): JazzContext {
  return createJazzContext({
    appId: randomUUID(),
    app,
    permissions,
    driver: {
      type: "persistent",
      dataPath: join(mkdtempSync(join(tmpdir(), "skill-issues-")), "jazz.db"),
    },
    backendSecret: "skill-issues-test-backend-secret",
    env: "test",
    userBranch: "main",
  });
}

describe("issue repository", () => {
  let contexts: JazzContext[] = [];

  afterEach(async () => {
    await Promise.all(contexts.map((context) => context.shutdown()));
    contexts = [];
  });

  function repositories() {
    const context = createContext();
    contexts.push(context);

    return {
      backend: createIssueRepository(context.db(), app),
      alice: createIssueRepository(
        context.forSession({
          user_id: ALICE_JAZZ_ID,
          authMode: "local-first",
          claims: {},
        }),
        app,
      ),
      bob: createIssueRepository(
        context.forSession({
          user_id: BOB_JAZZ_ID,
          authMode: "local-first",
          claims: {},
        }),
        app,
      ),
    };
  }

  it("rejects item writes from an unverified local-first user", async () => {
    const { alice } = repositories();

    await expect(
      alice.upsertItem({
        kind: "issue",
        title: "Sync status loses assignee",
        description: "Alice can reproduce status changes clearing the current assignee.",
        slug: "sync-status-loses-assignee",
      }),
    ).rejects.toThrow(/verified GitHub identity/i);
  });

  it("lets a verified user create, assign, complete, and list an issue", async () => {
    const { backend, alice } = repositories();

    await backend.upsertVerifiedUser({
      id: ALICE_JAZZ_ID,
      githubUserId: "1001",
      githubLogin: "alice",
      verifiedAt: "2026-04-27T10:00:00.000Z",
    });

    await alice.upsertItem({
      kind: "issue",
      title: "Markdown capture should preserve notes",
      description: "Alice captured an issue with multiline notes from the CLI.",
      slug: "markdown-capture-preserve-notes",
    });
    await alice.assignMe("markdown-capture-preserve-notes");
    await alice.setStatus("markdown-capture-preserve-notes", "done");

    await expect(alice.listItems({})).resolves.toEqual([
      {
        kind: "issue",
        title: "Markdown capture should preserve notes",
        description: "Alice captured an issue with multiline notes from the CLI.",
        slug: "markdown-capture-preserve-notes",
        state: {
          itemSlug: "markdown-capture-preserve-notes",
          status: "done",
          assigneeUserId: ALICE_JAZZ_ID,
        },
        assignee: {
          id: ALICE_JAZZ_ID,
          githubUserId: "1001",
          githubLogin: "alice",
          verifiedAt: "2026-04-27T10:00:00.000Z",
        },
      },
    ]);
  });

  it("rejects verified-user writes from regular users", async () => {
    const { bob } = repositories();

    await expect(
      bob.upsertVerifiedUser({
        id: BOB_JAZZ_ID,
        githubUserId: "1002",
        githubLogin: "bob",
        verifiedAt: "2026-04-27T11:00:00.000Z",
      }),
    ).rejects.toThrow(/policy denied/i);
  });

  it("rejects verified-user updates from regular users", async () => {
    const { backend, alice } = repositories();

    await backend.upsertVerifiedUser({
      id: ALICE_JAZZ_ID,
      githubUserId: "1001",
      githubLogin: "alice",
      verifiedAt: "2026-04-27T10:00:00.000Z",
    });

    await expect(
      alice.upsertVerifiedUser({
        id: ALICE_JAZZ_ID,
        githubUserId: "9999",
        githubLogin: "mallory",
        verifiedAt: "2026-04-27T12:00:00.000Z",
      }),
    ).rejects.toThrow(/policy denied/i);

    await expect(alice.currentUser()).resolves.toEqual({
      id: ALICE_JAZZ_ID,
      githubUserId: "1001",
      githubLogin: "alice",
      verifiedAt: "2026-04-27T10:00:00.000Z",
    });
  });
});
