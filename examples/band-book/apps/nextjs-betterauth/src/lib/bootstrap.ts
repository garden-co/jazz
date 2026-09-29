import { createHash } from "node:crypto";
import type { Db } from "jazz-tools";
import { app } from "@/schema";
import { POSITION_STEP } from "./positions";
import { DEMO_PAGES, DEMO_WORKSPACE_NAME, flattenSeedPages, type SeedBlock } from "./seed";

/**
 * A stable UUID (version 5 layout) for one seeded row of one account. Stable
 * ids are what make the bootstrap retry-safe: a second run finds the rows the
 * first run wrote instead of creating a second demo workspace.
 */
export function seedId(account: string, key: string): string {
  const hash = createHash("sha1").update(`band-book:${account}:${key}`).digest();
  hash[6] = (hash[6] & 0x0f) | 0x50;
  hash[8] = (hash[8] & 0x3f) | 0x80;
  const hex = hash.subarray(0, 16).toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}

export type BootstrapResult = { workspaceId: string; created: boolean };

const RETRYABLE = /exclusive_conflict|transaction_conflict|cascade_rejected/;

/**
 * Create the account's demo band workspace once. This is the app's only
 * first-open side effect and runs on the server with backend authority, never
 * from a query hook.
 *
 * Everything is written in one exclusive transaction: a retry after a network
 * failure, a double click or two tabs opening at once either sees the finished
 * workspace or writes the whole thing, never half of it. Rows get creation
 * times one millisecond apart in tree order, so the sidebar's `$createdAt`
 * ordering shows the demo pages in the order they are listed in `seed.ts`.
 */
export async function ensureDemoWorkspace(
  db: Db,
  account: string,
  displayName: string,
  now: () => number = Date.now,
): Promise<BootstrapResult> {
  const workspaceId = seedId(account, "workspace");
  for (let attempt = 0; ; attempt++) {
    try {
      const write = await db.exclusiveTransaction(async (tx) => {
        const existing = await tx.one(app.workspaces.where({ id: workspaceId }).includeDeleted());
        if (existing) return false;
        let clock = now();
        const at = () => ({ updatedAt: clock++ });
        const id = (key: string) => seedId(account, key);

        tx.insert(app.workspaces, { name: DEMO_WORKSPACE_NAME }, { id: workspaceId, ...at() });
        tx.insert(
          app.members,
          { workspaceId, account, displayName, role: "owner" },
          { id: id("member:owner"), ...at() },
        );

        for (const page of flattenSeedPages(DEMO_PAGES)) {
          const pageId = id(`page:${page.key}`);
          tx.insert(
            app.pages,
            {
              workspaceId,
              parentId: page.parentKey ? id(`page:${page.parentKey}`) : null,
              title: page.title,
              kind: page.kind,
            },
            { id: pageId, ...at() },
          );
          if (page.issue) {
            tx.insert(
              app.issues,
              {
                workspaceId,
                pageId,
                databaseId: id(`page:${page.parentKey}`),
                status: page.issue.status,
                priority: page.issue.priority,
                assignee: page.issue.status === "done" ? null : account,
                labels: page.issue.labels,
              },
              { id: id(`issue:${page.key}`), ...at() },
            );
          }
          const insertBlocks = (blocks: SeedBlock[], parentBlockId: string | null, path: string) =>
            blocks.forEach((block, index) => {
              const blockId = id(`block:${page.key}:${path}${index}`);
              tx.insert(
                app.blocks,
                {
                  workspaceId,
                  pageId,
                  parentBlockId,
                  position: (index + 1) * POSITION_STEP,
                  kind: block.kind,
                  text: block.text ?? "",
                  checked: block.checked ?? false,
                  attachmentId: null,
                },
                { id: blockId, ...at() },
              );
              insertBlocks(block.children ?? [], blockId, `${path}${index}.`);
            });
          insertBlocks(page.blocks ?? [], null, "");
        }
        return true;
      });
      const created = await write.wait();
      return { workspaceId, created };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (attempt >= 5 || !RETRYABLE.test(message)) throw error;
    }
  }
}
