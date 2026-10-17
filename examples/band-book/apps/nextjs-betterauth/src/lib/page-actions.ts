import type { Db, DurabilityTier } from "jazz-tools";
import { app, type IssueStatus, type Page } from "@/schema";
import type { PageTree } from "./tree";

/** A new page is visible and editable immediately; it syncs in the background. */
export function createPage(
  db: Db,
  input: { workspaceId: string; parentId: string | null; title: string; kind?: Page["kind"] },
): Page {
  return db.insert(app.pages, {
    workspaceId: input.workspaceId,
    parentId: input.parentId,
    title: input.title,
    kind: input.kind ?? "doc",
  }).value;
}

/**
 * An issue is a page under the issues database plus a row of properties. Both
 * rows commit together, so no client ever sees an issue page without its
 * properties, or properties pointing at a page that is not there.
 */
export async function createIssue(
  db: Db,
  input: {
    workspaceId: string;
    databaseId: string;
    title: string;
    status: IssueStatus;
    assignee: string | null;
  },
): Promise<Page> {
  const result = await db.transaction((tx) => {
    const page = tx.insert(app.pages, {
      workspaceId: input.workspaceId,
      parentId: input.databaseId,
      title: input.title,
      kind: "issue",
    });
    tx.insert(app.issues, {
      workspaceId: input.workspaceId,
      pageId: page.id,
      databaseId: input.databaseId,
      status: input.status,
      priority: "none",
      assignee: input.assignee,
      labels: [],
    });
    return page;
  });
  return result.value;
}

/**
 * Delete a page with everything under it, in one transaction: the authority
 * accepts or rejects the whole subtree, so a failure never leaves half a tree.
 *
 * Order matters inside the transaction, because each delete is authorized
 * against the rows still there: content first, then the grants and invite
 * links that point at these pages (so an invite to a deleted page stops
 * working), then the pages, deepest first, so each parent still exists while
 * its children are removed.
 */
export async function deletePageTree(
  db: Db,
  tree: PageTree,
  pageId: string,
  durability: { tier: DurabilityTier } = { tier: "local" },
): Promise<void> {
  const ids = [pageId, ...tree.descendantIds(pageId)];
  const byDepthDesc = [...ids].sort((a, b) => tree.depth(b) - tree.depth(a));
  const inTree = { pageId: { in: ids } };
  const result = await db.transaction(async (tx) => {
    const [blocks, attachments, issues, grants, invites] = await Promise.all([
      tx.all(app.blocks.where(inTree).select("pageId")),
      tx.all(app.attachments.where(inTree).select("pageId")),
      tx.all(app.issues.where(inTree).select("pageId")),
      tx.all(app.pageGrants.where(inTree).select("pageId")),
      tx.all(app.invites.where(inTree).select("pageId")),
    ]);
    for (const row of blocks) tx.delete(app.blocks, row.id);
    for (const row of attachments) tx.delete(app.attachments, row.id);
    for (const row of issues) tx.delete(app.issues, row.id);
    for (const row of grants) tx.delete(app.pageGrants, row.id);
    for (const row of invites) tx.delete(app.invites, row.id);
    for (const id of byDepthDesc) tx.delete(app.pages, id);
  });
  await result.wait(durability);
}

/** Pages a page may move under: anything outside its own subtree. */
export function moveTargets<T extends Pick<Page, "id" | "kind">>(
  tree: PageTree,
  pages: readonly T[],
  pageId: string,
): T[] {
  const blocked = tree.descendantIds(pageId);
  blocked.add(pageId);
  return pages.filter((page) => page.kind === "doc" && !blocked.has(page.id));
}
