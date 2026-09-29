import type { Db } from "jazz-tools";
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

/** An issue is a page under the issues database plus a row of properties. */
export function createIssue(
  db: Db,
  input: {
    workspaceId: string;
    databaseId: string;
    title: string;
    status: IssueStatus;
    assignee: string | null;
  },
): Page {
  const page = createPage(db, {
    workspaceId: input.workspaceId,
    parentId: input.databaseId,
    title: input.title,
    kind: "issue",
  });
  db.insert(app.issues, {
    workspaceId: input.workspaceId,
    pageId: page.id,
    databaseId: input.databaseId,
    status: input.status,
    priority: "none",
    assignee: input.assignee,
    labels: [],
  });
  return page;
}

/**
 * Delete a page with everything under it. Deepest pages go first: each delete
 * is authorized through its parent chain, so parents must still exist while
 * their children are removed.
 */
export async function deletePageTree(db: Db, tree: PageTree, pageId: string): Promise<void> {
  const ids = [pageId, ...tree.descendantIds(pageId)];
  const byDepthDesc = ids.sort((a, b) => tree.depth(b) - tree.depth(a));
  const [blocks, attachments, issues] = await Promise.all([
    db.all(app.blocks.where({ pageId: { in: ids } }).select("pageId")),
    db.all(app.attachments.where({ pageId: { in: ids } }).select("pageId")),
    db.all(app.issues.where({ pageId: { in: ids } }).select("pageId")),
  ]);
  for (const block of blocks) db.delete(app.blocks, block.id);
  for (const attachment of attachments) db.delete(app.attachments, attachment.id);
  for (const issue of issues) db.delete(app.issues, issue.id);
  for (const id of byDepthDesc) db.delete(app.pages, id);
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
