import type { Page } from "@/schema";

export type PageNode = Pick<Page, "id" | "parentId" | "title" | "kind">;

export type PageTree<T extends PageNode = PageNode> = {
  byId: Map<string, T>;
  /** Pages whose parent this person cannot see: the tops of their tree. */
  roots: T[];
  children: (id: string) => T[];
  ancestors: (id: string) => T[];
  descendantIds: (id: string) => Set<string>;
  depth: (id: string) => number;
};

/**
 * Build the visible page tree from a flat list already ordered by
 * `$createdAt`. Sibling order is exactly that order. A page whose parent is not
 * readable (for example the top of a page shared with a guest) becomes a root.
 */
export function buildPageTree<T extends PageNode>(pages: readonly T[]): PageTree<T> {
  const byId = new Map(pages.map((page) => [page.id, page]));
  const childLists = new Map<string, T[]>();
  const roots: T[] = [];
  for (const page of pages) {
    if (page.parentId && byId.has(page.parentId)) {
      const list = childLists.get(page.parentId) ?? [];
      list.push(page);
      childLists.set(page.parentId, list);
    } else {
      roots.push(page);
    }
  }
  const children = (id: string) => childLists.get(id) ?? [];
  const ancestors = (id: string) => {
    const chain: T[] = [];
    const seen = new Set<string>([id]);
    let parentId = byId.get(id)?.parentId;
    while (parentId && byId.has(parentId) && !seen.has(parentId)) {
      seen.add(parentId);
      const parent = byId.get(parentId)!;
      chain.unshift(parent);
      parentId = parent.parentId;
    }
    return chain;
  };
  const descendantIds = (id: string) => {
    const found = new Set<string>();
    const stack = [id];
    while (stack.length) {
      for (const child of children(stack.pop()!)) {
        if (found.has(child.id)) continue;
        found.add(child.id);
        stack.push(child.id);
      }
    }
    return found;
  };
  const depth = (id: string) => ancestors(id).length;
  return { byId, roots, children, ancestors, descendantIds, depth };
}
