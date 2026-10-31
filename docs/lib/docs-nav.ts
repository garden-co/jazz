import { isValidElement, type ReactNode } from "react";
import type { Node, Root } from "fumadocs-core/page-tree";
import type { NavEntry, NavSection } from "@/components/docs/docs-side-nav";

/** Plain text of a page-tree name or TOC title (which may hold inline code). */
export function textOf(node: ReactNode): string {
  if (node == null || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textOf).join("");
  if (isValidElement<{ children?: ReactNode }>(node)) return textOf(node.props.children);
  return "";
}

function entryOf(node: Node): NavEntry | undefined {
  switch (node.type) {
    case "page":
      return { label: textOf(node.name), url: node.url };
    case "folder": {
      const url = node.index?.url;
      const children = node.children
        .map(entryOf)
        .filter((e): e is NavEntry => e != null && !(url && e.url === url));
      // A folder that holds only its index page is just a page in the nav.
      if (children.length === 0) return { label: textOf(node.name), url };
      return { label: textOf(node.name), url, defaultOpen: node.defaultOpen, children };
    }
    default:
      return undefined;
  }
}

/**
 * Flattens the Fumadocs page tree into serializable sidebar sections for the
 * client `DocsSideNav`: each `---Title---` separator in `meta.json` starts a
 * section, and pages before the first one form an untitled "Overview".
 */
export function docsNavSections(tree: Root): NavSection[] {
  const sections: NavSection[] = [{ title: "Overview", entries: [] }];
  for (const node of tree.children) {
    if (node.type === "separator") {
      sections.push({ title: textOf(node.name), entries: [] });
      continue;
    }
    const entry = entryOf(node);
    if (entry) sections[sections.length - 1].entries.push(entry);
  }
  return sections.filter((section) => section.entries.length > 0);
}
