import { disabledSetupPages } from "./index";

type File = { type: "page" | "meta"; path: string; data: unknown };

const stripExt = (p: string) => p.replace(/\.mdx?$/, "");

/**
 * Removes switched-off languages' setup pages from a docs source, and their
 * entries from `meta.json` page lists, so they get no URL and no sidebar entry.
 */
export function withoutDisabledLanguages<S extends { files: File[] }>(source: S): S {
  const hidden = disabledSetupPages();
  if (hidden.size === 0) return source;
  const files = source.files
    .filter((f) => f.type !== "page" || !hidden.has(stripExt(f.path)))
    .map((f) => {
      const pages = (f.data as { pages?: unknown }).pages;
      if (f.type !== "meta" || !Array.isArray(pages)) return f;
      const dir = f.path.includes("/") ? f.path.slice(0, f.path.lastIndexOf("/") + 1) : "";
      const kept = pages.filter((p) => !(typeof p === "string" && hidden.has(dir + p)));
      return { ...f, data: { ...(f.data as object), pages: kept } };
    });
  return { ...source, files };
}
