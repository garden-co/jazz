import { readFileSync } from "node:fs";
import path from "node:path";
import type { Plugin } from "fumadocs-mdx";
import { expandBindingsExcerpts } from "./excerpt";
import { filterLanguageTabs, LANGUAGES, markExperimental } from "./index";

const repoRoot = path.resolve(process.cwd(), "..");

/**
 * Fumadocs MDX plugin, run on each page's source before it compiles:
 * - drops switched-off languages' tabs, so their `<include>`s are never read
 *   and neither the page nor its Markdown export mentions them;
 *   (`<include>`d partials skip this hook, so a test keeps language tabs and
 *   `<BindingsExcerpt>` out of `content/partials`);
 * - labels experimental languages' tabs and setup pages (`markExperimental`);
 * - expands `<BindingsExcerpt language="…" />` into a code block cut by symbol
 *   from that language's generated bindings.
 * See `./index.ts` and `./README.md`.
 */
export function languageTabsPlugin(): Plugin {
  return {
    name: "jazz-language-tabs",
    doc: {
      vfile(file) {
        const slug = file.path
          .replace(/^(?:.*[\\/])?content[\\/]docs[\\/]/, "")
          .replace(/\.mdx?$/, "");
        const filtered = markExperimental(
          filterLanguageTabs(String(file.value), LANGUAGES, file.path),
          slug,
        );
        file.value = expandBindingsExcerpts(
          filtered,
          (label) => LANGUAGES.find((l) => l.label === label && l.enabled)?.bindings,
          (f) => readFileSync(path.join(repoRoot, f), "utf8"),
          file.path,
        );
        return file;
      },
    },
  };
}
