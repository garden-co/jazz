/**
 * The languages the docs show in their `jazz-environment` tabs, in tab order.
 *
 * Switch a language on once its setup page, example regions and generated
 * bindings are on main. While it's off, the build drops its tabs, setup page
 * and sidebar entry, so the docs don't mention it anywhere.
 */
import type { BindingsSource } from "./excerpt";

export type Language = {
  /** The tab's `value` and label. */
  label: string;
  /** The page with its setup and "Not supported yet" list, under `content/docs`. */
  setupPage?: string;
  /**
   * The "generated bindings" tab on Defining Tables: this language's committed
   * bindings for StagePlan (`file`, relative to the repository root), and the
   * declarations for its `tasks` table to show. `<BindingsExcerpt>` cuts them
   * out by name; a test fails if one is missing.
   */
  bindings?: BindingsSource;
  /**
   * Whether the language runs on a backend, as a backend client of your main
   * backend (`setup/backend-client`). Client-only languages (Swift, Kotlin in
   * .65) get no tabs on pages whose frontmatter says `languageTabs: backend`.
   */
  backend: boolean;
  /**
   * Whether the language can run your app's main backend (createJazz, serving
   * end users, publishing permissions). Only TypeScript in .65: pages whose
   * frontmatter says `languageTabs: main-backend` get no other tabs. `backend`
   * alone means a backend client (`languageTabs: backend` pages).
   */
  mainBackend?: boolean;
  enabled: boolean;
};

export const LANGUAGES: readonly Language[] = [
  {
    label: "TypeScript",
    mainBackend: true,
    backend: true,
    bindings: {
      file: "examples/stage-plan/apps/react-localfirst/jazz.generated.ts",
      lang: "ts",
      symbols: ["TaskStatus", "Task", "TaskInsert"],
    },
    enabled: true,
  },
  { label: "Effect", backend: true, mainBackend: true, enabled: true },
  {
    label: "Rust",
    backend: true,
    bindings: {
      file: "examples/stage-plan/apps/rust-cli/src/jazz_app.rs",
      lang: "rust",
      symbols: ["TaskStatus", "Task", "NewTask"],
    },
    setupPage: "setup/rust",
    enabled: true,
  },
  {
    label: "Swift",
    backend: false,
    setupPage: "setup/swift",
    bindings: {
      file: "examples/stage-plan/apps/swift-cli/JazzApp.swift",
      lang: "swift",
      symbols: ["TaskStatus", "Task", "NewTask"],
    },
    enabled: false,
  },
  {
    label: "Kotlin",
    backend: false,
    setupPage: "setup/kotlin",
    bindings: {
      file: "examples/stage-plan/apps/kotlin-cli/src/main/kotlin/JazzApp.kt",
      lang: "kotlin",
      symbols: ["TaskStatus", "Task", "NewTask"],
    },
    enabled: false,
  },
  {
    label: "Go",
    backend: true,
    bindings: {
      file: "examples/stage-plan/apps/go-cli/jazzapp/jazz_app.go",
      lang: "go",
      symbols: ["TaskStatus", "Task", "NewTask"],
    },
    enabled: true,
  },
];

export const LANGUAGE_TABS_GROUP = "jazz-environment";

/** Setup pages (`setup/swift`, …) of languages that are switched off. */
export function disabledSetupPages(languages: readonly Language[] = LANGUAGES): Set<string> {
  return new Set(
    languages.filter((l) => !l.enabled && l.setupPage).map((l) => l.setupPage as string),
  );
}

const TAG = /<(\/?)(Tabs|Tab)\b([^>]*)>/g;

function attribute(attrs: string, name: string): string | undefined {
  return new RegExp(`\\b${name}="([^"]*)"`).exec(attrs)?.[1];
}

/**
 * Drops the tabs of switched-off languages from every `jazz-environment`
 * `<Tabs>` in an MDX page, and rewrites its `items` to the tabs that remain.
 * Throws on a tab for a language not in `languages`, on tabs out of order,
 * and on `items` that don't match the tabs.
 */
export function filterLanguageTabs(
  source: string,
  languages: readonly Language[] = LANGUAGES,
  file = "page",
): string {
  const order = new Map(languages.map((l, i) => [l.label, i]));
  const enabled = new Set(languages.filter((l) => l.enabled).map((l) => l.label));
  const scope = /^---\n[\s\S]*?^languageTabs:\s*(backend|main-backend)\s*$[\s\S]*?^---$/m.exec(
    source,
  )?.[1];
  const allowed = (l: Language) =>
    scope === "main-backend" ? !!l.mainBackend : scope === "backend" ? l.backend : true;
  const refused = new Set(languages.filter((l) => !allowed(l)).map((l) => l.label));

  type Block = {
    open: { start: number; end: number; attrs: string };
    tabs: { label: string; start: number; end: number }[];
    tabStart?: { label: string; start: number };
    tabDepth: number;
    tabsDepth: number;
  };

  const edits: { start: number; end: number; text: string }[] = [];
  let block: Block | undefined;
  let tabsDepth = 0;

  for (const match of source.matchAll(TAG)) {
    const [whole, closing, name, attrs] = match;
    const start = match.index;
    const end = start + whole.length;
    const selfClosing = attrs.trimEnd().endsWith("/");

    if (name === "Tabs") {
      if (closing) {
        if (block && tabsDepth === block.tabsDepth) {
          finish(block, source, end);
          block = undefined;
        }
        tabsDepth--;
      } else if (!selfClosing) {
        tabsDepth++;
        if (!block && attribute(attrs, "groupId") === LANGUAGE_TABS_GROUP) {
          block = { open: { start, end, attrs }, tabs: [], tabDepth: 0, tabsDepth };
        }
      }
      continue;
    }

    // <Tab>
    if (!block || tabsDepth !== block.tabsDepth || selfClosing) continue;
    if (!closing) {
      if (block.tabDepth === 0) {
        const label = attribute(attrs, "value");
        if (!label) throw new Error(`${file}: a ${LANGUAGE_TABS_GROUP} <Tab> without a value`);
        block.tabStart = { label, start };
      }
      block.tabDepth++;
    } else {
      block.tabDepth--;
      if (block.tabDepth === 0 && block.tabStart) {
        block.tabs.push({ ...block.tabStart, end });
        block.tabStart = undefined;
      }
    }
  }
  if (block) throw new Error(`${file}: unclosed ${LANGUAGE_TABS_GROUP} <Tabs>`);

  function finish(b: Block, text: string, closeEnd: number) {
    const labels = b.tabs.map((t) => t.label);
    for (const label of labels) {
      if (!order.has(label))
        throw new Error(
          `${file}: ${LANGUAGE_TABS_GROUP} tab "${label}" isn't in docs/lib/languages`,
        );
    }
    if (scope) {
      const wrong = labels.filter((l) => refused.has(l));
      if (wrong.length)
        throw new Error(
          `${file}: a ${scope === "main-backend" ? "main-backend" : "backend-client"} page (languageTabs: ${scope}) has tabs for ${wrong.join(", ")}`,
        );
    }
    for (let i = 1; i < labels.length; i++) {
      if (order.get(labels[i - 1])! > order.get(labels[i])!)
        throw new Error(
          `${file}: ${LANGUAGE_TABS_GROUP} tabs out of order: ${labels.join(", ")} (see docs/lib/languages)`,
        );
    }
    const items = /\bitems=\{(\[[^\]]*\])\}/.exec(b.open.attrs);
    if (!items) throw new Error(`${file}: a ${LANGUAGE_TABS_GROUP} <Tabs> without items`);
    const listed = JSON.parse(items[1]) as string[];
    if (JSON.stringify(listed) !== JSON.stringify(labels))
      throw new Error(
        `${file}: <Tabs items> ${JSON.stringify(listed)} don't match its tabs ${JSON.stringify(labels)}`,
      );

    const kept = labels.filter((l) => enabled.has(l));
    if (kept.length === labels.length) return;
    if (kept.length === 0) {
      edits.push({ start: b.open.start, end: closeEnd, text: "" });
      return;
    }
    const open = text
      .slice(b.open.start, b.open.end)
      .replace(items[0], `items={${JSON.stringify(kept)}}`);
    edits.push({ start: b.open.start, end: b.open.end, text: open.replaceAll('","', '", "') });
    for (const tab of b.tabs) {
      if (enabled.has(tab.label)) continue;
      // Take the tab's own line, indentation and newline with it.
      let start = tab.start;
      while (start > 0 && (text[start - 1] === " " || text[start - 1] === "\t")) start--;
      let end = tab.end;
      if (text[end] === "\n") end++;
      edits.push({ start, end, text: "" });
    }
  }

  let out = source;
  for (const edit of edits.sort((a, b) => b.start - a.start)) {
    out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
  }
  return out;
}
