import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { disabledSetupPages, filterLanguageTabs, type Language, LANGUAGES } from "./index";
import { expandBindingsExcerpts, extractSymbol, extractSymbols } from "./excerpt";

const langs: Language[] = [
  { label: "TypeScript", backend: true, enabled: true },
  { label: "Rust", setupPage: "setup/rust", backend: true, enabled: true },
  { label: "Swift", setupPage: "setup/swift", backend: false, enabled: false },
];

const page = `Intro

<Tabs groupId="jazz-environment" items={["TypeScript", "Rust", "Swift"]} persist updateAnchor>
  <Tab value="TypeScript">
    ts
  </Tab>
  <Tab value="Rust">
    rust
  </Tab>
  <Tab value="Swift">
    <Tabs groupId="other" items={["A"]}>
      <Tab value="A">nested</Tab>
    </Tabs>
  </Tab>
</Tabs>

<Tabs groupId="jazz-framework" items={["React", "Swift"]}>
  <Tab value="React">r</Tab>
  <Tab value="Swift">not a language tab</Tab>
</Tabs>
`;

describe("filterLanguageTabs", () => {
  it("drops switched-off languages' tabs and items", () => {
    const out = filterLanguageTabs(page, langs);
    expect(out).toContain('items={["TypeScript", "Rust"]} persist updateAnchor>');
    expect(out).not.toContain('<Tab value="Swift">\n    <Tabs');
    expect(out).not.toContain("nested");
    expect(out).toContain("rust\n  </Tab>\n</Tabs>");
    // Other tab groups are left alone.
    expect(out).toContain("not a language tab");
  });

  it("leaves a page with only switched-on languages unchanged", () => {
    const on = langs.map((l) => ({ ...l, enabled: true }));
    expect(filterLanguageTabs(page, on)).toBe(page);
  });

  it("drops a block whose tabs are all switched off", () => {
    const only = `a\n<Tabs groupId="jazz-environment" items={["Swift"]}>\n  <Tab value="Swift">s</Tab>\n</Tabs>\nb\n`;
    expect(filterLanguageTabs(only, langs)).toBe("a\n\nb\n");
  });

  it("refuses unknown languages, wrong order and mismatched items", () => {
    const tabs = (items: string[], values: string[]) =>
      `<Tabs groupId="jazz-environment" items={${JSON.stringify(items)}}>\n${values
        .map((v) => `  <Tab value="${v}">x</Tab>\n`)
        .join("")}</Tabs>\n`;
    expect(() => filterLanguageTabs(tabs(["Cobol"], ["Cobol"]), langs)).toThrow(/isn't in/);
    expect(() =>
      filterLanguageTabs(tabs(["Rust", "TypeScript"], ["Rust", "TypeScript"]), langs),
    ).toThrow(/out of order/);
    expect(() => filterLanguageTabs(tabs(["TypeScript"], ["TypeScript", "Rust"]), langs)).toThrow(
      /don't match/,
    );
  });

  it("refuses client-only languages on backend pages", () => {
    const backend = `---\ntitle: X\nlanguageTabs: backend\n---\n\n<Tabs groupId="jazz-environment" items={["TypeScript", "Swift"]}>\n  <Tab value="TypeScript">t</Tab>\n  <Tab value="Swift">s</Tab>\n</Tabs>\n`;
    expect(() => filterLanguageTabs(backend, langs)).toThrow(/client-only Swift/);
    expect(() =>
      filterLanguageTabs(backend.replace("languageTabs: backend\n", ""), langs),
    ).not.toThrow();
  });

  it("lists switched-off setup pages", () => {
    expect([...disabledSetupPages(langs)]).toEqual(["setup/swift"]);
  });
});

describe("the docs pages", () => {
  const root = path.join(__dirname, "../../content/docs");
  const files: string[] = [];
  const walk = (dir: string) => {
    for (const name of readdirSync(dir)) {
      const p = path.join(dir, name);
      if (statSync(p).isDirectory()) walk(p);
      else if (p.endsWith(".mdx")) files.push(p);
    }
  };
  walk(root);

  it.each(files.map((f) => [path.relative(root, f), f]))(
    "%s has well-formed language tabs",
    (_, f) => {
      expect(() => filterLanguageTabs(readFileSync(f, "utf8"), LANGUAGES, f)).not.toThrow();
    },
  );

  it("keeps language tabs and excerpts out of partials, which skip the plugin", () => {
    const partials = path.join(__dirname, "../../content/partials");
    for (const name of readdirSync(partials)) {
      const text = readFileSync(path.join(partials, name), "utf8");
      expect(text, name).not.toMatch(/groupId="jazz-environment"|<BindingsExcerpt/);
    }
  });

  it("links to a switched-off language's setup page only from its own tabs", () => {
    const hidden = [...disabledSetupPages()];
    for (const f of files) {
      const text = filterLanguageTabs(readFileSync(f, "utf8"), LANGUAGES, f);
      for (const page of hidden)
        expect(text, path.relative(root, f)).not.toContain(`/docs/${page}`);
    }
  });

  it("has a setup page for every switched-on language that names one", () => {
    for (const l of LANGUAGES) {
      if (!l.enabled || !l.setupPage) continue;
      const text = readFileSync(path.join(root, `${l.setupPage}.mdx`), "utf8");
      expect(text, l.setupPage).toMatch(/^## Not supported yet$/m);
    }
  });
});

describe("generated bindings excerpts", () => {
  const rust = `/// The values.
#[derive(Debug)]
pub enum TaskStatus {
    Todo,
}

impl X {}

/// A row.
pub struct Task {
    pub id: Id,
    pub tags: Vec<(u8, u8)>,
}
`;

  it("cuts a declaration with its doc comments and attributes", () => {
    expect(extractSymbol(rust, "Task")).toBe(
      "/// A row.\npub struct Task {\n    pub id: Id,\n    pub tags: Vec<(u8, u8)>,\n}",
    );
    expect(extractSymbol(rust, "TaskStatus")).toBe(
      "/// The values.\n#[derive(Debug)]\npub enum TaskStatus {\n    Todo,\n}",
    );
  });

  it("handles one-line and parenthesised declarations", () => {
    expect(extractSymbol('x\nexport type S = "a" | "b";\ny', "S")).toBe(
      'export type S = "a" | "b";',
    );
    expect(extractSymbol("type S string\nconst (\n)", "S")).toBe("type S string");
    expect(
      extractSymbol("@Serializable\ndata class Task(\n  val id: String,\n)\nfun f() {}", "Task"),
    ).toBe("@Serializable\ndata class Task(\n  val id: String,\n)");
  });

  it("refuses a declaration it would cut short", () => {
    expect(() => extractSymbol('export type S =\n  | "a"\n  | "b"\nnext', "S")).toThrow(
      /doesn't end cleanly/,
    );
    expect(extractSymbol('export type S =\n  | "a"\n  | "b";\nnext', "S")).toBe(
      'export type S =\n  | "a"\n  | "b";',
    );
  });

  it("ignores brackets in strings and comments", () => {
    const src = '#[serde(rename = "}")]\npub struct T {\n    // a } here\n    pub a: u8,\n}\nafter';
    expect(extractSymbol(src, "T")).toBe(
      '#[serde(rename = "}")]\npub struct T {\n    // a } here\n    pub a: u8,\n}',
    );
  });

  it("takes the const block of a Go string enum", () => {
    const go =
      'type TaskStatus string\n\n// The values.\nconst (\n\tTaskStatusTodo TaskStatus = "todo"\n)\n\ntype Task struct {\n}\n';
    expect(extractSymbol(go, "TaskStatus")).toBe(
      'type TaskStatus string\n\n// The values.\nconst (\n\tTaskStatusTodo TaskStatus = "todo"\n)',
    );
    expect(extractSymbol("type ID string\n\nconst (\n\tOther = 1\n)\n", "ID")).toBe(
      "type ID string",
    );
  });

  it("dedents a declaration nested in a namespace", () => {
    const swift =
      "public enum JazzApp {\n    /// A row.\n    public struct Task: Jazz.Row {\n        public let id: Jazz.ID\n    }\n}\n";
    expect(extractSymbol(swift, "Task")).toBe(
      "/// A row.\npublic struct Task: Jazz.Row {\n    public let id: Jazz.ID\n}",
    );
  });

  it("throws on a missing symbol", () => {
    expect(() => extractSymbols(rust, ["Nope"], "f.rs")).toThrow(/no declaration of Nope/);
  });

  it("expands <BindingsExcerpt>", () => {
    const out = expandBindingsExcerpts(
      '  <BindingsExcerpt language="Rust" />',
      () => ({ file: "a/jazz_app.rs", lang: "rust", symbols: ["Task"] }),
      () => rust,
    );
    expect(out.split("\n")[0]).toBe('  ```rust title="jazz_app.rs"');
    expect(out).toContain("  pub struct Task {");
  });

  const repo = path.join(__dirname, "../../..");
  it.each(LANGUAGES.filter((l) => l.enabled && l.bindings).map((l) => [l.label, l.bindings!]))(
    "%s bindings declare every excerpted symbol",
    (_, { file, symbols }) => {
      expect(() =>
        extractSymbols(readFileSync(path.join(repo, file), "utf8"), symbols, file),
      ).not.toThrow();
    },
  );
});
