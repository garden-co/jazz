import { describe, expect, it } from "vitest";
import { stripTodoComments } from "./llm-text";

describe("stripTodoComments", () => {
  it("drops TODO comment lines from the Markdown export", () => {
    expect(stripTodoComments("Intro\n\n{/* TODO, coming next: a; b */}\n\nEnd\n")).toBe(
      "Intro\n\n\nEnd\n",
    );
  });

  it("keeps JSX comments inside code samples", () => {
    const code = "```tsx\n<App>\n  {/* TODO: wire this up */}\n</App>\n```\n";
    expect(stripTodoComments(code)).toBe(code);
  });
});
