import { describe, expect, it } from "vitest";
import { customTokenizer } from "./code-tokenizers";

const types = (lang: string, code: string) =>
  Object.fromEntries(customTokenizer(lang)!(code).map((t) => [code.slice(t.start, t.end), t.type]));

describe("custom tokenizers", () => {
  it("highlights Swift", () => {
    const t = types("swift", 'public struct Task: Jazz.Row { let title: String = "x" } // c');
    expect(t).toMatchObject({
      public: "keyword",
      struct: "keyword",
      Task: "type",
      let: "keyword",
      '"x"': "string",
      "// c": "comment",
    });
  });

  it("highlights Kotlin", () => {
    const t = types(
      "kotlin",
      '@Serializable data class Task(val title: kotlin.String)\nfun main() { println("hi") }',
    );
    expect(t).toMatchObject({
      "@Serializable": "constant",
      data: "keyword",
      val: "keyword",
      fun: "keyword",
      println: "function",
      '"hi"': "string",
    });
    expect(customTokenizer("kt")).toBe(customTokenizer("kotlin"));
  });

  it("highlights Go", () => {
    const t = types(
      "go",
      'type Task struct {\n\tTitle string `json:"title"`\n}\nfunc f() { return nil }',
    );
    expect(t).toMatchObject({
      type: "keyword",
      Task: "type",
      struct: "keyword",
      '`json:"title"`': "string",
      func: "keyword",
      nil: "keyword",
    });
  });
});
