import { describe, expect, it } from "vitest";
import { isIncludeRef, withoutIncludeRefs } from "./search-include-refs";

describe("search include refs", () => {
  it("recognises snippet references", () => {
    expect(
      isIncludeRef(
        "../examples/docs/todo-server-ts/src/permissions-snippets.ts#permissions-session-claims-ts",
      ),
    ).toBe(true);
    expect(isIncludeRef("../../partials/install-alpha.mdx")).toBe(true);
    expect(isIncludeRef("./schema.ts")).toBe(true);
  });

  it("keeps prose, even prose that mentions a path", () => {
    expect(isIncludeRef("See ../examples for more.")).toBe(false);
    expect(isIncludeRef("`claims` from `authenticate` are available to your policies.")).toBe(
      false,
    );
  });

  it("drops only the references", () => {
    const data = {
      headings: [],
      contents: [
        { heading: "a", content: "Prose." },
        { heading: "a", content: "../examples/docs/x/src/y.ts#z" },
      ],
    };
    expect(withoutIncludeRefs(data).contents).toEqual([{ heading: "a", content: "Prose." }]);
  });
});
