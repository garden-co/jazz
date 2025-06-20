import type { Element, Text } from "hast";
import { describe, expect, it } from "vitest";
import { processHeadingNode } from "../processing-heading-node.mjs";

describe("processHeadingNode", () => {
  it("should return undefined for non-heading nodes", () => {
    const node: Element = {
      type: "element",
      tagName: "div",
      properties: {},
      children: [],
    };
    const frameworkVisibility = {};
    expect(processHeadingNode(node, frameworkVisibility)).toBeUndefined();
    expect(frameworkVisibility).toEqual({});
  });

  it("should generate ID for heading without framework marker", () => {
    const node: Element = {
      type: "element",
      tagName: "h2",
      properties: { id: undefined },
      children: [
        {
          type: "text",
          value: "Test Header",
        } as Text,
      ],
    };
    const frameworkVisibility: Record<string, string[]> = {};
    const id = processHeadingNode(node, frameworkVisibility);
    expect(id).toBeDefined();
    expect(frameworkVisibility).toEqual({});
    expect(node.properties.id).toBeUndefined(); // Shouldn't modify node directly
  });

  it("should handle framework marker in header text", () => {
    const node: Element = {
      type: "element",
      tagName: "h2",
      properties: { id: undefined },
      children: [
        {
          type: "text",
          value: "Test Header [!framework=react,vue]",
        } as Text,
      ],
    };
    const frameworkVisibility: Record<string, string[]> = {};
    const id = processHeadingNode(node, frameworkVisibility);

    expect(id).toBeDefined();
    expect(frameworkVisibility).toEqual({
      [id!]: ["react", "vue"],
    });
    expect((node.children?.[0] as Text).value).toBe("Test Header");
  });
});
