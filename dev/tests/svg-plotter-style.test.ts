import { describe, expect, it } from "vitest";
import {
  getEmbeddedSvgStyleValue,
  parseEmbeddedSvgStyles,
} from "../../docs/components/playgrounds/svg-plotter-style";

describe("parseEmbeddedSvgStyles", () => {
  it("resolves class-based stroke properties from embedded style blocks", () => {
    const rules = parseEmbeddedSvgStyles([
      `
        .cls-1 {
          fill: none;
          stroke: #2e4ae3;
          stroke-width: 3.28px;
        }
      `,
    ]);

    expect(
      getEmbeddedSvgStyleValue(
        rules,
        {
          classNames: ["cls-1"],
          tagName: "path",
        },
        "stroke",
      ),
    ).toBe("#2e4ae3");
    expect(
      getEmbeddedSvgStyleValue(
        rules,
        {
          classNames: ["cls-1"],
          tagName: "path",
        },
        "stroke-width",
      ),
    ).toBe("3.28px");
  });

  it("lets class rules override tag fallbacks", () => {
    const rules = parseEmbeddedSvgStyles([
      `
        path { stroke: black; }
        .cls-1 { stroke: #2e4ae3; }
      `,
    ]);

    expect(
      getEmbeddedSvgStyleValue(
        rules,
        {
          classNames: ["cls-1"],
          tagName: "path",
        },
        "stroke",
      ),
    ).toBe("#2e4ae3");
  });
});
