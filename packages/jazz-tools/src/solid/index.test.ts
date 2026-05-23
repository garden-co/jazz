import { describe, expect, it } from "vitest";
import { helloWorld } from "./index.js";

describe("solid/index", () => {
  it("returns hello world message", () => {
    expect(helloWorld()).toBe("Hello World from SolidJS");
  });
});
