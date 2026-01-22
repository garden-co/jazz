import { describe, expect, it } from "vitest";
import {
  formatDuration,
  getCallerLocation,
  getCallerStack,
} from "../../../pages/performance/helpers.js";

describe("formatDuration", () => {
  it("returns microseconds for duration < 1ms", () => {
    expect(formatDuration(0.5)).toBe("500μs");
    expect(formatDuration(0.001)).toBe("1μs");
    expect(formatDuration(0.999)).toBe("999μs");
  });

  it("returns milliseconds for duration < 1000ms", () => {
    expect(formatDuration(1)).toBe("1.00ms");
    expect(formatDuration(123.456)).toBe("123.46ms");
    expect(formatDuration(999.99)).toBe("999.99ms");
  });

  it("returns seconds for duration >= 1000ms", () => {
    expect(formatDuration(1000)).toBe("1.00s");
    expect(formatDuration(1500)).toBe("1.50s");
    expect(formatDuration(60000)).toBe("60.00s");
  });
});

describe("getCallerLocation", () => {
  it("returns undefined for undefined stack", () => {
    expect(getCallerLocation(undefined)).toBeUndefined();
  });

  it("returns undefined for empty stack", () => {
    expect(getCallerLocation("")).toBeUndefined();
  });

  it("extracts user frame location filtering out internals", () => {
    const stack = `Error
    at trackLoadingPerformance (jazz-tools/src/subscribe.js:100:10)
    at useCoState (jazz-tools/src/hooks.js:50:5)
    at MyComponent (src/components/MyComponent.tsx:25:10)
    at renderWithHooks (react-dom.js:100:5)`;

    const result = getCallerLocation(stack);
    expect(result).toContain("MyComponent.tsx:25:10");
  });

  it("filters out node_modules frames", () => {
    const stack = `Error
    at someFunction (node_modules/some-lib/index.js:10:5)
    at MyComponent (src/App.tsx:15:3)`;

    const result = getCallerLocation(stack);
    expect(result).toContain("App.tsx:15:3");
  });
});

describe("getCallerStack", () => {
  it("returns undefined for undefined stack", () => {
    expect(getCallerStack(undefined)).toBeUndefined();
  });

  it("filters out Error: and React internals", () => {
    // Stack trace format: first two lines are skipped by slice(2, 15)
    const stack = `Error: test
    at internalFunction (jazz-tools/src/internal.js:10:5)
    at Component (src/App.tsx:10:5)
    at renderWithHooks (react-dom.js:100:5)
    at react-stack-bottom-frame (react.js:50:3)
    at Parent (src/Parent.tsx:20:10)`;

    const result = getCallerStack(stack);
    expect(result).not.toContain("Error:");
    expect(result).not.toContain("renderWithHooks");
    expect(result).not.toContain("react-stack-bottom-frame");
    expect(result).toContain("App.tsx");
    expect(result).toContain("Parent.tsx");
  });

  it("reverses the stack order", () => {
    // Stack trace format: first two lines are skipped, so we need padding lines
    const stack = `Error
    at internalFunction (jazz-tools/src/internal.js:10:5)
    at First (src/First.tsx:10:5)
    at Second (src/Second.tsx:20:5)`;

    const result = getCallerStack(stack);
    // Second should come before First after reversing
    expect(result!.indexOf("Second")).toBeLessThan(result!.indexOf("First"));
  });
});
