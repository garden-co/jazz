import { describe, expect, it } from "vitest";
import { optionalPeerImportError } from "./optional-peer.js";

function withCode(message: string, code: string): Error {
  return Object.assign(new Error(message), { code });
}

describe("optionalPeerImportError", () => {
  it("asks to install the package when that package is missing", () => {
    const missing = withCode(
      "Cannot find package '@vercel/sandbox' imported from /app/x.js",
      "ERR_MODULE_NOT_FOUND",
    );
    const error = optionalPeerImportError("vercel()", "@vercel/sandbox", missing);
    expect(error.message).toBe(
      "vercel() needs the @vercel/sandbox package: npm install @vercel/sandbox",
    );
    expect(error.cause).toBe(missing);
  });

  it("recognises a bundler's module-not-found", () => {
    const missing = withCode("Cannot find module '@vercel/functions'", "MODULE_NOT_FOUND");
    expect(optionalPeerImportError("Jazz on Vercel", "@vercel/functions", missing).message).toBe(
      "Jazz on Vercel needs the @vercel/functions package: npm install @vercel/functions",
    );
  });

  it("does not ask to install an installed package that fails to load", () => {
    const broken = new SyntaxError("Unexpected token 'export'");
    const error = optionalPeerImportError("vercel()", "@vercel/sandbox", broken);
    expect(error.message).toBe(
      "vercel() could not load the @vercel/sandbox package: Unexpected token 'export'",
    );
    expect(error.cause).toBe(broken);
  });

  it("does not blame the package for its own missing dependency", () => {
    const transitive = withCode(
      "Cannot find package 'undici' imported from /app/node_modules/@vercel/sandbox/dist/index.js",
      "ERR_MODULE_NOT_FOUND",
    );
    const error = optionalPeerImportError("vercel()", "@vercel/sandbox", transitive);
    expect(error.message).toMatch(/^vercel\(\) could not load the @vercel\/sandbox package: /);
    expect(error.cause).toBe(transitive);
  });
});
