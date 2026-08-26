import { afterEach, describe, expect, it, vi } from "vitest";

const context = vi.hoisted(() => {
  const one = vi.fn();
  const asBackend = vi.fn(() => ({ one }));
  return { one, asBackend, authJazzContext: vi.fn(() => ({ asBackend })) };
});

vi.mock("../src/lib/auth-jazz-context", () => ({ authJazzContext: context.authJazzContext }));

import { ensureAccountBootstrap } from "../src/lib/bootstrap";

describe("trusted RecordPlayer bootstrap", () => {
  afterEach(() => {
    context.one.mockReset();
    context.asBackend.mockClear();
    context.authJazzContext.mockClear();
  });

  it("rejects a session whose Better Auth user is not in trusted backend storage", async () => {
    context.one.mockResolvedValueOnce(undefined);

    await expect(ensureAccountBootstrap("missing-user")).rejects.toThrow(
      "authenticated Better Auth user is missing from trusted storage",
    );
  });

  it("accepts a session only after its Better Auth user was found", async () => {
    context.one.mockResolvedValueOnce({ id: "better-auth-user" });

    await expect(ensureAccountBootstrap("better-auth-user")).resolves.toBeUndefined();
    expect(context.asBackend).toHaveBeenCalledOnce();
    expect(context.one).toHaveBeenCalledOnce();
  });
});
