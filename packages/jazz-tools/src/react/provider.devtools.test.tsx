import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, waitFor } from "@testing-library/react";
import * as React from "react";

const { attachSpy } = vi.hoisted(() => ({ attachSpy: vi.fn().mockResolvedValue({}) }));
vi.mock("../dev-tools/dev-tools.js", () => ({ attachDevTools: attachSpy }));
const fakeDb = {};
vi.mock("../react-core/provider.js", () => ({
  JazzProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useDb: () => fakeDb,
  useJazzClient: () => ({ db: fakeDb }),
  useSession: () => null,
}));
import { JazzProvider } from "./provider.js";
const fakeSchema = { tableA: {} } as never;

describe("JazzProvider dev auto-attach", () => {
  beforeEach(() => {
    attachSpy.mockClear();
    (process.env as Record<string, string>).NODE_ENV = "development";
  });
  it("auto-attaches in dev when wasmSchema is provided", async () => {
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema}>
        <div />
      </JazzProvider>,
    );
    await waitFor(() => expect(attachSpy).toHaveBeenCalledTimes(1));
  });
  it("does not auto-attach with autoAttachDevTools={false}", async () => {
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema} autoAttachDevTools={false}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(attachSpy).not.toHaveBeenCalled();
  });
  it("does not attach when NODE_ENV=production", async () => {
    (process.env as Record<string, string>).NODE_ENV = "production";
    render(
      <JazzProvider config={{} as never} wasmSchema={fakeSchema}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(attachSpy).not.toHaveBeenCalled();
  });
});
