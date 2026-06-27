import { describe, expect, it } from "vitest";
import {
  createExtensionJazzClient as createReactExtensionJazzClient,
  createJazzClient as createReactJazzClient,
} from "./create-jazz-client.js";
import {
  createExtensionJazzClient as createWebExtensionJazzClient,
  createJazzClient as createWebJazzClient,
} from "../web/create-jazz-client.js";

describe("react/create-jazz-client public API", () => {
  it("re-exports the canonical web client factories", () => {
    expect(createReactJazzClient).toBe(createWebJazzClient);
    expect(createReactExtensionJazzClient).toBe(createWebExtensionJazzClient);
  });
});
