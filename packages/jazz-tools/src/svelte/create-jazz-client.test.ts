import { describe, expect, it } from "vitest";
import {
  createExtensionJazzClient as createSvelteExtensionJazzClient,
  createJazzClient as createSvelteJazzClient,
} from "./create-jazz-client.js";
import {
  createExtensionJazzClient as createWebExtensionJazzClient,
  createJazzClient as createWebJazzClient,
} from "../web/create-jazz-client.js";

describe("svelte/create-jazz-client public API", () => {
  it("re-exports the canonical web client factories", () => {
    expect(createSvelteJazzClient).toBe(createWebJazzClient);
    expect(createSvelteExtensionJazzClient).toBe(createWebExtensionJazzClient);
  });
});
