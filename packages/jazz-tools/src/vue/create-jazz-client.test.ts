import { describe, expect, it } from "vitest";
import {
  createExtensionJazzClient as createVueExtensionJazzClient,
  createJazzClient as createVueJazzClient,
} from "./create-jazz-client.js";
import {
  createExtensionJazzClient as createWebExtensionJazzClient,
  createJazzClient as createWebJazzClient,
} from "../web/create-jazz-client.js";

describe("vue/create-jazz-client public API", () => {
  it("re-exports the canonical web client factories", () => {
    expect(createVueJazzClient).toBe(createWebJazzClient);
    expect(createVueExtensionJazzClient).toBe(createWebExtensionJazzClient);
  });
});
