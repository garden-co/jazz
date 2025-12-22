// @vitest-environment happy-dom
import { render as renderSvelte } from "@testing-library/svelte";
import { describe, expect, it } from "vitest";
import AuthProvider from "../index.svelte";
import { createAuthClient } from "better-auth/client";
import { jazzPluginClient } from "../client";
import TestAuthProviderWrapper from "./TestAuthProviderWrapper.svelte";

describe("AuthProvider", () => {
  it("should throw if no JazzContext is set", () => {
    const betterAuthClient = createAuthClient({
      plugins: [jazzPluginClient()],
    });

    expect(() => {
      renderSvelte(AuthProvider, {
        props: {
          betterAuthClient,
        },
      });
    }).toThrow("useJazzContext must be used within a JazzSvelteProvider");
  });

  it("should render with JazzSvelteProvider", () => {
    const betterAuthClient = createAuthClient({
      plugins: [jazzPluginClient()],
    });

    renderSvelte(TestAuthProviderWrapper, {
      props: {
        betterAuthClient,
      },
    });
  });
});
