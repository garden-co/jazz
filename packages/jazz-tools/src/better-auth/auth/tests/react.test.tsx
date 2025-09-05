// @vitest-environment jsdom
import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { AuthProvider } from "../react";
import { createAuthClient } from "better-auth/client";
import { jazzPluginClient } from "../client";
import { JazzReactProvider } from "jazz-tools/react";

describe("AuthProvider", () => {
  it("should throw if no JazzContext is set", () => {
    const betterAuthClient = createAuthClient({
      plugins: [jazzPluginClient()],
    });

    expect(() => {
      render(
        <AuthProvider betterAuthClient={betterAuthClient}>
          <div />
        </AuthProvider>,
      );
    }).toThrow(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  });

  it("should render with JazzReactProvider", () => {
    const betterAuthClient = createAuthClient({
      plugins: [jazzPluginClient()],
    });

    render(
      <JazzReactProvider
        // @ts-expect-error - no memory storage
        storage={["memory"]}
        sync={{ peer: "ws://", when: "never" }}
      >
        <AuthProvider betterAuthClient={betterAuthClient}>
          <div />
        </AuthProvider>
      </JazzReactProvider>,
    );
  });
});
