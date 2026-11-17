import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { parseInspectorLink } from "jazz-tools/inspect";
import { afterEach, describe, expect, it } from "vitest";
import { BackendApp } from "./BackendApp.js";

// #5224: viewing as a user with claims, in the backend-served Inspector:
// Enter in the claims field applies them, the address keeps them, and the
// Data Explorer shows the backend's warning text (not only a badge).

const PAGE = "http://localhost:3000/jazz/inspect/";
const WARNING =
  "This app's permissions read session claims, and none were given for manager, so this ran as a session with no claims.";

afterEach(() => {
  cleanup();
  window.history.replaceState(null, "", "/");
});

function stubBackend() {
  const calls: any[] = [];
  const call = (async (request: any) => {
    calls.push(request);
    switch (request.op) {
      case "whoami":
        return {
          appId: "app",
          by: "ops@example.com",
          via: "hook",
          write: false,
          maxTokenTtlSeconds: 86400,
          inspectorUrl: PAGE,
          protocol: 1,
        };
      case "schema":
        return {
          tables: {
            todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
          },
          permissions: {
            todos: {
              select: {
                using: { type: "SessionCmp", path: ["claims", "role"], op: "Eq", value: "manager" },
              },
            },
          },
        };
      case "query": {
        const withClaims = typeof request.as === "object";
        return {
          rows: withClaims ? [{ id: "11111111-1111-4111-8111-111111111111", title: "milk" }] : [],
          truncated: false,
          ...(request.as && !withClaims ? { warning: WARNING } : {}),
          inspectorUrl: PAGE,
        };
      }
      default:
        return {};
    }
  }) as never;
  return { call, calls };
}

describe("view as a user with claims", () => {
  it("shows the backend's warning in the Data Explorer, applies claims on Enter, and keeps them in the address", async () => {
    window.history.replaceState(null, "", "/jazz/inspect/#table=todos&as=manager");
    const { call, calls } = stubBackend();
    render(<BackendApp pageUrl={`${PAGE}#table=todos&as=manager`} call={call} />);

    expect(await screen.findByText(WARNING, {}, { timeout: 3000 })).toBeTruthy();

    const claims = screen.getByLabelText("with claims") as HTMLInputElement;
    fireEvent.change(claims, { target: { value: '{"role":"manager"}' } });
    fireEvent.keyDown(claims, { key: "Enter" });

    await waitFor(() =>
      expect(calls).toContainEqual(
        expect.objectContaining({
          op: "query",
          as: { userId: "manager", claims: { role: "manager" } },
        }),
      ),
    );
    expect(await screen.findByText("milk")).toBeTruthy();
    await waitFor(() => expect(screen.queryByText(WARNING)).toBeNull());
    expect(parseInspectorLink(window.location.hash)).toMatchObject({
      table: "todos",
      as: { userId: "manager", claims: { role: "manager" } },
    });
  });
});
