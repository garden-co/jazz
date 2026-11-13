import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApprovalPage } from "./ApprovalPage.js";

// #5180: the approval page named the app only by its app id, a bare UUID
// the person approving can't recognize.

afterEach(cleanup);

const APP_ID = "1f2e3d4c-5b6a-4978-8a1b-2c3d4e5f6a7b";

describe("the agent approval page", () => {
  it("names the app by where it is, with its id beside it", () => {
    render(
      <ApprovalPage
        grant={{
          state: "st4te",
          label: "debugging #123",
          callback: "http://127.0.0.1:5123/callback",
        }}
        whoami={
          {
            appId: APP_ID,
            by: "ops@example.com",
            via: "hook",
            write: false,
            inspectorUrl: "https://app.example.com/jazz/inspect/",
            protocol: 1,
          } as never
        }
        call={vi.fn() as never}
        endpoint="https://app.example.com/jazz"
        navigate={vi.fn()}
      />,
    );
    const ask = screen.getByText(/asks for/).closest("li")!;
    expect(ask.textContent).toBe(
      `debugging #123 asks for read-only access to the app at app.example.com (app id ${APP_ID}).`,
    );
    expect(screen.getByText("app.example.com").tagName).toBe("STRONG");
  });

  it("refuses a lifetime this app doesn't issue before anyone approves (#5179)", () => {
    render(
      <ApprovalPage
        grant={{
          state: "st4te",
          ttlSeconds: 2 * 86400,
          callback: "http://127.0.0.1:5123/callback",
        }}
        whoami={
          {
            appId: APP_ID,
            by: "ops@example.com",
            via: "hook",
            write: false,
            maxTokenTtlSeconds: 86400,
            inspectorUrl: "https://app.example.com/jazz/inspect/",
            protocol: 1,
          } as never
        }
        call={vi.fn() as never}
        endpoint="https://app.example.com/jazz"
        navigate={vi.fn()}
      />,
    );
    expect(screen.getByRole("alert").textContent).toBe(
      "This request asks for a token that lasts 2 days, but this app's tokens last at most 1 day. Ask again with a shorter --ttl.",
    );
    expect((screen.getByRole("button", { name: "Approve" }) as HTMLButtonElement).disabled).toBe(
      true,
    );
  });

  it("shows the label as the token keeps it: cleaned and at most 200 long", () => {
    render(
      <ApprovalPage
        grant={{
          state: "st4te",
          label: `\u202edebug\u2028${"y".repeat(300)}`,
          callback: "http://127.0.0.1:5123/callback",
        }}
        whoami={
          {
            appId: APP_ID,
            by: "ops@example.com",
            via: "hook",
            write: false,
            maxTokenTtlSeconds: 86400,
            inspectorUrl: "https://app.example.com/jazz/inspect/",
            protocol: 1,
          } as never
        }
        call={vi.fn() as never}
        endpoint="https://app.example.com/jazz"
        navigate={vi.fn()}
      />,
    );
    const shown = screen.getByText(/^debug y+$/);
    expect(shown.textContent).toHaveLength(200);
  });
});
