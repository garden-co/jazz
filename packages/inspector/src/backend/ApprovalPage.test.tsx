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
});
