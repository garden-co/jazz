// @vitest-environment jsdom
import { afterEach, expect, test, vi } from "vitest";

const createJazzSession = vi.hoisted(() => vi.fn());
vi.mock("jazz-tools/client", () => ({ createJazzSession }));
vi.mock("./app.js", () => ({ mountApp: vi.fn() }));

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
  vi.clearAllMocks();
});

test.each(["VITE_JAZZ_APP_ID", "VITE_JAZZ_SERVER_URL"])(
  "missing %s displays configuration guidance before opening an account",
  async (missing) => {
    vi.stubEnv("VITE_JAZZ_APP_ID", "startup-test");
    vi.stubEnv("VITE_JAZZ_SERVER_URL", "http://localhost:4200");
    vi.stubEnv(missing, "");
    document.body.innerHTML = '<div id="root"></div>';
    await import("./main.js");
    await vi.waitFor(() =>
      expect(document.getElementById("root")?.textContent).toContain(`${missing} not set`),
    );
    expect(document.getElementById("root")?.textContent).toContain(
      "in production, set them explicitly",
    );
    expect(createJazzSession).not.toHaveBeenCalled();
  },
);
