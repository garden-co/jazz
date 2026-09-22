import { stopJazzServer } from "./testing-server-node.js";

interface BrowserProjectHooks {
  onClose?: (cb: () => void | Promise<void>) => void;
  onTestsRerun?: (cb: () => void | Promise<void>) => void;
}

export function setup(project: BrowserProjectHooks): void {
  project.onClose?.(() => stopJazzServer());
  project.onTestsRerun?.(() => stopJazzServer());
}

export async function teardown(): Promise<void> {
  await stopJazzServer();
}
