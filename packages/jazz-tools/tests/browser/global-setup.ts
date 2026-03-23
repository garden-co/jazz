import { stopTestingServer } from "./testing-server-node.js";

interface BrowserProjectHooks {
  onClose?: (cb: () => void | Promise<void>) => void;
  onTestsRerun?: (cb: () => void | Promise<void>) => void;
}

export function setup(project: BrowserProjectHooks): void {
  project.onClose?.(() => stopTestingServer());
  project.onTestsRerun?.(() => stopTestingServer());
}

export async function teardown(): Promise<void> {
  await stopTestingServer();
}
