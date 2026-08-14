import { waitForClientRegistryIdleForTest } from "jazz-tools/_dev/client-registry";
import type { Root } from "react-dom/client";

export interface BrowserMount {
  root: Root;
  container: HTMLDivElement;
}

export async function unmountBrowserApp(
  mounts: BrowserMount[],
  container: HTMLDivElement,
): Promise<void> {
  const index = mounts.findIndex((mount) => mount.container === container);
  if (index === -1) return;
  const [{ root }] = mounts.splice(index, 1);
  try {
    root.unmount();
  } finally {
    container.remove();
  }
  await waitForClientRegistryIdleForTest();
}

export async function cleanupBrowserMounts(mounts: BrowserMount[]): Promise<void> {
  for (const { root, container } of mounts) {
    try {
      root.unmount();
    } catch {
      // Best effort: continue releasing every remaining provider.
    }
    container.remove();
  }
  mounts.length = 0;
  await waitForClientRegistryIdleForTest();
}
