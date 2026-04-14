/**
 * Type augmentation for custom BrowserCommands — used on the browser side
 * when calling `commands` from `vitest/browser`.
 */

interface OpenIsolatedAppOpts {
  label: string;
  appId: string;
  dbName: string;
  serverUrl: string;
  playerId?: string;
  physicsSpeed?: number;
  spawnX?: number;
  localFirstSecret?: string;
  adminSecret?: string;
}

declare module "vitest/browser" {
  interface BrowserCommands {
    openIsolatedApp(opts: OpenIsolatedAppOpts): Promise<void>;
    readIsolatedAttr(label: string, attr: string, testId?: string): Promise<string | null>;
    waitForIsolatedAttr(
      label: string,
      attr: string,
      expected: string,
      timeout?: number,
    ): Promise<void>;
    pressIsolatedKey(label: string, key: string): Promise<void>;
    releaseIsolatedKey(label: string, key: string): Promise<void>;
    closeIsolatedApp(label: string): Promise<void>;
    debugIsolatedState(label: string): Promise<string>;
  }
}
