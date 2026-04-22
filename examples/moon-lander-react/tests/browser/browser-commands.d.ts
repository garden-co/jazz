/**
 * Type augmentation for custom BrowserCommands — used on the browser side
 * when calling `commands` from `vitest/browser`.
 *
 * `export {}` makes this file a TypeScript module so `declare module` below
 * is treated as an augmentation of the existing vitest/browser module rather
 * than a new ambient module declaration that would shadow it.
 */

export {};

declare module "vitest/browser" {
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
    startFreshTestServer(label: string): Promise<string>;
    stopFreshTestServer(label: string): Promise<void>;
  }
}
