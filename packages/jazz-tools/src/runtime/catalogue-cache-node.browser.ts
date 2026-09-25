import type { CatalogueCache, CatalogueCacheScope } from "./catalogue-cache.js";

/** Browser resolution must never load Node filesystem modules or pretend to persist. */
export class NodeCatalogueCache implements CatalogueCache {
  constructor(_directory: string) {
    throw new Error("Node catalogue cache cannot be used in a browser");
  }
  async load(_scope: CatalogueCacheScope): Promise<Uint8Array | null> {
    throw new Error("Node catalogue cache cannot be used in a browser");
  }
  async publish(_scope: CatalogueCacheScope, _capture: Uint8Array): Promise<void> {
    throw new Error("Node catalogue cache cannot be used in a browser");
  }
}
