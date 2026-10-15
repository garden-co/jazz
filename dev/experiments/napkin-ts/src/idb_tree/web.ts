// Adapter over the TypeScript IndexedDB page store.
import { type Commit, type Metadata, PageStore, TreeOwnership } from "./store.js";

const MAX_SAFE = 9_007_199_254_740_991;

export class IndexedDbPageStore extends PageStore {
  // Clones used by one tree retain its token.
  private treeToken: number | undefined;
  constructor(private readonly handle: any) {
    super();
  }
  // Existing/custom JS stores remain non-reclaiming unless they implement
  // the complete single-tree ownership contract.
  private supportsTreeOwnership(): boolean {
    return ["claimTreeOwnership", "releaseTreeOwnership", "isTreeOwnershipActive"].every((n) =>
      this.hasMethod(n),
    );
  }
  private hasMethod(name: string): boolean {
    return typeof this.handle[name] === "function";
  }
  override claimTreeOwnership(): TreeOwnership {
    if (!this.supportsTreeOwnership()) return new TreeOwnership();
    let token: number;
    try {
      token = this.handle.claimTreeOwnership();
    } catch (e) {
      throw jsError(e);
    }
    this.treeToken = token;
    return new TreeOwnership(
      () => this.isActive(token),
      () => this.handle.releaseTreeOwnership(token),
    );
  }
  private isActive(token: number): boolean {
    try {
      return this.handle.isTreeOwnershipActive(token);
    } catch {
      return false;
    }
  }
  override canReclaimObsoletePages(): boolean {
    return (
      this.treeToken !== undefined &&
      this.isActive(this.treeToken) &&
      !!this.handle.canReclaimObsoletePages
    );
  }
  async loadMetadata(): Promise<Metadata | undefined> {
    const value = await this.call(() => this.handle.metadata());
    return value == null ? undefined : metadataFromJs(value);
  }
  async readPage(pageId: number): Promise<Uint8Array | undefined> {
    const value = await this.call(() => this.handle.readPage(pageIdToF64(pageId)));
    return value == null ? undefined : new Uint8Array(value);
  }
  // Custom JS stores without `readPages` keep one `readPage` per page.
  override async readPages(pageIds: number[]): Promise<(Uint8Array | undefined)[]> {
    if (!this.hasMethod("readPages")) return super.readPages(pageIds);
    const value = await this.call(() => this.handle.readPages(pageIds.map(pageIdToF64)));
    if (!Array.isArray(value)) throw "readPages did not return an array";
    return value.map((p) => (p == null ? undefined : new Uint8Array(p)));
  }
  async commit(commit: Commit): Promise<Metadata> {
    if (commit.metadata.pageSize > 0xffffffff) throw "page size does not fit u32";
    const value = await this.call(() =>
      this.handle.commitPages(
        pageIdToF64(commit.expectedGeneration),
        commit.metadata.pageSize,
        commit.metadata.rootPageId === undefined ? -1 : pageIdToF64(commit.metadata.rootPageId),
        pageIdToF64(commit.metadata.nextPageId),
        commit.pages.map(([id]) => pageIdToF64(id)),
        commit.pages.map(([, bytes]) => bytes),
        commit.deletedPageIds.map(pageIdToF64),
        this.treeToken,
      ),
    );
    return metadataFromJs(value);
  }
  private async call(f: () => Promise<any>): Promise<any> {
    try {
      return await f();
    } catch (e) {
      throw jsError(e);
    }
  }
}

function metadataFromJs(value: any): Metadata {
  const root = value.rootPageId;
  return {
    pageSize: integerField(value.pageSize, "pageSize"),
    generation: integerField(value.generation, "generation"),
    rootPageId: root == null ? undefined : integerField(root, "rootPageId"),
    nextPageId: integerField(value.nextPageId, "nextPageId"),
  };
}

function integerField(value: unknown, name: string): number {
  if (typeof value !== "number") throw `${name} is not a number`;
  if (!Number.isFinite(value) || value < 0 || !Number.isInteger(value) || value > MAX_SAFE)
    throw `${name} is not a safe non-negative integer`;
  return value;
}

function pageIdToF64(value: number): number {
  if (value > MAX_SAFE) throw "IDBTree page id exceeds JavaScript's safe integer range";
  return value;
}

const jsError = (value: unknown): string =>
  value instanceof Error ? value.message : String(value);
