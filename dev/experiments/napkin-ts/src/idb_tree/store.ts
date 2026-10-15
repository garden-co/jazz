// Page-store contract for IDBTree.
export type PageId = number;

export interface Metadata {
  pageSize: number;
  generation: number;
  rootPageId: PageId | undefined;
  nextPageId: PageId;
}
export const emptyMetadata = (pageSize: number): Metadata => ({
  pageSize,
  generation: 0,
  rootPageId: undefined,
  nextPageId: 0,
});

export interface Commit {
  expectedGeneration: number;
  metadata: Metadata;
  pages: [PageId, Uint8Array][];
  deletedPageIds: PageId[];
}

/** Keeps a store's single-tree admission alive until revoked or released. */
export class TreeOwnership {
  constructor(
    private readonly live?: () => boolean,
    private release?: () => void,
  ) {}
  isLive(): boolean {
    return this.live?.() ?? true;
  }
  drop(): void {
    const r = this.release;
    this.release = undefined;
    r?.();
  }
}

export abstract class PageStore {
  /** Opt in only while this tree exclusively owns the store. */
  claimTreeOwnership(): TreeOwnership {
    return new TreeOwnership();
  }
  canReclaimObsoletePages(): boolean {
    return false;
  }
  abstract loadMetadata(): Promise<Metadata | undefined>;
  abstract readPage(pageId: PageId): Promise<Uint8Array | undefined>;
  /** One slot per requested id in request order. */
  async readPages(pageIds: PageId[]): Promise<(Uint8Array | undefined)[]> {
    const out: (Uint8Array | undefined)[] = [];
    for (const id of pageIds) out.push(await this.readPage(id));
    return out;
  }
  abstract commit(commit: Commit): Promise<Metadata>;
}
