import { base64URLtoBytes, bytesToBase64url } from "../base64url.js";
import type { CoID, RawCoValue } from "../coValue.js";
import type { AvailableCoValueCore } from "../coValueCore/coValueCore.js";
import type { RawCoID } from "../ids.js";
import type { JsonObject } from "../jsonValue.js";
import type { RawGroup } from "./group.js";

export type BinaryStreamInfo = {
  mimeType: string;
  fileName?: string;
  totalSizeBytes?: number;
};

export type BinaryStreamStart = {
  type: "start";
} & BinaryStreamInfo;

export type BinaryStreamChunk = {
  type: "chunk";
  chunk: `binary_U${string}`;
};

export type BinaryStreamEnd = {
  type: "end";
};

export type BinaryCoStreamMeta = JsonObject & { type: "binary" };

export type BinaryStreamItem =
  | BinaryStreamStart
  | BinaryStreamChunk
  | BinaryStreamEnd;

const binary_U_prefixLength = 8; // "binary_U".length;

export class RawBinaryCoStreamView<
  Meta extends BinaryCoStreamMeta = { type: "binary" },
> implements RawCoValue
{
  id: CoID<this>;
  type = "costream" as const;
  core: AvailableCoValueCore;
  knownTransactions: Record<RawCoID, number>;
  totalValidTransactions: number = 0;
  version: number = 0;
  private chunks: string[];
  private start: BinaryStreamStart | undefined;
  private ended: boolean;

  private resetInternalState() {
    this.chunks = [];
    this.start = undefined;
    this.ended = false;
    this.knownTransactions = { [this.core.id]: 0 };
    this.totalValidTransactions = 0;
  }

  constructor(core: AvailableCoValueCore) {
    this.id = core.id as CoID<this>;
    this.core = core;
    this.ended = false;
    this.chunks = [];
    this.knownTransactions = { [core.id]: 0 };
    this.processNewTransactions();
  }

  rebuildFromCore() {
    this.version++;

    this.resetInternalState();
    this.processNewTransactions();
  }

  get headerMeta(): Meta {
    return this.core.verified.header.meta as Meta;
  }

  get group(): RawGroup {
    return this.core.getGroup();
  }

  /** Not yet implemented */
  atTime(_time: number): this {
    throw new Error("Not yet implemented");
  }

  processNewTransactions() {
    if (this.ended) return;

    const newValidTransactions = this.core.getValidTransactions({
      ignorePrivateTransactions: false,
      knownTransactions: this.knownTransactions,
    });

    if (newValidTransactions.length === 0) {
      return;
    }

    for (const { txID, madeAt, changes } of newValidTransactions) {
      for (const changeUntyped of changes) {
        const change = changeUntyped as BinaryStreamItem;

        if (change.type === "chunk") {
          this.chunks.push(change.chunk.slice(binary_U_prefixLength));
        } else if (change.type === "start") {
          this.start = change;
        } else if (change.type === "end") {
          this.ended = true;
        }
      }
    }

    this.totalValidTransactions += newValidTransactions.length;
  }

  isBinaryStreamEnded() {
    return this.ended;
  }

  getBinaryStreamInfo(): BinaryStreamInfo | undefined {
    if (!this.start) return;

    const start = this.start;

    return {
      mimeType: start.mimeType,
      fileName: start.fileName,
      totalSizeBytes: start.totalSizeBytes,
    };
  }

  getBinaryChunks(
    allowUnfinished?: boolean,
  ):
    | (BinaryStreamInfo & { chunks: Uint8Array[]; finished: boolean })
    | undefined {
    if (!this.start) return;
    if (!this.ended && !allowUnfinished) return;

    const start = this.start;

    return {
      mimeType: start.mimeType,
      fileName: start.fileName,
      totalSizeBytes: start.totalSizeBytes,
      chunks: this.chunks.map(base64URLtoBytes),
      finished: this.ended,
    };
  }

  toJSON() {
    return {};
  }

  subscribe(listener: (coStream: this) => void): () => void {
    return this.core.subscribe((core) => {
      listener(core.getCurrentContent() as this);
    });
  }
}

export class RawBinaryCoStream<
    Meta extends BinaryCoStreamMeta = { type: "binary" },
  >
  extends RawBinaryCoStreamView<Meta>
  implements RawCoValue
{
  /** @internal */
  push(
    item: BinaryStreamItem,
    privacy: "private" | "trusting" = "private",
    updateView: boolean = true,
  ): void {
    this.core.makeTransaction([item], privacy);
    if (updateView) {
      this.processNewTransactions();
    }
  }

  startBinaryStream(
    settings: BinaryStreamInfo,
    privacy: "private" | "trusting" = "private",
  ): void {
    this.push(
      {
        type: "start",
        ...settings,
      } satisfies BinaryStreamStart,
      privacy,
      false,
    );
  }

  pushBinaryStreamChunk(
    chunk: Uint8Array,
    privacy: "private" | "trusting" = "private",
  ): void {
    this.push(
      {
        type: "chunk",
        chunk: `binary_U${bytesToBase64url(chunk)}`,
      } satisfies BinaryStreamChunk,
      privacy,
      false,
    );
  }

  endBinaryStream(privacy: "private" | "trusting" = "private") {
    this.push(
      {
        type: "end",
      } satisfies BinaryStreamEnd,
      privacy,
      true,
    );
  }
}
