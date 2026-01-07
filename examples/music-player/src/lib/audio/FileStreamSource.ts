import { Source, type MaybePromise } from "mediabunny";
import { MusicTrack } from "@/1_schema";

type ReadResult = {
  bytes: Uint8Array;
  view: DataView;
  offset: number;
};

type Waiter = {
  start: number;
  end: number;
  resolve: (data: ReadResult) => void;
};

export type StreamingState = {
  progress: number;
  readyToPlay: boolean;
  isComplete: boolean;
};

/**
 * A custom MediaBunny Source that reads from Jazz FileStream chunks.
 * Enables streaming playback - audio can start playing as chunks arrive,
 * rather than waiting for the entire file to download.
 */
export class FileStreamSource extends Source {
  private chunks: Uint8Array[] = [];
  private totalSize = 0;
  private currentSize = 0;
  private waiters: Waiter[] = [];
  private streamEnded = false;
  private _disposed = false;

  // Jazz subscription
  private unsubscribeFromFile: (() => void) | null = null;
  private lastChunkCount = 0;

  // Streaming state for useSyncExternalStore
  private streamingStateListeners: Set<() => void> = new Set();
  private streamingState: StreamingState = {
    progress: 0,
    readyToPlay: false,
    isComplete: false,
  };

  constructor(fileId: string) {
    super();
    this.subscribeToFile(fileId);
  }

  /**
   * Subscribe to streaming state changes (for useSyncExternalStore)
   */
  subscribeToStreamingState = (callback: () => void): (() => void) => {
    this.streamingStateListeners.add(callback);
    return () => this.streamingStateListeners.delete(callback);
  };

  /**
   * Get current streaming state snapshot (for useSyncExternalStore)
   */
  getStreamingState = (): StreamingState => {
    return this.streamingState;
  };

  private emitStreamingState(state: StreamingState) {
    this.streamingState = state;
    for (const listener of this.streamingStateListeners) {
      listener();
    }
  }

  waitForReady() {
    if (this.streamingState.readyToPlay) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.subscribeToStreamingState(() => {
        if (this.streamingState.readyToPlay) {
          resolve();
        }

        if (this._disposed) {
          resolve();
        }
      });
    });
  }

  private subscribeToFile(fileId: string) {
    this.unsubscribeFromFile = MusicTrack.shape.file.subscribe(
      fileId,
      (file) => {
        if (this._disposed) {
          this.unsubscribeFromFile?.();
          return;
        }

        const chunks = file.getChunks({ allowUnfinished: true });
        if (!chunks || !chunks.totalSizeBytes) return;

        // Set total size on first update
        if (this.totalSize === 0) {
          this.totalSize = chunks.totalSizeBytes;
        }

        // Add new chunks
        for (let i = this.lastChunkCount; i < chunks.chunks.length; i++) {
          this.addChunk(chunks.chunks[i]);
        }
        this.lastChunkCount = chunks.chunks.length;

        // Emit streaming state
        const isComplete = file.isBinaryStreamEnded();
        this.emitStreamingState({
          progress: this.currentSize / this.totalSize,
          readyToPlay: this.streamingState.progress > 0.05,
          isComplete,
        });

        if (isComplete) {
          this.markStreamEnded();
          this.unsubscribeFromFile?.();
          this.unsubscribeFromFile = null;
        }
      },
    );
  }

  /**
   * Called when new chunks arrive from Jazz.
   * Adds the chunk to the buffer and resolves any pending reads.
   */
  private addChunk(chunk: Uint8Array) {
    this.chunks.push(chunk);
    this.currentSize += chunk.length;
    this.onread?.(this.currentSize - chunk.length, this.currentSize);
    this.resolveWaiters();
  }

  /**
   * Called when the Jazz FileStream has finished streaming all chunks.
   */
  private markStreamEnded() {
    this.streamEnded = true;
    this.resolveWaiters();
  }

  /**
   * Check if we have data available in the requested range.
   */
  private hasDataInRange(_start: number, end: number): boolean {
    return end <= this.currentSize;
  }

  /**
   * Extract data from the buffered chunks for a given range.
   */
  private extractData(start: number, end: number): ReadResult {
    const length = end - start;
    const result = new Uint8Array(length);
    let resultOffset = 0;
    let chunkOffset = 0;

    for (const chunk of this.chunks) {
      const chunkEnd = chunkOffset + chunk.length;

      // Check if this chunk overlaps with our requested range
      if (chunkEnd > start && chunkOffset < end) {
        const copyStart = Math.max(0, start - chunkOffset);
        const copyEnd = Math.min(chunk.length, end - chunkOffset);
        const copyLength = copyEnd - copyStart;

        result.set(chunk.subarray(copyStart, copyEnd), resultOffset);
        resultOffset += copyLength;
      }

      chunkOffset = chunkEnd;

      // Stop if we've copied all the data we need
      if (chunkOffset >= end) {
        break;
      }
    }

    return {
      bytes: result,
      view: new DataView(result.buffer, result.byteOffset, result.byteLength),
      offset: start,
    };
  }

  /**
   * Try to resolve any pending waiters that now have enough data.
   */
  private resolveWaiters() {
    const stillWaiting: Waiter[] = [];

    for (const waiter of this.waiters) {
      if (this.hasDataInRange(waiter.start, waiter.end)) {
        waiter.resolve(this.extractData(waiter.start, waiter.end));
      } else if (this.streamEnded) {
        // Stream ended but we don't have the data - resolve with what we have
        const availableEnd = Math.min(waiter.end, this.currentSize);
        if (availableEnd > waiter.start) {
          waiter.resolve(this.extractData(waiter.start, availableEnd));
        }
        // If no data available at all, the waiter will remain pending
        // This shouldn't happen in normal operation
      } else {
        stillWaiting.push(waiter);
      }
    }

    this.waiters = stillWaiting;
  }

  /** @internal */
  _retrieveSize(): number {
    return this.totalSize;
  }

  /** @internal */
  _read(start: number, end: number): MaybePromise<ReadResult | null> {
    if (this._disposed) {
      return null;
    }

    if (this.hasDataInRange(start, end)) {
      return this.extractData(start, end);
    }

    // If stream ended but we don't have data, return what we have
    if (this.streamEnded) {
      const availableEnd = Math.min(end, this.currentSize);
      if (availableEnd > start) {
        return this.extractData(start, availableEnd);
      }
      return null;
    }

    // Wait for more chunks to arrive
    return new Promise<ReadResult>((resolve) => {
      this.waiters.push({ start, end, resolve });
    });
  }

  /** @internal */
  _dispose() {
    this.unsubscribeFromFile?.();
    this.unsubscribeFromFile = null;
    this.streamingStateListeners.clear();
    this.chunks = [];
    this.waiters = [];
    this._disposed = true;
  }
}
