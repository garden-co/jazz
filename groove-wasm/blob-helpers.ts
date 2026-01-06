/**
 * Blob helper functions for working with ReadableStream/WritableStream in JavaScript.
 *
 * These helpers wrap the WASM blob APIs to provide a more ergonomic JS interface.
 */

import type { WasmDatabase, WasmBlobWriter } from "./pkg/groove_wasm.js";

/** Blob information returned from get_blob_info */
export interface BlobInfo {
  isInline: boolean;
  chunkCount: number;
  size?: number;
}

/** Stream configuration returned from create_blob_readable_stream */
interface StreamConfig {
  handleId: bigint;
  isInline: boolean;
  chunkCount: number;
}

/**
 * Create a ReadableStream from a blob handle.
 *
 * For small inline blobs, reads all data in one chunk.
 * For large chunked blobs, streams chunks one at a time.
 *
 * @example
 * ```typescript
 * const handleId = db.create_blob(myData);
 * const stream = blobToReadableStream(db, handleId);
 *
 * // Use with fetch Response
 * const response = new Response(stream);
 * const blob = await response.blob();
 *
 * // Or read manually
 * const reader = stream.getReader();
 * while (true) {
 *   const { done, value } = await reader.read();
 *   if (done) break;
 *   processChunk(value);
 * }
 * ```
 */
export function blobToReadableStream(db: WasmDatabase, handleId: bigint): ReadableStream<Uint8Array> {
  // Get blob configuration
  const config = db.get_blob_info(handleId) as unknown as BlobInfo;
  let chunkIndex = 0;

  return new ReadableStream({
    pull(controller) {
      if (chunkIndex >= config.chunkCount) {
        controller.close();
        return;
      }
      try {
        const chunk = db.read_blob_chunk(handleId, chunkIndex);
        controller.enqueue(chunk);
        chunkIndex++;
      } catch (e) {
        controller.error(e);
      }
    },
  });
}

/**
 * Create a blob from a ReadableStream.
 *
 * Reads all chunks from the stream and creates a blob handle.
 * The blob can then be used in insert_with_blobs or update_row_blob.
 *
 * @example
 * ```typescript
 * // From fetch response
 * const response = await fetch('/large-file.bin');
 * const handleId = await readableStreamToBlob(db, response.body!);
 *
 * // Use in insert
 * db.insert_with_blobs('files', [['name', 'large-file.bin']], [['content', handleId]]);
 * ```
 */
export async function readableStreamToBlob(
  db: WasmDatabase,
  stream: ReadableStream<Uint8Array>
): Promise<bigint> {
  const writer = db.create_blob_writer();
  const reader = stream.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      writer.write(value);
    }
    return writer.finish();
  } catch (e) {
    writer.abort();
    throw e;
  }
}

/**
 * Wrapper class for convenient blob access.
 *
 * @example
 * ```typescript
 * // Create from data
 * const blob = GrooveBlob.fromData(db, myData);
 *
 * // Get info
 * console.log(blob.info); // { isInline: true, chunkCount: 1, size: 1024 }
 *
 * // Read entire blob
 * const data = await blob.arrayBuffer();
 *
 * // Stream large blob
 * const stream = blob.stream();
 *
 * // Use in database operations
 * db.insert_with_blobs('files', [['name', 'test.txt']], [['content', blob.handleId]]);
 *
 * // Release when done
 * blob.release();
 * ```
 */
export class GrooveBlob {
  constructor(
    private db: WasmDatabase,
    public readonly handleId: bigint
  ) {}

  /**
   * Create a blob from raw byte data.
   */
  static fromData(db: WasmDatabase, data: Uint8Array): GrooveBlob {
    const handleId = db.create_blob(data);
    return new GrooveBlob(db, handleId);
  }

  /**
   * Create a blob from a ReadableStream.
   */
  static async fromStream(db: WasmDatabase, stream: ReadableStream<Uint8Array>): Promise<GrooveBlob> {
    const handleId = await readableStreamToBlob(db, stream);
    return new GrooveBlob(db, handleId);
  }

  /**
   * Get blob metadata.
   */
  get info(): BlobInfo {
    return this.db.get_blob_info(this.handleId) as unknown as BlobInfo;
  }

  /**
   * Read entire blob as Uint8Array.
   *
   * For large chunked blobs, this concatenates all chunks.
   * Consider using stream() for better memory efficiency with large blobs.
   */
  async arrayBuffer(): Promise<Uint8Array> {
    // read_blob handles both inline and chunked blobs
    return this.db.read_blob(this.handleId);
  }

  /**
   * Get a ReadableStream for streaming reads.
   *
   * Preferred for large blobs to avoid loading everything into memory.
   */
  stream(): ReadableStream<Uint8Array> {
    return blobToReadableStream(this.db, this.handleId);
  }

  /**
   * Release the blob handle.
   *
   * Call this when you're done with the blob to free memory.
   * The blob cannot be used after release.
   */
  release(): void {
    this.db.release_blob(this.handleId);
  }
}

/**
 * Helper class for streaming blob creation with WritableStream.
 *
 * @example
 * ```typescript
 * const writer = new GrooveBlobWriter(db);
 * const writableStream = writer.getWritableStream();
 *
 * // Pipe data to it
 * await someReadableStream.pipeTo(writableStream);
 *
 * // Get the blob handle
 * const blob = writer.getBlob();
 * db.insert_with_blobs('files', [['name', 'streamed.bin']], [['content', blob.handleId]]);
 * ```
 */
export class GrooveBlobWriter {
  private wasmWriter: WasmBlobWriter;
  private finished = false;
  private blob: GrooveBlob | null = null;

  constructor(private db: WasmDatabase) {
    this.wasmWriter = db.create_blob_writer();
  }

  /**
   * Write a chunk of data.
   */
  write(chunk: Uint8Array): void {
    if (this.finished) {
      throw new Error("Blob writer already finished");
    }
    this.wasmWriter.write(chunk);
  }

  /**
   * Get the current size of written data.
   */
  get size(): number {
    return this.wasmWriter.size();
  }

  /**
   * Finish writing and get the blob.
   */
  finish(): GrooveBlob {
    if (this.finished) {
      if (this.blob) return this.blob;
      throw new Error("Blob writer was aborted");
    }
    this.finished = true;
    const handleId = this.wasmWriter.finish();
    this.blob = new GrooveBlob(this.db, handleId);
    return this.blob;
  }

  /**
   * Abort writing and discard all data.
   */
  abort(): void {
    if (!this.finished) {
      this.finished = true;
      this.wasmWriter.abort();
    }
  }

  /**
   * Get a WritableStream for this blob writer.
   *
   * Data written to the stream will be accumulated into the blob.
   * After the stream closes, call getBlob() to get the finished blob.
   */
  getWritableStream(): WritableStream<Uint8Array> {
    return new WritableStream({
      write: (chunk) => {
        this.write(chunk);
      },
      close: () => {
        this.finish();
      },
      abort: () => {
        this.abort();
      },
    });
  }

  /**
   * Get the blob after the writable stream has closed.
   */
  getBlob(): GrooveBlob {
    if (!this.finished || !this.blob) {
      throw new Error("Blob writer not finished yet");
    }
    return this.blob;
  }
}
