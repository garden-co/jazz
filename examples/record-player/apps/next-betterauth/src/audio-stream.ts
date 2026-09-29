import { AUDIO_WINDOW_BYTES, type JazzRecordPlayerStore } from "./record-player";

export type PlayableTrack = {
  id: string;
  title: string;
  artist: string;
  albumId: string;
  hasCover?: boolean;
  durationMs: number;
  mimeType?: string | null;
  byteLength?: number | null;
};

export type OpenedAudio = {
  /** An object URL for an `<audio>` element. */
  url: string;
  /** Resolves when every byte has been read from Jazz. */
  loaded: Promise<void>;
  /** "stream" plays while windows arrive; "buffer" plays once all windows are read. */
  mode: "stream" | "buffer";
  dispose(): void;
};

type Options = {
  onProgress?: (loadedBytes: number, totalBytes: number) => void;
  signal?: AbortSignal;
};

/**
 * Opens a track's audio from its Jazz large value.
 *
 * Audio is read in fixed-size byte windows with typed range selections
 * (`select({ audio_bytes: { from, to } })`). Each window currently costs a
 * whole-value read inside Jazz (#2090); the app-side shape is already the one
 * exact chunk demand will make cheap. Formats the browser can append
 * to a MediaSource (MP3, WebM/Opus) start playing after the first window; other
 * formats (WAV, AAC in MP4, FLAC) are assembled into a Blob first.
 */
export async function openTrackAudio(
  store: JazzRecordPlayerStore,
  track: PlayableTrack,
  { onProgress, signal }: Options = {},
): Promise<OpenedAudio> {
  const total = track.byteLength ?? undefined;
  const mime = track.mimeType ?? "audio/wav";

  if (total === undefined) {
    // Tracks written without a recorded length fall back to one whole-value read.
    const bytes = await store.readAudio(track.id);
    if (!bytes) throw new Error("This track's audio is not available yet.");
    onProgress?.(bytes.byteLength, bytes.byteLength);
    return bufferedAudio([bytes], mime);
  }

  if (canStream(mime)) return streamedAudio(store, track.id, total, mime, onProgress, signal);

  const chunks: Uint8Array[] = [];
  for await (const chunk of readWindows(store, track.id, total, signal)) {
    chunks.push(chunk.bytes);
    onProgress?.(chunk.to, total);
  }
  return bufferedAudio(chunks, mime);
}

function canStream(mime: string): boolean {
  return typeof MediaSource !== "undefined" && MediaSource.isTypeSupported(mime);
}

async function* readWindows(
  store: JazzRecordPlayerStore,
  trackId: string,
  total: number,
  signal?: AbortSignal,
) {
  for (let from = 0; from < total; from += AUDIO_WINDOW_BYTES) {
    signal?.throwIfAborted();
    const to = Math.min(total, from + AUDIO_WINDOW_BYTES);
    const bytes = await store.readAudioRange(trackId, from, to);
    if (!bytes) throw new Error("This track's audio is not available yet.");
    yield { bytes, to };
  }
}

function bufferedAudio(chunks: Uint8Array[], mime: string): OpenedAudio {
  const url = URL.createObjectURL(new Blob(chunks as BlobPart[], { type: mime }));
  return {
    url,
    loaded: Promise.resolve(),
    mode: "buffer",
    dispose: () => URL.revokeObjectURL(url),
  };
}

function streamedAudio(
  store: JazzRecordPlayerStore,
  trackId: string,
  total: number,
  mime: string,
  onProgress: Options["onProgress"],
  signal: AbortSignal | undefined,
): OpenedAudio {
  const source = new MediaSource();
  const url = URL.createObjectURL(source);
  const loaded = new Promise<void>((resolve, reject) => {
    source.addEventListener(
      "sourceopen",
      () => {
        const buffer = source.addSourceBuffer(mime);
        void (async () => {
          for await (const chunk of readWindows(store, trackId, total, signal)) {
            await append(buffer, chunk.bytes);
            onProgress?.(chunk.to, total);
          }
          if (source.readyState === "open") source.endOfStream();
        })().then(resolve, reject);
      },
      { once: true },
    );
  });
  // The caller may dispose before loading finishes; don't surface that as unhandled.
  loaded.catch(() => {});
  return { url, loaded, mode: "stream", dispose: () => URL.revokeObjectURL(url) };
}

function append(buffer: SourceBuffer, bytes: Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => {
    buffer.addEventListener("updateend", () => resolve(), { once: true });
    buffer.addEventListener("error", () => reject(new Error("Could not decode audio.")), {
      once: true,
    });
    buffer.appendBuffer(bytes as BufferSource);
  });
}
