import { DEMO_LIBRARY, synthesizeWav } from "./demo-audio";
import type { JazzRecordPlayerStore } from "./record-player";

/**
 * Wraps a file's byte stream so upload progress can be reported while
 * `insertStreaming` consumes it. Nothing is buffered here: each chunk is
 * forwarded as soon as Jazz asks for it.
 */
export function countingStream(
  file: Blob,
  onBytes: (sentBytes: number) => void,
): ReadableStream<Uint8Array> {
  let sent = 0;
  return file.stream().pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        sent += chunk.byteLength;
        onBytes(sent);
        controller.enqueue(chunk);
      },
    }),
  );
}

/** Reads a local file's duration with the browser's own decoder. */
export function probeDurationMs(file: Blob): Promise<number> {
  return new Promise((resolve) => {
    const url = URL.createObjectURL(file);
    const audio = new Audio();
    const done = (ms: number) => {
      URL.revokeObjectURL(url);
      resolve(Number.isFinite(ms) ? Math.round(ms) : 0);
    };
    audio.preload = "metadata";
    audio.addEventListener("loadedmetadata", () => done(audio.duration * 1000), { once: true });
    audio.addEventListener("error", () => done(0), { once: true });
    audio.src = url;
  });
}

export function titleFromFileName(name: string): string {
  const base = name
    .replace(/\.[^.]+$/, "")
    .replace(/[_-]+/g, " ")
    .trim();
  return base.length > 0 ? base[0]!.toUpperCase() + base.slice(1) : "Untitled";
}

export type UploadProgress = { label: string; sentBytes: number; totalBytes: number };

/** Streams each file into a new track, in order, after `firstOrdinal`. */
export async function uploadTracks(
  store: JazzRecordPlayerStore,
  albumId: string,
  files: File[],
  firstOrdinal: number,
  onProgress: (progress: UploadProgress) => void,
  onTrackWritten: (index: number) => void = () => {},
): Promise<void> {
  const totalBytes = files.reduce((sum, file) => sum + file.size, 0);
  let done = 0;
  for (const [index, file] of files.entries()) {
    const title = titleFromFileName(file.name);
    const durationMs = await probeDurationMs(file);
    const before = done;
    await store.createTrackWithAudio(
      { albumId, title, ordinal: firstOrdinal + index, durationMs },
      countingStream(file, (sent) =>
        onProgress({ label: title, sentBytes: before + sent, totalBytes }),
      ),
      { mimeType: file.type || "application/octet-stream", byteLength: file.size },
    );
    done += file.size;
    onTrackWritten(index);
  }
}

/** Writes the deterministic demo library: synthesised WAV tones, streamed in 16 KiB chunks. */
export async function seedDemoLibrary(
  store: JazzRecordPlayerStore,
  onProgress: (progress: UploadProgress) => void,
): Promise<void> {
  const tracks = DEMO_LIBRARY.flatMap((album) => album.tracks);
  const totalBytes = tracks.reduce(
    (sum, track) => sum + 44 + (track.durationMs / 1000) * 16_000,
    0,
  );
  let done = 0;
  for (const album of DEMO_LIBRARY) {
    const albumId = store.createAlbum({ title: album.title, artist: album.artist });
    for (const [ordinal, track] of album.tracks.entries()) {
      const wav = synthesizeWav(track);
      const before = done;
      await store.createTrackWithAudio(
        { albumId, title: track.title, ordinal: ordinal + 1, durationMs: track.durationMs },
        countingStream(new Blob([wav as BlobPart]), (sent) =>
          onProgress({ label: track.title, sentBytes: before + sent, totalBytes }),
        ),
        { mimeType: "audio/wav", byteLength: wav.byteLength },
      );
      done += wav.byteLength;
    }
  }
}
