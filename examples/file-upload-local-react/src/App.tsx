import { useEffect, useRef, useState } from "react";
import {
  attachDevTools,
  getActiveSyntheticAuth,
  JazzProvider,
  useAll,
  useDb,
  useJazzClient,
  useSession,
} from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app, UploadWithIncludes, type File as JazzFile } from "../schema/app.js";

const BYTEA_MAX_BYTES = 1_048_576;
const PART_SIZE_BYTES = 256 * 1024;

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = -1;
  do {
    value /= 1024;
    unitIndex += 1;
  } while (value >= 1024 && unitIndex < units.length - 1);
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function canPreviewInline(mime: string): boolean {
  return mime.startsWith("image/") || mime.startsWith("video/") || mime === "application/pdf";
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.length;
  }
  return combined;
}

function splitIntoParts(bytes: Uint8Array, partSize: number): Uint8Array[] {
  const parts: Uint8Array[] = [];
  for (let offset = 0; offset < bytes.length; offset += partSize) {
    parts.push(bytes.slice(offset, Math.min(offset + partSize, bytes.length)));
  }
  return parts;
}

function toBlobBytes(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

// TODO make row+includes a subtype of row by separating the fk field from the resolved reference
function Preview({ file, objectUrl }: { file: Omit<JazzFile, "parts">; objectUrl: string }) {
  if (file.mimeType.startsWith("image/")) {
    return <img className="preview-media" src={objectUrl} alt={file.name} />;
  }
  if (file.mimeType.startsWith("video/")) {
    return <video className="preview-media" src={objectUrl} controls />;
  }
  if (file.mimeType === "application/pdf") {
    return <iframe className="preview-frame" src={objectUrl} title={file.name} />;
  }
  return null;
}

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? readEnvAppId() ?? "14fef0b4-4f6f-41f9-8884-8e6a8e52bb49";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  return {
    appId,
    env: "dev",
    userBranch: "main",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
    ...overrides,
  };
}

const devToolsAttachedClients = new WeakSet<object>();

function DevToolsRegistration() {
  const client = useJazzClient();

  useEffect(() => {
    if (devToolsAttachedClients.has(client as object)) {
      return;
    }

    void attachDevTools(client, app.wasmSchema);
    devToolsAttachedClients.add(client as object);

    if (location.origin.includes("localhost")) {
      Object.defineProperty(window, "jazzClient", {
        value: client,
        writable: true,
      });
    }
  }, [client]);

  return null;
}

function FileUploadScreen() {
  const [error, setError] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const db = useDb();
  const session = useSession();
  const uploads =
    useAll(
      app.uploads
        .include({
          file: {
            parts: true,
          },
        })
        .orderBy("last_modified", "desc")
        .limit(1),
    ) ?? [];
  const latestUpload = uploads[0];
  const [objectUrl, setObjectUrl] = useState<string | null>(null);

  useEffect(() => {
    if (!latestUpload) {
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
        setObjectUrl(null);
      }
      return;
    }
    if (objectUrl) {
      return;
    }
    const fileRecord = latestUpload.file;
    const combined = concatChunks(fileRecord.parts.map((part) => part.data));
    const blob = new Blob([toBlobBytes(combined)], {
      type: fileRecord.mimeType || "application/octet-stream",
    });
    setObjectUrl(URL.createObjectURL(blob));
  }, [latestUpload, objectUrl]);

  async function uploadFile(file: File) {
    if (!session?.user_id) {
      setError("Missing Jazz session user. Refresh and try again.");
      return;
    }

    setError(null);

    try {
      const buffer = new Uint8Array(await file.arrayBuffer());
      const chunks = splitIntoParts(buffer, PART_SIZE_BYTES);
      const oversizePart = chunks.find((chunk) => chunk.length > BYTEA_MAX_BYTES);
      if (oversizePart) {
        setError("A generated file part exceeded the Jazz BYTEA cell limit.");
        return;
      }

      const insertedParts = chunks.map((chunk) =>
        db.insert(app.file_parts, {
          data: chunk,
        }),
      );

      const insertedFile = db.insert(app.files, {
        name: file.name,
        mimeType: file.type || "application/octet-stream",
        parts: insertedParts.map((part) => part.id),
        partSizes: chunks.map((chunk) => chunk.length),
      });

      db.insert(app.uploads, {
        size: file.size,
        last_modified: new Date(file.lastModified),
        file_id: insertedFile.id,
        owner_id: session.user_id,
      });

      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Failed to upload file.");
    }
  }

  function deleteUpload(upload: UploadWithIncludes<{ file: { parts: true } }>) {
    const fileRecord = upload.file;
    db.delete(app.uploads, upload.id);
    if (fileRecord?.parts) {
      for (const part of fileRecord.parts) {
        db.delete(app.file_parts, part.id);
      }
    }
    if (fileRecord) {
      db.delete(app.files, fileRecord.id);
    }
  }

  return (
    <main className="page-shell">
      <header className="brand-header" aria-label="Jazz">
        <span className="brand-mark" aria-hidden="true">
          ♫
        </span>
        <span className="brand-name">jazz</span>
      </header>

      {!latestUpload ? (
        <section className="uploader-layout">
          <div
            className={`drop-zone${isDragging ? " is-dragging" : ""}`}
            onDragOver={(event) => {
              event.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => {
              setIsDragging(false);
            }}
            onDrop={(event) => {
              event.preventDefault();
              setIsDragging(false);
              const file = event.dataTransfer.files[0];
              uploadFile(file);
            }}
          >
            Drag &amp; drop your file here
          </div>

          <div className="upload-controls">
            <input
              ref={fileInputRef}
              className="file-input"
              type="file"
              onChange={(event) => {
                const file = event.currentTarget.files?.[0];
                if (file) {
                  uploadFile(file);
                }
              }}
            />
          </div>

          {error ? <p className="error-banner">{error}</p> : null}
        </section>
      ) : (
        <section className="details-layout">
          <div className="details-row">
            <div className="details-labels">
              <p>File name</p>
              <p>Type</p>
              <p>Size</p>
              <p>ID</p>
            </div>

            <div className="details-values">
              <p>{latestUpload.file.name}</p>
              <p>{latestUpload.file.mimeType || "unknown"}</p>
              <p>{formatBytes(latestUpload.size)}</p>
              <p className="hash-value">{latestUpload.file.id}</p>
            </div>
          </div>

          <div className="detail-actions">
            <button
              className="secondary-button"
              type="button"
              onClick={() => deleteUpload(latestUpload)}
            >
              Delete file
            </button>
            {objectUrl ? (
              <a className="secondary-button" href={objectUrl} download={latestUpload.file.name}>
                Download file
              </a>
            ) : null}
          </div>

          <div className="preview-wrap">
            {canPreviewInline(latestUpload.file.mimeType) && objectUrl ? (
              <Preview file={latestUpload.file} objectUrl={objectUrl} />
            ) : (
              <div className="fallback-preview">
                <p>No inline preview for this file type.</p>
                <p>Stored inside the local Jazz database.</p>
              </div>
            )}
          </div>
        </section>
      )}
    </main>
  );
}

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

export function App({ config, fallback }: AppProps = {}) {
  const resolvedConfig = defaultConfig(config);

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <DevToolsRegistration />
      <FileUploadScreen />
    </JazzProvider>
  );
}
