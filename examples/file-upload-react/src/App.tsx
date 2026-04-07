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
import { app, type UploadWithIncludes, type File as JazzFile } from "../schema.js";
import { Logo } from "./Logo.js";

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

function Preview({ file, objectUrl }: { file: JazzFile; objectUrl: string }) {
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
  const appId = overrides.appId ?? readEnvAppId() ?? "019d4349-2473-7006-857e-dd676070304b";
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
        .orderBy("lastModified", "desc")
        .limit(1)
        .requireIncludes(),
    ) ?? [];
  const latestUpload = uploads[0];
  const [objectUrl, setObjectUrl] = useState<string | null>(null);

  useEffect(() => {
    if (!latestUpload) {
      setObjectUrl((current) => {
        if (current) {
          URL.revokeObjectURL(current);
        }
        return null;
      });
      return;
    }

    let isActive = true;

    void (async () => {
      try {
        const blob = await db.loadFileAsBlob(app, latestUpload.file);
        if (!isActive) {
          return;
        }

        const nextObjectUrl = URL.createObjectURL(blob);
        setObjectUrl((current) => {
          if (current) {
            URL.revokeObjectURL(current);
          }
          return nextObjectUrl;
        });
      } catch (reason) {
        if (!isActive) {
          return;
        }

        setError(reason instanceof Error ? reason.message : "Failed to load file preview.");
      }
    })();

    return () => {
      isActive = false;
    };
  }, [db, latestUpload]);

  async function uploadFile(file: File) {
    if (!session?.user_id) {
      setError("Missing Jazz session user. Refresh and try again.");
      return;
    }

    setError(null);

    try {
      const insertedFile = await db.createFileFromBlob(app, file);

      db.insert(app.uploads, {
        size: file.size,
        lastModified: new Date(file.lastModified),
        fileId: insertedFile.id,
        owner_id: session.user_id,
      });

      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Failed to upload file.");
    }
  }

  function deleteUpload(upload: UploadWithIncludes) {
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
        <Logo />
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
