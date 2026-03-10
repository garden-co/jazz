import * as React from "react";

type LocalFileRecord = {
  file: File;
  objectUrl: string;
  localId: string;
};

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

function formatDate(timestamp: number): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(timestamp);
}

async function computeSha256(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const digest = await crypto.subtle.digest("SHA-256", buffer);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function canPreviewInline(file: File): boolean {
  return (
    file.type.startsWith("image/") ||
    file.type.startsWith("video/") ||
    file.type === "application/pdf"
  );
}

function Preview({ record }: { record: LocalFileRecord }) {
  if (record.file.type.startsWith("image/")) {
    return <img className="preview-media" src={record.objectUrl} alt={record.file.name} />;
  }
  if (record.file.type.startsWith("video/")) {
    return <video className="preview-media" src={record.objectUrl} controls />;
  }
  if (record.file.type === "application/pdf") {
    return <iframe className="preview-frame" src={record.objectUrl} title={record.file.name} />;
  }
  return null;
}

export function App() {
  const [record, setRecord] = React.useState<LocalFileRecord | null>(null);
  const [selectedFile, setSelectedFile] = React.useState<File | null>(null);
  const [pending, setPending] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [isDragging, setIsDragging] = React.useState(false);
  const fileInputRef = React.useRef<HTMLInputElement | null>(null);

  React.useEffect(() => {
    return () => {
      if (record) URL.revokeObjectURL(record.objectUrl);
    };
  }, [record]);

  const loadFile = React.useCallback(async (file: File | null) => {
    if (!file) return;

    setPending(true);
    setError(null);

    try {
      const [sha256, objectUrl] = await Promise.all([
        computeSha256(file),
        Promise.resolve(URL.createObjectURL(file)),
      ]);

      setRecord((previous) => {
        if (previous) URL.revokeObjectURL(previous.objectUrl);
        return { file, objectUrl, localId: sha256 };
      });
      setSelectedFile(file);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Failed to process file.");
    } finally {
      setPending(false);
    }
  }, []);

  const clearRecord = React.useCallback(() => {
    setRecord((previous) => {
      if (previous) URL.revokeObjectURL(previous.objectUrl);
      return null;
    });
    setSelectedFile(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  }, []);

  return (
    <main className="page-shell">
      <header className="brand-header" aria-label="Jazz">
        <span className="brand-mark" aria-hidden="true">
          ♫
        </span>
        <span className="brand-name">jazz</span>
      </header>

      {!record ? (
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
              const file = event.dataTransfer.files?.[0] ?? null;
              setSelectedFile(file);
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
                setSelectedFile(event.currentTarget.files?.[0] ?? null);
              }}
            />
            <button
              className="action-button"
              type="button"
              disabled={!selectedFile || pending}
              onClick={() => {
                void loadFile(selectedFile);
              }}
            >
              {pending ? "Uploading..." : "Upload file"}
            </button>
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
              <p>CoValue ID</p>
            </div>

            <div className="details-values">
              <p>{record.file.name}</p>
              <p>{record.file.type || "unknown"}</p>
              <p>{formatBytes(record.file.size)}</p>
              <p className="hash-value">{record.localId}</p>
            </div>
          </div>

          <div className="detail-actions">
            <button className="secondary-button" type="button" onClick={clearRecord}>
              Delete file
            </button>
            <a className="secondary-button" href={record.objectUrl} download={record.file.name}>
              Download file
            </a>
          </div>

          <div className="preview-wrap">
            {canPreviewInline(record.file) ? (
              <Preview record={record} />
            ) : (
              <div className="fallback-preview">
                <p>No inline preview for this file type.</p>
                <p>Last modified: {formatDate(record.file.lastModified)}</p>
              </div>
            )}
          </div>
        </section>
      )}
    </main>
  );
}
