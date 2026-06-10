import { useEffect, useMemo, useRef, useState } from "react";
import initWasm, { OpfsBTreeEntryScanner } from "jazz-wasm";
import { decodeStorageBundle, type StorageBundle } from "./storage-bundle.js";

type PreviewMode = "utf8" | "hex" | "base64";

interface RawEntry {
  key: string;
  keyBytes: Uint8Array;
  value: Uint8Array;
}

interface EntryScanState {
  status: "loading" | "ready" | "error";
  entries: RawEntry[];
  message?: string;
}

interface LoadedBundle {
  name: string;
  size: number;
  bundle: StorageBundle;
}

const utf8Decoder = new TextDecoder();
let wasmInit: Promise<void> | null = null;
const SCAN_BATCH_SIZE = 250;
const ENTRY_PAGE_SIZE = 100;

function ensureWasm(): Promise<void> {
  wasmInit ??= initWasm();
  return wasmInit;
}

export default function App() {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [loaded, setLoaded] = useState<LoadedBundle | null>(null);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [scanStates, setScanStates] = useState<Record<string, EntryScanState>>({});
  const [error, setError] = useState<string | null>(null);
  const [isOpening, setIsOpening] = useState(false);
  const [previewMode, setPreviewMode] = useState<PreviewMode>("utf8");
  const [filter, setFilter] = useState("");
  const [entryPage, setEntryPage] = useState(0);
  const [copyStatus, setCopyStatus] = useState<string | null>(null);

  const selectedFile = useMemo(() => {
    if (!loaded || !selectedPath) return null;
    return loaded.bundle.files.find((file) => file.path === selectedPath) ?? null;
  }, [loaded, selectedPath]);

  useEffect(() => {
    if (!selectedFile || scanStates[selectedFile.path]) return;

    let cancelled = false;
    setScanStates((current) => ({
      ...current,
      [selectedFile.path]: { status: "loading", entries: [] },
    }));

    void (async () => {
      try {
        await ensureWasm();
        if (cancelled) return;

        const scanner = new OpfsBTreeEntryScanner(selectedFile.bytes);
        let done = false;

        while (!done && !cancelled) {
          const batch = normalizeEntryBatch(scanner.nextBatch(SCAN_BATCH_SIZE));
          done = batch.done;

          setScanStates((current) => {
            const currentScan = current[selectedFile.path];
            if (!currentScan || currentScan.status === "error") return current;
            return {
              ...current,
              [selectedFile.path]: {
                status: done ? "ready" : "loading",
                entries: [...currentScan.entries, ...batch.entries],
              },
            };
          });

          if (!done) {
            await yieldToBrowser();
          }
        }
      } catch (cause: unknown) {
        if (cancelled) return;
        setScanStates((current) => ({
          ...current,
          [selectedFile.path]: {
            status: "error",
            entries: [],
            message: cause instanceof Error ? cause.message : String(cause),
          },
        }));
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [selectedFile]);

  useEffect(() => {
    setEntryPage(0);
  }, [filter, selectedPath]);

  const selectedScan = selectedPath ? scanStates[selectedPath] : undefined;
  const filteredEntries = useMemo(() => {
    const entries = selectedScan?.entries ?? [];
    const query = filter.trim().toLowerCase();
    if (!query) return entries;
    return entries.filter((entry) => {
      const keyText = entry.key.toLowerCase();
      const keyHex = bytesToHex(entry.keyBytes, Number.POSITIVE_INFINITY).toLowerCase();
      return keyText.includes(query) || keyHex.includes(query);
    });
  }, [filter, selectedScan]);
  const entryPageCount = Math.max(1, Math.ceil(filteredEntries.length / ENTRY_PAGE_SIZE));
  const selectedEntryPage = Math.min(entryPage, entryPageCount - 1);
  const pagedEntries = useMemo(() => {
    const start = selectedEntryPage * ENTRY_PAGE_SIZE;
    return filteredEntries.slice(start, start + ENTRY_PAGE_SIZE);
  }, [filteredEntries, selectedEntryPage]);

  useEffect(() => {
    if (entryPage !== selectedEntryPage) {
      setEntryPage(selectedEntryPage);
    }
  }, [entryPage, selectedEntryPage]);

  async function openBundle(file: File): Promise<void> {
    setIsOpening(true);
    setError(null);
    setCopyStatus(null);
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      const bundle = decodeStorageBundle(bytes);
      setLoaded({ name: file.name, size: file.size, bundle });
      setSelectedPath(bundle.files[0]?.path ?? null);
      setScanStates({});
      setFilter("");
      setEntryPage(0);
    } catch (cause) {
      setLoaded(null);
      setSelectedPath(null);
      setScanStates({});
      setEntryPage(0);
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setIsOpening(false);
    }
  }

  async function copyEntryValue(entry: RawEntry): Promise<void> {
    const text = formatValue(entry.value, previewMode, Number.POSITIVE_INFINITY);
    if (!navigator.clipboard?.writeText) {
      setCopyStatus("Clipboard access is not available in this browser context.");
      return;
    }
    await navigator.clipboard.writeText(text);
    setCopyStatus(`Copied ${entry.key || bytesToHex(entry.keyBytes, 24)}`);
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <h1>OPFS B-tree Viewer</h1>
          <p>Open a Jazz storage bundle and inspect raw opfs-btree key/value entries.</p>
        </div>
        <button type="button" className="primary-action" onClick={() => inputRef.current?.click()}>
          Open bundle
        </button>
        <input
          ref={inputRef}
          aria-label="Open storage bundle"
          className="file-input"
          type="file"
          accept=".jazz-opfs-bundle,application/vnd.jazz.opfs-btree-bundle"
          onChange={(event) => {
            const file = event.currentTarget.files?.[0];
            event.currentTarget.value = "";
            if (file) void openBundle(file);
          }}
        />
      </header>

      <section
        className={`drop-zone${isOpening ? " is-loading" : ""}`}
        onDragOver={(event) => event.preventDefault()}
        onDrop={(event) => {
          event.preventDefault();
          const file = event.dataTransfer.files[0];
          if (file) void openBundle(file);
        }}
      >
        <span>{isOpening ? "Opening bundle..." : "Drop a .jazz-opfs-bundle file here"}</span>
      </section>

      {error ? <p className="error-banner">{error}</p> : null}

      {loaded ? (
        <section className="workspace">
          <aside className="sidebar" aria-label="Bundle files">
            <div className="bundle-summary">
              <span className="eyebrow">Bundle</span>
              <strong>{loaded.name}</strong>
              <span>{formatBytes(loaded.size)}</span>
            </div>
            <pre className="metadata">{formatMetadata(loaded.bundle.metadata)}</pre>
            <div className="file-list">
              {loaded.bundle.files.map((file) => (
                <button
                  type="button"
                  key={file.path}
                  className={file.path === selectedPath ? "is-selected" : ""}
                  onClick={() => setSelectedPath(file.path)}
                >
                  <span>{file.path}</span>
                  <small>{formatBytes(file.bytes.byteLength)}</small>
                </button>
              ))}
            </div>
          </aside>

          <section className="entry-panel">
            <div className="entry-toolbar">
              <div>
                <span className="eyebrow">Raw entries</span>
                <h2>{selectedFile?.path ?? "No file selected"}</h2>
              </div>
              <div className="toolbar-controls">
                <input
                  aria-label="Filter entries"
                  type="search"
                  placeholder="Filter key or hex"
                  value={filter}
                  onChange={(event) => setFilter(event.currentTarget.value)}
                />
                <SegmentedPreviewMode value={previewMode} onChange={setPreviewMode} />
              </div>
            </div>

            {copyStatus ? <p className="status-line">{copyStatus}</p> : null}
            {renderEntries(
              selectedScan,
              filteredEntries,
              pagedEntries,
              selectedEntryPage,
              entryPageCount,
              previewMode,
              setEntryPage,
              copyEntryValue,
            )}
          </section>
        </section>
      ) : (
        <section className="empty-state">
          <h2>No bundle open</h2>
          <p>
            Paste the README snippet in the app origin console, then open the downloaded file here.
          </p>
        </section>
      )}
    </main>
  );
}

function SegmentedPreviewMode(props: {
  value: PreviewMode;
  onChange: (value: PreviewMode) => void;
}) {
  return (
    <div className="segmented" aria-label="Preview encoding">
      {(["utf8", "hex", "base64"] as const).map((mode) => (
        <button
          type="button"
          key={mode}
          className={props.value === mode ? "is-selected" : ""}
          onClick={() => props.onChange(mode)}
        >
          {mode}
        </button>
      ))}
    </div>
  );
}

function renderEntries(
  scan: EntryScanState | undefined,
  entries: RawEntry[],
  pagedEntries: RawEntry[],
  pageIndex: number,
  pageCount: number,
  previewMode: PreviewMode,
  setPageIndex: (pageIndex: number) => void,
  copyEntryValue: (entry: RawEntry) => Promise<void>,
) {
  if (!scan) {
    return <p className="loading-state">Scanning opfs-btree file...</p>;
  }

  if (scan.status === "error") {
    return <p className="error-banner">{scan.message ?? "Failed to scan file"}</p>;
  }

  if (scan.entries.length === 0) {
    if (scan.status === "loading") {
      return <p className="loading-state">Scanning opfs-btree file...</p>;
    }
    return <p className="loading-state">No raw entries found.</p>;
  }

  if (entries.length === 0) {
    return <p className="loading-state">No entries match the current filter.</p>;
  }

  return (
    <>
      <p className="status-line">
        {scan.status === "loading" ? "Scanning..." : "Scanned"} {scan.entries.length} raw entries
        {entries.length !== scan.entries.length ? `, ${entries.length} matching filter` : ""}.
      </p>
      <PaginationControls pageIndex={pageIndex} pageCount={pageCount} onChange={setPageIndex} />
      <div className="entry-table" role="table" aria-label="Raw opfs-btree entries">
        <div className="entry-row entry-heading" role="row">
          <span role="columnheader">Key</span>
          <span role="columnheader">Key bytes</span>
          <span role="columnheader">Value bytes</span>
          <span role="columnheader">Value preview</span>
          <span role="columnheader">Actions</span>
        </div>
        {pagedEntries.map((entry, index) => (
          <div
            className="entry-row"
            role="row"
            key={`${bytesToHex(entry.keyBytes, 64)}:${pageIndex * ENTRY_PAGE_SIZE + index}`}
          >
            <code role="cell" title={bytesToHex(entry.keyBytes, Number.POSITIVE_INFINITY)}>
              {entry.key || bytesToHex(entry.keyBytes, 48)}
            </code>
            <span role="cell">{formatBytes(entry.keyBytes.byteLength)}</span>
            <span role="cell">{formatBytes(entry.value.byteLength)}</span>
            <code role="cell" className="value-preview">
              {formatValue(entry.value, previewMode, 320)}
            </code>
            <span role="cell">
              <button
                type="button"
                className="text-action"
                onClick={() => void copyEntryValue(entry)}
              >
                Copy value
              </button>
            </span>
          </div>
        ))}
      </div>
      <PaginationControls pageIndex={pageIndex} pageCount={pageCount} onChange={setPageIndex} />
    </>
  );
}

function PaginationControls(props: {
  pageIndex: number;
  pageCount: number;
  onChange: (pageIndex: number) => void;
}) {
  if (props.pageCount <= 1) return null;
  return (
    <div className="pagination">
      <button
        type="button"
        className="text-action"
        disabled={props.pageIndex === 0}
        onClick={() => props.onChange(props.pageIndex - 1)}
      >
        Previous
      </button>
      <span>
        Page {props.pageIndex + 1} of {props.pageCount}
      </span>
      <button
        type="button"
        className="text-action"
        disabled={props.pageIndex + 1 >= props.pageCount}
        onClick={() => props.onChange(props.pageIndex + 1)}
      >
        Next
      </button>
    </div>
  );
}

function normalizeEntryBatch(batch: unknown): { entries: RawEntry[]; done: boolean } {
  if (!batch || typeof batch !== "object") {
    throw new Error("Scanner returned an invalid batch.");
  }

  const record = batch as Record<string, unknown>;
  if (typeof record.done !== "boolean") {
    throw new Error("Scanner returned an invalid batch status.");
  }

  return { entries: normalizeEntries(record.entries), done: record.done };
}

function normalizeEntries(entries: unknown): RawEntry[] {
  if (!Array.isArray(entries)) {
    throw new Error("Scanner returned an invalid entry list.");
  }

  return entries.map((entry, index) => {
    if (!entry || typeof entry !== "object") {
      throw new Error(`Scanner returned an invalid entry at index ${index}.`);
    }
    const record = entry as Record<string, unknown>;
    const keyBytes = normalizeUint8Array(record.keyBytes, `entry ${index} keyBytes`);
    const value = normalizeUint8Array(record.value, `entry ${index} value`);
    const key = typeof record.key === "string" ? record.key : utf8Decoder.decode(keyBytes);
    return { key, keyBytes, value };
  });
}

function normalizeUint8Array(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) return value;
  throw new Error(`Scanner returned invalid ${label}.`);
}

function formatMetadata(metadata: unknown): string {
  return JSON.stringify(metadata ?? null, null, 2);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}

function formatValue(bytes: Uint8Array, mode: PreviewMode, limit: number): string {
  if (mode === "hex") return bytesToHex(bytes, limit);
  if (mode === "base64") return bytesToBase64(bytes, limit);
  return bytesToUtf8(bytes, limit);
}

function bytesToUtf8(bytes: Uint8Array, limit: number): string {
  const sliced = bytes.slice(0, finiteLimit(limit, bytes.byteLength));
  const suffix = sliced.byteLength < bytes.byteLength ? "\n..." : "";
  return (
    utf8Decoder.decode(sliced).replace(/\p{Cc}/gu, (char) => {
      if (char === "\n" || char === "\t") return char;
      return ".";
    }) + suffix
  );
}

function bytesToHex(bytes: Uint8Array, limit: number): string {
  const clipped = bytes.slice(0, finiteLimit(limit, bytes.byteLength));
  const text = Array.from(clipped, (byte) => byte.toString(16).padStart(2, "0")).join(" ");
  return clipped.byteLength < bytes.byteLength ? `${text} ...` : text;
}

function bytesToBase64(bytes: Uint8Array, limit: number): string {
  const clipped = bytes.slice(0, finiteLimit(limit, bytes.byteLength));
  let binary = "";
  for (let index = 0; index < clipped.byteLength; index++) {
    binary += String.fromCharCode(clipped[index]!);
  }
  const text = btoa(binary);
  return clipped.byteLength < bytes.byteLength ? `${text}...` : text;
}

function finiteLimit(limit: number, fallback: number): number {
  return Number.isFinite(limit) ? limit : fallback;
}

function yieldToBrowser(): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, 0));
}
