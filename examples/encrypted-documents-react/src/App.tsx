import { use, useEffect, useMemo, useRef, useState } from "react";
import {
  attachDevTools,
  useAll,
  useDb,
  useJazzClient,
  useSession,
  JazzProvider,
} from "jazz-tools/react";
import { BrowserAuthSecretStore, type DbConfig } from "jazz-tools";
import { app, type DocumentWithParts, type Organization } from "../schema.js";
import { createEncryptedPdfDocument, deleteDocument, documentBlob, isPdf } from "./pdf-storage.js";

const fallbackAppId = "019d57e9-1f0f-7dcf-8b83-8a6e8b100001";
const devToolsAttachedClients = new WeakSet<object>();

function readEnv(name: string): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env?.[name];
}

function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId: overrides.appId ?? readEnv("VITE_JAZZ_APP_ID") ?? fallbackAppId,
    env: "dev",
    userBranch: "main",
    serverUrl: readEnv("VITE_JAZZ_SERVER_URL"),
    secret,
    ...overrides,
  };
}

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

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes;
  let unitIndex = -1;
  do {
    value /= 1024;
    unitIndex += 1;
  } while (value >= 1024 && unitIndex < units.length - 1);
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatDate(date: Date): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function EmptyPreview() {
  return (
    <div className="empty-preview">
      <div className="pdf-glyph" aria-hidden="true">
        PDF
      </div>
      <p>Select or upload a PDF.</p>
    </div>
  );
}

function OrganizationCreateForm({ onCreate }: { onCreate: (name: string) => void }) {
  const [name, setName] = useState("Legal");

  return (
    <form
      className="org-create"
      onSubmit={(event) => {
        event.preventDefault();
        const trimmed = name.trim();
        if (trimmed) {
          onCreate(trimmed);
        }
      }}
    >
      <label htmlFor="org-name">Organization</label>
      <div className="inline-form">
        <input
          id="org-name"
          value={name}
          onChange={(event) => setName(event.currentTarget.value)}
        />
        <button type="submit">Create</button>
      </div>
    </form>
  );
}

function DocumentVault() {
  const db = useDb();
  const session = useSession();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [selectedOrgId, setSelectedOrgId] = useState<string | null>(null);
  const [selectedDocumentId, setSelectedDocumentId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);

  const organizations = useAll(app.organizations.orderBy("name", "asc")) ?? [];
  const currentOrg = organizations.find((org) => org.id === selectedOrgId) ?? organizations[0];
  const orgId = currentOrg?.id ?? "__missing_org__";

  const documents =
    useAll(
      app.documents
        .where({ orgId })
        .include({ parts: true })
        .orderBy("createdAt", "desc")
        .requireIncludes(),
    ) ?? [];

  const selectedDocument =
    documents.find((document) => document.id === selectedDocumentId) ?? documents[0] ?? null;

  const [pdfUrl, setPdfUrl] = useState<string | null>(null);

  useEffect(() => {
    if (currentOrg && currentOrg.id !== selectedOrgId) {
      setSelectedOrgId(currentOrg.id);
    }
  }, [currentOrg, selectedOrgId]);

  useEffect(() => {
    if (selectedDocument && selectedDocument.id !== selectedDocumentId) {
      setSelectedDocumentId(selectedDocument.id);
    }
  }, [selectedDocument, selectedDocumentId]);

  useEffect(() => {
    if (!selectedDocument) {
      setPdfUrl((current) => {
        if (current) URL.revokeObjectURL(current);
        return null;
      });
      return;
    }

    try {
      const blob = documentBlob(selectedDocument);
      const nextUrl = URL.createObjectURL(blob);
      setPdfUrl((current) => {
        if (current) URL.revokeObjectURL(current);
        return nextUrl;
      });
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "PDF is locked or incomplete.");
      setPdfUrl((current) => {
        if (current) URL.revokeObjectURL(current);
        return null;
      });
    }
  }, [selectedDocument]);

  const totalSize = useMemo(
    () => documents.reduce((sum, document) => sum + document.size, 0),
    [documents],
  );

  function createOrganization(name: string) {
    if (!session?.user_id) {
      setError("Missing session user.");
      return;
    }

    const { value: organization } = db.insert(app.organizations, {
      name,
      createdBy: session.user_id,
    });
    db.insert(app.members, {
      orgId: organization.id,
      userId: session.user_id,
      role: "admin",
    });
    setSelectedOrgId(organization.id);
    setError(null);
  }

  async function uploadPdf(file: File | undefined) {
    if (!file || !currentOrg) return;
    if (!session?.user_id) {
      setError("Missing session user.");
      return;
    }
    if (!isPdf(file)) {
      setError("Choose a PDF file.");
      return;
    }

    setUploading(true);
    setError(null);
    try {
      const document = await createEncryptedPdfDocument(db, currentOrg.id, file, session.user_id);
      setSelectedDocumentId(document.id);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Upload failed.");
    } finally {
      setUploading(false);
    }
  }

  function removeDocument(document: DocumentWithParts) {
    deleteDocument(db, document);
    if (document.id === selectedDocumentId) {
      setSelectedDocumentId(null);
    }
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="product-mark">
          <span className="mark-box">ED</span>
          <div>
            <h1>Encrypted Docs</h1>
            <p>Org PDF vault</p>
          </div>
        </div>

        <OrganizationCreateForm onCreate={createOrganization} />

        {organizations.length > 0 ? (
          <label className="select-field">
            Current org
            <select
              value={currentOrg?.id ?? ""}
              onChange={(event) => setSelectedOrgId(event.currentTarget.value)}
            >
              {organizations.map((org) => (
                <option key={org.id} value={org.id}>
                  {org.name}
                </option>
              ))}
            </select>
          </label>
        ) : null}

        <div className="stats-row" aria-label="Document totals">
          <span>{documents.length} docs</span>
          <span>{formatBytes(totalSize)}</span>
        </div>

        <div className="doc-list" aria-label="Documents">
          {documents.map((document) => (
            <button
              key={document.id}
              type="button"
              className={`doc-item${document.id === selectedDocument?.id ? " is-active" : ""}`}
              onClick={() => setSelectedDocumentId(document.id)}
            >
              <span className="doc-title">{document.title}</span>
              <span className="doc-meta">{formatBytes(document.size)}</span>
            </button>
          ))}
        </div>
      </aside>

      <section className="workspace">
        <div
          className="drop-strip"
          onDragOver={(event) => event.preventDefault()}
          onDrop={(event) => {
            event.preventDefault();
            void uploadPdf(event.dataTransfer.files[0]);
          }}
        >
          <input
            ref={fileInputRef}
            id="pdf-upload"
            type="file"
            accept="application/pdf,.pdf"
            onChange={(event) => void uploadPdf(event.currentTarget.files?.[0])}
          />
          <label htmlFor="pdf-upload">{uploading ? "Uploading..." : "Upload PDF"}</label>
          <span>{currentOrg ? currentOrg.name : "Create an org first"}</span>
        </div>

        <div className="preview-stage">
          {pdfUrl && selectedDocument ? (
            <iframe src={pdfUrl} title={selectedDocument.title} />
          ) : (
            <EmptyPreview />
          )}
        </div>
      </section>

      <aside className="inspector">
        <h2>Details</h2>
        {selectedDocument ? (
          <>
            <dl>
              <div>
                <dt>Title</dt>
                <dd>{selectedDocument.title}</dd>
              </div>
              <div>
                <dt>Filename</dt>
                <dd>{selectedDocument.filename}</dd>
              </div>
              <div>
                <dt>Created</dt>
                <dd>{formatDate(selectedDocument.createdAt)}</dd>
              </div>
              <div>
                <dt>Parts</dt>
                <dd>{selectedDocument.partIds.length}</dd>
              </div>
              <div>
                <dt>Document ID</dt>
                <dd className="mono">{selectedDocument.id}</dd>
              </div>
            </dl>

            <div className="action-stack">
              {pdfUrl ? (
                <a href={pdfUrl} download={selectedDocument.filename}>
                  Download
                </a>
              ) : null}
              <button type="button" onClick={() => removeDocument(selectedDocument)}>
                Delete
              </button>
            </div>
          </>
        ) : (
          <p className="empty-copy">No document selected.</p>
        )}

        {error ? <p className="error-banner">{error}</p> : null}
      </aside>
    </main>
  );
}

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

export function App({ config, fallback }: AppProps = {}) {
  const appId = config?.appId ?? readEnv("VITE_JAZZ_APP_ID") ?? fallbackAppId;
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret({ appId }));
  const resolvedConfig = defaultConfig(secret, config);

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <DevToolsRegistration />
      <DocumentVault />
    </JazzProvider>
  );
}
