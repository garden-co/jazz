import * as React from "react";
import { JazzProvider, useAll, useDb, useLocalFirstAuth, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema.js";
import { fileListQuery } from "./file-list-query.js";

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

function config(secret: string): DbConfig {
  return { appId, env: "dev", serverUrl, secret };
}

function FileBrowser() {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id;
  const { data: folders = [] } = useAll(app.folders);
  const [folderId, setFolderId] = React.useState<string | undefined>();
  const [isUploading, setIsUploading] = React.useState(false);

  React.useEffect(() => {
    if (!folderId && folders[0]) setFolderId(folders[0].id);
  }, [folderId, folders]);

  const selectedFolder = folderId ?? folders[0]?.id;
  const { data: files = [] } = useAll(fileListQuery(selectedFolder));

  async function createFolder() {
    if (!userId) return;
    const name = window.prompt("Folder name", "My files")?.trim();
    if (!name) return;
    const folder = db.insert(app.folders, { name, owner_id: userId });
    setFolderId(folder.value.id);
  }

  async function upload(file: File) {
    if (!userId || !selectedFolder) return;
    setIsUploading(true);
    try {
      // Do not materialize `file` as one application-owned Uint8Array.
      await db.insertStreaming(app.files, {
        folder_id: selectedFolder,
        name: file.name,
        content_type: file.type || "application/octet-stream",
        size_bytes: file.size,
        owner_id: userId,
        contents: file.stream(),
      });
    } finally {
      setIsUploading(false);
    }
  }

  return (
    <main>
      <header>
        <div>
          <p className="eyebrow">Large-value foundation</p>
          <h1>EpicDrop</h1>
          <p>Stream a browser file into Jazz and browse its metadata.</p>
        </div>
        <button onClick={() => void createFolder()} disabled={!userId}>
          New folder
        </button>
      </header>
      <section className="browser">
        <aside aria-label="Folders">
          <h2>Folders</h2>
          {folders.map((folder) => (
            <button
              className={folder.id === selectedFolder ? "selected" : ""}
              key={folder.id}
              onClick={() => setFolderId(folder.id)}
            >
              {folder.name}
            </button>
          ))}
          {folders.length === 0 && <p>Create a folder to begin.</p>}
        </aside>
        <section>
          <label className="upload">
            <span>{isUploading ? "Uploading…" : "Upload a file"}</span>
            <input
              type="file"
              disabled={!selectedFolder || isUploading}
              onChange={(event) => {
                const file = event.currentTarget.files?.[0];
                if (file) void upload(file);
                event.currentTarget.value = "";
              }}
            />
          </label>
          <ul>
            {files.map((file) => (
              <li key={file.id}>
                <strong>{file.name}</strong>
                <span>
                  {file.content_type} · {file.size_bytes.toLocaleString()} bytes
                </span>
              </li>
            ))}
          </ul>
        </section>
      </section>
    </main>
  );
}

export function App() {
  const { secret, isLoading } = useLocalFirstAuth();
  if (isLoading || !secret) return <p>Opening EpicDrop…</p>;
  return (
    <JazzProvider config={config(secret)} fallback={<p>Opening EpicDrop…</p>}>
      <FileBrowser />
    </JazzProvider>
  );
}
