import React, { useState, useEffect, useRef } from 'react';
import { createDatabase } from './client';

let grooveDb = null;

async function initWasm() {
  const module = await import('../pkg/groove_wasm.js');
  await module.default();
  return module;
}

// Schema must match generated types/meta exactly
const SCHEMA = `
CREATE TABLE User (
    name STRING NOT NULL,
    email STRING NOT NULL,
    avatar STRING
);

CREATE TABLE Folder (
    name STRING NOT NULL,
    owner REFERENCES User NOT NULL,
    parent REFERENCES Folder
);

CREATE TABLE Note (
    title STRING NOT NULL,
    content STRING NOT NULL,
    author REFERENCES User NOT NULL,
    folder REFERENCES Folder,
    createdAt I64 NOT NULL,
    updatedAt I64 NOT NULL
);

CREATE TABLE Tag (
    name STRING NOT NULL,
    color STRING NOT NULL
);
`;

function App() {
  const [users, setUsers] = useState([]);
  const [folders, setFolders] = useState([]);
  const [notes, setNotes] = useState([]);
  const [selectedUserId, setSelectedUserId] = useState(null);
  const [selectedFolderId, setSelectedFolderId] = useState(null);
  const [isReady, setIsReady] = useState(false);
  const [activeTab, setActiveTab] = useState('all');
  const subscriptionsRef = useRef([]);

  // Form state
  const [newUserName, setNewUserName] = useState('');
  const [newFolderName, setNewFolderName] = useState('');
  const [newNoteTitle, setNewNoteTitle] = useState('');

  useEffect(() => {
    async function init() {
      const wasm = await initWasm();
      const wasmDb = new wasm.WasmDatabase();
      grooveDb = createDatabase(wasmDb);

      // Create schema - each statement separately
      for (const stmt of SCHEMA.split(';').map(s => s.trim()).filter(Boolean)) {
        grooveDb.raw.execute(stmt);
      }

      // Subscribe to all users
      const unsubUsers = grooveDb.user.subscribeAll({}, (newUsers) => {
        setUsers(newUsers);
        if (!selectedUserId && newUsers.length > 0) {
          setSelectedUserId(newUsers[0].id);
        }
      });

      // Subscribe to all folders
      const unsubFolders = grooveDb.folder.subscribeAll({}, setFolders);

      // Subscribe to all notes
      const unsubNotes = grooveDb.note.subscribeAll({}, setNotes);

      subscriptionsRef.current = [unsubUsers, unsubFolders, unsubNotes];
      setIsReady(true);
    }
    init();

    return () => {
      subscriptionsRef.current.forEach(unsub => unsub());
    };
  }, []);

  const handleAddUser = () => {
    if (grooveDb && newUserName.trim()) {
      const email = newUserName.toLowerCase().replace(/\s+/g, '.') + '@example.com';
      grooveDb.raw.execute(
        `INSERT INTO User (name, email) VALUES ('${newUserName}', '${email}')`
      );
      setNewUserName('');
    }
  };

  const handleAddFolder = () => {
    if (grooveDb && newFolderName.trim() && selectedUserId) {
      const parentClause = selectedFolderId ? `'${selectedFolderId}'` : 'NULL';
      grooveDb.raw.execute(
        `INSERT INTO Folder (name, owner, parent) VALUES ('${newFolderName}', '${selectedUserId}', ${parentClause})`
      );
      setNewFolderName('');
    }
  };

  const handleAddNote = () => {
    if (grooveDb && newNoteTitle.trim() && selectedUserId) {
      const now = BigInt(Date.now());
      const folderClause = selectedFolderId ? `'${selectedFolderId}'` : 'NULL';
      grooveDb.raw.execute(
        `INSERT INTO Note (title, content, author, folder, createdAt, updatedAt) VALUES ('${newNoteTitle}', '', '${selectedUserId}', ${folderClause}, ${now}, ${now})`
      );
      setNewNoteTitle('');
    }
  };

  const handleUpdateNote = (noteId, content) => {
    if (grooveDb) {
      const now = BigInt(Date.now());
      grooveDb.raw.update_row("Note", noteId, "content", content);
      grooveDb.raw.update_row("Note", noteId, "updatedAt", now);
    }
  };

  const handleDeleteNote = (noteId) => {
    if (grooveDb) {
      grooveDb.raw.execute(`DELETE FROM Note WHERE id = '${noteId}'`);
    }
  };

  // Helper to find user by ID
  const getUserName = (userId) => {
    const user = users.find(u => u.id === userId);
    return user?.name || 'Unknown';
  };

  // Helper to find folder by ID
  const getFolderName = (folderId) => {
    const folder = folders.find(f => f.id === folderId);
    return folder?.name || null;
  };

  // Filter notes based on active tab
  const filteredNotes = notes.filter(note => {
    if (activeTab === 'all') return true;
    if (activeTab === 'my' && selectedUserId) return note.author === selectedUserId;
    if (activeTab === 'folder' && selectedFolderId) return note.folder === selectedFolderId;
    return true;
  });

  if (!isReady) {
    return <div style={styles.loading}>Loading Groove Database...</div>;
  }

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>Groove Demo</h1>
      <p style={styles.subtitle}>
        Real-time reactive database with relations and binary encoding
      </p>

      <div style={styles.columns}>
        {/* Users Panel */}
        <div style={styles.panel}>
          <h2 style={styles.panelTitle}>Users ({users.length})</h2>
          <div style={styles.inputGroup}>
            <input
              type="text"
              value={newUserName}
              onChange={(e) => setNewUserName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddUser()}
              placeholder="New user name..."
              style={styles.input}
            />
            <button onClick={handleAddUser} style={styles.button}>
              Add
            </button>
          </div>
          <div style={styles.list}>
            {users.map((user) => (
              <div
                key={user.id}
                onClick={() => setSelectedUserId(user.id)}
                style={{
                  ...styles.listItem,
                  ...(selectedUserId === user.id ? styles.selected : {}),
                }}
              >
                <strong>{user.name}</strong>
                <br />
                <small style={styles.small}>{user.email}</small>
              </div>
            ))}
            {users.length === 0 && (
              <div style={styles.empty}>No users yet</div>
            )}
          </div>
        </div>

        {/* Folders Panel */}
        <div style={styles.panel}>
          <h2 style={styles.panelTitle}>Folders ({folders.length})</h2>
          <div style={styles.inputGroup}>
            <input
              type="text"
              value={newFolderName}
              onChange={(e) => setNewFolderName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddFolder()}
              placeholder="New folder..."
              disabled={!selectedUserId}
              style={styles.input}
            />
            <button
              onClick={handleAddFolder}
              disabled={!selectedUserId}
              style={styles.button}
            >
              Add
            </button>
          </div>
          <div style={styles.list}>
            <div
              onClick={() => setSelectedFolderId(null)}
              style={{
                ...styles.listItem,
                ...(selectedFolderId === null ? styles.selected : {}),
              }}
            >
              <em>No folder (root)</em>
            </div>
            {folders.map((folder) => (
              <div
                key={folder.id}
                onClick={() => setSelectedFolderId(folder.id)}
                style={{
                  ...styles.listItem,
                  ...(selectedFolderId === folder.id ? styles.selected : {}),
                }}
              >
                <strong>{folder.name}</strong>
                <br />
                <small style={styles.small}>
                  Owner: {getUserName(folder.owner)}
                </small>
              </div>
            ))}
          </div>
        </div>

        {/* Notes Panel */}
        <div style={{ ...styles.panel, flex: 2 }}>
          <h2 style={styles.panelTitle}>Notes ({filteredNotes.length})</h2>

          {/* Filter tabs */}
          <div style={styles.tabs}>
            <button
              onClick={() => setActiveTab('all')}
              style={{ ...styles.tab, ...(activeTab === 'all' ? styles.activeTab : {}) }}
            >
              All Notes
            </button>
            <button
              onClick={() => setActiveTab('my')}
              style={{ ...styles.tab, ...(activeTab === 'my' ? styles.activeTab : {}) }}
              disabled={!selectedUserId}
            >
              My Notes
            </button>
            <button
              onClick={() => setActiveTab('folder')}
              style={{ ...styles.tab, ...(activeTab === 'folder' ? styles.activeTab : {}) }}
              disabled={!selectedFolderId}
            >
              In Folder
            </button>
          </div>

          <div style={styles.inputGroup}>
            <input
              type="text"
              value={newNoteTitle}
              onChange={(e) => setNewNoteTitle(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddNote()}
              placeholder="New note title..."
              disabled={!selectedUserId}
              style={styles.input}
            />
            <button
              onClick={handleAddNote}
              disabled={!selectedUserId}
              style={styles.button}
            >
              Add Note
            </button>
          </div>

          <div style={styles.notesList}>
            {filteredNotes.map((note) => (
              <div key={note.id} style={styles.noteItem}>
                <div style={styles.noteHeader}>
                  <strong>{note.title}</strong>
                  <button
                    onClick={() => handleDeleteNote(note.id)}
                    style={styles.deleteButton}
                  >
                    Delete
                  </button>
                </div>
                <div style={styles.noteMeta}>
                  <span>By: {getUserName(note.author)}</span>
                  {note.folder && (
                    <span style={{ marginLeft: 10 }}>
                      In: {getFolderName(note.folder)}
                    </span>
                  )}
                  <span style={{ marginLeft: 10, color: '#999' }}>
                    {new Date(Number(note.updatedAt)).toLocaleTimeString()}
                  </span>
                </div>
                <textarea
                  value={note.content}
                  onChange={(e) => handleUpdateNote(note.id, e.target.value)}
                  placeholder="Write something..."
                  style={styles.textarea}
                />
              </div>
            ))}
            {filteredNotes.length === 0 && (
              <div style={styles.empty}>
                {notes.length === 0 ? 'No notes yet. Create one!' : 'No notes match this filter.'}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Features Demo */}
      <div style={styles.featuresSection}>
        <h3 style={styles.featuresTitle}>Features Demonstrated</h3>
        <ul style={styles.featuresList}>
          <li><strong>Real-time Subscriptions:</strong> All changes propagate instantly across all views</li>
          <li><strong>Binary Encoding:</strong> Efficient WASM-to-JS data transfer with delta updates</li>
          <li><strong>Relations:</strong> Notes reference authors, folders reference owners (foreign keys)</li>
          <li><strong>Client-side Filters:</strong> "My Notes" and "In Folder" tabs filter the view</li>
        </ul>
      </div>

      {/* Debug Info */}
      <details style={styles.debug}>
        <summary style={styles.debugSummary}>Debug: Raw Data</summary>
        <pre style={styles.debugPre}>
          Users: {JSON.stringify(users, replacer, 2)}
          {'\n\n'}
          Folders: {JSON.stringify(folders, replacer, 2)}
          {'\n\n'}
          Notes: {JSON.stringify(notes, replacer, 2)}
        </pre>
      </details>
    </div>
  );
}

// JSON replacer for BigInt
function replacer(key, value) {
  return typeof value === 'bigint' ? value.toString() : value;
}

const styles = {
  container: {
    maxWidth: '1200px',
    margin: '0 auto',
    padding: '20px',
    fontFamily: 'system-ui, -apple-system, sans-serif',
  },
  title: {
    margin: '0 0 5px 0',
    fontSize: '28px',
  },
  subtitle: {
    margin: '0 0 20px 0',
    color: '#666',
    fontSize: '14px',
  },
  loading: {
    padding: '50px',
    textAlign: 'center',
    color: '#666',
  },
  columns: {
    display: 'flex',
    gap: '20px',
  },
  panel: {
    flex: 1,
    border: '1px solid #ddd',
    borderRadius: '8px',
    padding: '15px',
    backgroundColor: '#fafafa',
  },
  panelTitle: {
    margin: '0 0 15px 0',
    fontSize: '18px',
    borderBottom: '1px solid #ddd',
    paddingBottom: '10px',
  },
  inputGroup: {
    display: 'flex',
    gap: '10px',
    marginBottom: '15px',
  },
  input: {
    flex: 1,
    padding: '8px 12px',
    border: '1px solid #ccc',
    borderRadius: '4px',
    fontSize: '14px',
  },
  button: {
    padding: '8px 16px',
    backgroundColor: '#0066cc',
    color: 'white',
    border: 'none',
    borderRadius: '4px',
    cursor: 'pointer',
    fontSize: '14px',
  },
  list: {
    maxHeight: '300px',
    overflowY: 'auto',
  },
  listItem: {
    padding: '10px',
    borderRadius: '4px',
    marginBottom: '8px',
    backgroundColor: 'white',
    border: '1px solid #ddd',
    cursor: 'pointer',
  },
  selected: {
    backgroundColor: '#e6f0ff',
    borderColor: '#0066cc',
  },
  notesList: {
    maxHeight: '400px',
    overflowY: 'auto',
  },
  noteItem: {
    padding: '12px',
    borderRadius: '4px',
    marginBottom: '10px',
    backgroundColor: 'white',
    border: '1px solid #ddd',
  },
  noteHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '6px',
  },
  noteMeta: {
    fontSize: '12px',
    color: '#666',
    marginBottom: '8px',
  },
  textarea: {
    width: '100%',
    padding: '8px',
    border: '1px solid #ccc',
    borderRadius: '4px',
    fontSize: '14px',
    minHeight: '60px',
    resize: 'vertical',
    boxSizing: 'border-box',
  },
  deleteButton: {
    padding: '4px 8px',
    backgroundColor: '#dc3545',
    color: 'white',
    border: 'none',
    borderRadius: '4px',
    cursor: 'pointer',
    fontSize: '12px',
  },
  tabs: {
    display: 'flex',
    gap: '8px',
    marginBottom: '15px',
  },
  tab: {
    padding: '6px 12px',
    backgroundColor: '#f0f0f0',
    border: '1px solid #ccc',
    borderRadius: '4px',
    cursor: 'pointer',
    fontSize: '13px',
  },
  activeTab: {
    backgroundColor: '#0066cc',
    color: 'white',
    borderColor: '#0066cc',
  },
  small: {
    color: '#666',
    fontSize: '12px',
  },
  empty: {
    color: '#999',
    textAlign: 'center',
    padding: '20px',
    fontStyle: 'italic',
  },
  featuresSection: {
    marginTop: '30px',
    padding: '20px',
    backgroundColor: '#f0f8ff',
    borderRadius: '8px',
    border: '1px solid #cce5ff',
  },
  featuresTitle: {
    margin: '0 0 10px 0',
    fontSize: '16px',
    color: '#004085',
  },
  featuresList: {
    margin: 0,
    paddingLeft: '20px',
    lineHeight: '1.8',
    fontSize: '14px',
  },
  debug: {
    marginTop: '30px',
    border: '1px solid #ddd',
    borderRadius: '4px',
    padding: '10px',
    backgroundColor: '#f5f5f5',
  },
  debugSummary: {
    cursor: 'pointer',
    fontWeight: 'bold',
    color: '#666',
  },
  debugPre: {
    marginTop: '10px',
    fontSize: '11px',
    overflow: 'auto',
    maxHeight: '300px',
  },
};

export default App;
