import React, { useState, useEffect, useRef } from 'react';
import { createDatabase } from './client';

let grooveDb = null;

async function initWasm() {
  const module = await import('../pkg/groove_wasm.js');
  await module.default();
  return module;
}

function App() {
  const [users, setUsers] = useState([]);
  const [notes, setNotes] = useState([]);
  const [newUserName, setNewUserName] = useState('');
  const [newNoteTitle, setNewNoteTitle] = useState('');
  const [selectedUserId, setSelectedUserId] = useState(null);
  const [isReady, setIsReady] = useState(false);
  const subscriptionsRef = useRef([]);

  useEffect(() => {
    async function init() {
      const wasm = await initWasm();
      const wasmDb = new wasm.WasmDatabase();
      grooveDb = createDatabase(wasmDb);

      // Create schema
      grooveDb.raw.execute(`
        CREATE TABLE User (
          name STRING NOT NULL,
          email STRING NOT NULL,
          avatar STRING
        )
      `);
      grooveDb.raw.execute(`
        CREATE TABLE Folder (
          name STRING NOT NULL,
          owner REFERENCES User NOT NULL,
          parent REFERENCES Folder
        )
      `);
      grooveDb.raw.execute(`
        CREATE TABLE Note (
          title STRING NOT NULL,
          content STRING NOT NULL,
          author REFERENCES User NOT NULL,
          folder REFERENCES Folder,
          createdAt I64 NOT NULL,
          updatedAt I64 NOT NULL
        )
      `);
      grooveDb.raw.execute(`
        CREATE TABLE Tag (
          name STRING NOT NULL,
          color STRING NOT NULL
        )
      `);

      // Subscribe to all users
      const unsubUsers = grooveDb.user.subscribeAll({}, (newUsers) => {
        setUsers(newUsers);
        // Auto-select first user if none selected
        if (!selectedUserId && newUsers.length > 0) {
          setSelectedUserId(newUsers[0].id);
        }
      });

      // Subscribe to all notes
      const unsubNotes = grooveDb.note.subscribeAll({}, setNotes);

      subscriptionsRef.current = [unsubUsers, unsubNotes];
      setIsReady(true);
    }
    init();

    return () => {
      subscriptionsRef.current.forEach(unsub => unsub());
    };
  }, []);

  const handleAddUser = () => {
    if (grooveDb && newUserName.trim()) {
      grooveDb.raw.execute(
        `INSERT INTO User (name, email) VALUES ('${newUserName}', '${newUserName.toLowerCase()}@example.com')`
      );
      setNewUserName('');
    }
  };

  const handleAddNote = () => {
    if (grooveDb && newNoteTitle.trim() && selectedUserId) {
      const now = BigInt(Date.now());
      grooveDb.raw.execute(
        `INSERT INTO Note (title, content, author, createdAt, updatedAt) VALUES ('${newNoteTitle}', '', '${selectedUserId}', ${now}, ${now})`
      );
      setNewNoteTitle('');
    }
  };

  const handleUpdateNote = (noteId, content) => {
    if (grooveDb) {
      grooveDb.raw.update_row("Note", noteId, "content", content);
    }
  };

  if (!isReady) {
    return <div style={styles.loading}>Loading Groove Database...</div>;
  }

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>Groove Demo</h1>
      <p style={styles.subtitle}>Real-time reactive database with binary encoding</p>

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
              placeholder="Enter name..."
              style={styles.input}
            />
            <button onClick={handleAddUser} style={styles.button}>
              Add User
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
              <div style={styles.empty}>No users yet. Add one above!</div>
            )}
          </div>
        </div>

        {/* Notes Panel */}
        <div style={styles.panel}>
          <h2 style={styles.panelTitle}>Notes ({notes.length})</h2>
          <div style={styles.inputGroup}>
            <input
              type="text"
              value={newNoteTitle}
              onChange={(e) => setNewNoteTitle(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddNote()}
              placeholder="Note title..."
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
          {!selectedUserId && (
            <div style={styles.hint}>Select a user to add notes</div>
          )}
          <div style={styles.list}>
            {notes.map((note) => (
              <div key={note.id} style={styles.noteItem}>
                <strong>{note.title}</strong>
                <br />
                <small style={styles.small}>
                  by {users.find(u => u.id === note.author)?.name || 'Unknown'}
                </small>
                <textarea
                  value={note.content}
                  onChange={(e) => handleUpdateNote(note.id, e.target.value)}
                  placeholder="Note content..."
                  style={styles.textarea}
                />
              </div>
            ))}
            {notes.length === 0 && (
              <div style={styles.empty}>No notes yet.</div>
            )}
          </div>
        </div>
      </div>

      {/* Debug Info */}
      <details style={styles.debug}>
        <summary style={styles.debugSummary}>Debug: Raw Data</summary>
        <pre style={styles.debugPre}>
          Users: {JSON.stringify(users, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2)}
          {'\n\n'}
          Notes: {JSON.stringify(notes, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2)}
        </pre>
      </details>
    </div>
  );
}

const styles = {
  container: {
    maxWidth: '1000px',
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
    maxHeight: '400px',
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
  noteItem: {
    padding: '10px',
    borderRadius: '4px',
    marginBottom: '8px',
    backgroundColor: 'white',
    border: '1px solid #ddd',
  },
  textarea: {
    width: '100%',
    marginTop: '8px',
    padding: '8px',
    border: '1px solid #ccc',
    borderRadius: '4px',
    fontSize: '14px',
    minHeight: '60px',
    resize: 'vertical',
    boxSizing: 'border-box',
  },
  small: {
    color: '#666',
    fontSize: '12px',
  },
  hint: {
    color: '#999',
    fontSize: '12px',
    marginBottom: '10px',
    fontStyle: 'italic',
  },
  empty: {
    color: '#999',
    textAlign: 'center',
    padding: '20px',
    fontStyle: 'italic',
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
