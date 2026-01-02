import React, { useState, useEffect, useRef } from 'react';

let db = null;

async function initWasm() {
  const module = await import('../pkg/groove_wasm.js');
  await module.default();
  return module;
}

function App() {
  const [rows, setRows] = useState([]);
  const [rowId, setRowId] = useState(null);
  const subscriptionRef = useRef(null);

  useEffect(() => {
    async function init() {
      const wasm = await initWasm();
      db = new wasm.WasmDatabase();

      db.execute("CREATE TABLE notes (content STRING NOT NULL)");
      const result = db.execute("INSERT INTO notes (content) VALUES ('')");
      const id = BigInt(result.split(':')[1]);
      setRowId(id);

      subscriptionRef.current = db.subscribe("SELECT * FROM notes", setRows);
    }
    init();
  }, []);

  const handleChange = (e) => {
    if (db && rowId !== null) {
      db.update_row("notes", rowId, "content", e.target.value);
    }
  };

  return (
    <div>
      <input type="text" onChange={handleChange} placeholder="Type here..." />
      <pre>{JSON.stringify(rows, null, 2)}</pre>
    </div>
  );
}

export default App;
